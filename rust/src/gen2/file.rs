//! Cowrie FILE container + Merkle file identity (SPEC-v1 §7).
//!
//! A **Cowrie file** packages an ordered list of **frames** (each frame is one complete, canonical
//! §2.2 COWR value's wire bytes) with a sealed footer index for O(1) random access, and a
//! **Merkle root** over the frames that is the file's content identity.
//!
//! Layout (all multi-byte ints little-endian; uvarint = §2.1):
//! ```text
//!     "CWRF" (0x43 0x57 0x52 0x46)        file magic
//!     version  u8 (0x01)
//!     reserved u8 (0x00)
//!     uvarint  frame_count
//!     repeat frame_count times:
//!         uvarint frame_len
//!         frame_bytes                      (a canonical COWR value, §2.2; uncompressed)
//!     FOOTER (starts at footer_offset):
//!         uvarint frame_count              (repeated, footer is self-contained)
//!         repeat: uvarint frame_offset, uvarint frame_len  (offset = file-start to frame_bytes)
//!         merkle_root  34 bytes            (multihash SHA-256, §3)
//!     u64 LE   footer_offset               (file-start to FOOTER; enables seek-from-end)
//!     "CWRF"                               end magic
//! ```
//!
//! Merkle (RFC 6962 domain separation, promote-odd — never duplicate; the count is bound into the
//! root so two different frame lists can never share a root, even structurally):
//! ```text
//!     leaf(f)    = SHA-256(0x00 || f)
//!     node(a, b) = SHA-256(0x01 || a || b)
//!     tree_digest = promote-odd reduction of the leaves   (empty file: SHA-256(b""))
//!     root_digest = SHA-256(0x02 || uvarint(frame_count) || tree_digest)
//!     merkle_root = 0x12 0x20 || root_digest              (multihash sha2-256)
//! ```
//!
//! File identity = merkle_root. Equal ordered frame contents ⇒ equal identity, cross-language.

use super::types::CowrieError;
use sha2::{Digest, Sha256};

const FILE_MAGIC: &[u8; 4] = b"CWRF";
const FILE_VERSION: u8 = 1;
/// §7 frame-count bound (DoS guard).
const MAX_FRAMES: u64 = 1 << 32;
/// Canonical frame alignment (SPEC-v1 §7): each frame's bytes begin at a 64-byte file offset so
/// that in-frame tensors stay 64B-aligned within the file. Equals the tensor alignment (§2.5).
const FRAME_ALIGN: usize = 64;

// ---------------------------------------------------------------- varint -----

fn write_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn encode_uvarint(v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    write_uvarint(&mut out, v);
    out
}

/// Decode a uvarint from `data` at `pos`, returning `(value, new_pos)`.
fn decode_uvarint(data: &[u8], mut pos: usize) -> Result<(u64, usize), CowrieError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if pos >= data.len() {
            return Err(CowrieError::Truncated);
        }
        let byte = data[pos];
        pos += 1;
        if shift >= 64 {
            return Err(CowrieError::InvalidData("uvarint overflow".into()));
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok((result, pos))
}

// ---------------------------------------------------------------- Merkle -----

fn leaf(frame: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([0x00u8]);
    h.update(frame);
    h.finalize().into()
}

fn node(a: &[u8], b: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([0x01u8]);
    h.update(a);
    h.update(b);
    h.finalize().into()
}

fn tree_digest(frames: &[Vec<u8>]) -> [u8; 32] {
    if frames.is_empty() {
        return Sha256::digest(b"").into();
    }
    let mut level: Vec<[u8; 32]> = frames.iter().map(|f| leaf(f)).collect();
    while level.len() > 1 {
        let mut next: Vec<[u8; 32]> = Vec::new();
        let mut i = 0;
        // pair up (0,1),(2,3),...; stop before the lone tail node if count is odd.
        while i + 1 < level.len() {
            next.push(node(&level[i], &level[i + 1]));
            i += 2;
        }
        if level.len() % 2 == 1 {
            // promote the odd node unchanged (RFC 6962, not duplicate).
            next.push(level[level.len() - 1]);
        }
        level = next;
    }
    level[0]
}

/// Multihash SHA-256 Merkle root over the ordered frame byte-blobs (§7), with the frame count
/// bound into the root (0x02 domain) so distinct frame lists can never share a root.
/// Returns the 34-byte multihash (`0x12 0x20` || 32 digest bytes).
pub fn merkle_root(frames: &[Vec<u8>]) -> Vec<u8> {
    let mut h = Sha256::new();
    h.update([0x02u8]);
    h.update(encode_uvarint(frames.len() as u64));
    h.update(tree_digest(frames));
    let root: [u8; 32] = h.finalize().into();
    let mut out = Vec::with_capacity(34);
    out.extend_from_slice(&[0x12, 0x20]);
    out.extend_from_slice(&root);
    out
}

// ---------------------------------------------------------------- encode -----

/// Build a Cowrie file from an ordered list of already-canonical frame byte-blobs (each one
/// complete §2.2 COWR wire bytes).
pub fn encode_file(frames: &[Vec<u8>]) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::new();
    out.extend_from_slice(FILE_MAGIC);
    out.push(FILE_VERSION);
    out.push(0x00); // reserved
    write_uvarint(&mut out, frames.len() as u64);
    let mut offsets: Vec<usize> = Vec::with_capacity(frames.len());
    for f in frames {
        write_uvarint(&mut out, f.len() as u64);
        // Each frame's bytes begin at a 64-byte FILE offset (SPEC-v1 §7) so that in-frame tensors
        // remain 64B-aligned within the file. Pad with canonical zero bytes; the footer offset
        // points to the aligned frame start.
        let pad = (FRAME_ALIGN - (out.len() % FRAME_ALIGN)) % FRAME_ALIGN;
        out.extend(std::iter::repeat_n(0u8, pad));
        offsets.push(out.len()); // offset to frame_bytes (64B-aligned)
        out.extend_from_slice(f);
    }
    let footer_offset = out.len();
    // footer
    write_uvarint(&mut out, frames.len() as u64);
    for (off, f) in offsets.iter().zip(frames.iter()) {
        write_uvarint(&mut out, *off as u64);
        write_uvarint(&mut out, f.len() as u64);
    }
    out.extend_from_slice(&merkle_root(frames));
    out.extend_from_slice(&(footer_offset as u64).to_le_bytes());
    out.extend_from_slice(FILE_MAGIC);
    out
}

// ---------------------------------------------------------------- decode -----

/// Return the ordered frame byte-blobs, fully validated (§7). The BODY is the source of truth:
/// frames are read sequentially via their length prefixes, and the footer MUST exactly mirror that
/// layout (count, contiguous offsets/lengths) — so there is exactly one canonical file
/// byte-string per ordered frame list. With `verify=true` the stored Merkle root must also match.
pub fn decode_file(data: &[u8], verify: bool) -> Result<Vec<Vec<u8>>, CowrieError> {
    // 4 (magic) + 1 (version) + 1 (reserved) + 1 (min body) + 8 (footer_offset) + 4 (end magic)
    if data.len() < 4 + 1 + 1 + 1 + 8 + 4 {
        return Err(CowrieError::Truncated);
    }
    if &data[..4] != FILE_MAGIC {
        return Err(CowrieError::InvalidMagic);
    }
    if &data[data.len() - 4..] != FILE_MAGIC {
        return Err(CowrieError::InvalidMagic);
    }
    if data[4] != FILE_VERSION {
        return Err(CowrieError::InvalidVersion(data[4]));
    }
    if data[5] != 0x00 {
        return Err(CowrieError::NonCanonical("reserved byte must be 0".into()));
    }
    let footer_offset = {
        let off_bytes: [u8; 8] = data[data.len() - 12..data.len() - 4]
            .try_into()
            .expect("slice is exactly 8 bytes");
        u64::from_le_bytes(off_bytes) as usize
    };
    if !(footer_offset > 6 && footer_offset <= data.len() - 12) {
        return Err(CowrieError::Truncated);
    }

    // --- body: read frames sequentially (the source of truth) ---
    let mut pos = 6usize;
    let (n, p) = decode_uvarint(data, pos)?;
    pos = p;
    if n > MAX_FRAMES {
        return Err(CowrieError::Truncated);
    }
    let mut frames: Vec<Vec<u8>> = Vec::with_capacity(n as usize);
    let mut layout: Vec<(usize, usize)> = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let (flen, p) = decode_uvarint(data, pos)?;
        pos = p;
        let flen = flen as usize;
        // Each frame's bytes begin at a 64-byte FILE offset (SPEC-v1 §7): skip the canonical
        // zero padding and verify it is zero, mirroring the Python reference.
        let pad = (FRAME_ALIGN - (pos % FRAME_ALIGN)) % FRAME_ALIGN;
        if pos + pad > data.len() {
            return Err(CowrieError::Truncated);
        }
        if data[pos..pos + pad].iter().any(|&b| b != 0) {
            return Err(CowrieError::NonCanonical(
                "frame alignment padding not zero".into(),
            ));
        }
        pos += pad;
        if pos + flen > footer_offset {
            return Err(CowrieError::Truncated);
        }
        layout.push((pos, flen));
        frames.push(data[pos..pos + flen].to_vec());
        pos += flen;
    }
    if pos != footer_offset {
        return Err(CowrieError::NonCanonical(
            "body does not end exactly at the footer".into(),
        ));
    }

    // --- footer: must mirror the body layout exactly (canonical) ---
    let (fcount, p) = decode_uvarint(data, pos)?;
    pos = p;
    if fcount != n {
        return Err(CowrieError::NonCanonical(
            "footer frame count != header".into(),
        ));
    }
    for (off, flen) in &layout {
        let (foff, p) = decode_uvarint(data, pos)?;
        pos = p;
        let (flen2, p) = decode_uvarint(data, pos)?;
        pos = p;
        if foff as usize != *off || flen2 as usize != *flen {
            return Err(CowrieError::NonCanonical(
                "footer index does not mirror body layout".into(),
            ));
        }
    }
    if pos + 34 > data.len() {
        return Err(CowrieError::Truncated);
    }
    let stored_root = &data[pos..pos + 34];
    pos += 34;
    if pos != data.len() - 12 {
        return Err(CowrieError::NonCanonical(
            "footer length not canonical".into(),
        ));
    }
    if verify && merkle_root(&frames) != stored_root {
        return Err(CowrieError::NonCanonical(
            "merkle root mismatch (tampered or non-canonical)".into(),
        ));
    }
    Ok(frames)
}

/// The file's content identity = its verified Merkle root (34-byte multihash).
pub fn file_identity(data: &[u8]) -> Result<Vec<u8>, CowrieError> {
    let frames = decode_file(data, true)?;
    Ok(merkle_root(&frames))
}
