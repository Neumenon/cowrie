//! 16 KB-aligned Shard Container (`CWRF`), RFC 6962 Merkle verification/localization
//! (committing to BOTH Plane A frames and Plane C tensor arenas), unified strided model
//! fingerprinting, atomic `O_CREAT|O_EXCL` `0600` `StatePage` save, optional HMAC-SHA256
//! authentication, and zero-copy `mmap` verified restore.

#[cfg(unix)]
use std::fs::{self, File, OpenOptions};
#[cfg(unix)]
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(unix)]
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};

use crate::canonical::{
    checked_tensor_min_bytes, verify_frame_with_tensor_hook_zero_alloc, Value,
};
#[cfg(unix)]
use crate::sha256::Sha256;
use crate::sha256::{
    ct_eq_32, hmac_sha256, merkle_leaf, sha256, sha256_parallel_merkle,
    shard_merkle_root_from_leaves, tensor_merkle_leaf, to_hex,
};
use crate::CowrieError;

pub const MAGIC_SHARD: &[u8; 4] = b"CWRF";
pub const SHARD_VERSION: u8 = 1;
pub const ALIGN_LOG2: u8 = 14;
pub const PAGE_ALIGN: usize = 16384; // 2^14 = 16 KB
pub const TENSOR_ENTRY_BYTES: usize = 48; // off(8) + len(8) + merkle_4m(32)
pub const STATEPAGE_FOOTER_BYTES: usize = TENSOR_ENTRY_BYTES + 34 + 8 + 4; // 94 bytes

pub const FLAG_STATEPAGE: u16 = 0x0001;
pub const FLAG_IVF_COLOCATED: u16 = 0x0002;

#[inline]
pub fn checked_align_16kb(n: usize) -> Result<usize, CowrieError> {
    n.checked_add(PAGE_ALIGN - 1)
        .map(|v| v & !(PAGE_ALIGN - 1))
        .ok_or(CowrieError::AllocLimit)
}

#[inline]
pub fn align_16kb(n: usize) -> usize {
    checked_align_16kb(n).expect("align_16kb overflow")
}

#[inline]
fn u64_to_usize(v: u64) -> Result<usize, CowrieError> {
    usize::try_from(v).map_err(|_| CowrieError::Truncated)
}

#[inline]
pub fn is_lower_hex_64(b: &[u8]) -> bool {
    b.len() == 64 && b.iter().all(|&c| matches!(c, b'0'..=b'9' | b'a'..=b'f'))
}

/// Decoded 64-byte Shard Header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardHeader {
    pub version: u8,
    pub align_log2: u8,
    pub flags: u16,
    pub frame_count: u32,
    pub tensor_count: u32,
    pub plane_a_off: u64,
    pub plane_a_len: u64,
    pub plane_c_off: u64,
    pub plane_c_len: u64,
    pub footer_off: u64,
}

/// Frame directory entry stored in Plane D (44 bytes).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameEntry {
    pub offset: u64,
    pub len: u32,
    pub leaf_sha256: [u8; 32],
}

/// Tensor directory entry stored in Plane D (48 bytes; all offsets strictly `% 16384 == 0`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TensorEntry {
    pub offset: u64,
    pub len: u64,
    pub merkle_4m_sha256: [u8; 32],
}

/// Verification report returned by `verify_shard_and_localize`.
#[derive(Clone, Debug)]
pub struct ShardReport {
    pub ok: bool,
    pub header: ShardHeader,
    pub frames: Vec<FrameEntry>,
    pub tensors: Vec<TensorEntry>,
    pub stored_root: [u8; 34],
    pub computed_root: [u8; 34],
    pub bad_frames: Vec<usize>,
    pub bad_tensors: Vec<usize>,
}

/// Recursively verify that every `TAG_TENSOR_REF (0x0A)` inside a Plane A value references
/// a valid Plane C tensor slot whose byte length covers `checked_tensor_min_bytes(dtype, dims)`.
pub fn validate_value_tensor_refs(v: &Value, tensors: &[TensorEntry]) -> Result<(), CowrieError> {
    match v {
        Value::TensorRef { arena_idx, dtype, dims, .. } => {
            let idx = *arena_idx as usize;
            if idx >= tensors.len() {
                return Err(CowrieError::InvalidContract);
            }
            let min_bytes = checked_tensor_min_bytes(*dtype, dims)?;
            if min_bytes > tensors[idx].len {
                return Err(CowrieError::InvalidContract);
            }
            Ok(())
        }
        Value::Array(a) => {
            for x in a {
                validate_value_tensor_refs(x, tensors)?;
            }
            Ok(())
        }
        Value::Map(m) => {
            for (_, x) in m {
                validate_value_tensor_refs(x, tensors)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Build a sealed, strictly 16 KB-aligned `CWRF` shard committing to BOTH Plane A frames and Plane C tensors.
pub fn build_shard_16kb(frames: &[&[u8]], tensors: &[&[u8]], flags: u16) -> Vec<u8> {
    let mut plane_a = Vec::new();
    let mut frame_meta: Vec<(u64, u32, [u8; 32])> = Vec::with_capacity(frames.len());
    let mut frame_leaves: Vec<[u8; 32]> = Vec::with_capacity(frames.len());
    let mut cur: usize = 64;
    for f in frames {
        let leaf = merkle_leaf(f);
        frame_meta.push((cur as u64, f.len() as u32, leaf));
        frame_leaves.push(leaf);
        plane_a.extend_from_slice(f);
        cur += f.len();
    }

    let pad_a = (PAGE_ALIGN - (cur % PAGE_ALIGN)) % PAGE_ALIGN;
    let plane_c_off = cur + pad_a;

    let mut plane_c = Vec::new();
    let mut tensor_meta: Vec<(u64, u64, [u8; 32])> = Vec::with_capacity(tensors.len());
    let mut tensor_leaves: Vec<[u8; 32]> = Vec::with_capacity(tensors.len());
    let mut t_cur = plane_c_off;
    for tb in tensors {
        debug_assert_eq!(t_cur % PAGE_ALIGN, 0);
        let t_merkle = sha256_parallel_merkle(tb);
        let t_leaf = tensor_merkle_leaf(tb.len() as u64, &t_merkle);
        tensor_meta.push((t_cur as u64, tb.len() as u64, t_merkle));
        tensor_leaves.push(t_leaf);
        plane_c.extend_from_slice(tb);
        t_cur += tb.len();
        let tpad = (PAGE_ALIGN - (t_cur % PAGE_ALIGN)) % PAGE_ALIGN;
        if tpad > 0 {
            plane_c.resize(plane_c.len() + tpad, 0u8);
            t_cur += tpad;
        }
    }

    let footer_off = t_cur;
    let mroot = shard_merkle_root_from_leaves(&frame_leaves, &tensor_leaves);

    let mut footer = Vec::with_capacity(frames.len() * 44 + tensors.len() * TENSOR_ENTRY_BYTES + 46);
    for (off, flen, leaf) in &frame_meta {
        footer.extend_from_slice(&off.to_le_bytes());
        footer.extend_from_slice(&flen.to_le_bytes());
        footer.extend_from_slice(leaf);
    }
    for (toff, tlen, t_merkle) in &tensor_meta {
        footer.extend_from_slice(&toff.to_le_bytes());
        footer.extend_from_slice(&tlen.to_le_bytes());
        footer.extend_from_slice(t_merkle);
    }
    footer.extend_from_slice(&mroot);
    footer.extend_from_slice(&(footer_off as u64).to_le_bytes());
    footer.extend_from_slice(MAGIC_SHARD);

    let mut out = Vec::with_capacity(footer_off + footer.len());
    out.extend_from_slice(MAGIC_SHARD);
    out.push(SHARD_VERSION);
    out.push(ALIGN_LOG2);
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(&(frames.len() as u32).to_le_bytes());
    out.extend_from_slice(&(tensors.len() as u32).to_le_bytes());
    out.extend_from_slice(&64u64.to_le_bytes());
    out.extend_from_slice(&(plane_a.len() as u64).to_le_bytes());
    out.extend_from_slice(&(plane_c_off as u64).to_le_bytes());
    out.extend_from_slice(&(plane_c.len() as u64).to_le_bytes());
    out.extend_from_slice(&(footer_off as u64).to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
    debug_assert_eq!(out.len(), 64);

    out.extend_from_slice(&plane_a);
    out.resize(out.len() + pad_a, 0u8);
    out.extend_from_slice(&plane_c);
    out.extend_from_slice(&footer);
    out
}

pub fn parse_shard_header(shard: &[u8]) -> Result<ShardHeader, CowrieError> {
    if shard.len() < 76 {
        return Err(CowrieError::Truncated);
    }
    if &shard[0..4] != MAGIC_SHARD {
        return Err(CowrieError::BadMagic);
    }
    if &shard[shard.len() - 4..] != MAGIC_SHARD {
        return Err(CowrieError::BadMagic);
    }
    let version = shard[4];
    let align_log2 = shard[5];
    let flags = u16::from_le_bytes(shard[6..8].try_into().unwrap());
    if version != SHARD_VERSION {
        return Err(CowrieError::UnsupportedVer);
    }
    if align_log2 != ALIGN_LOG2 || (flags & !(FLAG_STATEPAGE | FLAG_IVF_COLOCATED)) != 0 {
        return Err(CowrieError::NonCanonicalFlags);
    }
    let reserved = u64::from_le_bytes(shard[56..64].try_into().unwrap());
    if reserved != 0 {
        return Err(CowrieError::NonCanonicalFlags);
    }
    let frame_count = u32::from_le_bytes(shard[8..12].try_into().unwrap());
    let tensor_count = u32::from_le_bytes(shard[12..16].try_into().unwrap());
    let plane_a_off = u64::from_le_bytes(shard[16..24].try_into().unwrap());
    let plane_a_len = u64::from_le_bytes(shard[24..32].try_into().unwrap());
    let plane_c_off = u64::from_le_bytes(shard[32..40].try_into().unwrap());
    let plane_c_len = u64::from_le_bytes(shard[40..48].try_into().unwrap());
    let footer_off = u64::from_le_bytes(shard[48..56].try_into().unwrap());

    let trailer_foff = u64::from_le_bytes(shard[shard.len() - 12..shard.len() - 4].try_into().unwrap());
    if trailer_foff != footer_off {
        return Err(CowrieError::CorruptTrailer);
    }
    if plane_c_off % (PAGE_ALIGN as u64) != 0 {
        return Err(CowrieError::UnalignedArena);
    }
    Ok(ShardHeader {
        version,
        align_log2,
        flags,
        frame_count,
        tensor_count,
        plane_a_off,
        plane_a_len,
        plane_c_off,
        plane_c_len,
        footer_off,
    })
}

/// Strictly verify 16 KB alignment, `0x00` padding, EOF trailer, and RFC 6962 Merkle root
/// across BOTH Plane A frames and Plane C tensors, localizing exact corrupt frame/tensor indices.
pub fn verify_shard_and_localize(shard: &[u8]) -> Result<ShardReport, CowrieError> {
    let header = parse_shard_header(shard)?;
    if header.plane_a_off != 64 {
        return Err(CowrieError::InvalidContract);
    }
    let pa_off = u64_to_usize(header.plane_a_off)?;
    let pa_len = u64_to_usize(header.plane_a_len)?;
    let pa_end = pa_off.checked_add(pa_len).ok_or(CowrieError::Truncated)?;
    let pc_off = u64_to_usize(header.plane_c_off)?;
    if pa_end > pc_off || pc_off > shard.len() {
        return Err(CowrieError::Truncated);
    }
    if shard[pa_end..pc_off].iter().any(|&b| b != 0) {
        return Err(CowrieError::NonZeroPad);
    }
    if (header.flags & FLAG_STATEPAGE) != 0 && (header.frame_count != 0 || header.plane_a_len != 276) {
        return Err(CowrieError::InvalidContract);
    }

    let footer_off_usize = u64_to_usize(header.footer_off)?;
    let mut pos = footer_off_usize;
    let frame_cnt = usize::try_from(header.frame_count).map_err(|_| CowrieError::AllocLimit)?;
    let tensor_cnt = usize::try_from(header.tensor_count).map_err(|_| CowrieError::AllocLimit)?;
    let need_footer = frame_cnt
        .checked_mul(44)
        .and_then(|x| x.checked_add(tensor_cnt.checked_mul(TENSOR_ENTRY_BYTES)?))
        .and_then(|x| x.checked_add(34 + 12))
        .ok_or(CowrieError::Truncated)?;
    if pos.checked_add(need_footer).map_or(true, |end| end != shard.len()) {
        return Err(CowrieError::Truncated);
    }

    let mut frames_entries = Vec::with_capacity(frame_cnt);
    let mut actual_frame_leaves = Vec::with_capacity(frame_cnt);
    let mut bad_frames = Vec::new();
    let mut expected_cur: usize = pa_off;

    for idx in 0..frame_cnt {
        let off = u64::from_le_bytes(shard[pos..pos + 8].try_into().unwrap());
        let flen = u32::from_le_bytes(shard[pos + 8..pos + 12].try_into().unwrap());
        let mut expected_leaf = [0u8; 32];
        expected_leaf.copy_from_slice(&shard[pos + 12..pos + 44]);
        pos += 44;

        let off_usize = u64_to_usize(off)?;
        let flen_usize = usize::try_from(flen).map_err(|_| CowrieError::Truncated)?;
        if off_usize != expected_cur {
            return Err(CowrieError::InvalidContract);
        }
        let end = off_usize.checked_add(flen_usize).ok_or(CowrieError::Truncated)?;
        if end > pa_end {
            return Err(CowrieError::Truncated);
        }
        expected_cur = end;
        let fb = &shard[off_usize..end];
        let actual_leaf = merkle_leaf(fb);
        if actual_leaf != expected_leaf {
            bad_frames.push(idx);
        }
        frames_entries.push(FrameEntry {
            offset: off,
            len: flen,
            leaf_sha256: expected_leaf,
        });
        actual_frame_leaves.push(actual_leaf);
    }
    if (header.flags & FLAG_STATEPAGE) == 0 && expected_cur != pa_end {
        return Err(CowrieError::InvalidContract);
    }

    let pc_len = u64_to_usize(header.plane_c_len)?;
    let pc_end = pc_off.checked_add(pc_len).ok_or(CowrieError::Truncated)?;
    if pc_end != footer_off_usize {
        return Err(CowrieError::Truncated);
    }

    let mut tensor_entries = Vec::with_capacity(tensor_cnt);
    let mut actual_tensor_leaves = Vec::with_capacity(tensor_cnt);
    let mut bad_tensors = Vec::new();
    let mut expected_t_cur: usize = pc_off;

    for t_idx in 0..tensor_cnt {
        let toff = u64::from_le_bytes(shard[pos..pos + 8].try_into().unwrap());
        let tlen = u64::from_le_bytes(shard[pos + 8..pos + 16].try_into().unwrap());
        let mut expected_merkle = [0u8; 32];
        expected_merkle.copy_from_slice(&shard[pos + 16..pos + 48]);
        pos += TENSOR_ENTRY_BYTES;

        if toff % (PAGE_ALIGN as u64) != 0 {
            return Err(CowrieError::UnalignedArena);
        }
        let toff_usize = u64_to_usize(toff)?;
        let tlen_usize = u64_to_usize(tlen)?;
        if toff_usize != expected_t_cur {
            return Err(CowrieError::InvalidContract);
        }
        let t_end = toff_usize.checked_add(tlen_usize).ok_or(CowrieError::Truncated)?;
        let t_pad_end = checked_align_16kb(t_end).map_err(|_| CowrieError::Truncated)?;
        if t_pad_end > pc_end {
            return Err(CowrieError::Truncated);
        }
        if shard[t_end..t_pad_end].iter().any(|&b| b != 0) {
            return Err(CowrieError::NonZeroPad);
        }
        expected_t_cur = t_pad_end;

        let actual_merkle = sha256_parallel_merkle(&shard[toff_usize..t_end]);
        if actual_merkle != expected_merkle {
            bad_tensors.push(t_idx);
        }
        actual_tensor_leaves.push(tensor_merkle_leaf(tlen, &actual_merkle));
        tensor_entries.push(TensorEntry {
            offset: toff,
            len: tlen,
            merkle_4m_sha256: expected_merkle,
        });
    }
    if expected_t_cur != pc_end {
        return Err(CowrieError::InvalidContract);
    }

    let mut stored_root = [0u8; 34];
    stored_root.copy_from_slice(&shard[pos..pos + 34]);
    let computed_root = shard_merkle_root_from_leaves(&actual_frame_leaves, &actual_tensor_leaves);
    let ok = (stored_root == computed_root) && bad_frames.is_empty() && bad_tensors.is_empty();

    // If Merkle seal is intact, enforce zero-allocation canonical frame verification AND TensorRef bounds!
    if ok && (header.flags & FLAG_STATEPAGE) == 0 {
        for fe in &frames_entries {
            let foff = u64_to_usize(fe.offset)?;
            let flen = usize::try_from(fe.len).map_err(|_| CowrieError::Truncated)?;
            let fb = &shard[foff..foff + flen];
            verify_frame_with_tensor_hook_zero_alloc(fb, |arena_idx, dtype, dims| {
                let idx = arena_idx as usize;
                if idx >= tensor_entries.len() {
                    return Err(CowrieError::InvalidContract);
                }
                let min_bytes = checked_tensor_min_bytes(dtype, dims)?;
                if min_bytes > tensor_entries[idx].len {
                    return Err(CowrieError::InvalidContract);
                }
                Ok(())
            })?;
        }
    }

    Ok(ShardReport {
        ok,
        header,
        frames: frames_entries,
        tensors: tensor_entries,
        stored_root,
        computed_root,
        bad_frames,
        bad_tensors,
    })
}

/// Compute a repack-stable 34-byte resolved content identifier for `frame_idx` inside a verified `CWRF` shard.
///
/// Unlike `content_address(frame_bytes)` (which identifies the Plane A frame descriptor alone),
/// `resolved_frame_cid` commits to BOTH the frame descriptor CID AND the exact byte length, dtype/shape contract,
/// and 4 MiB Merkle digest of every Plane C tensor referenced via `TAG_TENSOR_REF (0x0A)` in that frame.
/// Two shards with identical frame descriptors pointing to different tensor bytes produce distinct resolved CIDs,
/// while repacking the same frame + referenced tensors into a different shard preserves the resolved CID.
pub fn resolved_frame_cid(shard: &[u8], frame_idx: usize) -> Result<[u8; 34], CowrieError> {
    let rep = verify_shard_and_localize(shard)?;
    if !rep.ok {
        return Err(CowrieError::RootMismatch);
    }
    let fe = rep.frames.get(frame_idx).ok_or(CowrieError::InvalidContract)?;
    let foff = u64_to_usize(fe.offset)?;
    let flen = usize::try_from(fe.len).map_err(|_| CowrieError::Truncated)?;
    let fb = shard
        .get(foff..foff.checked_add(flen).ok_or(CowrieError::Truncated)?)
        .ok_or(CowrieError::Truncated)?;
    let frame_cid = crate::sha256::content_address(fb);

    let mut refs: Vec<(u32, u8, Vec<u64>)> = Vec::new();
    verify_frame_with_tensor_hook_zero_alloc(fb, |arena_idx, dtype, dims| {
        if (arena_idx as usize) >= rep.tensors.len() {
            return Err(CowrieError::InvalidContract);
        }
        refs.push((arena_idx, dtype, dims.to_vec()));
        Ok(())
    })?;

    let mut h = crate::sha256::Sha256::new();
    h.update(b"COWRIE_RESOLVED_FRAME_V1\0");
    h.update(&frame_cid);
    h.update(&(refs.len() as u64).to_le_bytes());
    for (arena_idx, dtype, dims) in &refs {
        let te = &rep.tensors[*arena_idx as usize];
        h.update(&arena_idx.to_le_bytes());
        h.update(&[*dtype, dims.len() as u8]);
        for &d in dims {
            h.update(&d.to_le_bytes());
        }
        h.update(&te.len.to_le_bytes());
        h.update(&te.merkle_4m_sha256);
    }
    let digest = h.finalize();
    let mut out = [0u8; 34];
    out[0] = 0x12;
    out[1] = 0x20;
    out[2..34].copy_from_slice(&digest);
    Ok(out)
}

// =========================================================================
// UNIFIED STRIDED MODEL FINGERPRINTER (< 1 MS ON NVME, LORA/MID-LAYER SAFE)
// =========================================================================

/// Compute a deterministic 64-hex SHA-256 fingerprint of a model file by hashing:
/// - Files `<= 16 MiB` (tokenizers, configs, LoRA adapters): 100% full-file hardware SHA-256.
/// - Files `> 16 MiB` (multi-GB GGUF/Safetensors): `file_size_le64` + 64 evenly strided 64 KiB
///   windows (`4 MiB` total) spanning header, all interior transformer layers, and exact tail.
#[cfg(unix)]
pub fn fingerprint_model_file(path: &Path) -> Result<String, CowrieError> {
    const WINDOW: u64 = 65536;               // 64 KiB
    const STRIDES: u64 = 64;                 // 64 * 64 KiB = 4 MiB max read on multi-GB weights
    const FULL_HASH_MAX: u64 = 16 * 1024 * 1024; // 16 MiB full-file threshold

    let file = File::open(path).map_err(|_| CowrieError::IoError)?;
    let meta = file.metadata().map_err(|_| CowrieError::IoError)?;
    if !meta.file_type().is_file() {
        return Err(CowrieError::InvalidContract);
    }
    let fsz = meta.len();

    let mut hasher = Sha256::new();
    hasher.update(b"COWRIE_MODEL_FP_V1\0");
    hasher.update(&fsz.to_le_bytes());

    if fsz <= FULL_HASH_MAX {
        let mut rem = fsz as usize;
        let mut off = 0u64;
        let mut buf = vec![0u8; (WINDOW as usize).min(rem.max(1))];
        while rem > 0 {
            let take = buf.len().min(rem);
            file.read_exact_at(&mut buf[..take], off).map_err(|_| CowrieError::IoError)?;
            hasher.update(&buf[..take]);
            off += take as u64;
            rem -= take;
        }
    } else {
        let mut buf = vec![0u8; WINDOW as usize];
        let span = fsz - WINDOW;
        for i in 0..STRIDES {
            let off = if i + 1 == STRIDES {
                span // exact tail 64 KiB
            } else {
                (span * i) / (STRIDES - 1)
            };
            hasher.update(&off.to_le_bytes());
            file.read_exact_at(&mut buf, off).map_err(|_| CowrieError::IoError)?;
            hasher.update(&buf);
        }
    }

    Ok(to_hex(&hasher.finalize()))
}

// =========================================================================
// 16 KB-ALIGNED HYBRID SSM+KV STATEPAGE SHARDS + ZERO-COPY MMAP
// =========================================================================

pub const STATEPAGE_META_MAGIC: &[u8; 4] = b"CWSP";
pub const HASH_MODE_FLAT_SHA256: u8 = 0;
pub const HASH_MODE_PARALLEL_MERKLE_4M: u8 = 1;
pub const HMAC_FLAG_PRESENT: u8 = 1;

#[cfg(unix)]
static TMP_NONCE: AtomicU64 = AtomicU64::new(1);

fn env_hmac_key() -> Option<Vec<u8>> {
    match std::env::var("COWRIE_HMAC_KEY") {
        Ok(s) if !s.is_empty() => Some(s.into_bytes()),
        _ => None,
    }
}

/// Metadata stored in the first 16 KB page of a Cowrie `StatePage` shard (`flags = FLAG_STATEPAGE`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatePageMeta {
    pub model_sha: String,     // strictly 64 lowercase ASCII hex chars (no NULs)
    pub doc_cid: String,       // strictly 64 lowercase ASCII hex chars (no NULs)
    pub state_sha: String,     // strictly 64 lowercase ASCII hex chars (no NULs)
    pub runtime_id: String,    // optional 0..=16 ASCII runtime tag (e.g. "llama.cpp", "eureka")
    pub layout_ver: u16,       // runtime state layout version
    pub hmac_present: bool,    // true when authenticated by HMAC-SHA256 (`COWRIE_HMAC_KEY`)
    pub ctx_tokens: u32,
    pub kv_quant: u8,          // 0 = fp16, 1 = q4_0, 2 = q8_0, 3 = f32
    pub ssm_dtype: u8,         // 0 = fp16, 1 = bf16, 2 = fp32
    pub hash_mode: u8,         // 0 = flat SHA-256, 1 = parallel 4 MiB chunked RFC 6962 Merkle SHA-256
    pub state_offset: u64,     // strictly % 16384 == 0
    pub state_len: u64,
}

#[cfg(unix)]
fn unique_tmp_path(path: &Path) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let fname = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("statepage.cowrie");
    let pid = std::process::id();
    let seq = TMP_NONCE.fetch_add(1, Ordering::Relaxed);
    parent.join(format!(".{}.{}.{}.tmp", fname, pid, seq))
}

/// Atomically save a hybrid SSM+KV `StatePage` into a strictly 16 KB-aligned `CWRF` shard on NVMe.
#[cfg(unix)]
pub fn save_statepage_atomic(
    path: &Path,
    model_sha: &str,
    doc_cid: &str,
    ctx_tokens: u32,
    kv_quant: u8,
    ssm_dtype: u8,
    state_bytes: &[u8],
) -> Result<StatePageMeta, CowrieError> {
    save_statepage_atomic_ext(
        path,
        model_sha,
        doc_cid,
        "",
        0,
        ctx_tokens,
        kv_quant,
        ssm_dtype,
        state_bytes,
        None,
    )
}

/// Extended atomic `StatePage` save supporting `runtime_id`, `layout_ver`, `O_CREAT|O_EXCL` `0600`
/// temp-file isolation, parent directory `fsync`, and optional RFC 2104 `HMAC-SHA256` authentication.
#[cfg(unix)]
pub fn save_statepage_atomic_ext(
    path: &Path,
    model_sha: &str,
    doc_cid: &str,
    runtime_id: &str,
    layout_ver: u16,
    ctx_tokens: u32,
    kv_quant: u8,
    ssm_dtype: u8,
    state_bytes: &[u8],
    explicit_hmac_key: Option<&[u8]>,
) -> Result<StatePageMeta, CowrieError> {
    if !is_lower_hex_64(model_sha.as_bytes()) || !is_lower_hex_64(doc_cid.as_bytes()) {
        return Err(CowrieError::InvalidContract);
    }
    if runtime_id.len() > 16 || runtime_id.as_bytes().iter().any(|&b| b < 0x20 || b >= 0x7F) {
        return Err(CowrieError::InvalidContract);
    }

    let hash_mode = HASH_MODE_PARALLEL_MERKLE_4M;
    let state_merkle_32 = sha256_parallel_merkle(state_bytes);
    let state_sha = to_hex(&state_merkle_32);
    let state_len = state_bytes.len() as u64;
    let state_offset: u64 = PAGE_ALIGN as u64;
    let padded_state_len = checked_align_16kb(state_bytes.len())?;
    let footer_off = state_offset
        .checked_add(padded_state_len as u64)
        .ok_or(CowrieError::AllocLimit)?;

    let env_key = if explicit_hmac_key.is_none() { env_hmac_key() } else { None };
    let active_hmac_key: Option<&[u8]> = explicit_hmac_key.or(env_key.as_deref());
    let hmac_present = active_hmac_key.is_some();

    let mut page0 = vec![0u8; PAGE_ALIGN];
    // 64B CWRF shard header
    page0[0..4].copy_from_slice(MAGIC_SHARD);
    page0[4] = SHARD_VERSION;
    page0[5] = ALIGN_LOG2;
    page0[6..8].copy_from_slice(&FLAG_STATEPAGE.to_le_bytes());
    page0[8..12].copy_from_slice(&0u32.to_le_bytes()); // frame_count = 0
    page0[12..16].copy_from_slice(&1u32.to_le_bytes()); // tensor_count = 1 (the state slab)
    page0[16..24].copy_from_slice(&64u64.to_le_bytes());
    page0[24..32].copy_from_slice(&276u64.to_le_bytes()); // [64..340) StatePage descriptor + HMAC
    page0[32..40].copy_from_slice(&state_offset.to_le_bytes());
    page0[40..48].copy_from_slice(&(padded_state_len as u64).to_le_bytes());
    page0[48..56].copy_from_slice(&footer_off.to_le_bytes());
    page0[56..64].copy_from_slice(&0u64.to_le_bytes());

    // StatePage metadata block at [64..340)
    page0[64..68].copy_from_slice(STATEPAGE_META_MAGIC);
    page0[68..72].copy_from_slice(&ctx_tokens.to_le_bytes());
    page0[72] = kv_quant;
    page0[73] = ssm_dtype;
    page0[74] = hash_mode;
    page0[75] = if hmac_present { HMAC_FLAG_PRESENT } else { 0 };
    page0[76..80].copy_from_slice(&[0u8; 4]);
    page0[80..88].copy_from_slice(&state_offset.to_le_bytes());
    page0[88..96].copy_from_slice(&state_len.to_le_bytes());
    page0[96..160].copy_from_slice(model_sha.as_bytes());
    page0[160..224].copy_from_slice(doc_cid.as_bytes());
    page0[224..288].copy_from_slice(state_sha.as_bytes());
    page0[288..288 + runtime_id.len()].copy_from_slice(runtime_id.as_bytes());
    page0[304..306].copy_from_slice(&layout_ver.to_le_bytes());
    page0[306..308].copy_from_slice(&[0u8; 2]);

    if let Some(k) = active_hmac_key {
        let mac = hmac_sha256(k, &page0[0..308]);
        page0[308..340].copy_from_slice(&mac);
    }
    // bytes [340..16384) strictly zero

    // Full CWRF Plane D footer committing to the Plane C state slab:
    // 1 TensorEntry (48B) + shard Merkle root (34B) + footer_off (8B) + "CWRF" (4B) = 94B
    let t_leaf = tensor_merkle_leaf(state_len, &state_merkle_32);
    let mroot = shard_merkle_root_from_leaves(&[], &[t_leaf]);
    let mut footer = Vec::with_capacity(STATEPAGE_FOOTER_BYTES);
    footer.extend_from_slice(&state_offset.to_le_bytes());
    footer.extend_from_slice(&state_len.to_le_bytes());
    footer.extend_from_slice(&state_merkle_32);
    footer.extend_from_slice(&mroot);
    footer.extend_from_slice(&footer_off.to_le_bytes());
    footer.extend_from_slice(MAGIC_SHARD);

    let tmp_path = unique_tmp_path(path);
    let write_res = (|| -> Result<(), CowrieError> {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp_path)
            .map_err(|_| CowrieError::IoError)?;
        f.write_all(&page0).map_err(|_| CowrieError::IoError)?;
        f.write_all(state_bytes).map_err(|_| CowrieError::IoError)?;
        let pad_tail = padded_state_len - state_bytes.len();
        if pad_tail > 0 {
            let zeros = vec![0u8; pad_tail];
            f.write_all(&zeros).map_err(|_| CowrieError::IoError)?;
        }
        f.write_all(&footer).map_err(|_| CowrieError::IoError)?;
        f.sync_all().map_err(|_| CowrieError::IoError)?;
        Ok(())
    })();

    if let Err(e) = write_res {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    if let Err(_) = fs::rename(&tmp_path, path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(CowrieError::IoError);
    }

    // Best-effort parent directory fsync so atomic rename is crash-durable on ext4/xfs.
    if let Some(parent) = path.parent() {
        let p = if parent.as_os_str().is_empty() { Path::new(".") } else { parent };
        if let Ok(df) = File::open(p) {
            let _ = df.sync_all();
        }
    }

    Ok(StatePageMeta {
        model_sha: model_sha.to_string(),
        doc_cid: doc_cid.to_string(),
        state_sha,
        runtime_id: runtime_id.to_string(),
        layout_ver,
        hmac_present,
        ctx_tokens,
        kv_quant,
        ssm_dtype,
        hash_mode,
        state_offset,
        state_len,
    })
}

pub fn parse_statepage_meta(buf: &[u8]) -> Result<StatePageMeta, CowrieError> {
    parse_statepage_meta_with_hmac(buf, None)
}

pub fn parse_statepage_meta_with_hmac(
    buf: &[u8],
    explicit_hmac_key: Option<&[u8]>,
) -> Result<StatePageMeta, CowrieError> {
    if buf.len() < PAGE_ALIGN + STATEPAGE_FOOTER_BYTES {
        return Err(CowrieError::Truncated);
    }
    // Snapshot Page-0 header + HMAC onto stack before parsing to prevent double-fetch TOCTOU on mmap.
    let mut hdr_snap = [0u8; 340];
    hdr_snap.copy_from_slice(&buf[0..340]);

    let hmac_flag = hdr_snap[75];
    let env_key = if explicit_hmac_key.is_none() { env_hmac_key() } else { None };
    let active_key: Option<&[u8]> = explicit_hmac_key.or(env_key.as_deref());

    // [FORMAL-F13] Verify HMAC-SHA256 STRICTLY FIRST (even before checking `hmac_flag > 1` or
    // semantic header fields) whenever an HMAC key is active or `hmac_flag == 1`.
    let hmac_present = match (active_key, hmac_flag) {
        (Some(k), HMAC_FLAG_PRESENT) => {
            let expected_mac = hmac_sha256(k, &hdr_snap[0..308]);
            let stored_mac: &[u8; 32] = hdr_snap[308..340].try_into().unwrap();
            if !ct_eq_32(&expected_mac, stored_mac) {
                return Err(CowrieError::AuthMacMismatch);
            }
            true
        }
        (Some(_), _) | (None, HMAC_FLAG_PRESENT) => {
            return Err(CowrieError::AuthMacMismatch);
        }
        (None, 0) => {
            if hdr_snap[308..340].iter().any(|&b| b != 0) {
                return Err(CowrieError::NonZeroPad);
            }
            false
        }
        (None, _) => {
            return Err(CowrieError::NonCanonicalFlags);
        }
    };

    // Parse 64-byte CWRF shard header from the verified stack snapshot + EOF trailer.
    let mut hdr_and_trailer = [0u8; 76];
    hdr_and_trailer[0..64].copy_from_slice(&hdr_snap[0..64]);
    hdr_and_trailer[64..76].copy_from_slice(&buf[buf.len() - 12..]);
    let hdr = parse_shard_header(&hdr_and_trailer)?;
    if hdr.flags != FLAG_STATEPAGE
        || hdr.frame_count != 0
        || hdr.tensor_count != 1
        || hdr.plane_a_off != 64
        || hdr.plane_a_len != 276
    {
        return Err(CowrieError::InvalidContract);
    }
    if &hdr_snap[64..68] != STATEPAGE_META_MAGIC {
        return Err(CowrieError::BadMagic);
    }
    let ctx_tokens = u32::from_le_bytes(hdr_snap[68..72].try_into().unwrap());
    let kv_quant = hdr_snap[72];
    let ssm_dtype = hdr_snap[73];
    let hash_mode = hdr_snap[74];
    if hash_mode > HASH_MODE_PARALLEL_MERKLE_4M
        || hdr_snap[76..80].iter().any(|&b| b != 0)
        || hdr_snap[306..308].iter().any(|&b| b != 0)
    {
        return Err(CowrieError::NonCanonicalFlags);
    }
    if buf[340..PAGE_ALIGN].iter().any(|&b| b != 0) {
        return Err(CowrieError::NonZeroPad);
    }

    let state_offset = u64::from_le_bytes(hdr_snap[80..88].try_into().unwrap());
    let state_len = u64::from_le_bytes(hdr_snap[88..96].try_into().unwrap());
    if state_offset != PAGE_ALIGN as u64 || state_offset != hdr.plane_c_off {
        return Err(CowrieError::UnalignedArena);
    }
    let state_off_usize = u64_to_usize(state_offset)?;
    let state_len_usize = u64_to_usize(state_len)?;
    let end = state_off_usize
        .checked_add(state_len_usize)
        .ok_or(CowrieError::Truncated)?;
    let pad_end = checked_align_16kb(end).map_err(|_| CowrieError::Truncated)?;
    if pad_end
        .checked_add(STATEPAGE_FOOTER_BYTES)
        .map_or(true, |tot| tot != buf.len())
    {
        return Err(CowrieError::Truncated);
    }
    if hdr.plane_c_len != (pad_end - state_off_usize) as u64 || hdr.footer_off != pad_end as u64 {
        return Err(CowrieError::InvalidContract);
    }
    if buf[end..pad_end].iter().any(|&b| b != 0) {
        return Err(CowrieError::NonZeroPad);
    }

    // Strict lowercase ASCII hex check: guarantees zero NUL bytes (eliminates FFI CString panic).
    if !is_lower_hex_64(&hdr_snap[96..160])
        || !is_lower_hex_64(&hdr_snap[160..224])
        || !is_lower_hex_64(&hdr_snap[224..288])
    {
        return Err(CowrieError::InvalidContract);
    }
    let model_sha = std::str::from_utf8(&hdr_snap[96..160]).unwrap().to_string();
    let doc_cid = std::str::from_utf8(&hdr_snap[160..224]).unwrap().to_string();
    let state_sha = std::str::from_utf8(&hdr_snap[224..288]).unwrap().to_string();

    // Validate the 94-byte Plane D footer matches the HMAC-protected header fields & Merkle root.
    let footer = &buf[pad_end..pad_end + STATEPAGE_FOOTER_BYTES];
    let f_toff = u64::from_le_bytes(footer[0..8].try_into().unwrap());
    let f_tlen = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    let f_merkle: [u8; 32] = footer[16..48].try_into().unwrap();
    let f_mroot: [u8; 34] = footer[48..82].try_into().unwrap();
    let f_foff = u64::from_le_bytes(footer[82..90].try_into().unwrap());
    if f_toff != state_offset || f_tlen != state_len || f_foff != hdr.footer_off || &footer[90..94] != MAGIC_SHARD {
        return Err(CowrieError::CorruptTrailer);
    }
    if hash_mode == HASH_MODE_PARALLEL_MERKLE_4M && to_hex(&f_merkle) != state_sha {
        return Err(CowrieError::RootMismatch);
    }
    let exp_mroot = shard_merkle_root_from_leaves(&[], &[tensor_merkle_leaf(state_len, &f_merkle)]);
    if f_mroot != exp_mroot {
        return Err(CowrieError::RootMismatch);
    }

    let rt_slice = &hdr_snap[288..304];
    let rt_end = rt_slice.iter().position(|&b| b == 0).unwrap_or(16);
    if rt_slice[rt_end..].iter().any(|&b| b != 0)
        || rt_slice[..rt_end].iter().any(|&b| b < 0x20 || b >= 0x7F)
    {
        return Err(CowrieError::InvalidContract);
    }
    let runtime_id = std::str::from_utf8(&rt_slice[..rt_end]).unwrap().to_string();
    let layout_ver = u16::from_le_bytes(hdr_snap[304..306].try_into().unwrap());

    Ok(StatePageMeta {
        model_sha,
        doc_cid,
        state_sha,
        runtime_id,
        layout_ver,
        hmac_present,
        ctx_tokens,
        kv_quant,
        ssm_dtype,
        hash_mode,
        state_offset,
        state_len,
    })
}

// Raw Unix libc bindings for zero-copy mmap + POSIX_FADV_DONTNEED (Linux) + geteuid + O_NOFOLLOW
#[cfg(unix)]
mod sys {
    extern "C" {
        pub fn mmap(
            addr: *mut std::ffi::c_void,
            length: usize,
            prot: i32,
            flags: i32,
            fd: i32,
            offset: i64,
        ) -> *mut std::ffi::c_void;
        pub fn munmap(addr: *mut std::ffi::c_void, length: usize) -> i32;
        #[cfg(target_os = "linux")]
        pub fn posix_fadvise(fd: i32, offset: i64, len: i64, advice: i32) -> i32;
        pub fn geteuid() -> u32;
    }
    pub const PROT_READ: i32 = 1;
    pub const MAP_PRIVATE: i32 = 2;
    pub const MAP_FAILED: *mut std::ffi::c_void = !0usize as *mut std::ffi::c_void;
    #[cfg(target_os = "linux")]
    pub const POSIX_FADV_DONTNEED: i32 = 4;
    #[cfg(all(target_os = "linux", any(target_arch = "aarch64", target_arch = "arm")))]
    pub const O_NOFOLLOW: i32 = 0o100000;
    #[cfg(all(target_os = "linux", not(any(target_arch = "aarch64", target_arch = "arm"))))]
    pub const O_NOFOLLOW: i32 = 0o400000;
    #[cfg(target_os = "macos")]
    pub const O_NOFOLLOW: i32 = 0x0100;
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub const O_NOFOLLOW: i32 = 0;
}

/// Verify an in-memory `StatePage` byte slice (`CWSP` + optional HMAC + 4 MiB Merkle payload).
/// Works identically on native platforms and `wasm32` (browser / WebGPU / Node.js).
pub fn verify_statepage_bytes(
    buf: &[u8],
    explicit_hmac_key: Option<&[u8]>,
    verify_hash: bool,
) -> Result<StatePageMeta, CowrieError> {
    let meta = parse_statepage_meta_with_hmac(buf, explicit_hmac_key)?;
    if verify_hash {
        let so = u64_to_usize(meta.state_offset)?;
        let sl = u64_to_usize(meta.state_len)?;
        let end = so.checked_add(sl).ok_or(CowrieError::Truncated)?;
        let st = &buf[so..end];
        let got_sha = if meta.hash_mode == HASH_MODE_PARALLEL_MERKLE_4M {
            to_hex(&sha256_parallel_merkle(st))
        } else {
            // [FORMAL-F13] Even in flat SHA-256 mode, verify Plane D footer `f_merkle` matches payload.
            let pad_end = checked_align_16kb(end).map_err(|_| CowrieError::Truncated)?;
            let f_merkle: &[u8; 32] = buf[pad_end + 16..pad_end + 48].try_into().unwrap();
            if sha256_parallel_merkle(st) != *f_merkle {
                return Err(CowrieError::StatePageHashMismatch);
            }
            to_hex(&sha256(st))
        };
        if got_sha != meta.state_sha {
            return Err(CowrieError::StatePageHashMismatch);
        }
    }
    Ok(meta)
}

/// Zero-copy `mmap`-backed verified StatePage handle.
#[cfg(unix)]
pub struct MappedStatePage {
    ptr: *mut u8,
    map_len: usize,
    pub meta: StatePageMeta,
}

#[cfg(unix)]
unsafe impl Send for MappedStatePage {}
#[cfg(unix)]
unsafe impl Sync for MappedStatePage {}

#[cfg(unix)]
impl MappedStatePage {
    #[cfg(unix)]
    pub fn open_verified(
        path: &Path,
        expected_model_sha: Option<&str>,
        verify_hash: bool,
    ) -> Result<Self, CowrieError> {
        Self::open_verified_full(path, expected_model_sha, None, None, None, None, verify_hash)
    }

    #[cfg(unix)]
    pub fn open_verified_checked(
        path: &Path,
        expected_model_sha: Option<&str>,
        expected_kv_quant: Option<u8>,
        verify_hash: bool,
    ) -> Result<Self, CowrieError> {
        Self::open_verified_full(
            path,
            expected_model_sha,
            None,
            expected_kv_quant,
            None,
            None,
            verify_hash,
        )
    }

    /// Full security-hardened `StatePage` opener (delegates to `open_verified_strict` without `doc_cid` pin).
    #[cfg(unix)]
    pub fn open_verified_full(
        path: &Path,
        expected_model_sha: Option<&str>,
        expected_runtime_id: Option<&str>,
        expected_kv_quant: Option<u8>,
        expected_ssm_dtype: Option<u8>,
        explicit_hmac_key: Option<&[u8]>,
        verify_hash: bool,
    ) -> Result<Self, CowrieError> {
        Self::open_verified_strict(
            path,
            expected_model_sha,
            None,
            expected_runtime_id,
            expected_kv_quant,
            expected_ssm_dtype,
            explicit_hmac_key,
            verify_hash,
        )
    }

    /// Strict security-hardened `StatePage` opener:
    /// 1. Opens with `O_NOFOLLOW` and validates `fstat(fd)`: regular file, owned by `geteuid()` or `root`, mode `& 0o022 == 0`.
    /// 2. Snapshots Page-0 header onto stack, verifies HMAC-SHA256 first, validates 16 KB alignment, `0x00` padding, and Plane D footer.
    /// 3. Enforces `model_sha`, `doc_cid` (prevents cross-tenant KV swap), `runtime_id`, `kv_quant`, `ssm_dtype`, and parallel 4 MiB Merkle integrity.
    #[cfg(unix)]
    pub fn open_verified_strict(
        path: &Path,
        expected_model_sha: Option<&str>,
        expected_doc_cid: Option<&str>,
        expected_runtime_id: Option<&str>,
        expected_kv_quant: Option<u8>,
        expected_ssm_dtype: Option<u8>,
        explicit_hmac_key: Option<&[u8]>,
        verify_hash: bool,
    ) -> Result<Self, CowrieError> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(sys::O_NOFOLLOW)
            .open(path)
            .map_err(|_| CowrieError::IoError)?;
        let fmeta = file.metadata().map_err(|_| CowrieError::IoError)?;
        if !fmeta.file_type().is_file() {
            return Err(CowrieError::InvalidContract);
        }
        let euid = unsafe { sys::geteuid() };
        if fmeta.uid() != euid && fmeta.uid() != 0 {
            return Err(CowrieError::InvalidContract);
        }
        if (fmeta.mode() & 0o022) != 0 {
            return Err(CowrieError::InvalidContract);
        }

        let len = u64_to_usize(fmeta.len())?;
        if len < PAGE_ALIGN + STATEPAGE_FOOTER_BYTES {
            return Err(CowrieError::Truncated);
        }
        let raw = unsafe {
            sys::mmap(
                std::ptr::null_mut(),
                len,
                sys::PROT_READ,
                sys::MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        if raw == sys::MAP_FAILED {
            return Err(CowrieError::IoError);
        }
        let ptr = raw as *mut u8;
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };

        let meta = match parse_statepage_meta_with_hmac(slice, explicit_hmac_key) {
            Ok(m) => m,
            Err(e) => {
                unsafe { sys::munmap(raw, len) };
                return Err(e);
            }
        };

        if let Some(exp_model) = expected_model_sha {
            if !exp_model.is_empty() && meta.model_sha != exp_model {
                unsafe { sys::munmap(raw, len) };
                return Err(CowrieError::ModelMismatch);
            }
        }

        if let Some(exp_doc) = expected_doc_cid {
            if !exp_doc.is_empty() && meta.doc_cid != exp_doc {
                unsafe { sys::munmap(raw, len) };
                return Err(CowrieError::InvalidContract);
            }
        }

        if let Some(exp_rt) = expected_runtime_id {
            if !exp_rt.is_empty() && meta.runtime_id != exp_rt {
                unsafe { sys::munmap(raw, len) };
                return Err(CowrieError::RuntimeMismatch);
            }
        }

        if let Some(exp_kvq) = expected_kv_quant {
            if meta.kv_quant != exp_kvq {
                unsafe { sys::munmap(raw, len) };
                return Err(CowrieError::KvQuantMismatch);
            }
        }

        if let Some(exp_ssm) = expected_ssm_dtype {
            if meta.ssm_dtype != exp_ssm {
                unsafe { sys::munmap(raw, len) };
                return Err(CowrieError::SsmDtypeMismatch);
            }
        }

        if verify_hash {
            let so = u64_to_usize(meta.state_offset).map_err(|e| {
                unsafe { sys::munmap(raw, len) };
                e
            })?;
            let sl = u64_to_usize(meta.state_len).map_err(|e| {
                unsafe { sys::munmap(raw, len) };
                e
            })?;
            let st = &slice[so..so + sl];
            let got_sha = if meta.hash_mode == HASH_MODE_PARALLEL_MERKLE_4M {
                to_hex(&sha256_parallel_merkle(st))
            } else {
                let pad_end = checked_align_16kb(so + sl).map_err(|e| {
                    unsafe { sys::munmap(raw, len) };
                    e
                })?;
                let f_merkle: &[u8; 32] = slice[pad_end + 16..pad_end + 48].try_into().unwrap();
                if sha256_parallel_merkle(st) != *f_merkle {
                    unsafe { sys::munmap(raw, len) };
                    return Err(CowrieError::StatePageHashMismatch);
                }
                to_hex(&sha256(st))
            };
            if got_sha != meta.state_sha {
                unsafe { sys::munmap(raw, len) };
                return Err(CowrieError::StatePageHashMismatch);
            }
        }

        Ok(Self {
            ptr,
            map_len: len,
            meta,
        })
    }

    /// Zero-copy 16 KB-aligned slice of live hybrid SSM+KV state bytes.
    #[inline]
    pub fn state_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.ptr.add(self.meta.state_offset as usize),
                self.meta.state_len as usize,
            )
        }
    }

    #[inline]
    pub fn state_ptr(&self) -> *const u8 {
        unsafe { self.ptr.add(self.meta.state_offset as usize) }
    }
}

#[cfg(unix)]
impl Drop for MappedStatePage {
    fn drop(&mut self) {
        unsafe {
            sys::munmap(self.ptr as *mut std::ffi::c_void, self.map_len);
        }
    }
}

/// Evict a file from the Linux OS page cache (`POSIX_FADV_DONTNEED`) for cold NVMe benchmarking.
/// No-op on non-Linux Unix targets (e.g., macOS Apple Silicon).
#[cfg(unix)]
pub fn evict_page_cache(path: &Path) -> Result<(), CowrieError> {
    let f = File::open(path).map_err(|_| CowrieError::IoError)?;
    #[cfg(target_os = "linux")]
    unsafe {
        sys::posix_fadvise(f.as_raw_fd(), 0, 0, sys::POSIX_FADV_DONTNEED);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = f;
    Ok(())
}
