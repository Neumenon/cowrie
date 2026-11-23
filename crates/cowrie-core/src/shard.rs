//! 16 KB-aligned Shard Container (`CWRF`), RFC 6962 Merkle verification/localization,
//! atomic 16 KB `StatePage` save & `mmap` zero-copy verified restore.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use crate::sha256::{
    merkle_leaf, merkle_root_rfc6962, sha256, sha256_parallel_merkle, sha256_parallel_merkle_hex,
    to_hex,
};
use crate::CowrieError;

pub const MAGIC_SHARD: &[u8; 4] = b"CWRF";
pub const SHARD_VERSION: u8 = 1;
pub const ALIGN_LOG2: u8 = 14;
pub const PAGE_ALIGN: usize = 16384; // 2^14 = 16 KB

pub const FLAG_STATEPAGE: u16 = 0x0001;
pub const FLAG_IVF_COLOCATED: u16 = 0x0002;

#[inline]
pub fn align_16kb(n: usize) -> usize {
    (n + PAGE_ALIGN - 1) & !(PAGE_ALIGN - 1)
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

/// Frame directory entry stored in Plane D.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameEntry {
    pub offset: u64,
    pub len: u32,
    pub leaf_sha256: [u8; 32],
}

/// Tensor directory entry stored in Plane D (all offsets strictly `% 16384 == 0`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TensorEntry {
    pub offset: u64,
    pub len: u64,
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
}

/// Build a sealed, strictly 16 KB-aligned `CWRF` shard from canonical Plane A frames and Plane C tensors.
pub fn build_shard_16kb(frames: &[&[u8]], tensors: &[&[u8]], flags: u16) -> Vec<u8> {
    let mut plane_a = Vec::new();
    let mut frame_meta: Vec<(u64, u32, [u8; 32])> = Vec::with_capacity(frames.len());
    let mut cur: usize = 64;
    for f in frames {
        let leaf = merkle_leaf(f);
        frame_meta.push((cur as u64, f.len() as u32, leaf));
        plane_a.extend_from_slice(f);
        cur += f.len();
    }

    let pad_a = (PAGE_ALIGN - (cur % PAGE_ALIGN)) % PAGE_ALIGN;
    let plane_c_off = cur + pad_a;

    let mut plane_c = Vec::new();
    let mut tensor_meta: Vec<(u64, u64)> = Vec::with_capacity(tensors.len());
    let mut t_cur = plane_c_off;
    for tb in tensors {
        debug_assert_eq!(t_cur % PAGE_ALIGN, 0);
        tensor_meta.push((t_cur as u64, tb.len() as u64));
        plane_c.extend_from_slice(tb);
        t_cur += tb.len();
        let tpad = (PAGE_ALIGN - (t_cur % PAGE_ALIGN)) % PAGE_ALIGN;
        if tpad > 0 {
            plane_c.resize(plane_c.len() + tpad, 0u8);
            t_cur += tpad;
        }
    }

    let footer_off = t_cur;
    let mroot = merkle_root_rfc6962(frames);

    let mut footer = Vec::with_capacity(frames.len() * 44 + tensors.len() * 16 + 46);
    for (off, flen, leaf) in &frame_meta {
        footer.extend_from_slice(&off.to_le_bytes());
        footer.extend_from_slice(&flen.to_le_bytes());
        footer.extend_from_slice(leaf);
    }
    for (toff, tlen) in &tensor_meta {
        footer.extend_from_slice(&toff.to_le_bytes());
        footer.extend_from_slice(&tlen.to_le_bytes());
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
    if (plane_c_off as usize) % PAGE_ALIGN != 0 {
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

/// Strictly verify 16 KB alignment, `0x00` padding, EOF trailer, and RFC 6962 Merkle root,
/// localizing exact corrupt frame indices if any bit flipped.
pub fn verify_shard_and_localize(shard: &[u8]) -> Result<ShardReport, CowrieError> {
    let header = parse_shard_header(shard)?;
    let pa_end = (header.plane_a_off as usize)
        .checked_add(header.plane_a_len as usize)
        .ok_or(CowrieError::Truncated)?;
    let pc_off = header.plane_c_off as usize;
    if pa_end > pc_off || pc_off > shard.len() {
        return Err(CowrieError::Truncated);
    }
    if shard[pa_end..pc_off].iter().any(|&b| b != 0) {
        return Err(CowrieError::NonZeroPad);
    }

    let mut pos = header.footer_off as usize;
    let need_footer = (header.frame_count as usize)
        .checked_mul(44)
        .and_then(|x| x.checked_add((header.tensor_count as usize).checked_mul(16)?))
        .and_then(|x| x.checked_add(34 + 12))
        .ok_or(CowrieError::Truncated)?;
    if pos.checked_add(need_footer).map_or(true, |end| end != shard.len()) {
        return Err(CowrieError::Truncated);
    }

    let mut frames_entries = Vec::with_capacity(header.frame_count as usize);
    let mut frame_slices: Vec<&[u8]> = Vec::with_capacity(header.frame_count as usize);
    let mut bad_frames = Vec::new();

    for idx in 0..(header.frame_count as usize) {
        let off = u64::from_le_bytes(shard[pos..pos + 8].try_into().unwrap());
        let flen = u32::from_le_bytes(shard[pos + 8..pos + 12].try_into().unwrap());
        let mut expected_leaf = [0u8; 32];
        expected_leaf.copy_from_slice(&shard[pos + 12..pos + 44]);
        pos += 44;

        let end = (off as usize)
            .checked_add(flen as usize)
            .ok_or(CowrieError::Truncated)?;
        if end > shard.len() {
            return Err(CowrieError::Truncated);
        }
        let fb = &shard[off as usize..end];
        let actual_leaf = merkle_leaf(fb);
        if actual_leaf != expected_leaf {
            bad_frames.push(idx);
        }
        frames_entries.push(FrameEntry {
            offset: off,
            len: flen,
            leaf_sha256: expected_leaf,
        });
        frame_slices.push(fb);
    }

    let mut tensor_entries = Vec::with_capacity(header.tensor_count as usize);
    for _ in 0..(header.tensor_count as usize) {
        let toff = u64::from_le_bytes(shard[pos..pos + 8].try_into().unwrap());
        let tlen = u64::from_le_bytes(shard[pos + 8..pos + 16].try_into().unwrap());
        pos += 16;
        if (toff as usize) % PAGE_ALIGN != 0 {
            return Err(CowrieError::UnalignedArena);
        }
        let t_end = (toff as usize)
            .checked_add(tlen as usize)
            .ok_or(CowrieError::Truncated)?;
        let t_pad_end = align_16kb(t_end);
        if t_pad_end > shard.len() {
            return Err(CowrieError::Truncated);
        }
        if shard[t_end..t_pad_end].iter().any(|&b| b != 0) {
            return Err(CowrieError::NonZeroPad);
        }
        tensor_entries.push(TensorEntry {
            offset: toff,
            len: tlen,
        });
    }

    let mut stored_root = [0u8; 34];
    stored_root.copy_from_slice(&shard[pos..pos + 34]);
    let computed_root = merkle_root_rfc6962(&frame_slices);
    let ok = (stored_root == computed_root) && bad_frames.is_empty();

    Ok(ShardReport {
        ok,
        header,
        frames: frames_entries,
        tensors: tensor_entries,
        stored_root,
        computed_root,
        bad_frames,
    })
}

// =========================================================================
// 16 KB-ALIGNED HYBRID SSM+KV STATEPAGE SHARDS + ZERO-COPY MMAP
// =========================================================================

pub const STATEPAGE_META_MAGIC: &[u8; 4] = b"CWSP";
pub const HASH_MODE_FLAT_SHA256: u8 = 0;
pub const HASH_MODE_PARALLEL_MERKLE_4M: u8 = 1;

/// Metadata stored in the first 16 KB page of a Cowrie `StatePage` shard (`flags = FLAG_STATEPAGE`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatePageMeta {
    pub model_sha: String,     // 64 lowercase hex chars
    pub doc_cid: String,       // 64 lowercase hex chars
    pub state_sha: String,     // 64 lowercase hex chars (flat SHA-256 or 4 MiB Merkle root per hash_mode)
    pub ctx_tokens: u32,
    pub kv_quant: u8,          // 0 = fp16, 1 = q4_0, 2 = q8_0
    pub ssm_dtype: u8,         // 0 = fp16, 1 = bf16, 2 = fp32
    pub hash_mode: u8,         // 0 = flat SHA-256, 1 = parallel 4 MiB chunked RFC 6962 Merkle SHA-256
    pub state_offset: u64,     // strictly % 16384 == 0
    pub state_len: u64,
}

/// Atomically save a hybrid SSM+KV `StatePage` into a strictly 16 KB-aligned `CWRF` shard on NVMe
/// (write to `.tmp` -> `fsync` -> atomic `rename`) using parallel 4 MiB SHA-NI Merkle hashing.
pub fn save_statepage_atomic(
    path: &Path,
    model_sha: &str,
    doc_cid: &str,
    ctx_tokens: u32,
    kv_quant: u8,
    ssm_dtype: u8,
    state_bytes: &[u8],
) -> Result<StatePageMeta, CowrieError> {
    if model_sha.len() != 64 || doc_cid.len() != 64 {
        return Err(CowrieError::InvalidContract);
    }
    let hash_mode = HASH_MODE_PARALLEL_MERKLE_4M;
    let state_sha = sha256_parallel_merkle_hex(state_bytes);
    let state_len = state_bytes.len() as u64;
    let state_offset: u64 = PAGE_ALIGN as u64;
    let padded_state_len = align_16kb(state_bytes.len());
    let footer_off = state_offset + (padded_state_len as u64);

    // 256-byte StatePage descriptor inside Page 0 (bytes 64..320), rest strictly 0x00 to 16,384.
    let mut page0 = vec![0u8; PAGE_ALIGN];
    // 64B CWRF shard header
    page0[0..4].copy_from_slice(MAGIC_SHARD);
    page0[4] = SHARD_VERSION;
    page0[5] = ALIGN_LOG2;
    page0[6..8].copy_from_slice(&FLAG_STATEPAGE.to_le_bytes());
    page0[8..12].copy_from_slice(&0u32.to_le_bytes()); // frame_count = 0
    page0[12..16].copy_from_slice(&1u32.to_le_bytes()); // tensor_count = 1 (the state slab)
    page0[16..24].copy_from_slice(&64u64.to_le_bytes());
    page0[24..32].copy_from_slice(&256u64.to_le_bytes());
    page0[32..40].copy_from_slice(&state_offset.to_le_bytes());
    page0[40..48].copy_from_slice(&(padded_state_len as u64).to_le_bytes());
    page0[48..56].copy_from_slice(&footer_off.to_le_bytes());
    page0[56..64].copy_from_slice(&0u64.to_le_bytes());

    // StatePage metadata block at [64..320)
    page0[64..68].copy_from_slice(STATEPAGE_META_MAGIC);
    page0[68..72].copy_from_slice(&ctx_tokens.to_le_bytes());
    page0[72] = kv_quant;
    page0[73] = ssm_dtype;
    page0[74] = hash_mode;
    page0[75..80].copy_from_slice(&[0u8; 5]);
    page0[80..88].copy_from_slice(&state_offset.to_le_bytes());
    page0[88..96].copy_from_slice(&state_len.to_le_bytes());
    page0[96..160].copy_from_slice(model_sha.as_bytes());
    page0[160..224].copy_from_slice(doc_cid.as_bytes());
    page0[224..288].copy_from_slice(state_sha.as_bytes());
    // bytes 288..16384 strictly zero

    // Footer: 1 tensor entry (16B) + merkle_root(34B) + footer_off(8B) + "CWRF"(4B) = 62B
    let mut footer = Vec::with_capacity(62);
    footer.extend_from_slice(&state_offset.to_le_bytes());
    footer.extend_from_slice(&state_len.to_le_bytes());
    footer.extend_from_slice(&merkle_root_rfc6962(&[]));
    footer.extend_from_slice(&footer_off.to_le_bytes());
    footer.extend_from_slice(MAGIC_SHARD);

    let tmp_path = path.with_extension("cowrie.tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
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
    }
    fs::rename(&tmp_path, path).map_err(|_| CowrieError::IoError)?;

    Ok(StatePageMeta {
        model_sha: model_sha.to_string(),
        doc_cid: doc_cid.to_string(),
        state_sha,
        ctx_tokens,
        kv_quant,
        ssm_dtype,
        hash_mode,
        state_offset,
        state_len,
    })
}

pub fn parse_statepage_meta(buf: &[u8]) -> Result<StatePageMeta, CowrieError> {
    let hdr = parse_shard_header(buf)?;
    if (hdr.flags & FLAG_STATEPAGE) == 0 {
        return Err(CowrieError::InvalidContract);
    }
    if buf.len() < PAGE_ALIGN + 62 {
        return Err(CowrieError::Truncated);
    }
    if &buf[64..68] != STATEPAGE_META_MAGIC {
        return Err(CowrieError::BadMagic);
    }
    let ctx_tokens = u32::from_le_bytes(buf[68..72].try_into().unwrap());
    let kv_quant = buf[72];
    let ssm_dtype = buf[73];
    let hash_mode = buf[74];
    if hash_mode > HASH_MODE_PARALLEL_MERKLE_4M || buf[75..80].iter().any(|&b| b != 0) {
        return Err(CowrieError::NonCanonicalFlags);
    }
    let state_offset = u64::from_le_bytes(buf[80..88].try_into().unwrap());
    let state_len = u64::from_le_bytes(buf[88..96].try_into().unwrap());
    if (state_offset as usize) % PAGE_ALIGN != 0 || state_offset != hdr.plane_c_off {
        return Err(CowrieError::UnalignedArena);
    }
    if buf[288..PAGE_ALIGN].iter().any(|&b| b != 0) {
        return Err(CowrieError::NonZeroPad);
    }
    let end = (state_offset as usize)
        .checked_add(state_len as usize)
        .ok_or(CowrieError::Truncated)?;
    let pad_end = align_16kb(end);
    if pad_end + 62 != buf.len() {
        return Err(CowrieError::Truncated);
    }
    if buf[end..pad_end].iter().any(|&b| b != 0) {
        return Err(CowrieError::NonZeroPad);
    }
    let model_sha = std::str::from_utf8(&buf[96..160])
        .map_err(|_| CowrieError::InvalidUtf8)?
        .to_string();
    let doc_cid = std::str::from_utf8(&buf[160..224])
        .map_err(|_| CowrieError::InvalidUtf8)?
        .to_string();
    let state_sha = std::str::from_utf8(&buf[224..288])
        .map_err(|_| CowrieError::InvalidUtf8)?
        .to_string();

    Ok(StatePageMeta {
        model_sha,
        doc_cid,
        state_sha,
        ctx_tokens,
        kv_quant,
        ssm_dtype,
        hash_mode,
        state_offset,
        state_len,
    })
}

// Raw Unix libc bindings for zero-copy mmap + POSIX_FADV_DONTNEED (zero external crate deps!)
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
        pub fn posix_fadvise(fd: i32, offset: i64, len: i64, advice: i32) -> i32;
    }
    pub const PROT_READ: i32 = 1;
    pub const MAP_PRIVATE: i32 = 2;
    pub const MAP_FAILED: *mut std::ffi::c_void = !0usize as *mut std::ffi::c_void;
    pub const POSIX_FADV_DONTNEED: i32 = 4;
}

/// Zero-copy `mmap`-backed verified StatePage handle.
pub struct MappedStatePage {
    ptr: *mut u8,
    map_len: usize,
    pub meta: StatePageMeta,
}

unsafe impl Send for MappedStatePage {}
unsafe impl Sync for MappedStatePage {}

impl MappedStatePage {
    /// Open, `mmap`, strictly validate alignment + `model_sha` contract, and (when `verify_hash`)
    /// check full SHA-256 / parallel 4 MiB SHA-NI Merkle integrity before exposing state slice.
    #[cfg(unix)]
    pub fn open_verified(
        path: &Path,
        expected_model_sha: Option<&str>,
        verify_hash: bool,
    ) -> Result<Self, CowrieError> {
        Self::open_verified_checked(path, expected_model_sha, None, verify_hash)
    }

    /// Open, `mmap`, and strictly enforce alignment, `model_sha`, optional `kv_quant`, and full integrity.
    #[cfg(unix)]
    pub fn open_verified_checked(
        path: &Path,
        expected_model_sha: Option<&str>,
        expected_kv_quant: Option<u8>,
        verify_hash: bool,
    ) -> Result<Self, CowrieError> {
        let file = File::open(path).map_err(|_| CowrieError::IoError)?;
        let len = file.metadata().map_err(|_| CowrieError::IoError)?.len() as usize;
        if len < PAGE_ALIGN + 62 {
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

        let meta = match parse_statepage_meta(slice) {
            Ok(m) => m,
            Err(e) => {
                unsafe { sys::munmap(raw, len) };
                return Err(e);
            }
        };

        if let Some(exp_model) = expected_model_sha {
            if meta.model_sha != exp_model {
                unsafe { sys::munmap(raw, len) };
                return Err(CowrieError::ModelMismatch);
            }
        }

        if let Some(exp_kvq) = expected_kv_quant {
            if meta.kv_quant != exp_kvq {
                unsafe { sys::munmap(raw, len) };
                return Err(CowrieError::KvQuantMismatch);
            }
        }

        if verify_hash {
            let st = &slice[meta.state_offset as usize..(meta.state_offset + meta.state_len) as usize];
            let got_sha = if meta.hash_mode == HASH_MODE_PARALLEL_MERKLE_4M {
                to_hex(&sha256_parallel_merkle(st))
            } else {
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

impl Drop for MappedStatePage {
    fn drop(&mut self) {
        #[cfg(unix)]
        unsafe {
            sys::munmap(self.ptr as *mut std::ffi::c_void, self.map_len);
        }
    }
}

/// Evict a file from the Linux OS page cache (`POSIX_FADV_DONTNEED`) for cold NVMe benchmarking.
#[cfg(unix)]
pub fn evict_page_cache(path: &Path) -> Result<(), CowrieError> {
    let f = File::open(path).map_err(|_| CowrieError::IoError)?;
    unsafe {
        sys::posix_fadvise(f.as_raw_fd(), 0, 0, sys::POSIX_FADV_DONTNEED);
    }
    Ok(())
}
