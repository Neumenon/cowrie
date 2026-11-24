//! C-ABI FFI surface (`libcowrie_core.so` / `cowrie.h`) for C++, CUDA (`llama.cpp`, `Eureka`),
//! Swift/Metal, and Python (`ctypes`) consumers linking directly against one audited Rust core.
//! Every `extern "C"` entrypoint is guarded by `catch_unwind` so panics never unwind across FFI.

#[cfg(unix)]
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::panic::{catch_unwind, AssertUnwindSafe};
#[cfg(unix)]
use std::path::Path;
use std::ptr;

use crate::canonical::{
    decode_frame_strict, encode_frame, parse_json_to_value, value_to_compact_json,
    verify_frame_zero_alloc,
};
use crate::ed25519::{action_from_code, ed25519_public_key, DeclassificationToken};
#[cfg(unix)]
use crate::ivf::{ColocatedIvfReader, IvfSearchHit};
use crate::lens::{project_r0, project_r1, project_r2};
use crate::sha256::{content_address, to_hex};
use crate::shard::{build_shard_16kb, verify_shard_and_localize, verify_statepage_bytes};
#[cfg(unix)]
use crate::shard::{fingerprint_model_file, save_statepage_atomic_ext, MappedStatePage};
use crate::trust::{is_tainted, propagate_authority};
#[cfg(unix)]
use crate::wal::WalStore;
#[cfg(unix)]
use crate::zonemap::ZonedShardReader;
use crate::CowrieError;

#[inline]
fn ffi_code<F: FnOnce() -> c_int>(f: F) -> c_int {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(rc) => rc,
        Err(_) => CowrieError::Internal.code(),
    }
}

/// Opaque buffer owned by Rust; free with `cowrie_buf_free`.
#[repr(C)]
pub struct CowrieBuf {
    pub data: *mut u8,
    pub len: usize,
    pub cap: usize,
}

impl CowrieBuf {
    fn from_vec(mut v: Vec<u8>) -> Self {
        let b = Self {
            data: v.as_mut_ptr(),
            len: v.len(),
            cap: v.capacity(),
        };
        std::mem::forget(v);
        b
    }

    fn from_string(s: String) -> Self {
        Self::from_vec(s.into_bytes())
    }

    fn empty() -> Self {
        Self {
            data: ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }
}

/// Allocate `len` zeroed bytes in Rust/WASM linear memory (free with `cowrie_wasm_free`).
#[no_mangle]
pub extern "C" fn cowrie_wasm_alloc(len: usize) -> *mut u8 {
    if len == 0 {
        return ptr::null_mut();
    }
    let mut v = vec![0u8; len];
    let p = v.as_mut_ptr();
    debug_assert_eq!(v.capacity(), len);
    std::mem::forget(v);
    p
}

/// Free a buffer previously allocated by `cowrie_wasm_alloc(len)`.
#[no_mangle]
pub unsafe extern "C" fn cowrie_wasm_free(data: *mut u8, len: usize) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !data.is_null() && len > 0 {
            let _ = Vec::from_raw_parts(data, len, len);
        }
    }));
}

/// Free a buffer returned by any `cowrie_*` FFI call.
#[no_mangle]
pub unsafe extern "C" fn cowrie_buf_free(buf: CowrieBuf) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !buf.data.is_null() && buf.cap > 0 {
            let _ = Vec::from_raw_parts(buf.data, buf.len, buf.cap);
        }
    }));
}

/// Map a `CowrieError` integer status code to its static `ERR_*` C-string name.
#[no_mangle]
pub extern "C" fn cowrie_err_name(code: c_int) -> *const c_char {
    let s: &'static [u8] = match code {
        0 => b"OK\0",
        1 => b"ERR_TRUNCATED\0",
        2 => b"ERR_BAD_MAGIC\0",
        3 => b"ERR_UNSUPPORTED_VER\0",
        4 => b"ERR_NON_CANONICAL_FLAGS\0",
        5 => b"ERR_NON_CANONICAL_VARINT\0",
        6 => b"ERR_NON_CANONICAL_INT\0",
        7 => b"ERR_NON_CANONICAL_UINT\0",
        8 => b"ERR_NON_CANONICAL_FLOAT\0",
        9 => b"ERR_DUPLICATE_KEY\0",
        10 => b"ERR_UNSORTED_DICT\0",
        11 => b"ERR_UNSORTED_MAP_KEYS\0",
        12 => b"ERR_INVALID_UTF8\0",
        13 => b"ERR_BAD_DICT_INDEX\0",
        14 => b"ERR_UNUSED_DICT_KEY\0",
        15 => b"ERR_RESERVED_TAG\0",
        16 => b"ERR_TRAILING_BYTES\0",
        17 => b"ERR_MAX_DEPTH\0",
        18 => b"ERR_ALLOC_LIMIT\0",
        19 => b"ERR_UNALIGNED_ARENA\0",
        20 => b"ERR_NON_ZERO_PAD\0",
        21 => b"ERR_CORRUPT_TRAILER\0",
        22 => b"ERR_ROOT_MISMATCH\0",
        23 => b"ERR_MODEL_MISMATCH\0",
        24 => b"ERR_STATEPAGE_HASH_MISMATCH\0",
        25 => b"ERR_TAINTED_EXECUTION_BLOCKED\0",
        26 => b"ERR_INVALID_JSON\0",
        27 => b"ERR_INVALID_CONTRACT\0",
        28 => b"ERR_IO\0",
        29 => b"ERR_KV_QUANT_MISMATCH\0",
        30 => b"ERR_SSM_DTYPE_MISMATCH\0",
        31 => b"ERR_RUNTIME_MISMATCH\0",
        32 => b"ERR_AUTH_MAC_MISMATCH\0",
        33 => b"ERR_INTERNAL\0",
        _ => b"ERR_UNKNOWN\0",
    };
    s.as_ptr() as *const c_char
}

/// Zero-allocation strict canonical `COWR` frame verifier. Returns 0 (`OK`) or exact `ERR_*` code.
#[no_mangle]
pub unsafe extern "C" fn cowrie_frame_verify(
    data: *const u8,
    len: usize,
    out_auth: *mut u8,
    out_epi: *mut u8,
    out_cid_34: *mut u8,
) -> c_int {
    ffi_code(|| {
        if data.is_null() && len > 0 {
            return CowrieError::Truncated.code();
        }
        let slice = if len == 0 { &[] } else { std::slice::from_raw_parts(data, len) };
        match verify_frame_zero_alloc(slice) {
            Ok((authority, epistemic)) => {
                if !out_auth.is_null() {
                    *out_auth = authority;
                }
                if !out_epi.is_null() {
                    *out_epi = epistemic;
                }
                if !out_cid_34.is_null() {
                    let cid = content_address(slice);
                    ptr::copy_nonoverlapping(cid.as_ptr(), out_cid_34, 34);
                }
                0
            }
            Err(e) => e.code(),
        }
    })
}

/// Encode a UTF-8 JSON string into a canonical `COWR` binary frame.
#[no_mangle]
pub unsafe extern "C" fn cowrie_encode_json(
    json_utf8: *const u8,
    json_len: usize,
    authority: u8,
    epistemic: u8,
    out_buf: *mut CowrieBuf,
) -> c_int {
    ffi_code(|| {
        if out_buf.is_null() || (json_utf8.is_null() && json_len > 0) {
            return CowrieError::InvalidJson.code();
        }
        *out_buf = CowrieBuf::empty();
        let raw = if json_len == 0 { &[] } else { std::slice::from_raw_parts(json_utf8, json_len) };
        let s = match std::str::from_utf8(raw) {
            Ok(s) => s,
            Err(_) => return CowrieError::InvalidUtf8.code(),
        };
        let val = match parse_json_to_value(s) {
            Ok(v) => v,
            Err(e) => return e.code(),
        };
        match encode_frame(&val, authority, epistemic) {
            Ok(wire) => {
                *out_buf = CowrieBuf::from_vec(wire);
                0
            }
            Err(e) => e.code(),
        }
    })
}

/// Decode a strict canonical `COWR` frame into minified JSON UTF-8.
#[no_mangle]
pub unsafe extern "C" fn cowrie_decode_to_json(
    data: *const u8,
    len: usize,
    out_buf: *mut CowrieBuf,
) -> c_int {
    ffi_code(|| {
        if out_buf.is_null() || (data.is_null() && len > 0) {
            return CowrieError::Truncated.code();
        }
        *out_buf = CowrieBuf::empty();
        let slice = if len == 0 { &[] } else { std::slice::from_raw_parts(data, len) };
        match decode_frame_strict(slice) {
            Ok(dec) => {
                *out_buf = CowrieBuf::from_string(value_to_compact_json(&dec.value));
                0
            }
            Err(e) => e.code(),
        }
    })
}

/// Project a canonical `COWR` frame into a deterministic lens (`tier`: 0 = R0, 1 = R1, 2 = R2).
#[no_mangle]
pub unsafe extern "C" fn cowrie_project_lens(
    frame_data: *const u8,
    frame_len: usize,
    tier: u8,
    tensor_count: usize,
    out_buf: *mut CowrieBuf,
) -> c_int {
    ffi_code(|| {
        if out_buf.is_null() || (frame_data.is_null() && frame_len > 0) {
            return CowrieError::Truncated.code();
        }
        *out_buf = CowrieBuf::empty();
        let slice = if frame_len == 0 { &[] } else { std::slice::from_raw_parts(frame_data, frame_len) };
        let dec = match decode_frame_strict(slice) {
            Ok(d) => d,
            Err(e) => return e.code(),
        };
        let rendered = match tier {
            0 => project_r0(slice, &dec, tensor_count),
            1 => project_r1(slice, &dec),
            _ => project_r2(slice, &dec),
        };
        *out_buf = CowrieBuf::from_string(rendered);
        0
    })
}

/// Verify a `CWRF` shard buffer (both Plane A frames and Plane C tensor arenas).
#[no_mangle]
pub unsafe extern "C" fn cowrie_shard_verify(
    shard_data: *const u8,
    shard_len: usize,
    out_root_34: *mut u8,
) -> c_int {
    ffi_code(|| {
        if shard_data.is_null() && shard_len > 0 {
            return CowrieError::Truncated.code();
        }
        let slice = if shard_len == 0 { &[] } else { std::slice::from_raw_parts(shard_data, shard_len) };
        match verify_shard_and_localize(slice) {
            Ok(rep) => {
                if !out_root_34.is_null() {
                    ptr::copy_nonoverlapping(rep.computed_root.as_ptr(), out_root_34, 34);
                }
                if rep.ok { 0 } else { CowrieError::RootMismatch.code() }
            }
            Err(e) => e.code(),
        }
    })
}

/// Compute effective derived authority byte (sticky `BIT_TAINTED = 0x80` causal propagation).
#[no_mangle]
pub unsafe extern "C" fn cowrie_propagate_authority(
    base_auth: u8,
    parent_auths: *const u8,
    num_parents: usize,
) -> u8 {
    match catch_unwind(AssertUnwindSafe(|| {
        let parents = if parent_auths.is_null() || num_parents == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(parent_auths, num_parents)
        };
        propagate_authority(base_auth, parents)
    })) {
        Ok(a) => a,
        Err(_) => base_auth | crate::trust::BIT_TAINTED, // fail closed on panic
    }
}

#[no_mangle]
pub extern "C" fn cowrie_is_tainted(authority: u8) -> u8 {
    if is_tainted(authority) { 1 } else { 0 }
}

#[no_mangle]
pub extern "C" fn cowrie_has_shani() -> u8 {
    if crate::sha256::has_hw_sha256() { 1 } else { 0 }
}

/// Pack a single canonical `COWR` frame into a 16 KB-aligned `CWRF` shard buffer in memory.
#[no_mangle]
pub unsafe extern "C" fn cowrie_shard_pack_single(
    frame_data: *const u8,
    frame_len: usize,
    out_buf: *mut CowrieBuf,
) -> c_int {
    ffi_code(|| {
        if out_buf.is_null() || (frame_data.is_null() && frame_len > 0) {
            return CowrieError::Truncated.code();
        }
        *out_buf = CowrieBuf::empty();
        let fb = if frame_len == 0 { &[] } else { std::slice::from_raw_parts(frame_data, frame_len) };
        let _ = match verify_frame_zero_alloc(fb) {
            Ok(x) => x,
            Err(e) => return e.code(),
        };
        let shard = build_shard_16kb(&[fb], &[], 0);
        *out_buf = CowrieBuf::from_vec(shard);
        0
    })
}

/// Verify an in-memory `StatePage` byte slice (`CWSP` + optional HMAC + 4 MiB Merkle payload).
#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_verify_bytes(
    data: *const u8,
    len: usize,
    hmac_key: *const u8,
    hmac_len: usize,
    verify_hash: u8,
    out_ctx_tokens: *mut u32,
) -> c_int {
    ffi_code(|| {
        if data.is_null() && len > 0 {
            return CowrieError::Truncated.code();
        }
        let slice = if len == 0 { &[] } else { std::slice::from_raw_parts(data, len) };
        let key_opt = if !hmac_key.is_null() && hmac_len > 0 {
            Some(std::slice::from_raw_parts(hmac_key, hmac_len))
        } else {
            None
        };
        match verify_statepage_bytes(slice, key_opt, verify_hash != 0) {
            Ok(meta) => {
                if !out_ctx_tokens.is_null() {
                    *out_ctx_tokens = meta.ctx_tokens;
                }
                0
            }
            Err(e) => e.code(),
        }
    })
}

// =========================================================================
// UNIFIED MODEL FINGERPRINTING + STATEPAGE FFI (UNIX MMAP/FS)
// =========================================================================

#[cfg(unix)]
mod unix_sp_ffi {
use super::*;

pub struct CowrieStatePageHandle {
    inner: MappedStatePage,
    model_sha_c: CString,
    doc_cid_c: CString,
    state_sha_c: CString,
}

/// Unified strided 64-hex model fingerprinter (`file_size + 32 x 64 KiB strided windows`, `< 1 ms`).
#[no_mangle]
pub unsafe extern "C" fn cowrie_model_fingerprint(
    path_cstr: *const c_char,
    out_hex_65: *mut c_char,
) -> c_int {
    ffi_code(|| {
        if path_cstr.is_null() || out_hex_65.is_null() {
            return CowrieError::InvalidContract.code();
        }
        let path = match CStr::from_ptr(path_cstr).to_str() {
            Ok(s) => Path::new(s),
            Err(_) => return CowrieError::InvalidUtf8.code(),
        };
        match fingerprint_model_file(path) {
            Ok(hex) => {
                ptr::copy_nonoverlapping(hex.as_ptr(), out_hex_65 as *mut u8, 64);
                *(out_hex_65.add(64)) = 0;
                0
            }
            Err(e) => e.code(),
        }
    })
}

/// Atomically save live model state bytes into a 16 KB-aligned Cowrie `StatePage` shard.
#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_save(
    path_cstr: *const c_char,
    model_sha_cstr: *const c_char,
    doc_cid_cstr: *const c_char,
    ctx_tokens: u32,
    kv_quant: u8,
    ssm_dtype: u8,
    state_data: *const u8,
    state_len: usize,
) -> c_int {
    cowrie_statepage_save_ext(
        path_cstr,
        model_sha_cstr,
        doc_cid_cstr,
        ptr::null(),
        0,
        ctx_tokens,
        kv_quant,
        ssm_dtype,
        state_data,
        state_len,
    )
}

/// Extended atomic `StatePage` save binding `runtime_id` and `layout_ver`.
#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_save_ext(
    path_cstr: *const c_char,
    model_sha_cstr: *const c_char,
    doc_cid_cstr: *const c_char,
    runtime_id_cstr: *const c_char,
    layout_ver: u16,
    ctx_tokens: u32,
    kv_quant: u8,
    ssm_dtype: u8,
    state_data: *const u8,
    state_len: usize,
) -> c_int {
    ffi_code(|| {
        if path_cstr.is_null() || model_sha_cstr.is_null() || doc_cid_cstr.is_null() || (state_data.is_null() && state_len > 0) {
            return CowrieError::InvalidContract.code();
        }
        let path = match CStr::from_ptr(path_cstr).to_str() {
            Ok(s) => Path::new(s),
            Err(_) => return CowrieError::InvalidUtf8.code(),
        };
        let model_sha = match CStr::from_ptr(model_sha_cstr).to_str() {
            Ok(s) => s,
            Err(_) => return CowrieError::InvalidUtf8.code(),
        };
        let doc_cid = match CStr::from_ptr(doc_cid_cstr).to_str() {
            Ok(s) => s,
            Err(_) => return CowrieError::InvalidUtf8.code(),
        };
        let runtime_id = if runtime_id_cstr.is_null() {
            ""
        } else {
            match CStr::from_ptr(runtime_id_cstr).to_str() {
                Ok(s) => s,
                Err(_) => return CowrieError::InvalidUtf8.code(),
            }
        };
        let slice = if state_len == 0 { &[] } else { std::slice::from_raw_parts(state_data, state_len) };
        match save_statepage_atomic_ext(
            path,
            model_sha,
            doc_cid,
            runtime_id,
            layout_ver,
            ctx_tokens,
            kv_quant,
            ssm_dtype,
            slice,
            None,
        ) {
            Ok(_) => 0,
            Err(e) => e.code(),
        }
    })
}

/// Open, `mmap`, strictly verify 16 KB alignment + `model_sha` contract + optional full hash.
#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_open(
    path_cstr: *const c_char,
    expected_model_sha_cstr: *const c_char,
    verify_hash: u8,
    out_handle: *mut *mut CowrieStatePageHandle,
) -> c_int {
    cowrie_statepage_open_full(
        path_cstr,
        expected_model_sha_cstr,
        ptr::null(),
        -1,
        -1,
        verify_hash,
        out_handle,
    )
}

/// Open StatePage enforcing alignment, `model_sha`, optional `kv_quant` (-1 = any), and integrity hash.
#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_open_checked(
    path_cstr: *const c_char,
    expected_model_sha_cstr: *const c_char,
    expected_kv_quant: c_int,
    verify_hash: u8,
    out_handle: *mut *mut CowrieStatePageHandle,
) -> c_int {
    cowrie_statepage_open_full(
        path_cstr,
        expected_model_sha_cstr,
        ptr::null(),
        expected_kv_quant,
        -1,
        verify_hash,
        out_handle,
    )
}

/// Full StatePage opener enforcing `model_sha`, `runtime_id`, `kv_quant`, `ssm_dtype`, file ownership, and HMAC/Merkle.
#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_open_full(
    path_cstr: *const c_char,
    expected_model_sha_cstr: *const c_char,
    expected_runtime_id_cstr: *const c_char,
    expected_kv_quant: c_int,
    expected_ssm_dtype: c_int,
    verify_hash: u8,
    out_handle: *mut *mut CowrieStatePageHandle,
) -> c_int {
    ffi_code(|| {
        if path_cstr.is_null() || out_handle.is_null() {
            return CowrieError::InvalidContract.code();
        }
        *out_handle = ptr::null_mut();
        let path = match CStr::from_ptr(path_cstr).to_str() {
            Ok(s) => Path::new(s),
            Err(_) => return CowrieError::InvalidUtf8.code(),
        };
        let exp_model = if expected_model_sha_cstr.is_null() {
            None
        } else {
            match CStr::from_ptr(expected_model_sha_cstr).to_str() {
                Ok(s) if !s.is_empty() => Some(s),
                Ok(_) => None,
                Err(_) => return CowrieError::InvalidUtf8.code(),
            }
        };
        let exp_rt = if expected_runtime_id_cstr.is_null() {
            None
        } else {
            match CStr::from_ptr(expected_runtime_id_cstr).to_str() {
                Ok(s) if !s.is_empty() => Some(s),
                Ok(_) => None,
                Err(_) => return CowrieError::InvalidUtf8.code(),
            }
        };
        let exp_kvq = if expected_kv_quant < 0 { None } else { Some(expected_kv_quant as u8) };
        let exp_ssm = if expected_ssm_dtype < 0 { None } else { Some(expected_ssm_dtype as u8) };

        match MappedStatePage::open_verified_full(
            path,
            exp_model,
            exp_rt,
            exp_kvq,
            exp_ssm,
            None,
            verify_hash != 0,
        ) {
            Ok(mapped) => {
                // Guaranteed NUL-free by `is_lower_hex_64` in `parse_statepage_meta_with_hmac`,
                // but still handled without `.unwrap()` for zero-panic FFI discipline.
                let Ok(model_sha_c) = CString::new(mapped.meta.model_sha.as_str()) else {
                    return CowrieError::InvalidContract.code();
                };
                let Ok(doc_cid_c) = CString::new(mapped.meta.doc_cid.as_str()) else {
                    return CowrieError::InvalidContract.code();
                };
                let Ok(state_sha_c) = CString::new(mapped.meta.state_sha.as_str()) else {
                    return CowrieError::InvalidContract.code();
                };
                let h = Box::new(CowrieStatePageHandle {
                    inner: mapped,
                    model_sha_c,
                    doc_cid_c,
                    state_sha_c,
                });
                *out_handle = Box::into_raw(h);
                0
            }
            Err(e) => e.code(),
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_data(handle: *const CowrieStatePageHandle) -> *const u8 {
    if handle.is_null() { return ptr::null(); }
    (*handle).inner.state_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_len(handle: *const CowrieStatePageHandle) -> usize {
    if handle.is_null() { return 0; }
    (*handle).inner.meta.state_len as usize
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_ctx_tokens(handle: *const CowrieStatePageHandle) -> u32 {
    if handle.is_null() { return 0; }
    (*handle).inner.meta.ctx_tokens
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_kv_quant(handle: *const CowrieStatePageHandle) -> u8 {
    if handle.is_null() { return 0; }
    (*handle).inner.meta.kv_quant
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_ssm_dtype(handle: *const CowrieStatePageHandle) -> u8 {
    if handle.is_null() { return 0; }
    (*handle).inner.meta.ssm_dtype
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_hmac_verified(handle: *const CowrieStatePageHandle) -> u8 {
    if handle.is_null() { return 0; }
    if (*handle).inner.meta.hmac_present { 1 } else { 0 }
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_hash_mode(handle: *const CowrieStatePageHandle) -> u8 {
    if handle.is_null() { return 0; }
    (*handle).inner.meta.hash_mode
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_model_sha(handle: *const CowrieStatePageHandle) -> *const c_char {
    if handle.is_null() { return ptr::null(); }
    (*handle).model_sha_c.as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_doc_cid(handle: *const CowrieStatePageHandle) -> *const c_char {
    if handle.is_null() { return ptr::null(); }
    (*handle).doc_cid_c.as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_state_sha(handle: *const CowrieStatePageHandle) -> *const c_char {
    if handle.is_null() { return ptr::null(); }
    (*handle).state_sha_c.as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_close(handle: *mut CowrieStatePageHandle) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !handle.is_null() {
            let _ = Box::from_raw(handle);
        }
    }));
}

} // mod unix_sp_ffi
#[cfg(unix)]
pub use unix_sp_ffi::*;

/// Helper: SHA-256 hex of arbitrary bytes (64 hex chars + NUL written into `out_hex_65`).
#[no_mangle]
pub unsafe extern "C" fn cowrie_sha256_hex(data: *const u8, len: usize, out_hex_65: *mut c_char) -> c_int {
    ffi_code(|| {
        if out_hex_65.is_null() || (data.is_null() && len > 0) {
            return CowrieError::Truncated.code();
        }
        let slice = if len == 0 { &[] } else { std::slice::from_raw_parts(data, len) };
        let hex = to_hex(&crate::sha256::sha256(slice));
        ptr::copy_nonoverlapping(hex.as_ptr(), out_hex_65 as *mut u8, 64);
        *(out_hex_65.add(64)) = 0;
        0
    })
}

// =========================================================================
// CO-LOCATED 16 KB IVF-INT8 (`CWIV`) C-ABI
// =========================================================================

#[cfg(unix)]
mod unix_ivf_ffi {
use super::*;

pub struct CowrieIvfHandle {
    inner: ColocatedIvfReader,
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_ivf_open(
    path_cstr: *const c_char,
    out_handle: *mut *mut CowrieIvfHandle,
) -> c_int {
    ffi_code(|| {
        if path_cstr.is_null() || out_handle.is_null() {
            return CowrieError::InvalidContract.code();
        }
        *out_handle = ptr::null_mut();
        let path = match CStr::from_ptr(path_cstr).to_str() {
            Ok(s) => Path::new(s),
            Err(_) => return CowrieError::InvalidUtf8.code(),
        };
        match ColocatedIvfReader::open(path) {
            Ok(reader) => {
                *out_handle = Box::into_raw(Box::new(CowrieIvfHandle { inner: reader }));
                0
            }
            Err(e) => e.code(),
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_ivf_search_trusted(
    handle: *const CowrieIvfHandle,
    query_i8: *const i8,
    query_dim: usize,
    nprobe: usize,
    top_k: usize,
    conf_thresh: f32,
    out_hits: *mut IvfSearchHit,
    max_hits: usize,
    out_hit_count: *mut usize,
    out_bytes_read: *mut usize,
) -> c_int {
    ffi_code(|| {
        if handle.is_null() || query_i8.is_null() || out_hit_count.is_null() || (out_hits.is_null() && max_hits > 0) {
            return CowrieError::InvalidContract.code();
        }
        *out_hit_count = 0;
        if !out_bytes_read.is_null() {
            *out_bytes_read = 0;
        }
        let q = std::slice::from_raw_parts(query_i8, query_dim);
        let want = top_k.min(max_hits);
        match (*handle).inner.search_trusted(q, nprobe, want, conf_thresh) {
            Ok((hits, bread)) => {
                let n = hits.len().min(max_hits);
                if n > 0 {
                    ptr::copy_nonoverlapping(hits.as_ptr(), out_hits, n);
                }
                *out_hit_count = n;
                if !out_bytes_read.is_null() {
                    *out_bytes_read = bread;
                }
                0
            }
            Err(e) => e.code(),
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_ivf_close(handle: *mut CowrieIvfHandle) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !handle.is_null() {
            let _ = Box::from_raw(handle);
        }
    }));
}

} // mod unix_ivf_ffi
#[cfg(unix)]
pub use unix_ivf_ffi::*;

// =========================================================================
// ED25519 DECLASSIFICATION, FRAMED WAL, & ZONE-MAP SHARDS C-ABI (v1.2)
// =========================================================================

#[no_mangle]
pub unsafe extern "C" fn cowrie_ed25519_pubkey(
    seed_32: *const u8,
    out_pubkey_32: *mut u8,
) -> c_int {
    ffi_code(|| {
        if seed_32.is_null() || out_pubkey_32.is_null() {
            return CowrieError::InvalidContract.code();
        }
        let seed: &[u8; 32] = std::slice::from_raw_parts(seed_32, 32).try_into().unwrap();
        let pk = ed25519_public_key(seed);
        ptr::copy_nonoverlapping(pk.as_ptr(), out_pubkey_32, 32);
        0
    })
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_declassify_sign(
    seed_32: *const u8,
    target_cid_34: *const u8,
    action_code: u8,
    expiry_unix_secs: u64,
    out_sig_64: *mut u8,
) -> c_int {
    ffi_code(|| {
        if seed_32.is_null() || target_cid_34.is_null() || out_sig_64.is_null() {
            return CowrieError::InvalidContract.code();
        }
        let Some(act) = action_from_code(action_code) else {
            return CowrieError::InvalidContract.code();
        };
        let seed: &[u8; 32] = std::slice::from_raw_parts(seed_32, 32).try_into().unwrap();
        let mut cid = [0u8; 34];
        cid.copy_from_slice(std::slice::from_raw_parts(target_cid_34, 34));
        let tok = DeclassificationToken::sign(seed, cid, act, expiry_unix_secs);
        ptr::copy_nonoverlapping(tok.signature.as_ptr(), out_sig_64, 64);
        0
    })
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_declassify_verify(
    pubkey_32: *const u8,
    target_cid_34: *const u8,
    action_code: u8,
    expiry_unix_secs: u64,
    sig_64: *const u8,
    now_unix_secs: u64,
) -> c_int {
    ffi_code(|| {
        if pubkey_32.is_null() || target_cid_34.is_null() || sig_64.is_null() {
            return CowrieError::InvalidContract.code();
        }
        let Some(act) = action_from_code(action_code) else {
            return CowrieError::InvalidContract.code();
        };
        let pk: &[u8; 32] = std::slice::from_raw_parts(pubkey_32, 32).try_into().unwrap();
        let mut cid = [0u8; 34];
        cid.copy_from_slice(std::slice::from_raw_parts(target_cid_34, 34));
        let mut sig = [0u8; 64];
        sig.copy_from_slice(std::slice::from_raw_parts(sig_64, 64));
        let tok = DeclassificationToken {
            target_cid: cid,
            action: act,
            expiry_unix_secs,
            signature: sig,
        };
        match tok.verify(pk, act, now_unix_secs) {
            Ok(()) => 0,
            Err(e) => e.code(),
        }
    })
}

#[cfg(unix)]
mod unix_wal_zm_ffi {
use super::*;

pub struct CowrieWalHandle {
    inner: WalStore,
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_wal_open(
    path_cstr: *const c_char,
    recover_torn_tail: u8,
    out_handle: *mut *mut CowrieWalHandle,
) -> c_int {
    ffi_code(|| {
        if path_cstr.is_null() || out_handle.is_null() {
            return CowrieError::InvalidContract.code();
        }
        *out_handle = ptr::null_mut();
        let path = match CStr::from_ptr(path_cstr).to_str() {
            Ok(s) => Path::new(s),
            Err(_) => return CowrieError::InvalidUtf8.code(),
        };
        match WalStore::open(path, recover_torn_tail != 0) {
            Ok(w) => {
                *out_handle = Box::into_raw(Box::new(CowrieWalHandle { inner: w }));
                0
            }
            Err(e) => e.code(),
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_wal_append_frame(
    handle: *mut CowrieWalHandle,
    frame_data: *const u8,
    frame_len: usize,
    fsync_now: u8,
    out_seq: *mut u64,
    out_cid_34: *mut u8,
) -> c_int {
    ffi_code(|| {
        if handle.is_null() || (frame_data.is_null() && frame_len > 0) {
            return CowrieError::InvalidContract.code();
        }
        let slice = if frame_len == 0 { &[] } else { std::slice::from_raw_parts(frame_data, frame_len) };
        match (*handle).inner.append_frame(slice, fsync_now != 0) {
            Ok((seq, cid)) => {
                if !out_seq.is_null() {
                    *out_seq = seq;
                }
                if !out_cid_34.is_null() {
                    ptr::copy_nonoverlapping(cid.as_ptr(), out_cid_34, 34);
                }
                0
            }
            Err(e) => e.code(),
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_wal_append_tombstone(
    handle: *mut CowrieWalHandle,
    cid_34: *const u8,
    fsync_now: u8,
    out_seq: *mut u64,
) -> c_int {
    ffi_code(|| {
        if handle.is_null() || cid_34.is_null() {
            return CowrieError::InvalidContract.code();
        }
        let mut cid = [0u8; 34];
        cid.copy_from_slice(std::slice::from_raw_parts(cid_34, 34));
        match (*handle).inner.append_tombstone(&cid, fsync_now != 0) {
            Ok(seq) => {
                if !out_seq.is_null() {
                    *out_seq = seq;
                }
                0
            }
            Err(e) => e.code(),
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_wal_len(handle: *const CowrieWalHandle) -> usize {
    if handle.is_null() { return 0; }
    (*handle).inner.len()
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_wal_live_len(handle: *const CowrieWalHandle) -> usize {
    if handle.is_null() { return 0; }
    (*handle).inner.live_frames().len()
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_wal_compact_shard(
    handle: *const CowrieWalHandle,
    out_shard_path_cstr: *const c_char,
    out_root_34: *mut u8,
) -> c_int {
    ffi_code(|| {
        if handle.is_null() || out_shard_path_cstr.is_null() {
            return CowrieError::InvalidContract.code();
        }
        let out_path = match CStr::from_ptr(out_shard_path_cstr).to_str() {
            Ok(s) => Path::new(s),
            Err(_) => return CowrieError::InvalidUtf8.code(),
        };
        match (*handle).inner.compact_to_shard(out_path) {
            Ok(root) => {
                if !out_root_34.is_null() {
                    ptr::copy_nonoverlapping(root.as_ptr(), out_root_34, 34);
                }
                0
            }
            Err(e) => e.code(),
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_wal_close(handle: *mut CowrieWalHandle) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !handle.is_null() {
            let _ = Box::from_raw(handle);
        }
    }));
}

#[repr(C)]
pub struct CowrieZonedInput {
    pub primary_key: i64,
    pub secondary_val: i64,
    pub frame_data: *const u8,
    pub frame_len: usize,
}

pub struct CowrieZonedHandle {
    inner: ZonedShardReader,
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_zoned_write(
    path_cstr: *const c_char,
    inputs: *const CowrieZonedInput,
    count: usize,
    out_total_bytes: *mut u64,
    out_page_count: *mut u32,
    out_root_34: *mut u8,
) -> c_int {
    ffi_code(|| {
        if path_cstr.is_null() || (inputs.is_null() && count > 0) {
            return CowrieError::InvalidContract.code();
        }
        let path = match CStr::from_ptr(path_cstr).to_str() {
            Ok(s) => Path::new(s),
            Err(_) => return CowrieError::InvalidUtf8.code(),
        };
        let raw_in = if count == 0 { &[] } else { std::slice::from_raw_parts(inputs, count) };
        let mut recs = Vec::with_capacity(count);
        for item in raw_in {
            if item.frame_data.is_null() && item.frame_len > 0 {
                return CowrieError::InvalidContract.code();
            }
            let fb = if item.frame_len == 0 {
                &[][..]
            } else {
                std::slice::from_raw_parts(item.frame_data, item.frame_len)
            };
            recs.push(crate::zonemap::ZonedRecord {
                primary_key: item.primary_key,
                secondary_val: item.secondary_val,
                frame: fb,
            });
        }
        match crate::zonemap::write_zoned_shard(path, &mut recs) {
            Ok((tot, pages, root)) => {
                if !out_total_bytes.is_null() {
                    *out_total_bytes = tot;
                }
                if !out_page_count.is_null() {
                    *out_page_count = pages;
                }
                if !out_root_34.is_null() {
                    ptr::copy_nonoverlapping(root.as_ptr(), out_root_34, 34);
                }
                0
            }
            Err(e) => e.code(),
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_zoned_open(
    path_cstr: *const c_char,
    out_handle: *mut *mut CowrieZonedHandle,
) -> c_int {
    ffi_code(|| {
        if path_cstr.is_null() || out_handle.is_null() {
            return CowrieError::InvalidContract.code();
        }
        *out_handle = ptr::null_mut();
        let path = match CStr::from_ptr(path_cstr).to_str() {
            Ok(s) => Path::new(s),
            Err(_) => return CowrieError::InvalidUtf8.code(),
        };
        match ZonedShardReader::open(path) {
            Ok(r) => {
                *out_handle = Box::into_raw(Box::new(CowrieZonedHandle { inner: r }));
                0
            }
            Err(e) => e.code(),
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_zoned_query_keys(
    handle: *const CowrieZonedHandle,
    lo: i64,
    hi: i64,
    trusted_only: u8,
    verify_merkle: u8,
    out_hit_count: *mut usize,
    out_bytes_read: *mut usize,
) -> c_int {
    ffi_code(|| {
        if handle.is_null() || out_hit_count.is_null() {
            return CowrieError::InvalidContract.code();
        }
        match (*handle).inner.query_key_range(lo, hi, trusted_only != 0, verify_merkle != 0) {
            Ok((hits, bread)) => {
                *out_hit_count = hits.len();
                if !out_bytes_read.is_null() {
                    *out_bytes_read = bread;
                }
                0
            }
            Err(e) => e.code(),
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_zoned_query_secondary(
    handle: *const CowrieZonedHandle,
    lo: i64,
    hi: i64,
    trusted_only: u8,
    verify_merkle: u8,
    out_hit_count: *mut usize,
    out_bytes_read: *mut usize,
) -> c_int {
    ffi_code(|| {
        if handle.is_null() || out_hit_count.is_null() {
            return CowrieError::InvalidContract.code();
        }
        match (*handle).inner.query_secondary_range(lo, hi, trusted_only != 0, verify_merkle != 0) {
            Ok((hits, bread)) => {
                *out_hit_count = hits.len();
                if !out_bytes_read.is_null() {
                    *out_bytes_read = bread;
                }
                0
            }
            Err(e) => e.code(),
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn cowrie_zoned_close(handle: *mut CowrieZonedHandle) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !handle.is_null() {
            let _ = Box::from_raw(handle);
        }
    }));
}

} // mod unix_wal_zm_ffi
#[cfg(unix)]
pub use unix_wal_zm_ffi::*;
