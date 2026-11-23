//! C-ABI FFI surface (`libcowrie_core.so` / `cowrie.h`) for C++, CUDA (`llama.cpp`),
//! Swift/Metal, and Python (`ctypes`) consumers linking directly against one audited Rust core.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::path::Path;
use std::ptr;

use crate::canonical::{decode_frame_strict, encode_frame, parse_json_to_value, value_to_compact_json};
use crate::lens::{project_r0, project_r1, project_r2};
use crate::sha256::{content_address, to_hex};
use crate::shard::{save_statepage_atomic, verify_shard_and_localize, MappedStatePage};
use crate::trust::{is_tainted, propagate_authority};
use crate::CowrieError;

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

/// Free a buffer returned by any `cowrie_*` FFI call.
#[no_mangle]
pub unsafe extern "C" fn cowrie_buf_free(buf: CowrieBuf) {
    if !buf.data.is_null() && buf.cap > 0 {
        let _ = Vec::from_raw_parts(buf.data, buf.len, buf.cap);
    }
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
        _ => b"ERR_UNKNOWN\0",
    };
    s.as_ptr() as *const c_char
}

/// Strictly verify a canonical `COWR` frame buffer. Returns 0 (`OK`) or exact `ERR_*` code.
#[no_mangle]
pub unsafe extern "C" fn cowrie_frame_verify(
    data: *const u8,
    len: usize,
    out_auth: *mut u8,
    out_epi: *mut u8,
    out_cid_34: *mut u8,
) -> c_int {
    if data.is_null() && len > 0 {
        return CowrieError::Truncated.code();
    }
    let slice = if len == 0 { &[] } else { std::slice::from_raw_parts(data, len) };
    match decode_frame_strict(slice) {
        Ok(dec) => {
            if !out_auth.is_null() {
                *out_auth = dec.authority;
            }
            if !out_epi.is_null() {
                *out_epi = dec.epistemic;
            }
            if !out_cid_34.is_null() {
                let cid = content_address(slice);
                ptr::copy_nonoverlapping(cid.as_ptr(), out_cid_34, 34);
            }
            0
        }
        Err(e) => e.code(),
    }
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
    if out_buf.is_null() || (json_utf8.is_null() && json_len > 0) {
        return CowrieError::InvalidJson.code();
    }
    *out_buf = CowrieBuf::empty();
    let raw = std::slice::from_raw_parts(json_utf8, json_len);
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
}

/// Decode a strict canonical `COWR` frame into minified JSON UTF-8.
#[no_mangle]
pub unsafe extern "C" fn cowrie_decode_to_json(
    data: *const u8,
    len: usize,
    out_buf: *mut CowrieBuf,
) -> c_int {
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
}

/// Verify a `CWRF` shard buffer; returns 0 (`OK`) if valid and Merkle-intact, else error code.
#[no_mangle]
pub unsafe extern "C" fn cowrie_shard_verify(
    shard_data: *const u8,
    shard_len: usize,
    out_root_34: *mut u8,
) -> c_int {
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
}

/// Compute effective derived authority byte (sticky `BIT_TAINTED = 0x80` causal propagation).
#[no_mangle]
pub unsafe extern "C" fn cowrie_propagate_authority(
    base_auth: u8,
    parent_auths: *const u8,
    num_parents: usize,
) -> u8 {
    let parents = if parent_auths.is_null() || num_parents == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(parent_auths, num_parents)
    };
    propagate_authority(base_auth, parents)
}

#[no_mangle]
pub extern "C" fn cowrie_is_tainted(authority: u8) -> u8 {
    if is_tainted(authority) { 1 } else { 0 }
}

// =========================================================================
// STATEPAGE FFI (ZERO-COPY 16 KB MMAP FOR C++ / LLAMA.CPP!)
// =========================================================================

pub struct CowrieStatePageHandle {
    inner: MappedStatePage,
    model_sha_c: CString,
    doc_cid_c: CString,
    state_sha_c: CString,
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
    if path_cstr.is_null() || model_sha_cstr.is_null() || doc_cid_cstr.is_null() || state_data.is_null() {
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
    let slice = std::slice::from_raw_parts(state_data, state_len);
    match save_statepage_atomic(path, model_sha, doc_cid, ctx_tokens, kv_quant, ssm_dtype, slice) {
        Ok(_) => 0,
        Err(e) => e.code(),
    }
}

/// Open, `mmap`, strictly verify 16 KB alignment + `model_sha` contract + optional full hash.
#[no_mangle]
pub unsafe extern "C" fn cowrie_statepage_open(
    path_cstr: *const c_char,
    expected_model_sha_cstr: *const c_char,
    verify_hash: u8,
    out_handle: *mut *mut CowrieStatePageHandle,
) -> c_int {
    cowrie_statepage_open_checked(path_cstr, expected_model_sha_cstr, -1, verify_hash, out_handle)
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
    let exp_kvq = if expected_kv_quant < 0 {
        None
    } else {
        Some(expected_kv_quant as u8)
    };
    match MappedStatePage::open_verified_checked(path, exp_model, exp_kvq, verify_hash != 0) {
        Ok(mapped) => {
            let h = Box::new(CowrieStatePageHandle {
                model_sha_c: CString::new(mapped.meta.model_sha.as_str()).unwrap(),
                doc_cid_c: CString::new(mapped.meta.doc_cid.as_str()).unwrap(),
                state_sha_c: CString::new(mapped.meta.state_sha.as_str()).unwrap(),
                inner: mapped,
            });
            *out_handle = Box::into_raw(h);
            0
        }
        Err(e) => e.code(),
    }
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
pub unsafe extern "C" fn cowrie_statepage_hash_mode(handle: *const CowrieStatePageHandle) -> u8 {
    if handle.is_null() { return 0; }
    (*handle).inner.meta.hash_mode
}

#[no_mangle]
pub extern "C" fn cowrie_has_shani() -> u8 {
    if crate::sha256::has_shani() { 1 } else { 0 }
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
    if !handle.is_null() {
        let _ = Box::from_raw(handle);
    }
}

/// Helper: SHA-256 hex of arbitrary bytes (64 hex chars + NUL written into `out_hex_65`).
#[no_mangle]
pub unsafe extern "C" fn cowrie_sha256_hex(data: *const u8, len: usize, out_hex_65: *mut c_char) -> c_int {
    if out_hex_65.is_null() || (data.is_null() && len > 0) {
        return CowrieError::Truncated.code();
    }
    let slice = if len == 0 { &[] } else { std::slice::from_raw_parts(data, len) };
    let hex = to_hex(&crate::sha256::sha256(slice));
    ptr::copy_nonoverlapping(hex.as_ptr(), out_hex_65 as *mut u8, 64);
    *(out_hex_65.add(64)) = 0;
    0
}
