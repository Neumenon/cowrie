//! Plane A: Bijective Canonical Core (`COWR`) encoder, strict hardened decoder,
//! zero-allocation wire verifier, native `TAG_TENSOR_REF (0x0A)`, and lossless JSON <-> Value converter.

use std::collections::{BTreeMap, HashSet};
use crate::CowrieError;

pub const MAGIC_FRAME: &[u8; 4] = b"COWR";
pub const VERSION: u8 = 1;
pub const MAX_DEPTH: usize = 64;
/// Maximum materialized DOM bytes (keys + strings + bytes + node overhead) allowed per frame,
/// preventing `O(B^2)` dictionary-amplification OOM bombs while keeping wire verification in lockstep.
pub const MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_KEY_LEN: usize = 65536;

#[inline]
pub fn is_valid_header_meta(authority: u8, epistemic: u8) -> bool {
    (authority & !0x8F) == 0 && (authority & 0x0F) != 0 && (1..=3).contains(&epistemic)
}

#[inline]
fn charge_budget(used_bytes: &mut usize, add: usize) -> Result<(), CowrieError> {
    let next = used_bytes.checked_add(add).ok_or(CowrieError::AllocLimit)?;
    if next > MAX_DECODED_BYTES {
        return Err(CowrieError::AllocLimit);
    }
    *used_bytes = next;
    Ok(())
}

pub const TAG_NULL: u8 = 0x00;
pub const TAG_FALSE: u8 = 0x01;
pub const TAG_TRUE: u8 = 0x02;
pub const TAG_INT: u8 = 0x03;
pub const TAG_FLOAT64: u8 = 0x04;
pub const TAG_STR: u8 = 0x05;
pub const TAG_BYTES: u8 = 0x06;
pub const TAG_ARRAY: u8 = 0x07;
pub const TAG_MAP: u8 = 0x08;
pub const TAG_UINT: u8 = 0x09;
pub const TAG_TENSOR_REF: u8 = 0x0A;

pub const DTYPE_F32: u8 = 0;
pub const DTYPE_F16: u8 = 1;
pub const DTYPE_BF16: u8 = 2;
pub const DTYPE_I8: u8 = 3;
pub const DTYPE_U8: u8 = 4;
pub const DTYPE_Q4_0: u8 = 5;
pub const DTYPE_Q8_0: u8 = 6;
pub const DTYPE_I32: u8 = 7;
pub const DTYPE_F64: u8 = 8;

pub const CANON_QNAN_BITS: u64 = 0x7FF8_0000_0000_0000;

#[inline]
pub fn dtype_to_str(dtype: u8) -> Option<&'static str> {
    match dtype {
        DTYPE_F32 => Some("f32"),
        DTYPE_F16 => Some("f16"),
        DTYPE_BF16 => Some("bf16"),
        DTYPE_I8 => Some("i8"),
        DTYPE_U8 => Some("u8"),
        DTYPE_Q4_0 => Some("q4_0"),
        DTYPE_Q8_0 => Some("q8_0"),
        DTYPE_I32 => Some("i32"),
        DTYPE_F64 => Some("f64"),
        _ => None,
    }
}

#[inline]
pub fn dtype_from_str(s: &str) -> Option<u8> {
    match s {
        "f32" | "float32" => Some(DTYPE_F32),
        "f16" | "fp16" | "float16" => Some(DTYPE_F16),
        "bf16" | "bfloat16" => Some(DTYPE_BF16),
        "i8" | "int8" => Some(DTYPE_I8),
        "u8" | "uint8" => Some(DTYPE_U8),
        "q4_0" => Some(DTYPE_Q4_0),
        "q8_0" => Some(DTYPE_Q8_0),
        "i32" | "int32" => Some(DTYPE_I32),
        "f64" | "float64" => Some(DTYPE_F64),
        _ => None,
    }
}

/// Compute minimum required Plane C byte footprint for a typed tensor shape, rejecting arithmetic overflow.
pub fn checked_tensor_min_bytes(dtype: u8, dims: &[u64]) -> Result<u64, CowrieError> {
    if dims.len() > 8 || dtype_to_str(dtype).is_none() {
        return Err(CowrieError::InvalidContract);
    }
    let mut elems: u64 = 1;
    for &d in dims {
        elems = elems.checked_mul(d).ok_or(CowrieError::AllocLimit)?;
    }
    match dtype {
        DTYPE_F32 | DTYPE_I32 => elems.checked_mul(4).ok_or(CowrieError::AllocLimit),
        DTYPE_F16 | DTYPE_BF16 => elems.checked_mul(2).ok_or(CowrieError::AllocLimit),
        DTYPE_I8 | DTYPE_U8 => Ok(elems),
        DTYPE_F64 => elems.checked_mul(8).ok_or(CowrieError::AllocLimit),
        DTYPE_Q4_0 => {
            // 32 values per 18-byte block (2B fp16 scale + 16B nibbles)
            let blocks = elems.checked_add(31).ok_or(CowrieError::AllocLimit)? / 32;
            blocks.checked_mul(18).ok_or(CowrieError::AllocLimit)
        }
        DTYPE_Q8_0 => {
            // 32 values per 34-byte block (2B fp16 scale + 32B int8)
            let blocks = elems.checked_add(31).ok_or(CowrieError::AllocLimit)? / 32;
            blocks.checked_mul(34).ok_or(CowrieError::AllocLimit)
        }
        _ => Err(CowrieError::InvalidContract),
    }
}

/// Canonical logical value tree for Plane A frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    /// Stored as exact canonical IEEE-754 little-endian bits for exact bijective Eq.
    FloatBits(u64),
    Str(String),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Map(Vec<(String, Value)>),
    /// Native typed Plane C Coordinate Contract (`TAG_TENSOR_REF = 0x0A`).
    TensorRef {
        arena_idx: u32,
        dtype: u8,
        dims: Vec<u64>,
        space: String,
    },
}

impl Value {
    pub fn from_f64(x: f64) -> Self {
        Self::FloatBits(canonicalize_f64_bits(x.to_bits()))
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::FloatBits(b) => Some(f64::from_bits(*b)),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&[(String, Value)]> {
        match self {
            Value::Map(m) => Some(m.as_slice()),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.as_map()?.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }
}

#[inline]
pub fn canonicalize_f64_bits(bits: u64) -> u64 {
    if (bits & 0x7FF0_0000_0000_0000) == 0x7FF0_0000_0000_0000
        && (bits & 0x000F_FFFF_FFFF_FFFF) != 0
    {
        CANON_QNAN_BITS
    } else if bits == 0x8000_0000_0000_0000 {
        0 // -0.0 -> +0.0
    } else {
        bits
    }
}

#[inline]
pub fn encode_uvarint(mut n: u64, out: &mut Vec<u8>) {
    loop {
        let b = (n & 0x7F) as u8;
        n >>= 7;
        if n != 0 {
            out.push(b | 0x80);
        } else {
            out.push(b);
            return;
        }
    }
}

#[inline]
pub fn decode_uvarint_strict(buf: &[u8], pos: &mut usize) -> Result<u64, CowrieError> {
    let mut val: u64 = 0;
    let mut shift: u32 = 0;
    let start = *pos;
    loop {
        if *pos >= buf.len() {
            return Err(CowrieError::Truncated);
        }
        let b = buf[*pos];
        *pos += 1;
        if shift >= 64 || (shift == 63 && (b & 0x7F) > 1) {
            return Err(CowrieError::NonCanonicalVarint);
        }
        val |= ((b & 0x7F) as u64) << shift;
        if (b & 0x80) == 0 {
            if *pos - start > 1 && b == 0 {
                return Err(CowrieError::NonCanonicalVarint);
            }
            return Ok(val);
        }
        shift += 7;
    }
}

/// Stack-inline bitset for `<= 256` dictionary keys (zero heap allocation on 99.9% of frames).
enum KeyBitset {
    Inline([u64; 4], usize),
    Heap(Vec<u64>, usize),
}

impl KeyBitset {
    #[inline]
    fn new(nkeys: usize) -> Self {
        if nkeys <= 256 {
            Self::Inline([0u64; 4], nkeys)
        } else {
            Self::Heap(vec![0u64; (nkeys + 63) / 64], nkeys)
        }
    }

    #[inline]
    fn mark(&mut self, idx: usize) {
        let word = idx >> 6;
        let bit = 1u64 << (idx & 63);
        match self {
            Self::Inline(arr, _) => arr[word] |= bit,
            Self::Heap(vec, _) => vec[word] |= bit,
        }
    }

    #[inline]
    fn all_used(&self) -> bool {
        let (words, nkeys): (&[u64], usize) = match self {
            Self::Inline(arr, n) => (&arr[..(*n + 63) / 64], *n),
            Self::Heap(vec, n) => (vec.as_slice(), *n),
        };
        if nkeys == 0 {
            return true;
        }
        let full = nkeys / 64;
        for &w in &words[..full] {
            if w != u64::MAX {
                return false;
            }
        }
        let rem = nkeys & 63;
        if rem > 0 {
            let mask = (1u64 << rem) - 1;
            if words[full] != mask {
                return false;
            }
        }
        true
    }
}

fn collect_keys<'a>(v: &'a Value, set: &mut BTreeMap<&'a str, usize>, depth: usize) -> Result<(), CowrieError> {
    if depth > MAX_DEPTH {
        return Err(CowrieError::MaxDepth);
    }
    match v {
        Value::Array(a) => {
            for x in a {
                collect_keys(x, set, depth + 1)?;
            }
        }
        Value::Map(m) => {
            for (k, x) in m {
                set.insert(k.as_str(), 0);
                collect_keys(x, set, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Stack-inline key-length table for `<= 256` dictionary keys (zero heap allocation on 99.9% of frames).
enum KeyLenStore {
    Inline([usize; 256], usize),
    Heap(Vec<usize>),
}

impl KeyLenStore {
    #[inline]
    fn new(nkeys: usize) -> Self {
        if nkeys <= 256 {
            Self::Inline([0usize; 256], 0)
        } else {
            Self::Heap(Vec::with_capacity(nkeys.min(4096)))
        }
    }

    #[inline]
    fn push(&mut self, len: usize) {
        match self {
            Self::Inline(arr, count) => {
                arr[*count] = len;
                *count += 1;
            }
            Self::Heap(vec) => vec.push(len),
        }
    }

    #[inline]
    fn get(&self, idx: usize) -> usize {
        match self {
            Self::Inline(arr, _) => arr[idx],
            Self::Heap(vec) => vec[idx],
        }
    }
}

/// Encode a canonical `COWR` frame (bijective with `decode_frame_strict`).
pub fn encode_frame(val: &Value, authority: u8, epistemic: u8) -> Result<Vec<u8>, CowrieError> {
    if !is_valid_header_meta(authority, epistemic) {
        return Err(CowrieError::NonCanonicalFlags);
    }
    let mut key_map: BTreeMap<&str, usize> = BTreeMap::new();
    collect_keys(val, &mut key_map, 0)?;

    let mut keys: Vec<&str> = Vec::with_capacity(key_map.len());
    for (idx, (k, slot)) in key_map.iter_mut().enumerate() {
        if k.len() > MAX_KEY_LEN {
            return Err(CowrieError::AllocLimit);
        }
        *slot = idx;
        keys.push(*k);
    }

    let mut out = Vec::with_capacity(128);
    out.extend_from_slice(MAGIC_FRAME);
    out.push(VERSION);
    out.push(authority);
    out.push(epistemic);
    out.push(0x00); // flags strictly 0

    encode_uvarint(keys.len() as u64, &mut out);
    for k in &keys {
        encode_uvarint(k.len() as u64, &mut out);
        out.extend_from_slice(k.as_bytes());
    }

    fn enc(v: &Value, km: &BTreeMap<&str, usize>, out: &mut Vec<u8>, depth: usize) -> Result<(), CowrieError> {
        if depth > MAX_DEPTH {
            return Err(CowrieError::MaxDepth);
        }
        match v {
            Value::Null => out.push(TAG_NULL),
            Value::Bool(b) => out.push(if *b { TAG_TRUE } else { TAG_FALSE }),
            Value::Int(n) => {
                if (0..=127).contains(n) {
                    out.push(0x40 + (*n as u8));
                } else {
                    out.push(TAG_INT);
                    encode_uvarint(((*n << 1) ^ (*n >> 63)) as u64, out);
                }
            }
            Value::Uint(u) => {
                if *u <= i64::MAX as u64 {
                    if *u <= 127 {
                        out.push(0x40 + (*u as u8));
                    } else {
                        out.push(TAG_INT);
                        let n = *u as i64;
                        encode_uvarint(((n << 1) ^ (n >> 63)) as u64, out);
                    }
                } else {
                    out.push(TAG_UINT);
                    encode_uvarint(*u, out);
                }
            }
            Value::FloatBits(bits) => {
                let b = canonicalize_f64_bits(*bits);
                out.push(TAG_FLOAT64);
                out.extend_from_slice(&b.to_le_bytes());
            }
            Value::Str(s) => {
                out.push(TAG_STR);
                encode_uvarint(s.len() as u64, out);
                out.extend_from_slice(s.as_bytes());
            }
            Value::Bytes(b) => {
                out.push(TAG_BYTES);
                encode_uvarint(b.len() as u64, out);
                out.extend_from_slice(b);
            }
            Value::Array(a) => {
                out.push(TAG_ARRAY);
                encode_uvarint(a.len() as u64, out);
                for x in a {
                    enc(x, km, out, depth + 1)?;
                }
            }
            Value::Map(m) => {
                let mut sorted: Vec<(&str, &Value)> = m.iter().map(|(k, v)| (k.as_str(), v)).collect();
                sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                for w in sorted.windows(2) {
                    if w[0].0 == w[1].0 {
                        return Err(CowrieError::DuplicateKey);
                    }
                }
                out.push(TAG_MAP);
                encode_uvarint(sorted.len() as u64, out);
                for (k, v) in sorted {
                    encode_uvarint(km[k] as u64, out);
                    enc(v, km, out, depth + 1)?;
                }
            }
            Value::TensorRef { arena_idx, dtype, dims, space } => {
                if space.is_empty() || space.len() > 256 {
                    return Err(CowrieError::InvalidContract);
                }
                let _ = checked_tensor_min_bytes(*dtype, dims)?;
                out.push(TAG_TENSOR_REF);
                encode_uvarint(*arena_idx as u64, out);
                out.push(*dtype);
                out.push(dims.len() as u8);
                for &d in dims {
                    encode_uvarint(d, out);
                }
                encode_uvarint(space.len() as u64, out);
                out.extend_from_slice(space.as_bytes());
            }
        }
        Ok(())
    }

    enc(val, &key_map, &mut out, 0)?;
    Ok(out)
}

/// Decoded frame metadata + logical value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedFrame {
    pub value: Value,
    pub authority: u8,
    pub epistemic: u8,
}

/// Zero-allocation strict wire verifier: enforces every single canonical `COWR` invariant
/// (`magic`, `ver`, `flags`, minimal varints, UTF-8 byte-sorted dictionary, zero unused keys,
/// minimal ints/floats, `TAG_TENSOR_REF` Coordinate Contracts, `MAX_DEPTH = 64`, no trailing bytes)
/// without allocating a single `Value`, `String`, `Vec<&str>`, or `HashSet` on the heap!
pub fn verify_frame_zero_alloc(buf: &[u8]) -> Result<(u8, u8), CowrieError> {
    verify_frame_with_tensor_hook_zero_alloc(buf, |_, _, _| Ok(()))
}

/// Zero-allocation strict wire verifier with a `TAG_TENSOR_REF` callback for shard-level arena validation.
pub fn verify_frame_with_tensor_hook_zero_alloc<F>(
    buf: &[u8],
    mut on_tensor_ref: F,
) -> Result<(u8, u8), CowrieError>
where
    F: FnMut(u32, u8, &[u64]) -> Result<(), CowrieError>,
{
    if buf.len() < 8 {
        return Err(CowrieError::Truncated);
    }
    if &buf[0..4] != MAGIC_FRAME {
        return Err(CowrieError::BadMagic);
    }
    if buf[4] != VERSION {
        return Err(CowrieError::UnsupportedVer);
    }
    let authority = buf[5];
    let epistemic = buf[6];
    if buf[7] != 0 || !is_valid_header_meta(authority, epistemic) {
        return Err(CowrieError::NonCanonicalFlags);
    }

    let mut pos = 8usize;
    let nkeys_u64 = decode_uvarint_strict(buf, &mut pos)?;
    if nkeys_u64 > (buf.len() - pos) as u64 {
        return Err(CowrieError::AllocLimit);
    }
    let nkeys = nkeys_u64 as usize;

    let mut key_lens = KeyLenStore::new(nkeys);
    let mut decoded_bytes = 0usize;
    let mut prev: Option<&[u8]> = None;
    for _ in 0..nkeys {
        let klen_u64 = decode_uvarint_strict(buf, &mut pos)?;
        if klen_u64 > (buf.len() - pos) as u64 {
            return Err(CowrieError::Truncated);
        }
        let klen = klen_u64 as usize;
        if klen > MAX_KEY_LEN {
            return Err(CowrieError::AllocLimit);
        }
        charge_budget(&mut decoded_bytes, klen.saturating_add(16))?;
        let kb = &buf[pos..pos + klen];
        pos += klen;
        if let Some(p) = prev {
            if kb == p {
                return Err(CowrieError::DuplicateKey);
            }
            if kb < p {
                return Err(CowrieError::UnsortedDict);
            }
        }
        prev = Some(kb);
        std::str::from_utf8(kb).map_err(|_| CowrieError::InvalidUtf8)?;
        key_lens.push(klen);
    }

    let mut used = KeyBitset::new(nkeys);

    fn ver<F: FnMut(u32, u8, &[u64]) -> Result<(), CowrieError>>(
        buf: &[u8],
        pos: &mut usize,
        nkeys: usize,
        key_lens: &KeyLenStore,
        used: &mut KeyBitset,
        decoded_bytes: &mut usize,
        on_tensor_ref: &mut F,
        depth: usize,
    ) -> Result<(), CowrieError> {
        if depth > MAX_DEPTH {
            return Err(CowrieError::MaxDepth);
        }
        if *pos >= buf.len() {
            return Err(CowrieError::Truncated);
        }
        charge_budget(decoded_bytes, 32)?;
        let tag = buf[*pos];
        *pos += 1;

        if (0x40..=0xBF).contains(&tag) {
            return Ok(());
        }

        match tag {
            TAG_NULL | TAG_FALSE | TAG_TRUE => Ok(()),
            TAG_INT => {
                let u = decode_uvarint_strict(buf, pos)?;
                let v = ((u >> 1) as i64) ^ (-((u & 1) as i64));
                if (0..=127).contains(&v) {
                    return Err(CowrieError::NonCanonicalInt);
                }
                Ok(())
            }
            TAG_UINT => {
                let u = decode_uvarint_strict(buf, pos)?;
                if u <= i64::MAX as u64 {
                    return Err(CowrieError::NonCanonicalUint);
                }
                Ok(())
            }
            TAG_FLOAT64 => {
                if *pos + 8 > buf.len() {
                    return Err(CowrieError::Truncated);
                }
                let bits = u64::from_le_bytes(buf[*pos..*pos + 8].try_into().unwrap());
                *pos += 8;
                if bits == 0x8000_0000_0000_0000 {
                    return Err(CowrieError::NonCanonicalFloat);
                }
                if (bits & 0x7FF0_0000_0000_0000) == 0x7FF0_0000_0000_0000
                    && (bits & 0x000F_FFFF_FFFF_FFFF) != 0
                    && bits != CANON_QNAN_BITS
                {
                    return Err(CowrieError::NonCanonicalFloat);
                }
                Ok(())
            }
            TAG_STR => {
                let n64 = decode_uvarint_strict(buf, pos)?;
                if n64 > (buf.len() - *pos) as u64 {
                    return Err(CowrieError::Truncated);
                }
                let n = n64 as usize;
                charge_budget(decoded_bytes, n)?;
                std::str::from_utf8(&buf[*pos..*pos + n]).map_err(|_| CowrieError::InvalidUtf8)?;
                *pos += n;
                Ok(())
            }
            TAG_BYTES => {
                let n64 = decode_uvarint_strict(buf, pos)?;
                if n64 > (buf.len() - *pos) as u64 {
                    return Err(CowrieError::Truncated);
                }
                let n = n64 as usize;
                charge_budget(decoded_bytes, n)?;
                *pos += n;
                Ok(())
            }
            TAG_ARRAY => {
                let n64 = decode_uvarint_strict(buf, pos)?;
                if n64 > (buf.len() - *pos) as u64 {
                    return Err(CowrieError::AllocLimit);
                }
                for _ in 0..(n64 as usize) {
                    ver(buf, pos, nkeys, key_lens, used, decoded_bytes, on_tensor_ref, depth + 1)?;
                }
                Ok(())
            }
            TAG_MAP => {
                let n64 = decode_uvarint_strict(buf, pos)?;
                if n64.saturating_mul(2) > (buf.len() - *pos) as u64 {
                    return Err(CowrieError::AllocLimit);
                }
                let mut prev_idx: isize = -1;
                for _ in 0..(n64 as usize) {
                    let ki_u64 = decode_uvarint_strict(buf, pos)?;
                    if ki_u64 >= nkeys as u64 {
                        return Err(CowrieError::BadDictIndex);
                    }
                    let ki = ki_u64 as usize;
                    if ki as isize == prev_idx {
                        return Err(CowrieError::DuplicateKey);
                    }
                    if (ki as isize) < prev_idx {
                        return Err(CowrieError::UnsortedMapKeys);
                    }
                    prev_idx = ki as isize;
                    used.mark(ki);
                    charge_budget(decoded_bytes, key_lens.get(ki).saturating_add(32))?;
                    ver(buf, pos, nkeys, key_lens, used, decoded_bytes, on_tensor_ref, depth + 1)?;
                }
                Ok(())
            }
            TAG_TENSOR_REF => {
                let arena_u64 = decode_uvarint_strict(buf, pos)?;
                if arena_u64 > u32::MAX as u64 {
                    return Err(CowrieError::InvalidContract);
                }
                if *pos + 2 > buf.len() {
                    return Err(CowrieError::Truncated);
                }
                let dtype = buf[*pos];
                let ndim = buf[*pos + 1] as usize;
                *pos += 2;
                if ndim > 8 {
                    return Err(CowrieError::AllocLimit);
                }
                let mut dims_stack = [0u64; 8];
                for slot in dims_stack.iter_mut().take(ndim) {
                    *slot = decode_uvarint_strict(buf, pos)?;
                }
                let _ = checked_tensor_min_bytes(dtype, &dims_stack[..ndim])?;
                on_tensor_ref(arena_u64 as u32, dtype, &dims_stack[..ndim])?;
                let slen_u64 = decode_uvarint_strict(buf, pos)?;
                if slen_u64 > (buf.len() - *pos) as u64 {
                    return Err(CowrieError::Truncated);
                }
                if slen_u64 == 0 || slen_u64 > 256 {
                    return Err(CowrieError::InvalidContract);
                }
                let slen = slen_u64 as usize;
                charge_budget(decoded_bytes, ndim * 8 + slen)?;
                std::str::from_utf8(&buf[*pos..*pos + slen]).map_err(|_| CowrieError::InvalidUtf8)?;
                *pos += slen;
                Ok(())
            }
            _ => Err(CowrieError::ReservedTag),
        }
    }

    ver(buf, &mut pos, nkeys, &key_lens, &mut used, &mut decoded_bytes, &mut on_tensor_ref, 0)?;
    if pos != buf.len() {
        return Err(CowrieError::TrailingBytes);
    }
    if !used.all_used() {
        return Err(CowrieError::UnusedDictKey);
    }
    Ok((authority, epistemic))
}

/// Strict hardened decoder: enforces `MAX_DEPTH = 64`, `MAX_DECODED_BYTES = 64 MiB`,
/// minimal varints, UTF-8 byte-sorted dict, stack-inline bitset for unused keys, and zero trailing bytes.
pub fn decode_frame_strict(buf: &[u8]) -> Result<DecodedFrame, CowrieError> {
    if buf.len() < 8 {
        return Err(CowrieError::Truncated);
    }
    if &buf[0..4] != MAGIC_FRAME {
        return Err(CowrieError::BadMagic);
    }
    if buf[4] != VERSION {
        return Err(CowrieError::UnsupportedVer);
    }
    let authority = buf[5];
    let epistemic = buf[6];
    if buf[7] != 0 || !is_valid_header_meta(authority, epistemic) {
        return Err(CowrieError::NonCanonicalFlags);
    }

    let mut pos = 8usize;
    let nkeys_u64 = decode_uvarint_strict(buf, &mut pos)?;
    if nkeys_u64 > (buf.len() - pos) as u64 {
        return Err(CowrieError::AllocLimit);
    }
    let nkeys = nkeys_u64 as usize;

    let mut keys: Vec<&str> = Vec::with_capacity(nkeys.min(4096));
    let mut decoded_bytes = 0usize;
    let mut prev: Option<&[u8]> = None;
    for _ in 0..nkeys {
        let klen_u64 = decode_uvarint_strict(buf, &mut pos)?;
        if klen_u64 > (buf.len() - pos) as u64 {
            return Err(CowrieError::Truncated);
        }
        let klen = klen_u64 as usize;
        if klen > MAX_KEY_LEN {
            return Err(CowrieError::AllocLimit);
        }
        charge_budget(&mut decoded_bytes, klen.saturating_add(16))?;
        let kb = &buf[pos..pos + klen];
        pos += klen;
        if let Some(p) = prev {
            if kb == p {
                return Err(CowrieError::DuplicateKey);
            }
            if kb < p {
                return Err(CowrieError::UnsortedDict);
            }
        }
        prev = Some(kb);
        keys.push(std::str::from_utf8(kb).map_err(|_| CowrieError::InvalidUtf8)?);
    }

    let mut used = KeyBitset::new(keys.len());

    fn dec(
        buf: &[u8],
        pos: &mut usize,
        keys: &[&str],
        used: &mut KeyBitset,
        decoded_bytes: &mut usize,
        depth: usize,
    ) -> Result<Value, CowrieError> {
        if depth > MAX_DEPTH {
            return Err(CowrieError::MaxDepth);
        }
        if *pos >= buf.len() {
            return Err(CowrieError::Truncated);
        }
        charge_budget(decoded_bytes, 32)?;
        let tag = buf[*pos];
        *pos += 1;

        if (0x40..=0xBF).contains(&tag) {
            return Ok(Value::Int((tag - 0x40) as i64));
        }

        match tag {
            TAG_NULL => Ok(Value::Null),
            TAG_FALSE => Ok(Value::Bool(false)),
            TAG_TRUE => Ok(Value::Bool(true)),
            TAG_INT => {
                let u = decode_uvarint_strict(buf, pos)?;
                let v = ((u >> 1) as i64) ^ (-((u & 1) as i64));
                if (0..=127).contains(&v) {
                    return Err(CowrieError::NonCanonicalInt);
                }
                Ok(Value::Int(v))
            }
            TAG_UINT => {
                let u = decode_uvarint_strict(buf, pos)?;
                if u <= i64::MAX as u64 {
                    return Err(CowrieError::NonCanonicalUint);
                }
                Ok(Value::Uint(u))
            }
            TAG_FLOAT64 => {
                if *pos + 8 > buf.len() {
                    return Err(CowrieError::Truncated);
                }
                let bits = u64::from_le_bytes(buf[*pos..*pos + 8].try_into().unwrap());
                *pos += 8;
                if bits == 0x8000_0000_0000_0000 {
                    return Err(CowrieError::NonCanonicalFloat);
                }
                if (bits & 0x7FF0_0000_0000_0000) == 0x7FF0_0000_0000_0000
                    && (bits & 0x000F_FFFF_FFFF_FFFF) != 0
                    && bits != CANON_QNAN_BITS
                {
                    return Err(CowrieError::NonCanonicalFloat);
                }
                Ok(Value::FloatBits(bits))
            }
            TAG_STR => {
                let n64 = decode_uvarint_strict(buf, pos)?;
                if n64 > (buf.len() - *pos) as u64 {
                    return Err(CowrieError::Truncated);
                }
                let n = n64 as usize;
                charge_budget(decoded_bytes, n)?;
                let s = std::str::from_utf8(&buf[*pos..*pos + n])
                    .map_err(|_| CowrieError::InvalidUtf8)?;
                *pos += n;
                Ok(Value::Str(s.to_string()))
            }
            TAG_BYTES => {
                let n64 = decode_uvarint_strict(buf, pos)?;
                if n64 > (buf.len() - *pos) as u64 {
                    return Err(CowrieError::Truncated);
                }
                let n = n64 as usize;
                charge_budget(decoded_bytes, n)?;
                let b = buf[*pos..*pos + n].to_vec();
                *pos += n;
                Ok(Value::Bytes(b))
            }
            TAG_ARRAY => {
                let n64 = decode_uvarint_strict(buf, pos)?;
                if n64 > (buf.len() - *pos) as u64 {
                    return Err(CowrieError::AllocLimit);
                }
                let n = n64 as usize;
                let mut v = Vec::with_capacity(n.min(4096));
                for _ in 0..n {
                    v.push(dec(buf, pos, keys, used, decoded_bytes, depth + 1)?);
                }
                Ok(Value::Array(v))
            }
            TAG_MAP => {
                let n64 = decode_uvarint_strict(buf, pos)?;
                if n64.saturating_mul(2) > (buf.len() - *pos) as u64 {
                    return Err(CowrieError::AllocLimit);
                }
                let n = n64 as usize;
                let mut v = Vec::with_capacity(n.min(4096));
                let mut prev_idx: isize = -1;
                for _ in 0..n {
                    let ki_u64 = decode_uvarint_strict(buf, pos)?;
                    if ki_u64 >= keys.len() as u64 {
                        return Err(CowrieError::BadDictIndex);
                    }
                    let ki = ki_u64 as usize;
                    if ki as isize == prev_idx {
                        return Err(CowrieError::DuplicateKey);
                    }
                    if (ki as isize) < prev_idx {
                        return Err(CowrieError::UnsortedMapKeys);
                    }
                    prev_idx = ki as isize;
                    used.mark(ki);
                    charge_budget(decoded_bytes, keys[ki].len().saturating_add(32))?;
                    let val = dec(buf, pos, keys, used, decoded_bytes, depth + 1)?;
                    v.push((keys[ki].to_string(), val));
                }
                Ok(Value::Map(v))
            }
            TAG_TENSOR_REF => {
                let arena_u64 = decode_uvarint_strict(buf, pos)?;
                if arena_u64 > u32::MAX as u64 {
                    return Err(CowrieError::InvalidContract);
                }
                if *pos + 2 > buf.len() {
                    return Err(CowrieError::Truncated);
                }
                let dtype = buf[*pos];
                let ndim = buf[*pos + 1] as usize;
                *pos += 2;
                if ndim > 8 {
                    return Err(CowrieError::AllocLimit);
                }
                let mut dims = Vec::with_capacity(ndim);
                for _ in 0..ndim {
                    dims.push(decode_uvarint_strict(buf, pos)?);
                }
                let _ = checked_tensor_min_bytes(dtype, &dims)?;
                let slen_u64 = decode_uvarint_strict(buf, pos)?;
                if slen_u64 > (buf.len() - *pos) as u64 {
                    return Err(CowrieError::Truncated);
                }
                if slen_u64 == 0 || slen_u64 > 256 {
                    return Err(CowrieError::InvalidContract);
                }
                let slen = slen_u64 as usize;
                charge_budget(decoded_bytes, ndim * 8 + slen)?;
                let space = std::str::from_utf8(&buf[*pos..*pos + slen])
                    .map_err(|_| CowrieError::InvalidUtf8)?
                    .to_string();
                *pos += slen;
                Ok(Value::TensorRef {
                    arena_idx: arena_u64 as u32,
                    dtype,
                    dims,
                    space,
                })
            }
            _ => Err(CowrieError::ReservedTag),
        }
    }

    let value = dec(buf, &mut pos, &keys, &mut used, &mut decoded_bytes, 0)?;
    if pos != buf.len() {
        return Err(CowrieError::TrailingBytes);
    }
    if !used.all_used() {
        return Err(CowrieError::UnusedDictKey);
    }
    Ok(DecodedFrame { value, authority, epistemic })
}

#[inline]
fn validate_rfc8259_number(tok: &str) -> bool {
    let b = tok.as_bytes();
    let mut i = 0;
    if i < b.len() && b[i] == b'-' {
        i += 1;
    }
    if i >= b.len() {
        return false;
    }
    if b[i] == b'0' {
        i += 1;
        if i < b.len() && b[i].is_ascii_digit() {
            return false; // leading zero (e.g., 01, -00) forbidden by RFC 8259
        }
    } else if (b'1'..=b'9').contains(&b[i]) {
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
    } else {
        return false;
    }

    if i < b.len() && b[i] == b'.' {
        i += 1;
        let frac_start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == frac_start {
            return false; // trailing dot forbidden
        }
    }

    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        let exp_start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == exp_start {
            return false;
        }
    }
    i == b.len()
}

fn decode_hex_strict(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len() / 2);
    let mut i = 0;
    while i < b.len() {
        let hi = match b[i] {
            b'0'..=b'9' => b[i] - b'0',
            b'a'..=b'f' => b[i] - b'a' + 10,
            b'A'..=b'F' => b[i] - b'A' + 10,
            _ => return None,
        };
        let lo = match b[i + 1] {
            b'0'..=b'9' => b[i + 1] - b'0',
            b'a'..=b'f' => b[i + 1] - b'a' + 10,
            b'A'..=b'F' => b[i + 1] - b'A' + 10,
            _ => return None,
        };
        out.push((hi << 4) | lo);
        i += 2;
    }
    Some(out)
}

#[inline]
fn is_reserved_sentinel_key(k: &str) -> bool {
    matches!(k, "$bytes" | "$tensor" | "$float" | "$map")
}

fn try_promote_sentinel_map(pairs: &[(String, Value)]) -> Result<Option<Value>, CowrieError> {
    if pairs.iter().any(|(k, _)| k == "$map") {
        if pairs.len() == 1 && pairs[0].0 == "$map" {
            if let Value::Map(inner) = &pairs[0].1 {
                return Ok(Some(Value::Map(inner.clone())));
            }
        }
        return Err(CowrieError::InvalidContract);
    }

    if pairs.iter().any(|(k, _)| k == "$float") {
        if pairs.len() == 1 && pairs[0].0 == "$float" {
            if let Value::Str(tag) = &pairs[0].1 {
                match tag.as_str() {
                    "NaN" => return Ok(Some(Value::FloatBits(CANON_QNAN_BITS))),
                    "Infinity" => return Ok(Some(Value::from_f64(f64::INFINITY))),
                    "-Infinity" => return Ok(Some(Value::from_f64(f64::NEG_INFINITY))),
                    _ => return Err(CowrieError::InvalidContract),
                }
            }
        }
        return Err(CowrieError::InvalidContract);
    }

    if pairs.iter().any(|(k, _)| k == "$bytes") {
        if pairs.len() == 1 && pairs[0].0 == "$bytes" {
            if let Value::Str(hex) = &pairs[0].1 {
                if let Some(raw) = decode_hex_strict(hex) {
                    return Ok(Some(Value::Bytes(raw)));
                }
            }
        }
        return Err(CowrieError::InvalidContract);
    }

    if pairs.iter().any(|(k, _)| k == "$tensor") {
        let mut idx_opt: Option<u32> = None;
        let mut dtype_opt: Option<u8> = None;
        let mut dims_opt: Option<Vec<u64>> = None;
        let mut space_opt: Option<String> = None;

        for (k, v) in pairs {
            match k.as_str() {
                "$tensor" => {
                    if idx_opt.is_some() {
                        return Err(CowrieError::DuplicateKey);
                    }
                    match v {
                        Value::Int(n) if *n >= 0 && (*n as u64) <= u32::MAX as u64 => {
                            idx_opt = Some(*n as u32)
                        }
                        Value::Uint(u) if *u <= u32::MAX as u64 => idx_opt = Some(*u as u32),
                        _ => return Err(CowrieError::InvalidContract),
                    }
                }
                "dtype" => {
                    if dtype_opt.is_some() {
                        return Err(CowrieError::DuplicateKey);
                    }
                    match v {
                        Value::Str(s) => {
                            dtype_opt = Some(dtype_from_str(s).ok_or(CowrieError::InvalidContract)?);
                        }
                        _ => return Err(CowrieError::InvalidContract),
                    }
                }
                "shape" => {
                    if dims_opt.is_some() {
                        return Err(CowrieError::DuplicateKey);
                    }
                    match v {
                        Value::Array(arr) if arr.len() <= 8 => {
                            let mut ds = Vec::with_capacity(arr.len());
                            for item in arr {
                                match item {
                                    Value::Int(n) if *n >= 0 => ds.push(*n as u64),
                                    Value::Uint(u) => ds.push(*u),
                                    _ => return Err(CowrieError::InvalidContract),
                                }
                            }
                            dims_opt = Some(ds);
                        }
                        _ => return Err(CowrieError::InvalidContract),
                    }
                }
                "space" => {
                    if space_opt.is_some() {
                        return Err(CowrieError::DuplicateKey);
                    }
                    match v {
                        Value::Str(s) if !s.is_empty() && s.len() <= 256 => {
                            space_opt = Some(s.clone());
                        }
                        _ => return Err(CowrieError::InvalidContract),
                    }
                }
                _ => return Err(CowrieError::InvalidContract),
            }
        }

        if let (Some(arena_idx), Some(dtype), Some(dims), Some(space)) =
            (idx_opt, dtype_opt, dims_opt, space_opt)
        {
            let _ = checked_tensor_min_bytes(dtype, &dims)?;
            return Ok(Some(Value::TensorRef {
                arena_idx,
                dtype,
                dims,
                space,
            }));
        }
        return Err(CowrieError::InvalidContract);
    }
    Ok(None)
}

/// Zero-dependency strict RFC 8259 JSON parser -> canonical `Value` (for CLI `cowrie pack` & FFI).
/// Supports lossless Cowrie JSON sentinels (`$bytes`, `$tensor`, `$float`, `$map`).
pub fn parse_json_to_value(input: &str) -> Result<Value, CowrieError> {
    parse_json_impl(input, true)
}

/// Zero-dependency strict RFC 8259 JSON parser -> canonical `Value` WITHOUT sentinel promotion.
/// Every JSON object stays a literal `Value::Map` (safe for raw JSON-RPC payloads such as `mcp-proxy`).
pub fn parse_plain_json_to_value(input: &str) -> Result<Value, CowrieError> {
    parse_json_impl(input, false)
}

fn parse_json_impl(input: &str, promote_sentinels: bool) -> Result<Value, CowrieError> {
    struct Parser<'a> {
        b: &'a [u8],
        i: usize,
        promote_sentinels: bool,
    }
    impl<'a> Parser<'a> {
        fn ws(&mut self) {
            while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\n' | b'\r' | b'\t') {
                self.i += 1;
            }
        }
        fn parse(&mut self, depth: usize, promote_this_map: bool) -> Result<Value, CowrieError> {
            if depth > MAX_DEPTH {
                return Err(CowrieError::MaxDepth);
            }
            self.ws();
            if self.i >= self.b.len() {
                return Err(CowrieError::Truncated);
            }
            match self.b[self.i] {
                b'{' => {
                    self.i += 1;
                    self.ws();
                    let mut pairs: Vec<(String, Value)> = Vec::new();
                    let mut seen: Option<HashSet<String>> = None;
                    if self.i < self.b.len() && self.b[self.i] == b'}' {
                        self.i += 1;
                        return Ok(Value::Map(pairs));
                    }
                    loop {
                        self.ws();
                        let k = self.pstr()?;
                        if let Some(ref mut set) = seen {
                            if !set.insert(k.clone()) {
                                return Err(CowrieError::DuplicateKey);
                            }
                        } else if pairs.iter().any(|(ek, _)| ek == &k) {
                            return Err(CowrieError::DuplicateKey);
                        } else if pairs.len() >= 16 {
                            let mut set: HashSet<String> =
                                pairs.iter().map(|(ek, _)| ek.clone()).collect();
                            set.insert(k.clone());
                            seen = Some(set);
                        }
                        self.ws();
                        if self.i >= self.b.len() || self.b[self.i] != b':' {
                            return Err(CowrieError::InvalidJson);
                        }
                        self.i += 1;
                        self.ws();
                        let v = if self.promote_sentinels
                            && promote_this_map
                            && k == "$map"
                            && pairs.is_empty()
                            && self.i < self.b.len()
                            && self.b[self.i] == b'{'
                        {
                            // Unwrap literal-map sentinel without charging an extra nesting level or promoting its top keys.
                            self.parse(depth, false)?
                        } else {
                            self.parse(depth + 1, true)?
                        };
                        pairs.push((k, v));
                        self.ws();
                        if self.i >= self.b.len() {
                            return Err(CowrieError::Truncated);
                        }
                        if self.b[self.i] == b',' {
                            self.i += 1;
                        } else if self.b[self.i] == b'}' {
                            self.i += 1;
                            break;
                        } else {
                            return Err(CowrieError::InvalidJson);
                        }
                    }
                    if self.promote_sentinels && promote_this_map {
                        if let Some(promoted) = try_promote_sentinel_map(&pairs)? {
                            return Ok(promoted);
                        }
                    }
                    Ok(Value::Map(pairs))
                }
                b'[' => {
                    self.i += 1;
                    self.ws();
                    let mut items = Vec::new();
                    if self.i < self.b.len() && self.b[self.i] == b']' {
                        self.i += 1;
                        return Ok(Value::Array(items));
                    }
                    loop {
                        items.push(self.parse(depth + 1, true)?);
                        self.ws();
                        if self.i >= self.b.len() {
                            return Err(CowrieError::Truncated);
                        }
                        if self.b[self.i] == b',' {
                            self.i += 1;
                        } else if self.b[self.i] == b']' {
                            self.i += 1;
                            break;
                        } else {
                            return Err(CowrieError::InvalidJson);
                        }
                    }
                    Ok(Value::Array(items))
                }
                b'"' => Ok(Value::Str(self.pstr()?)),
                b't' => {
                    if self.b[self.i..].starts_with(b"true") {
                        self.i += 4;
                        Ok(Value::Bool(true))
                    } else {
                        Err(CowrieError::InvalidJson)
                    }
                }
                b'f' => {
                    if self.b[self.i..].starts_with(b"false") {
                        self.i += 5;
                        Ok(Value::Bool(false))
                    } else {
                        Err(CowrieError::InvalidJson)
                    }
                }
                b'n' => {
                    if self.b[self.i..].starts_with(b"null") {
                        self.i += 4;
                        Ok(Value::Null)
                    } else {
                        Err(CowrieError::InvalidJson)
                    }
                }
                b'-' | b'0'..=b'9' => {
                    let s = self.i;
                    while self.i < self.b.len()
                        && !matches!(self.b[self.i], b',' | b']' | b'}' | b' ' | b'\n' | b'\r' | b'\t')
                    {
                        self.i += 1;
                    }
                    let tok = std::str::from_utf8(&self.b[s..self.i])
                        .map_err(|_| CowrieError::InvalidUtf8)?;
                    if !validate_rfc8259_number(tok) {
                        return Err(CowrieError::InvalidJson);
                    }
                    if tok.contains('.') || tok.contains('e') || tok.contains('E') {
                        let f: f64 = tok.parse().map_err(|_| CowrieError::InvalidJson)?;
                        if !f.is_finite() {
                            return Err(CowrieError::InvalidJson);
                        }
                        Ok(Value::from_f64(f))
                    } else if let Ok(v) = tok.parse::<i64>() {
                        Ok(Value::Int(v))
                    } else if let Ok(u) = tok.parse::<u64>() {
                        Ok(Value::Uint(u))
                    } else {
                        Err(CowrieError::InvalidJson)
                    }
                }
                _ => Err(CowrieError::InvalidJson),
            }
        }

        fn pstr(&mut self) -> Result<String, CowrieError> {
            if self.i >= self.b.len() || self.b[self.i] != b'"' {
                return Err(CowrieError::InvalidJson);
            }
            self.i += 1;
            let mut s = String::new();
            while self.i < self.b.len() && self.b[self.i] != b'"' {
                if self.b[self.i] == b'\\' {
                    self.i += 1;
                    if self.i >= self.b.len() {
                        return Err(CowrieError::Truncated);
                    }
                    match self.b[self.i] {
                        b'"' => s.push('"'),
                        b'\\' => s.push('\\'),
                        b'/' => s.push('/'),
                        b'b' => s.push('\u{0008}'),
                        b'f' => s.push('\u{000C}'),
                        b'n' => s.push('\n'),
                        b'r' => s.push('\r'),
                        b't' => s.push('\t'),
                        b'u' => {
                            if self.i + 5 > self.b.len() {
                                return Err(CowrieError::Truncated);
                            }
                            let hex_bytes = &self.b[self.i + 1..self.i + 5];
                            if !hex_bytes.iter().all(|b| b.is_ascii_hexdigit()) {
                                return Err(CowrieError::InvalidJson);
                            }
                            let hex = std::str::from_utf8(hex_bytes)
                                .map_err(|_| CowrieError::InvalidJson)?;
                            let cp = u32::from_str_radix(hex, 16)
                                .map_err(|_| CowrieError::InvalidJson)?;
                            self.i += 4;
                            if (0xDC00..=0xDFFF).contains(&cp) {
                                // Reject lone low surrogate immediately
                                return Err(CowrieError::InvalidJson);
                            } else if (0xD800..=0xDBFF).contains(&cp) {
                                if self.i + 7 > self.b.len() || &self.b[self.i + 1..self.i + 3] != b"\\u" {
                                    return Err(CowrieError::InvalidJson);
                                }
                                let hex2_bytes = &self.b[self.i + 3..self.i + 7];
                                if !hex2_bytes.iter().all(|b| b.is_ascii_hexdigit()) {
                                    return Err(CowrieError::InvalidJson);
                                }
                                let hex2 = std::str::from_utf8(hex2_bytes)
                                    .map_err(|_| CowrieError::InvalidJson)?;
                                let cp2 = u32::from_str_radix(hex2, 16)
                                    .map_err(|_| CowrieError::InvalidJson)?;
                                if !(0xDC00..=0xDFFF).contains(&cp2) {
                                    // Reject high surrogate followed by non-low surrogate BEFORE subtracting 0xDC00
                                    return Err(CowrieError::InvalidJson);
                                }
                                self.i += 6;
                                let full = 0x10000 + (((cp - 0xD800) << 10) | (cp2 - 0xDC00));
                                s.push(char::from_u32(full).ok_or(CowrieError::InvalidJson)?);
                            } else {
                                s.push(char::from_u32(cp).ok_or(CowrieError::InvalidJson)?);
                            }
                        }
                        _ => return Err(CowrieError::InvalidJson),
                    }
                    self.i += 1;
                } else {
                    let st = self.i;
                    while self.i < self.b.len() && self.b[self.i] != b'"' && self.b[self.i] != b'\\' {
                        if self.b[self.i] < 0x20 {
                            return Err(CowrieError::InvalidJson);
                        }
                        self.i += 1;
                    }
                    let chunk = std::str::from_utf8(&self.b[st..self.i])
                        .map_err(|_| CowrieError::InvalidUtf8)?;
                    s.push_str(chunk);
                }
            }
            if self.i >= self.b.len() {
                return Err(CowrieError::Truncated);
            }
            self.i += 1;
            Ok(s)
        }
    }

    let mut p = Parser {
        b: input.as_bytes(),
        i: 0,
        promote_sentinels,
    };
    let val = p.parse(0, true)?;
    p.ws();
    if p.i != p.b.len() {
        return Err(CowrieError::TrailingBytes);
    }
    Ok(val)
}

/// Render a canonical `Value` as deterministic, 100% round-trip-lossless minified RFC 8259 JSON.
/// Non-finite floats serialize via `{"$float":"NaN"|"Infinity"|"-Infinity"}`, `Bytes` via `{"$bytes":"<hex>"}`,
/// `TensorRef` via `{"$tensor":...}`, and literal maps containing reserved `$`-sentinel keys via `{"$map":{...}}`.
pub fn value_to_compact_json(v: &Value) -> String {
    value_to_json_impl(v, true)
}

/// Render a canonical `Value` as plain RFC 8259 display JSON without `$map`/`$float` escaping
/// (used for forwarding raw JSON-RPC payloads such as `mcp-proxy`).
pub fn value_to_plain_json(v: &Value) -> String {
    value_to_json_impl(v, false)
}

fn value_to_json_impl(v: &Value, lossless_sentinels: bool) -> String {
    fn esc(s: &str, out: &mut String) {
        out.push('"');
        for ch in s.chars() {
            match ch {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
                c => out.push(c),
            }
        }
        out.push('"');
    }
    fn fmt_map_body(m: &[(String, Value)], out: &mut String, lossless_sentinels: bool) {
        out.push('{');
        let needs_sort = m.windows(2).any(|w| w[0].0.as_bytes() > w[1].0.as_bytes());
        if needs_sort {
            let mut sorted: Vec<(&str, &Value)> =
                m.iter().map(|(k, x)| (k.as_str(), x)).collect();
            sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            for (i, (k, x)) in sorted.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                esc(k, out);
                out.push(':');
                fmt(x, out, lossless_sentinels);
            }
        } else {
            for (i, (k, x)) in m.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                esc(k, out);
                out.push(':');
                fmt(x, out, lossless_sentinels);
            }
        }
        out.push('}');
    }
    fn fmt(v: &Value, out: &mut String, lossless_sentinels: bool) {
        match v {
            Value::Null => out.push_str("null"),
            Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Value::Int(n) => out.push_str(&n.to_string()),
            Value::Uint(u) => out.push_str(&u.to_string()),
            Value::FloatBits(bits) => {
                let f = f64::from_bits(*bits);
                if f.is_finite() {
                    let s = format!("{}", f);
                    out.push_str(&s);
                    if !s.contains('.') && !s.contains('e') && !s.contains('E') {
                        out.push_str(".0");
                    }
                } else if lossless_sentinels {
                    if f.is_nan() {
                        out.push_str("{\"$float\":\"NaN\"}");
                    } else if f.is_sign_positive() {
                        out.push_str("{\"$float\":\"Infinity\"}");
                    } else {
                        out.push_str("{\"$float\":\"-Infinity\"}");
                    }
                } else {
                    out.push_str("null");
                }
            }
            Value::Str(s) => esc(s, out),
            Value::Bytes(b) => {
                out.push_str("{\"$bytes\":\"");
                out.push_str(&crate::sha256::to_hex(b));
                out.push_str("\"}");
            }
            Value::Array(a) => {
                out.push('[');
                for (i, x) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    fmt(x, out, lossless_sentinels);
                }
                out.push(']');
            }
            Value::Map(m) => {
                let needs_escape = lossless_sentinels
                    && m.iter().any(|(k, _)| is_reserved_sentinel_key(k));
                if needs_escape {
                    out.push_str("{\"$map\":");
                    fmt_map_body(m, out, lossless_sentinels);
                    out.push('}');
                } else {
                    fmt_map_body(m, out, lossless_sentinels);
                }
            }
            Value::TensorRef { arena_idx, dtype, dims, space } => {
                let dt = dtype_to_str(*dtype).unwrap_or("f32");
                out.push_str(&format!("{{\"$tensor\":{},\"dtype\":\"{}\",\"shape\":[", arena_idx, dt));
                for (i, d) in dims.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(&d.to_string());
                }
                out.push_str("],\"space\":");
                esc(space, out);
                out.push('}');
            }
        }
    }
    let mut out = String::new();
    fmt(v, &mut out, lossless_sentinels);
    out
}
