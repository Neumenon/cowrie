//! Plane A: Bijective Canonical Core (`COWR`) encoder, strict hardened decoder,
//! and minimal JSON <-> Value converter.

use std::collections::{BTreeMap, HashSet};
use crate::CowrieError;

pub const MAGIC_FRAME: &[u8; 4] = b"COWR";
pub const VERSION: u8 = 1;
pub const MAX_DEPTH: usize = 64;

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

pub const CANON_QNAN_BITS: u64 = 0x7FF8_0000_0000_0000;

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

/// Encode a canonical `COWR` frame (bijective with `decode_frame_strict`).
pub fn encode_frame(val: &Value, authority: u8, epistemic: u8) -> Result<Vec<u8>, CowrieError> {
    let mut key_map: BTreeMap<&str, usize> = BTreeMap::new();
    collect_keys(val, &mut key_map, 0)?;

    let mut keys: Vec<&str> = Vec::with_capacity(key_map.len());
    for (idx, (k, slot)) in key_map.iter_mut().enumerate() {
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

/// Strict hardened decoder: enforces `MAX_DEPTH = 64`, byte-bounded allocations,
/// minimal varints, UTF-8 byte-sorted dict, zero unused keys, and zero trailing bytes.
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
    if buf[7] != 0 {
        return Err(CowrieError::NonCanonicalFlags);
    }

    let mut pos = 8usize;
    let nkeys_u64 = decode_uvarint_strict(buf, &mut pos)?;
    if nkeys_u64 > (buf.len() - pos) as u64 {
        return Err(CowrieError::AllocLimit);
    }
    let nkeys = nkeys_u64 as usize;

    let mut keys: Vec<&str> = Vec::with_capacity(nkeys.min(4096));
    let mut prev: Option<&[u8]> = None;
    for _ in 0..nkeys {
        let klen_u64 = decode_uvarint_strict(buf, &mut pos)?;
        if klen_u64 > (buf.len() - pos) as u64 {
            return Err(CowrieError::Truncated);
        }
        let klen = klen_u64 as usize;
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

    let mut used: HashSet<usize> = HashSet::with_capacity(keys.len().min(4096));

    fn dec(
        buf: &[u8],
        pos: &mut usize,
        keys: &[&str],
        used: &mut HashSet<usize>,
        depth: usize,
    ) -> Result<Value, CowrieError> {
        if depth > MAX_DEPTH {
            return Err(CowrieError::MaxDepth);
        }
        if *pos >= buf.len() {
            return Err(CowrieError::Truncated);
        }
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
                    v.push(dec(buf, pos, keys, used, depth + 1)?);
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
                    let ki = decode_uvarint_strict(buf, pos)? as usize;
                    if ki >= keys.len() {
                        return Err(CowrieError::BadDictIndex);
                    }
                    if ki as isize == prev_idx {
                        return Err(CowrieError::DuplicateKey);
                    }
                    if (ki as isize) < prev_idx {
                        return Err(CowrieError::UnsortedMapKeys);
                    }
                    prev_idx = ki as isize;
                    used.insert(ki);
                    let val = dec(buf, pos, keys, used, depth + 1)?;
                    v.push((keys[ki].to_string(), val));
                }
                Ok(Value::Map(v))
            }
            _ => Err(CowrieError::ReservedTag),
        }
    }

    let value = dec(buf, &mut pos, &keys, &mut used, 0)?;
    if pos != buf.len() {
        return Err(CowrieError::TrailingBytes);
    }
    if used.len() != keys.len() {
        return Err(CowrieError::UnusedDictKey);
    }
    Ok(DecodedFrame { value, authority, epistemic })
}

/// Zero-dependency strict JSON parser -> canonical `Value` (for CLI `cowrie pack` & FFI).
pub fn parse_json_to_value(input: &str) -> Result<Value, CowrieError> {
    struct Parser<'a> {
        b: &'a [u8],
        i: usize,
    }
    impl<'a> Parser<'a> {
        fn ws(&mut self) {
            while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\n' | b'\r' | b'\t') {
                self.i += 1;
            }
        }
        fn parse(&mut self, depth: usize) -> Result<Value, CowrieError> {
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
                    let mut pairs = Vec::new();
                    if self.i < self.b.len() && self.b[self.i] == b'}' {
                        self.i += 1;
                        return Ok(Value::Map(pairs));
                    }
                    loop {
                        self.ws();
                        let k = self.pstr()?;
                        self.ws();
                        if self.i >= self.b.len() || self.b[self.i] != b':' {
                            return Err(CowrieError::InvalidJson);
                        }
                        self.i += 1;
                        let v = self.parse(depth + 1)?;
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
                        items.push(self.parse(depth + 1)?);
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
                    if tok.contains('.') || tok.contains('e') || tok.contains('E') {
                        let f: f64 = tok.parse().map_err(|_| CowrieError::InvalidJson)?;
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
                            let hex = std::str::from_utf8(&self.b[self.i + 1..self.i + 5])
                                .map_err(|_| CowrieError::InvalidJson)?;
                            let cp = u32::from_str_radix(hex, 16)
                                .map_err(|_| CowrieError::InvalidJson)?;
                            self.i += 4;
                            if (0xD800..=0xDBFF).contains(&cp) {
                                if self.i + 7 > self.b.len() || &self.b[self.i + 1..self.i + 3] != b"\\u" {
                                    return Err(CowrieError::InvalidJson);
                                }
                                let hex2 = std::str::from_utf8(&self.b[self.i + 3..self.i + 7])
                                    .map_err(|_| CowrieError::InvalidJson)?;
                                let cp2 = u32::from_str_radix(hex2, 16)
                                    .map_err(|_| CowrieError::InvalidJson)?;
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

    let mut p = Parser { b: input.as_bytes(), i: 0 };
    let val = p.parse(0)?;
    p.ws();
    if p.i != p.b.len() {
        return Err(CowrieError::TrailingBytes);
    }
    Ok(val)
}

/// Render a canonical `Value` as deterministic minified JSON.
pub fn value_to_compact_json(v: &Value) -> String {
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
    fn fmt(v: &Value, out: &mut String) {
        match v {
            Value::Null => out.push_str("null"),
            Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Value::Int(n) => out.push_str(&n.to_string()),
            Value::Uint(u) => out.push_str(&u.to_string()),
            Value::FloatBits(bits) => {
                let f = f64::from_bits(*bits);
                if f.is_finite() {
                    out.push_str(&format!("{}", f));
                } else {
                    out.push_str("null");
                }
            }
            Value::Str(s) => esc(s, out),
            Value::Bytes(b) => {
                out.push_str("\"hex:");
                out.push_str(&crate::sha256::to_hex(b));
                out.push('"');
            }
            Value::Array(a) => {
                out.push('[');
                for (i, x) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    fmt(x, out);
                }
                out.push(']');
            }
            Value::Map(m) => {
                out.push('{');
                for (i, (k, x)) in m.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    esc(k, out);
                    out.push(':');
                    fmt(x, out);
                }
                out.push('}');
            }
        }
    }
    let mut out = String::new();
    fmt(v, &mut out);
    out
}
