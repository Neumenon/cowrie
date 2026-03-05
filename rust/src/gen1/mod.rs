//! Gen1: Lightweight binary JSON codec with proto-tensor support.
//!
//! Gen1 provides a compact binary JSON format that's simpler than Gen2:
//! - 11 core types (null, bool, int64, float64, string, bytes, object, arrays)
//! - Proto-tensor support for efficient numeric arrays
//! - 6 graph types (Node, Edge, AdjList, batches)
//! - No dictionary coding (simpler implementation)
//! - No compression
//!
//! Use Gen1 when you need a lightweight codec without the full Gen2 complexity.
//!
//! # Example
//! ```
//! use cowrie_rs::gen1::{encode, decode, Value};
//!
//! let val = Value::Object(vec![
//!     ("name".to_string(), Value::String("test".to_string())),
//!     ("scores".to_string(), Value::Float64Array(vec![1.0, 2.0, 3.0])),
//! ]);
//!
//! let encoded = encode(&val).unwrap();
//! let decoded = decode(&encoded).unwrap();
//! ```

use std::collections::HashMap;
use std::io::{self, Read, Write};

/// Gen1 type tags
pub mod tags {
    pub const NULL: u8 = 0x00;
    pub const FALSE: u8 = 0x01;
    pub const TRUE: u8 = 0x02;
    pub const INT64: u8 = 0x03;
    pub const FLOAT64: u8 = 0x04;
    pub const STRING: u8 = 0x05;
    pub const BYTES: u8 = 0x06;
    pub const ARRAY: u8 = 0x07;
    pub const OBJECT: u8 = 0x08;
    // Proto-tensor types
    pub const INT64_ARRAY: u8 = 0x09;
    pub const FLOAT64_ARRAY: u8 = 0x0A;
    pub const STRING_ARRAY: u8 = 0x0B;
    // Graph types
    pub const NODE: u8 = 0x10;
    pub const EDGE: u8 = 0x11;
    pub const ADJLIST: u8 = 0x12;
    pub const NODE_BATCH: u8 = 0x13;
    pub const EDGE_BATCH: u8 = 0x14;
    pub const GRAPH_SHARD: u8 = 0x15;
}

/// Security limits - prevent DoS from malicious input
pub mod limits {
    pub const MAX_DEPTH: usize = 1000;
    pub const MAX_ARRAY_LEN: usize = 100_000_000;  // 100M elements
    pub const MAX_OBJECT_LEN: usize = 10_000_000;  // 10M fields
    pub const MAX_STRING_LEN: usize = 500_000_000; // 500MB
    pub const MAX_BYTES_LEN: usize = 1_000_000_000; // 1GB
}

/// Gen1 Error type
#[derive(Debug, Clone)]
pub enum Gen1Error {
    InvalidTag(u8),
    UnexpectedEof,
    InvalidUtf8,
    IoError(String),
    MaxDepthExceeded,
    MaxArrayLen,
    MaxObjectLen,
    MaxStringLen,
    MaxBytesLen,
}

impl std::fmt::Display for Gen1Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Gen1Error::InvalidTag(tag) => write!(f, "Invalid tag: 0x{:02x}", tag),
            Gen1Error::UnexpectedEof => write!(f, "Unexpected end of data"),
            Gen1Error::InvalidUtf8 => write!(f, "Invalid UTF-8 string"),
            Gen1Error::IoError(msg) => write!(f, "IO error: {}", msg),
            Gen1Error::MaxDepthExceeded => write!(f, "Maximum nesting depth exceeded"),
            Gen1Error::MaxArrayLen => write!(f, "Array too large"),
            Gen1Error::MaxObjectLen => write!(f, "Object has too many fields"),
            Gen1Error::MaxStringLen => write!(f, "String too long"),
            Gen1Error::MaxBytesLen => write!(f, "Bytes too long"),
        }
    }
}

impl std::error::Error for Gen1Error {}

impl From<io::Error> for Gen1Error {
    fn from(err: io::Error) -> Self {
        Gen1Error::IoError(err.to_string())
    }
}

/// Gen1 Value type
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Object(Vec<(String, Value)>),
    // Proto-tensor types
    Int64Array(Vec<i64>),
    Float64Array(Vec<f64>),
    StringArray(Vec<String>),
    // Graph types
    Node { id: i64, label: String, properties: HashMap<String, Value> },
    Edge { src: i64, dst: i64, label: String, properties: HashMap<String, Value> },
    AdjList { id_width: u8, node_count: u64, edge_count: u64, row_offsets: Vec<u64>, col_indices: Vec<u8> },
    NodeBatch(Vec<Value>),
    EdgeBatch(Vec<Value>),
    GraphShard { nodes: Vec<Value>, edges: Vec<Value>, meta: HashMap<String, Value> },
}

/// Encode a Gen1 value to bytes.
pub fn encode(val: &Value) -> Result<Vec<u8>, Gen1Error> {
    let mut buf = Vec::new();
    encode_value(&mut buf, val)?;
    Ok(buf)
}

/// Decode Gen1 bytes to a value.
pub fn decode(data: &[u8]) -> Result<Value, Gen1Error> {
    let mut cursor = io::Cursor::new(data);
    decode_value_depth(&mut cursor, 0)
}

// Helper: Write uvarint
fn write_uvarint<W: Write>(w: &mut W, mut n: u64) -> Result<(), Gen1Error> {
    loop {
        let mut byte = (n & 0x7F) as u8;
        n >>= 7;
        if n != 0 {
            byte |= 0x80;
        }
        w.write_all(&[byte])?;
        if n == 0 {
            break;
        }
    }
    Ok(())
}

// Helper: Read uvarint
fn read_uvarint<R: Read>(r: &mut R) -> Result<u64, Gen1Error> {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        let mut byte = [0u8; 1];
        if r.read(&mut byte)? == 0 {
            return Err(Gen1Error::UnexpectedEof);
        }
        result |= ((byte[0] & 0x7F) as u64) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 63 {
            return Err(Gen1Error::IoError("varint overflow".into()));
        }
    }
    Ok(result)
}

// Helper: Zigzag encode
fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

// Helper: Zigzag decode
fn zigzag_decode(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

fn encode_value<W: Write>(w: &mut W, val: &Value) -> Result<(), Gen1Error> {
    match val {
        Value::Null => {
            w.write_all(&[tags::NULL])?;
        }
        Value::Bool(false) => {
            w.write_all(&[tags::FALSE])?;
        }
        Value::Bool(true) => {
            w.write_all(&[tags::TRUE])?;
        }
        Value::Int64(n) => {
            w.write_all(&[tags::INT64])?;
            write_uvarint(w, zigzag_encode(*n))?;
        }
        Value::Float64(f) => {
            w.write_all(&[tags::FLOAT64])?;
            w.write_all(&f.to_le_bytes())?;
        }
        Value::String(s) => {
            w.write_all(&[tags::STRING])?;
            write_uvarint(w, s.len() as u64)?;
            w.write_all(s.as_bytes())?;
        }
        Value::Bytes(b) => {
            w.write_all(&[tags::BYTES])?;
            write_uvarint(w, b.len() as u64)?;
            w.write_all(b)?;
        }
        Value::Array(arr) => {
            w.write_all(&[tags::ARRAY])?;
            write_uvarint(w, arr.len() as u64)?;
            for item in arr {
                encode_value(w, item)?;
            }
        }
        Value::Object(members) => {
            w.write_all(&[tags::OBJECT])?;
            write_uvarint(w, members.len() as u64)?;
            for (key, val) in members {
                write_uvarint(w, key.len() as u64)?;
                w.write_all(key.as_bytes())?;
                encode_value(w, val)?;
            }
        }
        Value::Int64Array(arr) => {
            w.write_all(&[tags::INT64_ARRAY])?;
            write_uvarint(w, arr.len() as u64)?;
            for n in arr {
                w.write_all(&n.to_le_bytes())?;
            }
        }
        Value::Float64Array(arr) => {
            w.write_all(&[tags::FLOAT64_ARRAY])?;
            write_uvarint(w, arr.len() as u64)?;
            for f in arr {
                w.write_all(&f.to_le_bytes())?;
            }
        }
        Value::StringArray(arr) => {
            w.write_all(&[tags::STRING_ARRAY])?;
            write_uvarint(w, arr.len() as u64)?;
            for s in arr {
                write_uvarint(w, s.len() as u64)?;
                w.write_all(s.as_bytes())?;
            }
        }
        // Graph types - simplified encoding
        Value::Node { id, label, properties } => {
            w.write_all(&[tags::NODE])?;
            write_uvarint(w, zigzag_encode(*id))?;
            write_uvarint(w, label.len() as u64)?;
            w.write_all(label.as_bytes())?;
            write_uvarint(w, properties.len() as u64)?;
            for (k, v) in properties {
                write_uvarint(w, k.len() as u64)?;
                w.write_all(k.as_bytes())?;
                encode_value(w, v)?;
            }
        }
        Value::Edge { src, dst, label, properties } => {
            w.write_all(&[tags::EDGE])?;
            write_uvarint(w, zigzag_encode(*src))?;
            write_uvarint(w, zigzag_encode(*dst))?;
            write_uvarint(w, label.len() as u64)?;
            w.write_all(label.as_bytes())?;
            write_uvarint(w, properties.len() as u64)?;
            for (k, v) in properties {
                write_uvarint(w, k.len() as u64)?;
                w.write_all(k.as_bytes())?;
                encode_value(w, v)?;
            }
        }
        Value::AdjList { id_width, node_count, edge_count, row_offsets, col_indices } => {
            w.write_all(&[tags::ADJLIST])?;
            w.write_all(&[*id_width])?;
            write_uvarint(w, *node_count)?;
            write_uvarint(w, *edge_count)?;
            for offset in row_offsets {
                write_uvarint(w, *offset)?;
            }
            w.write_all(col_indices)?;
        }
        Value::NodeBatch(nodes) => {
            w.write_all(&[tags::NODE_BATCH])?;
            write_uvarint(w, nodes.len() as u64)?;
            for node in nodes {
                encode_value(w, node)?;
            }
        }
        Value::EdgeBatch(edges) => {
            w.write_all(&[tags::EDGE_BATCH])?;
            write_uvarint(w, edges.len() as u64)?;
            for edge in edges {
                encode_value(w, edge)?;
            }
        }
        Value::GraphShard { nodes, edges, meta } => {
            w.write_all(&[tags::GRAPH_SHARD])?;
            // Write nodes
            write_uvarint(w, nodes.len() as u64)?;
            for node in nodes {
                encode_value(w, node)?;
            }
            // Write edges
            write_uvarint(w, edges.len() as u64)?;
            for edge in edges {
                encode_value(w, edge)?;
            }
            // Write meta
            write_uvarint(w, meta.len() as u64)?;
            for (k, v) in meta {
                write_uvarint(w, k.len() as u64)?;
                w.write_all(k.as_bytes())?;
                encode_value(w, v)?;
            }
        }
    }
    Ok(())
}

fn decode_value_depth<R: Read>(r: &mut R, depth: usize) -> Result<Value, Gen1Error> {
    // Security: check depth limit
    if depth > limits::MAX_DEPTH {
        return Err(Gen1Error::MaxDepthExceeded);
    }

    let mut tag = [0u8; 1];
    if r.read(&mut tag)? == 0 {
        return Err(Gen1Error::UnexpectedEof);
    }

    match tag[0] {
        tags::NULL => Ok(Value::Null),
        tags::FALSE => Ok(Value::Bool(false)),
        tags::TRUE => Ok(Value::Bool(true)),
        tags::INT64 => {
            let n = read_uvarint(r)?;
            Ok(Value::Int64(zigzag_decode(n)))
        }
        tags::FLOAT64 => {
            let mut buf = [0u8; 8];
            r.read_exact(&mut buf)?;
            Ok(Value::Float64(f64::from_le_bytes(buf)))
        }
        tags::STRING => {
            let len = read_uvarint(r)? as usize;
            if len > limits::MAX_STRING_LEN {
                return Err(Gen1Error::MaxStringLen);
            }
            let mut buf = vec![0u8; len];
            r.read_exact(&mut buf)?;
            String::from_utf8(buf)
                .map(Value::String)
                .map_err(|_| Gen1Error::InvalidUtf8)
        }
        tags::BYTES => {
            let len = read_uvarint(r)? as usize;
            if len > limits::MAX_BYTES_LEN {
                return Err(Gen1Error::MaxBytesLen);
            }
            let mut buf = vec![0u8; len];
            r.read_exact(&mut buf)?;
            Ok(Value::Bytes(buf))
        }
        tags::ARRAY => {
            let count = read_uvarint(r)? as usize;
            if count > limits::MAX_ARRAY_LEN {
                return Err(Gen1Error::MaxArrayLen);
            }
            let mut arr = Vec::with_capacity(count);
            for _ in 0..count {
                arr.push(decode_value_depth(r, depth + 1)?);
            }
            Ok(Value::Array(arr))
        }
        tags::OBJECT => {
            let count = read_uvarint(r)? as usize;
            if count > limits::MAX_OBJECT_LEN {
                return Err(Gen1Error::MaxObjectLen);
            }
            let mut members = Vec::with_capacity(count);
            for _ in 0..count {
                let key_len = read_uvarint(r)? as usize;
                if key_len > limits::MAX_STRING_LEN {
                    return Err(Gen1Error::MaxStringLen);
                }
                let mut key_buf = vec![0u8; key_len];
                r.read_exact(&mut key_buf)?;
                let key = String::from_utf8(key_buf).map_err(|_| Gen1Error::InvalidUtf8)?;
                let val = decode_value_depth(r, depth + 1)?;
                members.push((key, val));
            }
            Ok(Value::Object(members))
        }
        tags::INT64_ARRAY => {
            let count = read_uvarint(r)? as usize;
            if count > limits::MAX_ARRAY_LEN {
                return Err(Gen1Error::MaxArrayLen);
            }
            let mut arr = Vec::with_capacity(count);
            for _ in 0..count {
                let mut buf = [0u8; 8];
                r.read_exact(&mut buf)?;
                arr.push(i64::from_le_bytes(buf));
            }
            Ok(Value::Int64Array(arr))
        }
        tags::FLOAT64_ARRAY => {
            let count = read_uvarint(r)? as usize;
            if count > limits::MAX_ARRAY_LEN {
                return Err(Gen1Error::MaxArrayLen);
            }
            let mut arr = Vec::with_capacity(count);
            for _ in 0..count {
                let mut buf = [0u8; 8];
                r.read_exact(&mut buf)?;
                arr.push(f64::from_le_bytes(buf));
            }
            Ok(Value::Float64Array(arr))
        }
        tags::STRING_ARRAY => {
            let count = read_uvarint(r)? as usize;
            if count > limits::MAX_ARRAY_LEN {
                return Err(Gen1Error::MaxArrayLen);
            }
            let mut arr = Vec::with_capacity(count);
            for _ in 0..count {
                let len = read_uvarint(r)? as usize;
                if len > limits::MAX_STRING_LEN {
                    return Err(Gen1Error::MaxStringLen);
                }
                let mut buf = vec![0u8; len];
                r.read_exact(&mut buf)?;
                arr.push(String::from_utf8(buf).map_err(|_| Gen1Error::InvalidUtf8)?);
            }
            Ok(Value::StringArray(arr))
        }
        // Graph types - simplified decoding
        tags::NODE => {
            let id = zigzag_decode(read_uvarint(r)?);
            let label_len = read_uvarint(r)? as usize;
            if label_len > limits::MAX_STRING_LEN {
                return Err(Gen1Error::MaxStringLen);
            }
            let mut label_buf = vec![0u8; label_len];
            r.read_exact(&mut label_buf)?;
            let label = String::from_utf8(label_buf).map_err(|_| Gen1Error::InvalidUtf8)?;
            let prop_count = read_uvarint(r)? as usize;
            if prop_count > limits::MAX_OBJECT_LEN {
                return Err(Gen1Error::MaxObjectLen);
            }
            let mut properties = HashMap::with_capacity(prop_count);
            for _ in 0..prop_count {
                let key_len = read_uvarint(r)? as usize;
                if key_len > limits::MAX_STRING_LEN {
                    return Err(Gen1Error::MaxStringLen);
                }
                let mut key_buf = vec![0u8; key_len];
                r.read_exact(&mut key_buf)?;
                let key = String::from_utf8(key_buf).map_err(|_| Gen1Error::InvalidUtf8)?;
                let val = decode_value_depth(r, depth + 1)?;
                properties.insert(key, val);
            }
            Ok(Value::Node { id, label, properties })
        }
        tags::EDGE => {
            let src = zigzag_decode(read_uvarint(r)?);
            let dst = zigzag_decode(read_uvarint(r)?);
            let label_len = read_uvarint(r)? as usize;
            if label_len > limits::MAX_STRING_LEN {
                return Err(Gen1Error::MaxStringLen);
            }
            let mut label_buf = vec![0u8; label_len];
            r.read_exact(&mut label_buf)?;
            let label = String::from_utf8(label_buf).map_err(|_| Gen1Error::InvalidUtf8)?;
            let prop_count = read_uvarint(r)? as usize;
            if prop_count > limits::MAX_OBJECT_LEN {
                return Err(Gen1Error::MaxObjectLen);
            }
            let mut properties = HashMap::with_capacity(prop_count);
            for _ in 0..prop_count {
                let key_len = read_uvarint(r)? as usize;
                if key_len > limits::MAX_STRING_LEN {
                    return Err(Gen1Error::MaxStringLen);
                }
                let mut key_buf = vec![0u8; key_len];
                r.read_exact(&mut key_buf)?;
                let key = String::from_utf8(key_buf).map_err(|_| Gen1Error::InvalidUtf8)?;
                let val = decode_value_depth(r, depth + 1)?;
                properties.insert(key, val);
            }
            Ok(Value::Edge { src, dst, label, properties })
        }
        tags::ADJLIST => {
            let mut id_width_buf = [0u8; 1];
            r.read_exact(&mut id_width_buf)?;
            let id_width = id_width_buf[0];
            let node_count = read_uvarint(r)?;
            let edge_count = read_uvarint(r)?;
            let mut row_offsets = Vec::with_capacity((node_count + 1) as usize);
            for _ in 0..=node_count {
                row_offsets.push(read_uvarint(r)?);
            }
            let col_bytes_len = if id_width == 1 { edge_count as usize * 4 } else { edge_count as usize * 8 };
            let mut col_indices = vec![0u8; col_bytes_len];
            r.read_exact(&mut col_indices)?;
            Ok(Value::AdjList { id_width, node_count, edge_count, row_offsets, col_indices })
        }
        tags::NODE_BATCH => {
            let count = read_uvarint(r)? as usize;
            if count > limits::MAX_ARRAY_LEN {
                return Err(Gen1Error::MaxArrayLen);
            }
            let mut nodes = Vec::with_capacity(count);
            for _ in 0..count {
                nodes.push(decode_value_depth(r, depth + 1)?);
            }
            Ok(Value::NodeBatch(nodes))
        }
        tags::EDGE_BATCH => {
            let count = read_uvarint(r)? as usize;
            if count > limits::MAX_ARRAY_LEN {
                return Err(Gen1Error::MaxArrayLen);
            }
            let mut edges = Vec::with_capacity(count);
            for _ in 0..count {
                edges.push(decode_value_depth(r, depth + 1)?);
            }
            Ok(Value::EdgeBatch(edges))
        }
        tags::GRAPH_SHARD => {
            let node_count = read_uvarint(r)? as usize;
            if node_count > limits::MAX_ARRAY_LEN {
                return Err(Gen1Error::MaxArrayLen);
            }
            let mut nodes = Vec::with_capacity(node_count);
            for _ in 0..node_count {
                nodes.push(decode_value_depth(r, depth + 1)?);
            }
            let edge_count = read_uvarint(r)? as usize;
            if edge_count > limits::MAX_ARRAY_LEN {
                return Err(Gen1Error::MaxArrayLen);
            }
            let mut edges = Vec::with_capacity(edge_count);
            for _ in 0..edge_count {
                edges.push(decode_value_depth(r, depth + 1)?);
            }
            let meta_count = read_uvarint(r)? as usize;
            if meta_count > limits::MAX_OBJECT_LEN {
                return Err(Gen1Error::MaxObjectLen);
            }
            let mut meta = HashMap::with_capacity(meta_count);
            for _ in 0..meta_count {
                let key_len = read_uvarint(r)? as usize;
                if key_len > limits::MAX_STRING_LEN {
                    return Err(Gen1Error::MaxStringLen);
                }
                let mut key_buf = vec![0u8; key_len];
                r.read_exact(&mut key_buf)?;
                let key = String::from_utf8(key_buf).map_err(|_| Gen1Error::InvalidUtf8)?;
                let val = decode_value_depth(r, depth + 1)?;
                meta.insert(key, val);
            }
            Ok(Value::GraphShard { nodes, edges, meta })
        }
        _ => Err(Gen1Error::InvalidTag(tag[0])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_basic() {
        let val = Value::Object(vec![
            ("name".to_string(), Value::String("test".to_string())),
            ("count".to_string(), Value::Int64(42)),
        ]);

        let encoded = encode(&val).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        assert_eq!(val, decoded);
    }

    #[test]
    fn test_proto_tensor_float64() {
        let val = Value::Float64Array(vec![1.0, 2.5, 3.14159, -42.0]);
        let encoded = encode(&val).expect("encode");
        let decoded = decode(&encoded).expect("decode");
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_proto_tensor_int64() {
        let val = Value::Int64Array(vec![1, -2, 1000000, -999999]);
        let encoded = encode(&val).expect("encode");
        let decoded = decode(&encoded).expect("decode");
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_empty_array() {
        let val = Value::Array(vec![]);
        let encoded = encode(&val).expect("encode");
        let decoded = decode(&encoded).expect("decode");
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_empty_object() {
        let val = Value::Object(vec![]);
        let encoded = encode(&val).expect("encode");
        let decoded = decode(&encoded).expect("decode");
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_empty_string() {
        let val = Value::String("".to_string());
        let encoded = encode(&val).expect("encode");
        let decoded = decode(&encoded).expect("decode");
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_empty_bytes() {
        let val = Value::Bytes(vec![]);
        let encoded = encode(&val).expect("encode");
        let decoded = decode(&encoded).expect("decode");
        assert_eq!(val, decoded);
    }
}
