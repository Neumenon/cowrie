//! Cowrie encoder.

use super::tags;
use super::types::{CowrieError, EdgeData, NodeData, Value};
use crate::{MAGIC, VERSION};
use std::collections::{BTreeMap, HashMap};

/// Canonical alignment for tensor data (SPEC-v1 §2.5): a tensor's data run begins at a 64-byte
/// boundary relative to byte 0 of the message, with the gap filled by canonical zero padding.
const TENSOR_ALIGN: usize = 64;

/// Encoding options.
#[derive(Debug, Clone, Default)]
pub struct EncodeOptions {
    /// Omit null values from objects.
    pub omit_null: bool,
    /// Sort object keys (always true since we use BTreeMap).
    pub deterministic: bool,
}

/// Encode a value to Cowrie bytes.
pub fn encode(value: &Value) -> Result<Vec<u8>, CowrieError> {
    encode_with_options(value, &EncodeOptions::default())
}

/// Encode a value with options.
pub fn encode_with_options(value: &Value, opts: &EncodeOptions) -> Result<Vec<u8>, CowrieError> {
    let mut buf = Vec::with_capacity(256);

    // Build dictionary
    let dict = build_dictionary(value, opts);

    // Build O(1) lookup map for dictionary indices
    let dict_map: HashMap<&str, usize> = dict
        .iter()
        .enumerate()
        .map(|(i, k)| (k.as_str(), i))
        .collect();

    // Write header
    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
    buf.push(0); // flags

    // Write dictionary
    write_uvarint(&mut buf, dict.len() as u64);
    for key in &dict {
        write_string(&mut buf, key);
    }

    // Write root value
    encode_value(&mut buf, value, &dict_map, opts)?;

    Ok(buf)
}

/// Build dictionary of object keys.
fn build_dictionary(value: &Value, opts: &EncodeOptions) -> Vec<String> {
    let mut keys = Vec::new();
    let mut seen = std::collections::HashSet::new();
    collect_keys(value, &mut keys, &mut seen, opts);
    // Global byte-sorted canonical dictionary order (SPEC-v1 §2.4), not DFS-discovery order.
    keys.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    keys
}

fn collect_keys(
    value: &Value,
    keys: &mut Vec<String>,
    seen: &mut std::collections::HashSet<String>,
    opts: &EncodeOptions,
) {
    match value {
        Value::Array(arr) => {
            for item in arr {
                collect_keys(item, keys, seen, opts);
            }
        }
        Value::Object(obj) => {
            for (k, v) in obj {
                // Skip null values if omit_null is set
                if opts.omit_null && matches!(v, Value::Null) {
                    continue;
                }
                if !seen.contains(k) {
                    seen.insert(k.clone());
                    keys.push(k.clone());
                }
                collect_keys(v, keys, seen, opts);
            }
        }
        // Graph types - collect property keys
        Value::Node(node) => {
            collect_props_keys(&node.props, keys, seen, opts);
        }
        Value::Edge(edge) => {
            collect_props_keys(&edge.props, keys, seen, opts);
        }
        Value::NodeBatch(batch) => {
            for node in &batch.nodes {
                collect_props_keys(&node.props, keys, seen, opts);
            }
        }
        Value::EdgeBatch(batch) => {
            for edge in &batch.edges {
                collect_props_keys(&edge.props, keys, seen, opts);
            }
        }
        _ => {}
    }
}

/// Helper to collect keys from a properties map.
fn collect_props_keys(
    props: &BTreeMap<String, Value>,
    keys: &mut Vec<String>,
    seen: &mut std::collections::HashSet<String>,
    opts: &EncodeOptions,
) {
    for (k, v) in props {
        if opts.omit_null && matches!(v, Value::Null) {
            continue;
        }
        if !seen.contains(k) {
            seen.insert(k.clone());
            keys.push(k.clone());
        }
        collect_keys(v, keys, seen, opts);
    }
}

fn encode_value(
    buf: &mut Vec<u8>,
    value: &Value,
    dict: &HashMap<&str, usize>,
    opts: &EncodeOptions,
) -> Result<(), CowrieError> {
    match value {
        Value::Null => {
            buf.push(tags::NULL);
        }
        Value::Bool(true) => {
            buf.push(tags::TRUE);
        }
        Value::Bool(false) => {
            buf.push(tags::FALSE);
        }
        Value::Int(i) => {
            let v = *i;
            if (0..=127).contains(&v) {
                buf.push(tags::FIXINT_BASE + v as u8);
            } else if (-16..=-1).contains(&v) {
                buf.push(tags::FIXNEG_BASE + (-1 - v) as u8);
            } else {
                buf.push(tags::INT64);
                write_uvarint(buf, zigzag_encode(v));
            }
        }
        Value::Uint(u) => {
            buf.push(tags::UINT64);
            write_uvarint(buf, *u);
        }
        Value::Float(f) => {
            buf.push(tags::FLOAT64);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Decimal(data) => {
            buf.push(tags::DECIMAL128);
            buf.extend_from_slice(data);
        }
        Value::String(s) => {
            buf.push(tags::STRING);
            write_string(buf, s);
        }
        Value::Bytes(b) => {
            buf.push(tags::BYTES);
            write_uvarint(buf, b.len() as u64);
            buf.extend_from_slice(b);
        }
        Value::DateTime(dt) => {
            buf.push(tags::DATETIME64);
            buf.extend_from_slice(&dt.to_le_bytes());
        }
        Value::Uuid(uuid) => {
            buf.push(tags::UUID128);
            buf.extend_from_slice(uuid);
        }
        Value::BigInt(data) => {
            buf.push(tags::BIGINT);
            write_uvarint(buf, data.len() as u64);
            buf.extend_from_slice(data);
        }
        Value::Array(arr) => {
            let len = arr.len();
            if len <= 15 {
                buf.push(tags::FIXARRAY_BASE + len as u8);
            } else {
                buf.push(tags::ARRAY);
                write_uvarint(buf, len as u64);
            }
            for item in arr {
                encode_value(buf, item, dict, opts)?;
            }
        }
        Value::Object(obj) => {
            // Filter nulls if omit_null is set
            let filtered: Vec<_> = if opts.omit_null {
                obj.iter()
                    .filter(|(_, v)| !matches!(v, Value::Null))
                    .collect()
            } else {
                obj.iter().collect()
            };

            let flen = filtered.len();
            if flen <= 15 {
                buf.push(tags::FIXMAP_BASE + flen as u8);
            } else {
                buf.push(tags::OBJECT);
                write_uvarint(buf, flen as u64);
            }

            for (key, val) in filtered {
                // O(1) key index lookup
                let idx = *dict.get(key.as_str()).expect("key should be in dictionary");
                write_uvarint(buf, idx as u64);
                encode_value(buf, val, dict, opts)?;
            }
        }
        Value::Tensor(t) => {
            // Validate dataLen against shape/dtype on encode too, so we never emit
            // bytes the decoder would reject (symmetric with decode).
            if let Some(expected) = super::decode::tensor_expected_bytes(t.dtype, &t.shape) {
                if t.data.len() != expected {
                    return Err(CowrieError::InvalidData(format!(
                        "tensor dataLen {} does not match shape/dtype (expected {} bytes)",
                        t.data.len(),
                        expected
                    )));
                }
            }
            buf.push(tags::TENSOR);
            buf.push(t.dtype as u8);
            buf.push(t.shape.len() as u8);
            for dim in &t.shape {
                write_uvarint(buf, *dim);
            }
            write_uvarint(buf, t.data.len() as u64);
            // Tensor data MUST begin at a 64-byte boundary relative to byte 0 of the message
            // (SPEC-v1 §2.5). The encoder builds the message top-down into a single `buf` that
            // starts with the COWR header, so `buf.len()` here IS the absolute byte offset right
            // after the dataLen uvarint — exactly the Python reference's `base + len(out)`.
            let pad = (TENSOR_ALIGN - (buf.len() % TENSOR_ALIGN)) % TENSOR_ALIGN;
            buf.extend(std::iter::repeat_n(0u8, pad));
            buf.extend_from_slice(&t.data);
        }

        Value::TensorRef(r) => {
            buf.push(tags::TENSOR_REF);
            buf.push(r.store_id);
            write_uvarint(buf, r.key.len() as u64);
            buf.extend_from_slice(&r.key);
        }
        Value::Image(img) => {
            buf.push(tags::IMAGE);
            buf.push(img.format as u8);
            buf.extend_from_slice(&img.width.to_le_bytes());
            buf.extend_from_slice(&img.height.to_le_bytes());
            write_uvarint(buf, img.data.len() as u64);
            buf.extend_from_slice(&img.data);
        }
        Value::Audio(aud) => {
            buf.push(tags::AUDIO);
            buf.push(aud.encoding as u8);
            buf.extend_from_slice(&aud.sample_rate.to_le_bytes());
            buf.push(aud.channels);
            write_uvarint(buf, aud.data.len() as u64);
            buf.extend_from_slice(&aud.data);
        }
        Value::Ext(ext) => {
            buf.push(tags::EXT);
            write_uvarint(buf, ext.type_id);
            write_uvarint(buf, ext.payload.len() as u64);
            buf.extend_from_slice(&ext.payload);
        }
        Value::Bitmask { count, bits } => {
            buf.push(tags::BITMASK);
            write_uvarint(buf, *count);
            buf.extend_from_slice(bits);
        }
        // Graph types
        Value::Node(node) => {
            buf.push(tags::NODE);
            encode_node_data(buf, node, dict, opts)?;
        }
        Value::Edge(edge) => {
            buf.push(tags::EDGE);
            encode_edge_data(buf, edge, dict, opts)?;
        }
        Value::NodeBatch(batch) => {
            buf.push(tags::NODE_BATCH);
            write_uvarint(buf, batch.nodes.len() as u64);
            for node in &batch.nodes {
                encode_node_data(buf, node, dict, opts)?;
            }
        }
        Value::EdgeBatch(batch) => {
            buf.push(tags::EDGE_BATCH);
            write_uvarint(buf, batch.edges.len() as u64);
            for edge in &batch.edges {
                encode_edge_data(buf, edge, dict, opts)?;
            }
        }
    }
    Ok(())
}

/// Encode a node (without tag byte).
fn encode_node_data(
    buf: &mut Vec<u8>,
    node: &NodeData,
    dict: &HashMap<&str, usize>,
    opts: &EncodeOptions,
) -> Result<(), CowrieError> {
    // ID
    write_string(buf, &node.id);
    // Labels
    write_uvarint(buf, node.labels.len() as u64);
    for label in &node.labels {
        write_string(buf, label);
    }
    // Properties (dict-coded)
    encode_props(buf, &node.props, dict, opts)?;
    Ok(())
}

/// Encode an edge (without tag byte).
fn encode_edge_data(
    buf: &mut Vec<u8>,
    edge: &EdgeData,
    dict: &HashMap<&str, usize>,
    opts: &EncodeOptions,
) -> Result<(), CowrieError> {
    // From, To, Type
    write_string(buf, &edge.from);
    write_string(buf, &edge.to);
    write_string(buf, &edge.edge_type);
    // Properties (dict-coded)
    encode_props(buf, &edge.props, dict, opts)?;
    Ok(())
}

/// Encode dictionary-coded properties.
fn encode_props(
    buf: &mut Vec<u8>,
    props: &BTreeMap<String, Value>,
    dict: &HashMap<&str, usize>,
    opts: &EncodeOptions,
) -> Result<(), CowrieError> {
    // Filter nulls if needed
    let filtered: Vec<_> = if opts.omit_null {
        props
            .iter()
            .filter(|(_, v)| !matches!(v, Value::Null))
            .collect()
    } else {
        props.iter().collect()
    };

    write_uvarint(buf, filtered.len() as u64);
    for (key, val) in filtered {
        // O(1) key index lookup
        let idx = *dict.get(key.as_str()).expect("key should be in dictionary");
        write_uvarint(buf, idx as u64);
        encode_value(buf, val, dict, opts)?;
    }
    Ok(())
}

/// Write a varint-encoded unsigned integer.
fn write_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Write a length-prefixed string.
fn write_string(buf: &mut Vec<u8>, s: &str) {
    write_uvarint(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

/// Zigzag encode a signed integer.
fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_null() {
        let val = Value::Null;
        let encoded = encode(&val).unwrap();
        assert!(encoded.starts_with(MAGIC));
        // Header is 4-byte MAGIC ("COWR") + version byte, so version is at index 4.
        assert_eq!(encoded[MAGIC.len()], VERSION);
    }

    #[test]
    fn test_encode_object() {
        let val = Value::object(vec![
            ("name", Value::String("test".into())),
            ("count", Value::Int(42)),
        ]);
        let encoded = encode(&val).unwrap();
        assert!(encoded.len() > 10);
    }

    #[test]
    fn test_omit_null() {
        let val = Value::object(vec![
            ("name", Value::String("test".into())),
            ("nullable", Value::Null),
            ("count", Value::Int(42)),
        ]);

        // With omit_null
        let opts = EncodeOptions {
            omit_null: true,
            ..Default::default()
        };
        let with_omit = encode_with_options(&val, &opts).unwrap();

        // Without omit_null
        let without_omit = encode(&val).unwrap();

        // With omit should be smaller (no null field)
        assert!(with_omit.len() < without_omit.len());
    }

    #[test]
    fn test_zigzag() {
        assert_eq!(zigzag_encode(0), 0);
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(-2), 3);
        assert_eq!(zigzag_encode(2), 4);
    }
}
