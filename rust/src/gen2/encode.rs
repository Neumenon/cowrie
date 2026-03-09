//! Cowrie encoder.

use super::types::{Value, CowrieError, NodeData, EdgeData};
use crate::{MAGIC, VERSION};
use std::collections::{BTreeMap, HashMap};

/// Wire format tags (must match Go implementation).
mod tags {
    pub const NULL: u8 = 0x00;
    pub const FALSE: u8 = 0x01;
    pub const TRUE: u8 = 0x02;
    pub const INT64: u8 = 0x03;
    pub const FLOAT64: u8 = 0x04;
    pub const STRING: u8 = 0x05;
    pub const ARRAY: u8 = 0x06;
    pub const OBJECT: u8 = 0x07;
    pub const BYTES: u8 = 0x08;
    pub const UINT64: u8 = 0x09;
    pub const DECIMAL128: u8 = 0x0A;
    pub const DATETIME64: u8 = 0x0B;
    pub const UUID128: u8 = 0x0C;
    pub const BIGINT: u8 = 0x0D;
    pub const EXT: u8 = 0x0E;
    pub const TENSOR: u8 = 0x20;
    pub const TENSOR_REF: u8 = 0x21;
    pub const IMAGE: u8 = 0x22;
    pub const AUDIO: u8 = 0x23;
    pub const ADJLIST: u8 = 0x30;
    pub const RICHTEXT: u8 = 0x31;
    pub const DELTA: u8 = 0x32;
    // Graph types (v2.1)
    pub const NODE: u8 = 0x35;
    pub const EDGE: u8 = 0x36;
    pub const NODE_BATCH: u8 = 0x37;
    pub const EDGE_BATCH: u8 = 0x38;
    pub const GRAPH_SHARD: u8 = 0x39;
    pub const BITMASK: u8 = 0x24;
    // v3 inline types
    pub const FIXINT_BASE: u8 = 0x40;
    pub const FIXINT_MAX: u8 = 0xBF;
    pub const FIXARRAY_BASE: u8 = 0xC0;
    pub const FIXARRAY_MAX: u8 = 0xCF;
    pub const FIXMAP_BASE: u8 = 0xD0;
    pub const FIXMAP_MAX: u8 = 0xDF;
    pub const FIXNEG_BASE: u8 = 0xE0;
    pub const FIXNEG_MAX: u8 = 0xEF;
}

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
    keys
}

fn collect_keys(value: &Value, keys: &mut Vec<String>, seen: &mut std::collections::HashSet<String>, opts: &EncodeOptions) {
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
        Value::GraphShard(shard) => {
            for node in &shard.nodes {
                collect_props_keys(&node.props, keys, seen, opts);
            }
            for edge in &shard.edges {
                collect_props_keys(&edge.props, keys, seen, opts);
            }
            collect_props_keys(&shard.metadata, keys, seen, opts);
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

fn encode_value(buf: &mut Vec<u8>, value: &Value, dict: &HashMap<&str, usize>, opts: &EncodeOptions) -> Result<(), CowrieError> {
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
            if v >= 0 && v <= 127 {
                buf.push(tags::FIXINT_BASE + v as u8);
            } else if v >= -16 && v <= -1 {
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
                let idx = *dict.get(key.as_str())
                    .expect("key should be in dictionary");
                write_uvarint(buf, idx as u64);
                encode_value(buf, val, dict, opts)?;
            }
        }
        Value::Tensor(t) => {
            buf.push(tags::TENSOR);
            buf.push(t.dtype as u8);
            buf.push(t.shape.len() as u8);
            for dim in &t.shape {
                write_uvarint(buf, *dim);
            }
            write_uvarint(buf, t.data.len() as u64);
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
        Value::Adjlist(adj) => {
            buf.push(tags::ADJLIST);
            buf.push(adj.id_width);
            write_uvarint(buf, adj.node_count);
            write_uvarint(buf, adj.edge_count);
            for &offset in &adj.row_offsets {
                write_uvarint(buf, offset);
            }
            buf.extend_from_slice(&adj.col_indices);
        }
        Value::RichText(rt) => {
            buf.push(tags::RICHTEXT);
            let text_bytes = rt.text.as_bytes();
            write_uvarint(buf, text_bytes.len() as u64);
            buf.extend_from_slice(text_bytes);

            let mut flags = 0u8;
            if rt.tokens.is_some() { flags |= 0x01; }
            if rt.spans.is_some() { flags |= 0x02; }
            buf.push(flags);

            if let Some(ref tokens) = rt.tokens {
                write_uvarint(buf, tokens.len() as u64);
                for &tok in tokens {
                    buf.extend_from_slice(&tok.to_le_bytes());
                }
            }
            if let Some(ref spans) = rt.spans {
                write_uvarint(buf, spans.len() as u64);
                for span in spans {
                    write_uvarint(buf, span.start);
                    write_uvarint(buf, span.end);
                    write_uvarint(buf, span.kind_id);
                }
            }
        }
        Value::Delta(delta) => {
            buf.push(tags::DELTA);
            write_uvarint(buf, delta.base_id);
            write_uvarint(buf, delta.ops.len() as u64);
            for op in &delta.ops {
                buf.push(op.op_code as u8);
                write_uvarint(buf, op.field_id);
                if let Some(ref val) = op.value {
                    encode_value(buf, val, dict, opts)?;
                }
            }
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
        Value::GraphShard(shard) => {
            buf.push(tags::GRAPH_SHARD);
            // Encode nodes
            write_uvarint(buf, shard.nodes.len() as u64);
            for node in &shard.nodes {
                encode_node_data(buf, node, dict, opts)?;
            }
            // Encode edges
            write_uvarint(buf, shard.edges.len() as u64);
            for edge in &shard.edges {
                encode_edge_data(buf, edge, dict, opts)?;
            }
            // Encode metadata (as dict-coded properties)
            encode_props(buf, &shard.metadata, dict, opts)?;
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
        props.iter().filter(|(_, v)| !matches!(v, Value::Null)).collect()
    } else {
        props.iter().collect()
    };

    write_uvarint(buf, filtered.len() as u64);
    for (key, val) in filtered {
        // O(1) key index lookup
        let idx = *dict.get(key.as_str())
            .expect("key should be in dictionary");
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
        assert_eq!(encoded[2], VERSION);
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
        let opts = EncodeOptions { omit_null: true, ..Default::default() };
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
