//! Cowrie decoder.

use super::types::{Value, CowrieError, DType, TensorData, TensorRef, ImageFormat, ImageData, AudioEncoding, AudioData, AdjlistData, RichTextData, RichTextSpan, DeltaData, DeltaOp, DeltaOpCode, ExtData, NodeData, EdgeData, NodeBatchData, EdgeBatchData, GraphShardData};
use crate::{MAGIC, VERSION};
use std::collections::BTreeMap;

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
    pub const FLOAT32: u8 = 0x0F; // compact float32 → decoded as f64
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
}

const FLAG_HAS_COLUMN_HINTS: u8 = 0x08;

/// Configurable limits for the Cowrie decoder.
///
/// All limits have safe defaults that match the Go reference implementation.
/// Use `DecodeOptions::default()` for standard limits, or construct custom
/// options to restrict (or relax) individual bounds.
#[derive(Debug, Clone)]
pub struct DecodeOptions {
    /// Maximum nesting depth for recursive values (arrays, objects, deltas).
    pub max_depth: usize,
    /// Maximum number of elements in an array.
    pub max_array_len: usize,
    /// Maximum number of entries in an object / property map.
    pub max_object_len: usize,
    /// Maximum byte length of a decoded string.
    pub max_string_len: usize,
    /// Maximum byte length of a decoded bytes blob.
    pub max_bytes_len: usize,
    /// Maximum byte length of an extension payload.
    pub max_ext_len: usize,
    /// Maximum number of entries in the dictionary.
    pub max_dict_len: usize,
    /// Maximum tensor rank (number of dimensions).
    pub max_rank: usize,
    /// Maximum number of column hints.
    pub max_hint_count: usize,
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self {
            max_depth: 1_000,
            max_array_len: 1_000_000,     // Tightened: was 100M
            max_object_len: 1_000_000,    // Tightened: was 10M
            max_string_len: 10_000_000,   // Tightened: was 500M
            max_bytes_len: 50_000_000,    // Tightened: was 1G
            max_ext_len: 1_000_000,       // Tightened: was 100M
            max_dict_len: 1_000_000,      // Tightened: was 10M
            max_rank: 32,
            max_hint_count: 10_000,
        }
    }
}

/// Decode Cowrie bytes to a Value using default options.
pub fn decode(data: &[u8]) -> Result<Value, CowrieError> {
    decode_with_options(data, &DecodeOptions::default())
}

/// Decode Cowrie bytes to a Value with configurable limits.
pub fn decode_with_options(data: &[u8], opts: &DecodeOptions) -> Result<Value, CowrieError> {
    let mut reader = Reader::new(data, opts);
    reader.decode()
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
    dict: Vec<String>,
    depth: usize,
    opts: &'a DecodeOptions,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8], opts: &'a DecodeOptions) -> Self {
        Reader {
            data,
            pos: 0,
            dict: Vec::new(),
            depth: 0,
            opts,
        }
    }

    fn decode(&mut self) -> Result<Value, CowrieError> {
        // Check magic first (even if truncated, bad magic takes priority)
        if self.remaining() < 2 {
            return Err(CowrieError::Truncated);
        }
        if &self.data[0..2] != MAGIC {
            return Err(CowrieError::InvalidMagic);
        }
        self.pos = 2;

        // Check version
        let version = self.read_byte()?;
        if version != VERSION {
            return Err(CowrieError::InvalidVersion(version));
        }

        // Read flags
        let flags = self.read_byte()?;
        if (flags & FLAG_HAS_COLUMN_HINTS) != 0 {
            self.skip_hints()?;
        }

        // Read dictionary
        let dict_len = self.read_uvarint()? as usize;
        if dict_len > self.opts.max_dict_len {
            return Err(CowrieError::TooLarge);
        }
        self.dict = Vec::with_capacity(dict_len);
        for _ in 0..dict_len {
            let key = self.read_string()?;
            self.dict.push(key);
        }

        // Read root value
        let value = self.decode_value()?;

        // Verify all input consumed — trailing bytes indicate corruption or concatenated data
        if self.pos < self.data.len() {
            return Err(CowrieError::TrailingData {
                pos: self.pos,
                remaining: self.data.len() - self.pos,
            });
        }

        Ok(value)
    }

    fn decode_value(&mut self) -> Result<Value, CowrieError> {
        self.depth += 1;
        if self.depth > self.opts.max_depth {
            return Err(CowrieError::TooDeep);
        }

        let tag = self.read_byte()?;
        let value = match tag {
            tags::NULL => Value::Null,
            tags::TRUE => Value::Bool(true),
            tags::FALSE => Value::Bool(false),
            tags::INT64 => {
                let z = self.read_uvarint()?;
                Value::Int(zigzag_decode(z))
            }
            tags::UINT64 => {
                let u = self.read_uvarint()?;
                Value::Uint(u)
            }
            tags::FLOAT64 => {
                let bytes = self.read_bytes_fixed::<8>()?;
                Value::Float(f64::from_le_bytes(bytes))
            }
            tags::FLOAT32 => {
                let bytes = self.read_bytes_fixed::<4>()?;
                Value::Float(f32::from_le_bytes(bytes) as f64)
            }
            tags::DECIMAL128 => {
                let mut data = vec![0u8; 17];
                data[0] = self.read_byte()?; // scale
                self.read_into(&mut data[1..])?; // 16-byte coef
                Value::Decimal(data)
            }
            tags::STRING => {
                let s = self.read_string()?;
                Value::String(s)
            }
            tags::BYTES => {
                let len = self.read_uvarint()? as usize;
                if len > self.opts.max_bytes_len {
                    return Err(CowrieError::TooLarge);
                }
                let bytes = self.read_bytes(len)?;
                Value::Bytes(bytes)
            }
            tags::DATETIME64 => {
                let bytes = self.read_bytes_fixed::<8>()?;
                Value::DateTime(i64::from_le_bytes(bytes))
            }
            tags::UUID128 => {
                let bytes = self.read_bytes_fixed::<16>()?;
                Value::Uuid(bytes)
            }
            tags::BIGINT => {
                let len = self.read_uvarint()? as usize;
                if len > self.opts.max_bytes_len {
                    return Err(CowrieError::TooLarge);
                }
                let data = self.read_bytes(len)?;
                Value::BigInt(data)
            }
            tags::ARRAY => {
                let len = self.read_uvarint()? as usize;
                if len > self.opts.max_array_len {
                    return Err(CowrieError::TooLarge);
                }
                let mut arr = Vec::with_capacity(len);
                for _ in 0..len {
                    arr.push(self.decode_value()?);
                }
                Value::Array(arr)
            }
            tags::OBJECT => {
                let len = self.read_uvarint()? as usize;
                if len > self.opts.max_object_len {
                    return Err(CowrieError::TooLarge);
                }
                let mut obj = BTreeMap::new();
                for _ in 0..len {
                    let key_idx = self.read_uvarint()? as usize;
                    if key_idx >= self.dict.len() {
                        return Err(CowrieError::InvalidDictIndex {
                            index: key_idx,
                            dict_len: self.dict.len(),
                        });
                    }
                    let key = self.dict[key_idx].clone();
                    let val = self.decode_value()?;
                    obj.insert(key, val);
                }
                Value::Object(obj)
            }
            tags::EXT => {
                let type_id = self.read_uvarint()?;
                let len = self.read_uvarint()? as usize;
                if len > self.opts.max_ext_len {
                    return Err(CowrieError::TooLarge);
                }
                let payload = self.read_bytes(len)?;
                Value::Ext(ExtData { type_id, payload })
            }
            tags::TENSOR => {
                let dtype = DType::try_from(self.read_byte()?)?;
                let rank = self.read_byte()? as usize;
                if rank > self.opts.max_rank {
                    return Err(CowrieError::RankExceeded {
                        rank,
                        max: self.opts.max_rank,
                    });
                }
                let mut shape = Vec::with_capacity(rank);
                for _ in 0..rank {
                    shape.push(self.read_uvarint()?);
                }
                let data_len = self.read_uvarint()? as usize;
                if data_len > self.opts.max_bytes_len {
                    return Err(CowrieError::TooLarge);
                }
                let data = self.read_bytes(data_len)?;
                Value::Tensor(TensorData::new(dtype, shape, data))
            }
            tags::TENSOR_REF => {
                let store_id = self.read_byte()?;
                let key_len = self.read_uvarint()? as usize;
                if key_len > self.opts.max_bytes_len {
                    return Err(CowrieError::TooLarge);
                }
                let key = self.read_bytes(key_len)?;
                Value::TensorRef(TensorRef { store_id, key })
            }
            tags::IMAGE => {
                let format_byte = self.read_byte()?;
                let format = ImageFormat::try_from(format_byte)?;
                let width = u16::from_le_bytes(self.read_bytes_fixed::<2>()?);
                let height = u16::from_le_bytes(self.read_bytes_fixed::<2>()?);
                let data_len = self.read_uvarint()? as usize;
                if data_len > self.opts.max_bytes_len {
                    return Err(CowrieError::TooLarge);
                }
                let data = self.read_bytes(data_len)?;
                Value::Image(ImageData { format, width, height, data })
            }
            tags::AUDIO => {
                let encoding_byte = self.read_byte()?;
                let encoding = AudioEncoding::try_from(encoding_byte)?;
                let sample_rate = u32::from_le_bytes(self.read_bytes_fixed::<4>()?);
                let channels = self.read_byte()?;
                let data_len = self.read_uvarint()? as usize;
                if data_len > self.opts.max_bytes_len {
                    return Err(CowrieError::TooLarge);
                }
                let data = self.read_bytes(data_len)?;
                Value::Audio(AudioData { encoding, sample_rate, channels, data })
            }
            tags::ADJLIST => {
                let id_width = self.read_byte()?;
                let node_count = self.read_uvarint()?;
                let edge_count = self.read_uvarint()?;

                // Row offsets: node_count + 1 elements
                let mut row_offsets = Vec::with_capacity((node_count + 1) as usize);
                for _ in 0..=node_count {
                    row_offsets.push(self.read_uvarint()?);
                }

                // Col indices size depends on id_width and edge_count
                let col_size = if id_width == 1 { 4 } else { 8 };
                let col_len = edge_count as usize * col_size;
                if col_len > self.opts.max_bytes_len {
                    return Err(CowrieError::TooLarge);
                }
                let col_indices = self.read_bytes(col_len)?;

                Value::Adjlist(AdjlistData {
                    id_width,
                    node_count,
                    edge_count,
                    row_offsets,
                    col_indices,
                })
            }
            tags::RICHTEXT => {
                let text_len = self.read_uvarint()? as usize;
                if text_len > self.opts.max_string_len {
                    return Err(CowrieError::TooLarge);
                }
                let text_bytes = self.read_bytes(text_len)?;
                let text = String::from_utf8(text_bytes).map_err(|_| CowrieError::InvalidUtf8)?;

                let flags = self.read_byte()?;
                let has_tokens = (flags & 0x01) != 0;
                let has_spans = (flags & 0x02) != 0;

                let tokens = if has_tokens {
                    let token_count = self.read_uvarint()? as usize;
                    let mut toks = Vec::with_capacity(token_count);
                    for _ in 0..token_count {
                        let tok = i32::from_le_bytes(self.read_bytes_fixed::<4>()?);
                        toks.push(tok);
                    }
                    Some(toks)
                } else {
                    None
                };

                let spans = if has_spans {
                    let span_count = self.read_uvarint()? as usize;
                    let mut sp = Vec::with_capacity(span_count);
                    for _ in 0..span_count {
                        let start = self.read_uvarint()?;
                        let end = self.read_uvarint()?;
                        let kind_id = self.read_uvarint()?;
                        sp.push(RichTextSpan { start, end, kind_id });
                    }
                    Some(sp)
                } else {
                    None
                };

                Value::RichText(RichTextData { text, tokens, spans })
            }
            tags::DELTA => {
                let base_id = self.read_uvarint()?;
                let op_count = self.read_uvarint()? as usize;

                let mut ops = Vec::with_capacity(op_count);
                for _ in 0..op_count {
                    let op_byte = self.read_byte()?;
                    let op_code = match op_byte {
                        0x01 => DeltaOpCode::SetField,
                        0x02 => DeltaOpCode::DeleteField,
                        0x03 => DeltaOpCode::AppendArray,
                        _ => return Err(CowrieError::InvalidTag(op_byte)),
                    };
                    let field_id = self.read_uvarint()?;

                    let value = if op_code == DeltaOpCode::DeleteField {
                        None
                    } else {
                        Some(Box::new(self.decode_value()?))
                    };

                    ops.push(DeltaOp { op_code, field_id, value });
                }

                Value::Delta(DeltaData { base_id, ops })
            }
            // Graph types
            tags::NODE => {
                let node = self.decode_node_data()?;
                Value::Node(node)
            }
            tags::EDGE => {
                let edge = self.decode_edge_data()?;
                Value::Edge(edge)
            }
            tags::NODE_BATCH => {
                let count = self.read_uvarint()? as usize;
                if count > self.opts.max_array_len {
                    return Err(CowrieError::TooLarge);
                }
                let mut nodes = Vec::with_capacity(count);
                for _ in 0..count {
                    nodes.push(self.decode_node_data()?);
                }
                Value::NodeBatch(NodeBatchData { nodes })
            }
            tags::EDGE_BATCH => {
                let count = self.read_uvarint()? as usize;
                if count > self.opts.max_array_len {
                    return Err(CowrieError::TooLarge);
                }
                let mut edges = Vec::with_capacity(count);
                for _ in 0..count {
                    edges.push(self.decode_edge_data()?);
                }
                Value::EdgeBatch(EdgeBatchData { edges })
            }
            tags::GRAPH_SHARD => {
                // Decode nodes
                let node_count = self.read_uvarint()? as usize;
                if node_count > self.opts.max_array_len {
                    return Err(CowrieError::TooLarge);
                }
                let mut nodes = Vec::with_capacity(node_count);
                for _ in 0..node_count {
                    nodes.push(self.decode_node_data()?);
                }
                // Decode edges
                let edge_count = self.read_uvarint()? as usize;
                if edge_count > self.opts.max_array_len {
                    return Err(CowrieError::TooLarge);
                }
                let mut edges = Vec::with_capacity(edge_count);
                for _ in 0..edge_count {
                    edges.push(self.decode_edge_data()?);
                }
                // Decode metadata
                let metadata = self.decode_props()?;
                Value::GraphShard(GraphShardData { nodes, edges, metadata })
            }
            _ => return Err(CowrieError::InvalidTag(tag)),
        };

        self.depth -= 1;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn read_byte(&mut self) -> Result<u8, CowrieError> {
        if self.pos >= self.data.len() {
            return Err(CowrieError::Truncated);
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>, CowrieError> {
        if self.pos + len > self.data.len() {
            return Err(CowrieError::Truncated);
        }
        let bytes = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(bytes)
    }

    fn read_bytes_fixed<const N: usize>(&mut self) -> Result<[u8; N], CowrieError> {
        if self.pos + N > self.data.len() {
            return Err(CowrieError::Truncated);
        }
        let mut bytes = [0u8; N];
        bytes.copy_from_slice(&self.data[self.pos..self.pos + N]);
        self.pos += N;
        Ok(bytes)
    }

    fn read_into(&mut self, buf: &mut [u8]) -> Result<(), CowrieError> {
        let len = buf.len();
        if self.pos + len > self.data.len() {
            return Err(CowrieError::Truncated);
        }
        buf.copy_from_slice(&self.data[self.pos..self.pos + len]);
        self.pos += len;
        Ok(())
    }

    fn read_uvarint(&mut self) -> Result<u64, CowrieError> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            let b = self.read_byte()?;
            result |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift >= 64 {
                return Err(CowrieError::TooLarge);
            }
        }
        Ok(result)
    }

    fn read_string(&mut self) -> Result<String, CowrieError> {
        let len = self.read_uvarint()? as usize;
        if len > self.opts.max_string_len {
            return Err(CowrieError::TooLarge);
        }
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes).map_err(|_| CowrieError::InvalidUtf8)
    }

    fn skip_hints(&mut self) -> Result<(), CowrieError> {
        let count = self.read_uvarint()? as usize;
        if count > self.opts.max_hint_count {
            return Err(CowrieError::TooLarge);
        }
        for _ in 0..count {
            let _field = self.read_string()?;
            let _typ = self.read_byte()?;
            let shape_len = self.read_uvarint()?;
            if shape_len > self.opts.max_rank as u64 {
                return Err(CowrieError::TooLarge);
            }
            for _ in 0..shape_len {
                let _ = self.read_uvarint()?;
            }
            let _flags = self.read_byte()?;
        }
        Ok(())
    }

    /// Decode a node (without tag byte).
    fn decode_node_data(&mut self) -> Result<NodeData, CowrieError> {
        // ID
        let id = self.read_string()?;
        // Labels
        let label_count = self.read_uvarint()? as usize;
        if label_count > self.opts.max_array_len {
            return Err(CowrieError::TooLarge);
        }
        let mut labels = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            labels.push(self.read_string()?);
        }
        // Properties
        let props = self.decode_props()?;
        Ok(NodeData { id, labels, props })
    }

    /// Decode an edge (without tag byte).
    fn decode_edge_data(&mut self) -> Result<EdgeData, CowrieError> {
        // From, To, Type
        let from = self.read_string()?;
        let to = self.read_string()?;
        let edge_type = self.read_string()?;
        // Properties
        let props = self.decode_props()?;
        Ok(EdgeData { from, to, edge_type, props })
    }

    /// Decode dictionary-coded properties.
    fn decode_props(&mut self) -> Result<BTreeMap<String, Value>, CowrieError> {
        let prop_count = self.read_uvarint()? as usize;
        if prop_count > self.opts.max_object_len {
            return Err(CowrieError::TooLarge);
        }
        let mut props = BTreeMap::new();
        for _ in 0..prop_count {
            let key_idx = self.read_uvarint()? as usize;
            if key_idx >= self.dict.len() {
                return Err(CowrieError::InvalidData(format!(
                    "dictionary index {} out of range (dict size: {})",
                    key_idx, self.dict.len()
                )));
            }
            let key = self.dict[key_idx].clone();
            let val = self.decode_value()?;
            props.insert(key, val);
        }
        Ok(props)
    }
}

/// Zigzag decode an unsigned integer to signed.
fn zigzag_decode(z: u64) -> i64 {
    ((z >> 1) as i64) ^ (-((z & 1) as i64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gen2::encode::encode;

    #[test]
    fn test_roundtrip_primitives() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Int(42),
            Value::Int(-42),
            Value::Int(i64::MAX),
            Value::Int(i64::MIN),
            Value::Uint(u64::MAX),
            Value::Float(3.14159),
            Value::String("hello".into()),
            Value::Bytes(vec![1, 2, 3]),
        ];

        for val in values {
            let encoded = encode(&val).expect("encode");
            let decoded = decode(&encoded).expect("decode");
            assert_eq!(val, decoded, "roundtrip failed for {:?}", val);
        }
    }

    #[test]
    fn test_roundtrip_complex() {
        let val = Value::object(vec![
            ("name", Value::String("test".into())),
            ("count", Value::Int(42)),
            ("scores", Value::Array(vec![
                Value::Float(1.1),
                Value::Float(2.2),
            ])),
            ("nested", Value::object(vec![
                ("inner", Value::Bool(true)),
            ])),
        ]);

        let encoded = encode(&val).expect("encode");
        let decoded = decode(&encoded).expect("decode");
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_zigzag() {
        assert_eq!(zigzag_decode(0), 0);
        assert_eq!(zigzag_decode(1), -1);
        assert_eq!(zigzag_decode(2), 1);
        assert_eq!(zigzag_decode(3), -2);
        assert_eq!(zigzag_decode(4), 2);
    }

    #[test]
    fn test_invalid_magic() {
        let data = b"XX\x02\x00\x00";
        let result = decode(data);
        assert!(matches!(result, Err(CowrieError::InvalidMagic)));
    }

    #[test]
    fn test_truncated() {
        let data = b"SJ";
        let result = decode(data);
        assert!(matches!(result, Err(CowrieError::Truncated)));
    }

    #[test]
    fn test_roundtrip_node() {
        use super::super::types::NodeData;

        let mut props = BTreeMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));

        let node = Value::Node(NodeData {
            id: "person_42".to_string(),
            labels: vec!["Person".to_string(), "Employee".to_string()],
            props,
        });

        let encoded = encode(&node).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        match decoded {
            Value::Node(n) => {
                assert_eq!(n.id, "person_42");
                assert_eq!(n.labels, vec!["Person", "Employee"]);
                assert_eq!(n.props.get("name").and_then(|v| v.as_str()), Some("Alice"));
                assert_eq!(n.props.get("age").and_then(|v| v.as_i64()), Some(30));
            }
            _ => panic!("Expected Node, got {:?}", decoded),
        }
    }

    #[test]
    fn test_roundtrip_edge() {
        use super::super::types::EdgeData;

        let mut props = BTreeMap::new();
        props.insert("since".to_string(), Value::Int(2020));
        props.insert("role".to_string(), Value::String("Engineer".to_string()));

        let edge = Value::Edge(EdgeData {
            from: "person_42".to_string(),
            to: "company_1".to_string(),
            edge_type: "WORKS_AT".to_string(),
            props,
        });

        let encoded = encode(&edge).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        match decoded {
            Value::Edge(e) => {
                assert_eq!(e.from, "person_42");
                assert_eq!(e.to, "company_1");
                assert_eq!(e.edge_type, "WORKS_AT");
                assert_eq!(e.props.get("since").and_then(|v| v.as_i64()), Some(2020));
            }
            _ => panic!("Expected Edge, got {:?}", decoded),
        }
    }

    #[test]
    fn test_roundtrip_node_batch() {
        use super::super::types::{NodeData, NodeBatchData};

        let nodes = vec![
            NodeData {
                id: "n1".to_string(),
                labels: vec!["A".to_string()],
                props: BTreeMap::new(),
            },
            NodeData {
                id: "n2".to_string(),
                labels: vec!["B".to_string()],
                props: BTreeMap::new(),
            },
        ];

        let batch = Value::NodeBatch(NodeBatchData { nodes });

        let encoded = encode(&batch).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        match decoded {
            Value::NodeBatch(nb) => {
                assert_eq!(nb.nodes.len(), 2);
                assert_eq!(nb.nodes[0].id, "n1");
                assert_eq!(nb.nodes[1].id, "n2");
            }
            _ => panic!("Expected NodeBatch, got {:?}", decoded),
        }
    }

    #[test]
    fn test_roundtrip_image() {
        use super::super::types::{ImageData, ImageFormat};

        let val = Value::Image(ImageData {
            format: ImageFormat::Png,
            width: 640,
            height: 480,
            data: vec![0x89, 0x50, 0x4E, 0x47],
        });

        let encoded = encode(&val).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        match decoded {
            Value::Image(img) => {
                assert_eq!(img.format, ImageFormat::Png);
                assert_eq!(img.width, 640);
                assert_eq!(img.height, 480);
                assert_eq!(img.data, vec![0x89, 0x50, 0x4E, 0x47]);
            }
            _ => panic!("Expected Image, got {:?}", decoded),
        }
    }

    #[test]
    fn test_roundtrip_audio() {
        use super::super::types::{AudioData, AudioEncoding};

        let val = Value::Audio(AudioData {
            encoding: AudioEncoding::Opus,
            sample_rate: 48000,
            channels: 2,
            data: vec![0x01, 0x02, 0x03, 0x04],
        });

        let encoded = encode(&val).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        match decoded {
            Value::Audio(aud) => {
                assert_eq!(aud.encoding, AudioEncoding::Opus);
                assert_eq!(aud.sample_rate, 48000);
                assert_eq!(aud.channels, 2);
                assert_eq!(aud.data, vec![0x01, 0x02, 0x03, 0x04]);
            }
            _ => panic!("Expected Audio, got {:?}", decoded),
        }
    }

    #[test]
    fn test_roundtrip_all_image_formats() {
        use super::super::types::{ImageData, ImageFormat};

        let formats = [
            ImageFormat::Jpeg,
            ImageFormat::Png,
            ImageFormat::Webp,
            ImageFormat::Avif,
            ImageFormat::Bmp,
        ];

        for fmt in formats {
            let val = Value::Image(ImageData {
                format: fmt,
                width: 100,
                height: 200,
                data: vec![0xFF],
            });

            let encoded = encode(&val).expect("encode");
            let decoded = decode(&encoded).expect("decode");

            match decoded {
                Value::Image(img) => assert_eq!(img.format, fmt),
                _ => panic!("Expected Image"),
            }
        }
    }

    #[test]
    fn test_roundtrip_all_audio_encodings() {
        use super::super::types::{AudioData, AudioEncoding};

        let encodings = [
            AudioEncoding::PcmInt16,
            AudioEncoding::PcmFloat32,
            AudioEncoding::Opus,
            AudioEncoding::Aac,
        ];

        for enc in encodings {
            let val = Value::Audio(AudioData {
                encoding: enc,
                sample_rate: 44100,
                channels: 1,
                data: vec![0x00],
            });

            let encoded = encode(&val).expect("encode");
            let decoded = decode(&encoded).expect("decode");

            match decoded {
                Value::Audio(aud) => assert_eq!(aud.encoding, enc),
                _ => panic!("Expected Audio"),
            }
        }
    }

    #[test]
    fn test_invalid_image_format_rejected() {
        // Craft a raw Cowrie payload with an invalid image format byte (0x00)
        use crate::{MAGIC, VERSION};
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.push(VERSION);
        buf.push(0); // flags
        buf.push(0); // dict len = 0
        buf.push(0x22); // IMAGE tag
        buf.push(0x00); // invalid format
        buf.extend_from_slice(&100u16.to_le_bytes()); // width
        buf.extend_from_slice(&200u16.to_le_bytes()); // height
        buf.push(0); // data len = 0

        let result = decode(&buf);
        assert!(result.is_err(), "should reject invalid image format 0x00");
    }

    #[test]
    fn test_invalid_audio_encoding_rejected() {
        // Craft a raw Cowrie payload with an invalid audio encoding byte (0xFF)
        use crate::{MAGIC, VERSION};
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.push(VERSION);
        buf.push(0); // flags
        buf.push(0); // dict len = 0
        buf.push(0x23); // AUDIO tag
        buf.push(0xFF); // invalid encoding
        buf.extend_from_slice(&44100u32.to_le_bytes()); // sample_rate
        buf.push(2); // channels
        buf.push(0); // data len = 0

        let result = decode(&buf);
        assert!(result.is_err(), "should reject invalid audio encoding 0xFF");
    }

    #[test]
    fn test_tensor_rank_limit() {
        // Craft a tensor with rank=33, which exceeds MAX_RANK=32
        use crate::{MAGIC, VERSION};
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.push(VERSION);
        buf.push(0); // flags
        buf.push(0); // dict len = 0
        buf.push(0x20); // TENSOR tag
        buf.push(0x01); // dtype = Float32
        buf.push(33);   // rank = 33 (exceeds MAX_RANK=32)
        // Shape: 33 dimensions all = 1
        for _ in 0..33 {
            buf.push(1); // uvarint 1
        }
        buf.push(0); // data len = 0

        let result = decode(&buf);
        assert!(result.is_err(), "should reject tensor rank > 32");
    }

    #[test]
    fn test_tensor_rank_32_accepted() {
        // Craft a tensor with rank=32, which is exactly at MAX_RANK
        use crate::{MAGIC, VERSION};
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.push(VERSION);
        buf.push(0); // flags
        buf.push(0); // dict len = 0
        buf.push(0x20); // TENSOR tag
        buf.push(0x01); // dtype = Float32
        buf.push(32);   // rank = 32 (exactly MAX_RANK)
        // Shape: 32 dimensions all = 1
        for _ in 0..32 {
            buf.push(1); // uvarint 1
        }
        // data_len = 4 bytes (1 float32 element = product of all dims * 4)
        buf.push(4); // uvarint 4
        buf.extend_from_slice(&1.0f32.to_le_bytes());

        let result = decode(&buf);
        assert!(result.is_ok(), "should accept tensor rank = 32");
    }

    #[test]
    fn test_roundtrip_graph_shard() {
        use super::super::types::{NodeData, EdgeData, GraphShardData};

        let mut node_props = BTreeMap::new();
        node_props.insert("x".to_string(), Value::Float(0.1));

        let nodes = vec![
            NodeData {
                id: "1".to_string(),
                labels: vec!["Node".to_string()],
                props: node_props.clone(),
            },
            NodeData {
                id: "2".to_string(),
                labels: vec!["Node".to_string()],
                props: {
                    let mut p = BTreeMap::new();
                    p.insert("x".to_string(), Value::Float(0.2));
                    p
                },
            },
        ];

        let mut edge_props = BTreeMap::new();
        edge_props.insert("weight".to_string(), Value::Float(0.85));

        let edges = vec![EdgeData {
            from: "1".to_string(),
            to: "2".to_string(),
            edge_type: "EDGE".to_string(),
            props: edge_props,
        }];

        let mut metadata = BTreeMap::new();
        metadata.insert("version".to_string(), Value::Int(1));

        let shard = Value::GraphShard(GraphShardData { nodes, edges, metadata });

        let encoded = encode(&shard).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        match decoded {
            Value::GraphShard(gs) => {
                assert_eq!(gs.nodes.len(), 2);
                assert_eq!(gs.edges.len(), 1);
                assert_eq!(gs.metadata.get("version").and_then(|v| v.as_i64()), Some(1));
                assert_eq!(gs.nodes[0].id, "1");
                assert_eq!(gs.edges[0].edge_type, "EDGE");
            }
            _ => panic!("Expected GraphShard, got {:?}", decoded),
        }
    }

    // ============================================================
    // DecodeOptions tests
    // ============================================================

    #[test]
    fn test_decode_with_default_options() {
        let val = Value::object(vec![
            ("name", Value::String("test".into())),
            ("count", Value::Int(42)),
        ]);
        let encoded = encode(&val).expect("encode");
        let decoded = decode_with_options(&encoded, &DecodeOptions::default()).expect("decode");
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_decode_with_options_max_depth() {
        // Build a deeply nested array: [[[[...]]]]
        fn nested_array(depth: usize) -> Value {
            let mut v = Value::Int(1);
            for _ in 0..depth {
                v = Value::Array(vec![v]);
            }
            v
        }

        // depth=5 should work with max_depth=10
        let val = nested_array(5);
        let encoded = encode(&val).expect("encode");
        let opts = DecodeOptions { max_depth: 10, ..DecodeOptions::default() };
        let decoded = decode_with_options(&encoded, &opts);
        assert!(decoded.is_ok(), "depth 5 should pass with max_depth=10");

        // depth=5 should fail with max_depth=3
        let opts_tight = DecodeOptions { max_depth: 3, ..DecodeOptions::default() };
        let result = decode_with_options(&encoded, &opts_tight);
        assert!(matches!(result, Err(CowrieError::TooDeep)),
            "depth 5 should fail with max_depth=3");
    }

    #[test]
    fn test_decode_with_options_max_array_len() {
        let val = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let encoded = encode(&val).expect("encode");

        // max_array_len=3: should pass
        let opts = DecodeOptions { max_array_len: 3, ..DecodeOptions::default() };
        assert!(decode_with_options(&encoded, &opts).is_ok());

        // max_array_len=2: should fail
        let opts_tight = DecodeOptions { max_array_len: 2, ..DecodeOptions::default() };
        assert!(matches!(decode_with_options(&encoded, &opts_tight), Err(CowrieError::TooLarge)));
    }

    #[test]
    fn test_decode_with_options_max_object_len() {
        let val = Value::object(vec![
            ("a", Value::Int(1)),
            ("b", Value::Int(2)),
            ("c", Value::Int(3)),
        ]);
        let encoded = encode(&val).expect("encode");

        // max_object_len=3: should pass
        let opts = DecodeOptions { max_object_len: 3, ..DecodeOptions::default() };
        assert!(decode_with_options(&encoded, &opts).is_ok());

        // max_object_len=2: should fail
        let opts_tight = DecodeOptions { max_object_len: 2, ..DecodeOptions::default() };
        assert!(matches!(decode_with_options(&encoded, &opts_tight), Err(CowrieError::TooLarge)));
    }

    #[test]
    fn test_decode_with_options_max_string_len() {
        let val = Value::String("hello".into()); // 5 bytes
        let encoded = encode(&val).expect("encode");

        // max_string_len=10: should pass
        let opts = DecodeOptions { max_string_len: 10, ..DecodeOptions::default() };
        assert!(decode_with_options(&encoded, &opts).is_ok());

        // max_string_len=3: should fail
        let opts_tight = DecodeOptions { max_string_len: 3, ..DecodeOptions::default() };
        assert!(matches!(decode_with_options(&encoded, &opts_tight), Err(CowrieError::TooLarge)));
    }

    #[test]
    fn test_decode_with_options_max_bytes_len() {
        let val = Value::Bytes(vec![1, 2, 3, 4, 5]); // 5 bytes
        let encoded = encode(&val).expect("encode");

        // max_bytes_len=10: should pass
        let opts = DecodeOptions { max_bytes_len: 10, ..DecodeOptions::default() };
        assert!(decode_with_options(&encoded, &opts).is_ok());

        // max_bytes_len=3: should fail
        let opts_tight = DecodeOptions { max_bytes_len: 3, ..DecodeOptions::default() };
        assert!(matches!(decode_with_options(&encoded, &opts_tight), Err(CowrieError::TooLarge)));
    }

    #[test]
    fn test_decode_with_options_max_rank() {
        use crate::{MAGIC, VERSION};

        // Craft a tensor with rank=5
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.push(VERSION);
        buf.push(0); // flags
        buf.push(0); // dict len = 0
        buf.push(0x20); // TENSOR tag
        buf.push(0x01); // dtype = Float32
        buf.push(5);    // rank = 5
        for _ in 0..5 {
            buf.push(1); // uvarint 1 per dimension
        }
        buf.push(4); // data_len = 4
        buf.extend_from_slice(&1.0f32.to_le_bytes());

        // max_rank=5: should pass
        let opts = DecodeOptions { max_rank: 5, ..DecodeOptions::default() };
        assert!(decode_with_options(&buf, &opts).is_ok());

        // max_rank=3: should fail
        let opts_tight = DecodeOptions { max_rank: 3, ..DecodeOptions::default() };
        let result = decode_with_options(&buf, &opts_tight);
        assert!(result.is_err(), "rank 5 should fail with max_rank=3");
    }

    #[test]
    fn test_decode_with_options_max_ext_len() {
        let val = Value::Ext(ExtData { type_id: 42, payload: vec![0xAB; 100] });
        let encoded = encode(&val).expect("encode");

        // max_ext_len=200: should pass
        let opts = DecodeOptions { max_ext_len: 200, ..DecodeOptions::default() };
        assert!(decode_with_options(&encoded, &opts).is_ok());

        // max_ext_len=50: should fail
        let opts_tight = DecodeOptions { max_ext_len: 50, ..DecodeOptions::default() };
        assert!(matches!(decode_with_options(&encoded, &opts_tight), Err(CowrieError::TooLarge)));
    }

    #[test]
    fn test_decode_with_options_roundtrip_unchanged() {
        // Verify decode_with_options(default) matches decode() exactly
        let val = Value::object(vec![
            ("data", Value::Bytes(vec![1, 2, 3])),
            ("items", Value::Array(vec![Value::Int(1), Value::Int(2)])),
            ("label", Value::String("test".into())),
        ]);
        let encoded = encode(&val).expect("encode");

        let d1 = decode(&encoded).expect("decode");
        let d2 = decode_with_options(&encoded, &DecodeOptions::default()).expect("decode_with_options");
        assert_eq!(d1, d2);
    }
}
