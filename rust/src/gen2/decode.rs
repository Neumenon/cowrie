//! SJSON decoder.

use super::types::{Value, SjsonError, DType, TensorData, TensorRef, ImageData, AudioData, AdjlistData, RichTextData, RichTextSpan, DeltaData, DeltaOp, DeltaOpCode, ExtData, NodeData, EdgeData, NodeBatchData, EdgeBatchData, GraphShardData};
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

/// Maximum nesting depth.
const MAX_DEPTH: usize = 1000;

/// Maximum array/object size.
const MAX_SIZE: usize = 10_000_000;
/// Maximum tensor rank (dimensions).
const MAX_RANK: u64 = 32;
/// Maximum column hints.
const MAX_HINT_COUNT: u64 = 10_000;

const FLAG_HAS_COLUMN_HINTS: u8 = 0x08;

/// Decode SJSON bytes to a Value.
pub fn decode(data: &[u8]) -> Result<Value, SjsonError> {
    let mut reader = Reader::new(data);
    reader.decode()
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
    dict: Vec<String>,
    depth: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Reader {
            data,
            pos: 0,
            dict: Vec::new(),
            depth: 0,
        }
    }

    fn decode(&mut self) -> Result<Value, SjsonError> {
        // Read header
        if self.remaining() < 4 {
            return Err(SjsonError::Truncated);
        }

        // Check magic
        if &self.data[0..2] != MAGIC {
            return Err(SjsonError::InvalidMagic);
        }
        self.pos = 2;

        // Check version
        let version = self.read_byte()?;
        if version != VERSION {
            return Err(SjsonError::InvalidVersion(version));
        }

        // Read flags
        let flags = self.read_byte()?;
        if (flags & FLAG_HAS_COLUMN_HINTS) != 0 {
            self.skip_hints()?;
        }

        // Read dictionary
        let dict_len = self.read_uvarint()? as usize;
        if dict_len > MAX_SIZE {
            return Err(SjsonError::TooLarge);
        }
        self.dict = Vec::with_capacity(dict_len);
        for _ in 0..dict_len {
            let key = self.read_string()?;
            self.dict.push(key);
        }

        // Read root value
        self.decode_value()
    }

    fn decode_value(&mut self) -> Result<Value, SjsonError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(SjsonError::TooDeep);
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
                if len > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
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
                if len > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
                }
                let data = self.read_bytes(len)?;
                Value::BigInt(data)
            }
            tags::ARRAY => {
                let len = self.read_uvarint()? as usize;
                if len > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
                }
                let mut arr = Vec::with_capacity(len);
                for _ in 0..len {
                    arr.push(self.decode_value()?);
                }
                Value::Array(arr)
            }
            tags::OBJECT => {
                let len = self.read_uvarint()? as usize;
                if len > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
                }
                let mut obj = BTreeMap::new();
                for _ in 0..len {
                    let key_idx = self.read_uvarint()? as usize;
                    if key_idx >= self.dict.len() {
                        return Err(SjsonError::InvalidTag(tag));
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
                if len > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
                }
                let payload = self.read_bytes(len)?;
                Value::Ext(ExtData { type_id, payload })
            }
            tags::TENSOR => {
                let dtype = DType::try_from(self.read_byte()?)?;
                let rank = self.read_byte()? as usize;
                let mut shape = Vec::with_capacity(rank);
                for _ in 0..rank {
                    shape.push(self.read_uvarint()?);
                }
                let data_len = self.read_uvarint()? as usize;
                if data_len > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
                }
                let data = self.read_bytes(data_len)?;
                Value::Tensor(TensorData::new(dtype, shape, data))
            }
            tags::TENSOR_REF => {
                let store_id = self.read_byte()?;
                let key_len = self.read_uvarint()? as usize;
                if key_len > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
                }
                let key = self.read_bytes(key_len)?;
                Value::TensorRef(TensorRef { store_id, key })
            }
            tags::IMAGE => {
                let format = self.read_byte()?;
                let width = u16::from_le_bytes(self.read_bytes_fixed::<2>()?);
                let height = u16::from_le_bytes(self.read_bytes_fixed::<2>()?);
                let data_len = self.read_uvarint()? as usize;
                if data_len > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
                }
                let data = self.read_bytes(data_len)?;
                Value::Image(ImageData { format, width, height, data })
            }
            tags::AUDIO => {
                let encoding = self.read_byte()?;
                let sample_rate = u32::from_le_bytes(self.read_bytes_fixed::<4>()?);
                let channels = self.read_byte()?;
                let data_len = self.read_uvarint()? as usize;
                if data_len > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
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
                if col_len > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
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
                if text_len > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
                }
                let text_bytes = self.read_bytes(text_len)?;
                let text = String::from_utf8(text_bytes).map_err(|_| SjsonError::InvalidUtf8)?;

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
                        _ => return Err(SjsonError::InvalidTag(op_byte)),
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
                if count > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
                }
                let mut nodes = Vec::with_capacity(count);
                for _ in 0..count {
                    nodes.push(self.decode_node_data()?);
                }
                Value::NodeBatch(NodeBatchData { nodes })
            }
            tags::EDGE_BATCH => {
                let count = self.read_uvarint()? as usize;
                if count > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
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
                if node_count > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
                }
                let mut nodes = Vec::with_capacity(node_count);
                for _ in 0..node_count {
                    nodes.push(self.decode_node_data()?);
                }
                // Decode edges
                let edge_count = self.read_uvarint()? as usize;
                if edge_count > MAX_SIZE {
                    return Err(SjsonError::TooLarge);
                }
                let mut edges = Vec::with_capacity(edge_count);
                for _ in 0..edge_count {
                    edges.push(self.decode_edge_data()?);
                }
                // Decode metadata
                let metadata = self.decode_props()?;
                Value::GraphShard(GraphShardData { nodes, edges, metadata })
            }
            _ => return Err(SjsonError::InvalidTag(tag)),
        };

        self.depth -= 1;
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn read_byte(&mut self) -> Result<u8, SjsonError> {
        if self.pos >= self.data.len() {
            return Err(SjsonError::Truncated);
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>, SjsonError> {
        if self.pos + len > self.data.len() {
            return Err(SjsonError::Truncated);
        }
        let bytes = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(bytes)
    }

    fn read_bytes_fixed<const N: usize>(&mut self) -> Result<[u8; N], SjsonError> {
        if self.pos + N > self.data.len() {
            return Err(SjsonError::Truncated);
        }
        let mut bytes = [0u8; N];
        bytes.copy_from_slice(&self.data[self.pos..self.pos + N]);
        self.pos += N;
        Ok(bytes)
    }

    fn read_into(&mut self, buf: &mut [u8]) -> Result<(), SjsonError> {
        let len = buf.len();
        if self.pos + len > self.data.len() {
            return Err(SjsonError::Truncated);
        }
        buf.copy_from_slice(&self.data[self.pos..self.pos + len]);
        self.pos += len;
        Ok(())
    }

    fn read_uvarint(&mut self) -> Result<u64, SjsonError> {
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
                return Err(SjsonError::TooLarge);
            }
        }
        Ok(result)
    }

    fn read_string(&mut self) -> Result<String, SjsonError> {
        let len = self.read_uvarint()? as usize;
        if len > MAX_SIZE {
            return Err(SjsonError::TooLarge);
        }
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes).map_err(|_| SjsonError::InvalidUtf8)
    }

    fn skip_hints(&mut self) -> Result<(), SjsonError> {
        let count = self.read_uvarint()?;
        if count > MAX_HINT_COUNT {
            return Err(SjsonError::TooLarge);
        }
        for _ in 0..count {
            let _field = self.read_string()?;
            let _typ = self.read_byte()?;
            let shape_len = self.read_uvarint()?;
            if shape_len > MAX_RANK {
                return Err(SjsonError::TooLarge);
            }
            for _ in 0..shape_len {
                let _ = self.read_uvarint()?;
            }
            let _flags = self.read_byte()?;
        }
        Ok(())
    }

    /// Decode a node (without tag byte).
    fn decode_node_data(&mut self) -> Result<NodeData, SjsonError> {
        // ID
        let id = self.read_string()?;
        // Labels
        let label_count = self.read_uvarint()? as usize;
        if label_count > MAX_SIZE {
            return Err(SjsonError::TooLarge);
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
    fn decode_edge_data(&mut self) -> Result<EdgeData, SjsonError> {
        // From, To, Type
        let from = self.read_string()?;
        let to = self.read_string()?;
        let edge_type = self.read_string()?;
        // Properties
        let props = self.decode_props()?;
        Ok(EdgeData { from, to, edge_type, props })
    }

    /// Decode dictionary-coded properties.
    fn decode_props(&mut self) -> Result<BTreeMap<String, Value>, SjsonError> {
        let prop_count = self.read_uvarint()? as usize;
        if prop_count > MAX_SIZE {
            return Err(SjsonError::TooLarge);
        }
        let mut props = BTreeMap::new();
        for _ in 0..prop_count {
            let key_idx = self.read_uvarint()? as usize;
            if key_idx >= self.dict.len() {
                return Err(SjsonError::InvalidData(format!(
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
        assert!(matches!(result, Err(SjsonError::InvalidMagic)));
    }

    #[test]
    fn test_truncated() {
        let data = b"SJ";
        let result = decode(data);
        assert!(matches!(result, Err(SjsonError::Truncated)));
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
}
