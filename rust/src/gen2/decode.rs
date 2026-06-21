//! Cowrie decoder.

use super::tags;
use super::types::{
    AudioData, AudioEncoding, CowrieError, DType, EdgeBatchData, EdgeData, ExtData, ImageData,
    ImageFormat, NodeBatchData, NodeData, TensorData, TensorRef, Value,
};
use crate::{MAGIC, VERSION};
use std::collections::BTreeMap;

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
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self {
            max_depth: 1_000,
            max_array_len: 1_000_000,   // Tightened: was 100M
            max_object_len: 1_000_000,  // Tightened: was 10M
            max_string_len: 10_000_000, // Tightened: was 500M
            max_bytes_len: 50_000_000,  // Tightened: was 1G
            max_ext_len: 1_000_000,     // Tightened: was 100M
            max_dict_len: 1_000_000,    // Tightened: was 10M
            max_rank: 32,
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

        // Read flags (reserved — ignore)
        let _flags = self.read_byte()?;

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
                // Validate dataLen == product(dims) * elemSize for byte-aligned dtypes.
                // Sub-byte packed dtypes (Bool, QINTs, Ternary, Binary) are skipped.
                if let Some(expected) = tensor_expected_bytes(dtype, &shape) {
                    if data_len != expected {
                        return Err(CowrieError::InvalidData(format!(
                            "tensor dataLen {} does not match shape/dtype (expected {} bytes)",
                            data_len, expected
                        )));
                    }
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
                Value::Image(ImageData {
                    format,
                    width,
                    height,
                    data,
                })
            }
            tags::AUDIO => {
                let encoding_byte = self.read_byte()?;
                let encoding = AudioEncoding::try_from(encoding_byte)?;
                let sample_rate = u32::from_le_bytes(self.read_bytes_fixed::<4>()?);
                let channels = self.read_byte()?;
                // channels is u8 on the wire but 0 is semantically invalid (no frames).
                if channels == 0 {
                    return Err(CowrieError::InvalidData(
                        "audio channel count must be >= 1".into(),
                    ));
                }
                let data_len = self.read_uvarint()? as usize;
                if data_len > self.opts.max_bytes_len {
                    return Err(CowrieError::TooLarge);
                }
                let data = self.read_bytes(data_len)?;
                Value::Audio(AudioData {
                    encoding,
                    sample_rate,
                    channels,
                    data,
                })
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
            tags::BITMASK => {
                let count = self.read_uvarint()?;
                let byte_len = count.div_ceil(8) as usize;
                if byte_len > self.opts.max_bytes_len {
                    return Err(CowrieError::TooLarge);
                }
                let bits = self.read_bytes(byte_len)?;
                Value::Bitmask { count, bits }
            }
            t if (tags::FIXINT_BASE..=tags::FIXINT_MAX).contains(&t) => {
                Value::Int((t - tags::FIXINT_BASE) as i64)
            }
            t if (tags::FIXARRAY_BASE..=tags::FIXARRAY_MAX).contains(&t) => {
                let len = (t - tags::FIXARRAY_BASE) as usize;
                if len > self.opts.max_array_len {
                    return Err(CowrieError::TooLarge);
                }
                let mut arr = Vec::with_capacity(len);
                for _ in 0..len {
                    arr.push(self.decode_value()?);
                }
                Value::Array(arr)
            }
            t if (tags::FIXMAP_BASE..=tags::FIXMAP_MAX).contains(&t) => {
                let len = (t - tags::FIXMAP_BASE) as usize;
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
            t if (tags::FIXNEG_BASE..=tags::FIXNEG_MAX).contains(&t) => {
                Value::Int(-1 - (t - tags::FIXNEG_BASE) as i64)
            }
            // Reserved / stripped tags in 0x30–0x34 and 0x39: length-prefixed payload.
            // Silently skip payload so that a reader can forward-scan past unknown tags.
            t if (0x30..=0x34).contains(&t) || t == 0x39 => {
                let payload_len = self.read_uvarint()? as usize;
                if payload_len > self.opts.max_bytes_len {
                    return Err(CowrieError::TooLarge);
                }
                let _ = self.read_bytes(payload_len)?;
                // Return Null as a placeholder — callers that care about type should inspect the tag.
                Value::Null
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
        Ok(EdgeData {
            from,
            to,
            edge_type,
            props,
        })
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
                    key_idx,
                    self.dict.len()
                )));
            }
            let key = self.dict[key_idx].clone();
            let val = self.decode_value()?;
            props.insert(key, val);
        }
        Ok(props)
    }
}

/// Returns the element size in bytes for byte-aligned dtypes.
/// Returns None for sub-byte packed types (Bool, QINT4, QINT2, QINT3, Ternary, Binary)
/// whose packing formula is non-trivial and should not be validated here.
fn dtype_elem_size(dtype: DType) -> Option<u64> {
    match dtype {
        DType::Float32 => Some(4),
        DType::Float16 => Some(2),
        DType::BFloat16 => Some(2),
        DType::Float64 => Some(8),
        DType::Int8 => Some(1),
        DType::Int16 => Some(2),
        DType::Int32 => Some(4),
        DType::Int64 => Some(8),
        DType::Uint8 => Some(1),
        DType::Uint16 => Some(2),
        DType::Uint32 => Some(4),
        DType::Uint64 => Some(8),
        // Bool stores one byte per element; bit-packed booleans use the Bitmask type.
        DType::Bool => Some(1),
        // Sub-byte packed types: skip validation
        DType::QINT4 | DType::QINT2 | DType::QINT3 | DType::Ternary | DType::Binary => None,
    }
}

/// Computes the expected byte length for a tensor with the given dtype and shape.
/// Returns Some(expected) for byte-aligned dtypes with overflow-safe product.
/// Returns None for sub-byte dtypes or when the product overflows u64.
/// A rank-0 tensor (no dims) is a scalar — one element; a zero dimension yields
/// an empty 0-byte tensor.
pub(crate) fn tensor_expected_bytes(dtype: DType, shape: &[u64]) -> Option<usize> {
    let elem_size = dtype_elem_size(dtype)?;

    // Rank-0 (no dims) is a scalar: one element. A zero dimension is handled below.
    if shape.is_empty() {
        return Some(elem_size as usize);
    }

    let mut product: u64 = 1;
    for &d in shape {
        if d == 0 {
            return Some(0);
        }
        // Overflow-safe multiplication
        product = product.checked_mul(d)?;
    }

    let total = product.checked_mul(elem_size)?;
    Some(total as usize)
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
            Value::Float(std::f64::consts::PI),
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
            (
                "scores",
                Value::Array(vec![Value::Float(1.1), Value::Float(2.2)]),
            ),
            ("nested", Value::object(vec![("inner", Value::Bool(true))])),
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
        use super::super::types::{NodeBatchData, NodeData};

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
        buf.push(33); // rank = 33 (exceeds MAX_RANK=32)
                      // Shape: 33 dimensions all = 1
        buf.extend(std::iter::repeat_n(1u8, 33)); // uvarint 1 per dimension
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
        buf.push(32); // rank = 32 (exactly MAX_RANK)
                      // Shape: 32 dimensions all = 1
        buf.extend(std::iter::repeat_n(1u8, 32)); // uvarint 1 per dimension
                                                  // data_len = 4 bytes (1 float32 element = product of all dims * 4)
        buf.push(4); // uvarint 4
        buf.extend_from_slice(&1.0f32.to_le_bytes());

        let result = decode(&buf);
        assert!(result.is_ok(), "should accept tensor rank = 32");
    }

    // ============================================================
    // FIX 3 — Tensor shape/dataLen mismatch validation
    // ============================================================

    fn make_tensor_buf(dtype_byte: u8, shape: &[u8], data: &[u8]) -> Vec<u8> {
        use crate::{MAGIC, VERSION};
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.push(VERSION);
        buf.push(0); // flags
        buf.push(0); // dict len = 0
        buf.push(0x20); // TENSOR tag
        buf.push(dtype_byte);
        buf.push(shape.len() as u8); // rank
        for &d in shape {
            buf.push(d); // each dim as uvarint (single byte, fits <=127)
        }
        buf.push(data.len() as u8); // data_len as uvarint
        buf.extend_from_slice(data);
        buf
    }

    #[test]
    fn test_tensor_shape_mismatch_rejected() {
        // Float32 tensor with shape [2,3] = 6 elements = 24 bytes expected.
        // Provide only 8 bytes — should be rejected.
        let buf = make_tensor_buf(0x01, &[2, 3], &[0u8; 8]);
        let result = decode(&buf);
        assert!(result.is_err(), "dataLen mismatch should be rejected");
        match result {
            Err(CowrieError::InvalidData(msg)) => {
                assert!(
                    msg.contains("dataLen"),
                    "error should mention dataLen: {}",
                    msg
                );
            }
            other => panic!("expected InvalidData, got {:?}", other),
        }
    }

    #[test]
    fn test_tensor_shape_match_accepted() {
        // Float32 tensor with shape [2,3] = 6 elements = 24 bytes.
        let buf = make_tensor_buf(0x01, &[2, 3], &[0u8; 24]);
        let result = decode(&buf);
        assert!(
            result.is_ok(),
            "valid tensor should be accepted: {:?}",
            result
        );
        match result.unwrap() {
            Value::Tensor(t) => {
                assert_eq!(t.shape, vec![2, 3]);
                assert_eq!(t.data.len(), 24);
            }
            other => panic!("expected Tensor, got {:?}", other),
        }
    }

    #[test]
    fn test_tensor_shape_mismatch_float64_rejected() {
        // Float64 tensor with shape [3] = 3 elements = 24 bytes expected.
        // Provide only 16 bytes (2 elements).
        let buf = make_tensor_buf(0x0C, &[3], &[0u8; 16]);
        let result = decode(&buf);
        assert!(
            result.is_err(),
            "float64 dataLen mismatch should be rejected"
        );
    }

    #[test]
    fn test_tensor_sub_byte_dtype_not_validated() {
        // QINT4 (0x10) is genuinely sub-byte: validation is skipped, any data len passes.
        let buf = make_tensor_buf(0x10, &[8], &[0xAB, 0xCD, 0xEF, 0x01]);
        assert!(
            decode(&buf).is_ok(),
            "sub-byte dtype should skip shape validation"
        );
    }

    #[test]
    fn test_tensor_bool_validated() {
        // Bool (0x0D) is one byte per element now: shape [4] needs exactly 4 bytes.
        let bad = make_tensor_buf(0x0D, &[4], &[0b1010_1010u8]);
        assert!(
            decode(&bad).is_err(),
            "bool[4] with 1 byte should be rejected"
        );
        let good = make_tensor_buf(0x0D, &[4], &[1, 0, 1, 0]);
        assert!(decode(&good).is_ok(), "bool[4] with 4 bytes should pass");
    }

    #[test]
    fn test_tensor_zero_dim_accepted() {
        // A shape with a zero dimension → 0 bytes expected. Empty data should pass.
        let buf = make_tensor_buf(0x01, &[0], &[]);
        let result = decode(&buf);
        assert!(result.is_ok(), "zero-dim tensor should accept 0 bytes");
    }

    // ============================================================
    // FIX 1 — BigInt two's-complement round-trip
    // These tests verify that the encoder/decoder faithfully preserve
    // arbitrary two's-complement big-endian byte sequences, which is
    // the required wire format per SPEC (tag 0x0D).
    // ============================================================

    #[test]
    fn test_bigint_twos_complement_roundtrip() {
        // Known two's-complement big-endian encodings:
        //   0          → [0x00]
        //   1          → [0x01]
        //   127        → [0x7F]
        //   128        → [0x00, 0x80]  (needs sign byte so it isn't read as negative)
        //   255        → [0x00, 0xFF]
        //   -1         → [0xFF]
        //   -128       → [0x80]
        //   -129       → [0xFF, 0x7F]
        //   -256       → [0xFF, 0x00]
        //  large negative (-2^63) → [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("zero", vec![0x00]),
            ("one", vec![0x01]),
            ("127", vec![0x7F]),
            ("128", vec![0x00, 0x80]),
            ("255", vec![0x00, 0xFF]),
            ("-1", vec![0xFF]),
            ("-128", vec![0x80]),
            ("-129", vec![0xFF, 0x7F]),
            ("-256", vec![0xFF, 0x00]),
            (
                "-2^63",
                vec![0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            ),
            (
                "large-pos",
                vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            ),
        ];

        for (label, bytes) in cases {
            let val = Value::BigInt(bytes.clone());
            let encoded = encode(&val).unwrap_or_else(|_| panic!("encode {}", label));
            let decoded = decode(&encoded).unwrap_or_else(|_| panic!("decode {}", label));
            assert_eq!(
                decoded,
                Value::BigInt(bytes),
                "round-trip failed for {}",
                label
            );
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
        let opts = DecodeOptions {
            max_depth: 10,
            ..DecodeOptions::default()
        };
        let decoded = decode_with_options(&encoded, &opts);
        assert!(decoded.is_ok(), "depth 5 should pass with max_depth=10");

        // depth=5 should fail with max_depth=3
        let opts_tight = DecodeOptions {
            max_depth: 3,
            ..DecodeOptions::default()
        };
        let result = decode_with_options(&encoded, &opts_tight);
        assert!(
            matches!(result, Err(CowrieError::TooDeep)),
            "depth 5 should fail with max_depth=3"
        );
    }

    #[test]
    fn test_decode_with_options_max_array_len() {
        let val = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let encoded = encode(&val).expect("encode");

        // max_array_len=3: should pass
        let opts = DecodeOptions {
            max_array_len: 3,
            ..DecodeOptions::default()
        };
        assert!(decode_with_options(&encoded, &opts).is_ok());

        // max_array_len=2: should fail
        let opts_tight = DecodeOptions {
            max_array_len: 2,
            ..DecodeOptions::default()
        };
        assert!(matches!(
            decode_with_options(&encoded, &opts_tight),
            Err(CowrieError::TooLarge)
        ));
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
        let opts = DecodeOptions {
            max_object_len: 3,
            ..DecodeOptions::default()
        };
        assert!(decode_with_options(&encoded, &opts).is_ok());

        // max_object_len=2: should fail
        let opts_tight = DecodeOptions {
            max_object_len: 2,
            ..DecodeOptions::default()
        };
        assert!(matches!(
            decode_with_options(&encoded, &opts_tight),
            Err(CowrieError::TooLarge)
        ));
    }

    #[test]
    fn test_decode_with_options_max_string_len() {
        let val = Value::String("hello".into()); // 5 bytes
        let encoded = encode(&val).expect("encode");

        // max_string_len=10: should pass
        let opts = DecodeOptions {
            max_string_len: 10,
            ..DecodeOptions::default()
        };
        assert!(decode_with_options(&encoded, &opts).is_ok());

        // max_string_len=3: should fail
        let opts_tight = DecodeOptions {
            max_string_len: 3,
            ..DecodeOptions::default()
        };
        assert!(matches!(
            decode_with_options(&encoded, &opts_tight),
            Err(CowrieError::TooLarge)
        ));
    }

    #[test]
    fn test_decode_with_options_max_bytes_len() {
        let val = Value::Bytes(vec![1, 2, 3, 4, 5]); // 5 bytes
        let encoded = encode(&val).expect("encode");

        // max_bytes_len=10: should pass
        let opts = DecodeOptions {
            max_bytes_len: 10,
            ..DecodeOptions::default()
        };
        assert!(decode_with_options(&encoded, &opts).is_ok());

        // max_bytes_len=3: should fail
        let opts_tight = DecodeOptions {
            max_bytes_len: 3,
            ..DecodeOptions::default()
        };
        assert!(matches!(
            decode_with_options(&encoded, &opts_tight),
            Err(CowrieError::TooLarge)
        ));
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
        buf.push(5); // rank = 5
        buf.extend(std::iter::repeat_n(1u8, 5)); // uvarint 1 per dimension
        buf.push(4); // data_len = 4
        buf.extend_from_slice(&1.0f32.to_le_bytes());

        // max_rank=5: should pass
        let opts = DecodeOptions {
            max_rank: 5,
            ..DecodeOptions::default()
        };
        assert!(decode_with_options(&buf, &opts).is_ok());

        // max_rank=3: should fail
        let opts_tight = DecodeOptions {
            max_rank: 3,
            ..DecodeOptions::default()
        };
        let result = decode_with_options(&buf, &opts_tight);
        assert!(result.is_err(), "rank 5 should fail with max_rank=3");
    }

    #[test]
    fn test_decode_with_options_max_ext_len() {
        let val = Value::Ext(ExtData {
            type_id: 42,
            payload: vec![0xAB; 100],
        });
        let encoded = encode(&val).expect("encode");

        // max_ext_len=200: should pass
        let opts = DecodeOptions {
            max_ext_len: 200,
            ..DecodeOptions::default()
        };
        assert!(decode_with_options(&encoded, &opts).is_ok());

        // max_ext_len=50: should fail
        let opts_tight = DecodeOptions {
            max_ext_len: 50,
            ..DecodeOptions::default()
        };
        assert!(matches!(
            decode_with_options(&encoded, &opts_tight),
            Err(CowrieError::TooLarge)
        ));
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
        let d2 =
            decode_with_options(&encoded, &DecodeOptions::default()).expect("decode_with_options");
        assert_eq!(d1, d2);
    }
}
