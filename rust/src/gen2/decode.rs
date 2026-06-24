//! Cowrie decoder.

use super::tags;
use super::types::{
    AudioData, AudioEncoding, CowrieError, DType, EdgeBatchData, EdgeData, ExtData, ImageData,
    ImageFormat, NodeBatchData, NodeData, TensorData, TensorRef, Value,
};
use crate::{MAGIC, VERSION};
use std::collections::BTreeMap;

/// Canonical alignment for tensor data (SPEC-v1 §2.5): a tensor's data run begins at a 64-byte
/// boundary relative to byte 0 of the message.
const TENSOR_ALIGN: usize = 64;

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
    /// Strict decode mode (SPEC-v1 §5.3): reject well-formed-but-non-canonical input
    /// with `CowrieError::NonCanonical` instead of silently accepting it. This mirrors
    /// the Python reference decoder (`tools/cowrie_ref/decode.py`) exactly. Off by default
    /// so existing decode behavior is unchanged unless explicitly opted in.
    pub strict: bool,
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
            strict: false,
        }
    }
}

/// Decode Cowrie bytes to a Value using default options.
pub fn decode(data: &[u8]) -> Result<Value, CowrieError> {
    decode_with_options(data, &DecodeOptions::default())
}

/// Decode Cowrie bytes in STRICT mode (SPEC-v1 §5.3): non-canonical input is rejected.
pub fn decode_strict(data: &[u8]) -> Result<Value, CowrieError> {
    decode_with_options(
        data,
        &DecodeOptions {
            strict: true,
            ..DecodeOptions::default()
        },
    )
}

/// Decode Cowrie bytes to a Value with configurable limits.
pub fn decode_with_options(data: &[u8], opts: &DecodeOptions) -> Result<Value, CowrieError> {
    let mut reader = Reader::new(data, opts);
    reader.decode()
}

/// Locate every tensor's `(dtype, shape, data_offset, data_len)` span within a canonical Cowrie
/// message, in document order (Phase 2 zero-copy tensor locator). `data_offset` is the ABSOLUTE
/// byte offset (from byte 0 of `data`) of the tensor's contiguous little-endian data run — a
/// zero-copy reader views `data[data_offset .. data_offset + data_len]` with no decode/copy.
///
/// Captures spans *during* decode (it does not re-walk or guess offsets), mirroring the Python
/// oracle `cowrie_ref.tensor_spans` exactly. Decodes in STRICT mode so it rejects
/// non-canonical input identically to the reference.
pub fn tensor_spans(data: &[u8]) -> Result<Vec<TensorSpan>, CowrieError> {
    let opts = DecodeOptions {
        strict: true,
        ..DecodeOptions::default()
    };
    let mut reader = Reader::new(data, &opts);
    reader.spans = Some(Vec::new());
    reader.decode()?;
    Ok(reader.spans.take().unwrap_or_default())
}

/// A located tensor data run within a canonical Cowrie message (Phase 2 zero-copy locator).
///
/// `data_offset` is the ABSOLUTE byte offset (from byte 0 of the message) of the tensor's
/// contiguous little-endian data run — the decoder position *after* the `dataLen` uvarint and
/// at the start of the data bytes. `data_len` is its byte length. Mirrors the Python oracle's
/// `(dtype, shape, data_offset, data_len)` span tuple (`tools/cowrie_ref/decode.py`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TensorSpan {
    /// Wire dtype byte (e.g. `0x01` = Float32).
    pub dtype: u8,
    /// Tensor shape dims; empty for a rank-0 scalar.
    pub shape: Vec<u64>,
    /// Absolute byte offset of the data run from byte 0 of the message.
    pub data_offset: usize,
    /// Byte length of the data run.
    pub data_len: usize,
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
    dict: Vec<String>,
    depth: usize,
    opts: &'a DecodeOptions,
    /// When `Some`, every tensor's data run is recorded here in document order during decode.
    spans: Option<Vec<TensorSpan>>,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8], opts: &'a DecodeOptions) -> Self {
        Reader {
            data,
            pos: 0,
            dict: Vec::new(),
            depth: 0,
            opts,
            spans: None,
        }
    }

    fn decode(&mut self) -> Result<Value, CowrieError> {
        // Check magic first (even if truncated, bad magic takes priority)
        if self.remaining() < 4 {
            return Err(CowrieError::Truncated);
        }
        if &self.data[0..4] != MAGIC {
            return Err(CowrieError::InvalidMagic);
        }
        self.pos = 4;

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
        let mut prev: Option<Vec<u8>> = None;
        for _ in 0..dict_len {
            // Read the raw key bytes so the strict byte-ascending/uniqueness check (§5.3)
            // compares raw UTF-8 exactly like the Python reference (`raw <= prev`).
            let key_len = self.read_uvarint()? as usize;
            if key_len > self.opts.max_string_len {
                return Err(CowrieError::TooLarge);
            }
            let raw = self.read_bytes(key_len)?;
            if self.opts.strict {
                if let Some(prev) = &prev {
                    if raw <= *prev {
                        return Err(CowrieError::NonCanonical(
                            "dictionary not byte-sorted / not unique".into(),
                        ));
                    }
                }
            }
            let key = String::from_utf8(raw.clone()).map_err(|_| CowrieError::InvalidUtf8)?;
            prev = Some(raw);
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
                let v = zigzag_decode(z);
                if self.opts.strict && (-16..=127).contains(&v) {
                    return Err(CowrieError::NonCanonical(
                        "int in FIXINT/FIXNEG range".into(),
                    ));
                }
                Value::Int(v)
            }
            tags::UINT64 => {
                let u = self.read_uvarint()?;
                if self.opts.strict && u <= i64::MAX as u64 {
                    return Err(CowrieError::NonCanonical("uint fits Int".into()));
                }
                Value::Uint(u)
            }
            tags::FLOAT64 => {
                let bytes = self.read_bytes_fixed::<8>()?;
                if self.opts.strict {
                    if bytes == [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80] {
                        return Err(CowrieError::NonCanonical("negative zero".into()));
                    }
                    let v = f64::from_le_bytes(bytes);
                    if v.is_nan()
                        && bytes != [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x7f]
                    {
                        return Err(CowrieError::NonCanonical("non-canonical NaN".into()));
                    }
                }
                Value::Float(f64::from_le_bytes(bytes))
            }
            tags::DECIMAL128 => {
                let mut data = vec![0u8; 17];
                data[0] = self.read_byte()?; // scale (svarint; single byte in canonical form)
                self.read_into(&mut data[1..])?; // 16-byte coef (little-endian, signed)
                if self.opts.strict {
                    // Lowest-terms check (§3 / Python `dec != dec.canonical()`):
                    // canonical() strips trailing-zero coefficient digits into a smaller
                    // scale, and maps any zero coefficient to scale 0. So a value is
                    // non-canonical iff (coeff != 0 && coeff % 10 == 0) OR (coeff == 0 && scale != 0).
                    let scale = zigzag_decode(data[0] as u64);
                    let mut coeff_bytes = [0u8; 16];
                    coeff_bytes.copy_from_slice(&data[1..17]);
                    let coeff = i128::from_le_bytes(coeff_bytes);
                    let non_canonical = if coeff == 0 {
                        scale != 0
                    } else {
                        coeff % 10 == 0
                    };
                    if non_canonical {
                        return Err(CowrieError::NonCanonical(
                            "decimal not lowest terms".into(),
                        ));
                    }
                }
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
                if self.opts.strict {
                    check_bigint_canonical(&data)?;
                }
                Value::BigInt(data)
            }
            tags::ARRAY => {
                let len = self.read_uvarint()? as usize;
                if self.opts.strict && len <= 15 {
                    return Err(CowrieError::NonCanonical(
                        "array should use FIXARRAY".into(),
                    ));
                }
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
                if self.opts.strict && len <= 15 {
                    return Err(CowrieError::NonCanonical("object should use FIXMAP".into()));
                }
                if len > self.opts.max_object_len {
                    return Err(CowrieError::TooLarge);
                }
                let mut obj = BTreeMap::new();
                let mut last: i64 = -1;
                for _ in 0..len {
                    let key_idx = self.read_uvarint()? as usize;
                    if key_idx >= self.dict.len() {
                        return Err(CowrieError::InvalidDictIndex {
                            index: key_idx,
                            dict_len: self.dict.len(),
                        });
                    }
                    if self.opts.strict && (key_idx as i64) <= last {
                        return Err(CowrieError::NonCanonical(
                            "object fields not in ascending dictIdx".into(),
                        ));
                    }
                    last = key_idx as i64;
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
                // Tensor data is 64-byte aligned relative to byte 0 of the message (SPEC-v1 §2.5).
                // `self.pos` here is the absolute offset right after the dataLen uvarint; consume
                // `pad = (-pos) mod 64` zero bytes so the data run starts on a 64-byte boundary,
                // mirroring the Python reference (`pad = (-self.pos) % TENSOR_ALIGN`).
                let pad = (TENSOR_ALIGN - (self.pos % TENSOR_ALIGN)) % TENSOR_ALIGN;
                let pad_bytes = self.read_bytes(pad)?;
                if self.opts.strict && pad_bytes.iter().any(|&b| b != 0) {
                    return Err(CowrieError::NonCanonical(
                        "tensor alignment padding not zero".into(),
                    ));
                }
                // Record this tensor's data span in document order (Phase 2 zero-copy locator).
                // `self.pos` here is the ABSOLUTE, 64B-ALIGNED offset of the data bytes — after the
                // dataLen uvarint and the alignment padding — matching the Python oracle's
                // `data_off = self.pos` (taken after consuming the pad).
                if let Some(spans) = self.spans.as_mut() {
                    spans.push(TensorSpan {
                        dtype: dtype as u8,
                        shape: shape.clone(),
                        data_offset: self.pos,
                        data_len,
                    });
                }
                let data = self.read_bytes(data_len)?;
                if self.opts.strict {
                    // Sub-byte dtype padding check (§5.3 / Python `data[-1] >> (bits % 8)`):
                    // the final byte must have its unused high bits zeroed.
                    let bits = tensor_total_bits(dtype, &shape);
                    if let Some(total_bits) = bits {
                        let rem = (total_bits % 8) as u32;
                        if rem != 0 {
                            if let Some(&last_byte) = data.last() {
                                if last_byte >> rem != 0 {
                                    return Err(CowrieError::NonCanonical(
                                        "tensor sub-byte padding not zero".into(),
                                    ));
                                }
                            }
                        }
                    }
                }
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
                if self.opts.strict {
                    let rem = (count % 8) as u32;
                    if rem != 0 {
                        if let Some(&last_byte) = bits.last() {
                            if last_byte >> rem != 0 {
                                return Err(CowrieError::NonCanonical(
                                    "bitmask trailing bits not zero".into(),
                                ));
                            }
                        }
                    }
                }
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
                let mut last: i64 = -1;
                for _ in 0..len {
                    let key_idx = self.read_uvarint()? as usize;
                    if key_idx >= self.dict.len() {
                        return Err(CowrieError::InvalidDictIndex {
                            index: key_idx,
                            dict_len: self.dict.len(),
                        });
                    }
                    if self.opts.strict && (key_idx as i64) <= last {
                        return Err(CowrieError::NonCanonical(
                            "object fields not in ascending dictIdx".into(),
                        ));
                    }
                    last = key_idx as i64;
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
            // 64-bit overflow: >10 bytes, or a 10th byte (shift 63) contributing bits above bit 63.
            if shift >= 64 || (shift == 63 && (b & 0x7f) > 1) {
                return Err(CowrieError::InvalidData("uvarint overflows 64 bits".into()));
            }
            result |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
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

/// Bits-per-element for a dtype, matching the Python reference `DTYPE_BITS` (§2.5).
/// Sub-byte dtypes (<8 bits) are what drive the strict padding check.
fn dtype_bits(dtype: DType) -> u64 {
    match dtype {
        DType::Float32 => 32,
        DType::Float16 => 16,
        DType::BFloat16 => 16,
        DType::Int8 => 8,
        DType::Int16 => 16,
        DType::Int32 => 32,
        DType::Int64 => 64,
        DType::Uint8 => 8,
        DType::Uint16 => 16,
        DType::Uint32 => 32,
        DType::Uint64 => 64,
        DType::Float64 => 64,
        DType::Bool => 8,
        DType::QINT4 => 4,
        DType::QINT2 => 2,
        DType::QINT3 => 3,
        DType::Ternary => 2,
        DType::Binary => 1,
    }
}

/// Total bit count for a tensor (`nelem * dtype_bits`), matching the Python reference.
/// Returns None on overflow. A rank-0 tensor is a single element; a zero dimension
/// yields zero elements.
fn tensor_total_bits(dtype: DType, shape: &[u64]) -> Option<u64> {
    let mut nelem: u64 = 1;
    for &d in shape {
        nelem = nelem.checked_mul(d)?;
    }
    nelem.checked_mul(dtype_bits(dtype))
}

/// Strict-mode canonical-form check for a BigInt's raw little-endian two's-complement
/// bytes (SPEC-v1 §5.3), mirroring the Python reference:
///   - reject if the value fits Int/Uint (INT64_MIN..=UINT64_MAX) — should use those tags
///   - reject if the bytes are not the minimal two's-complement length
fn check_bigint_canonical(raw: &[u8]) -> Result<(), CowrieError> {
    if raw.is_empty() {
        // Python int.from_bytes(b"", ...) == 0, whose minimal form is a single 0x00 byte;
        // an empty payload is therefore non-minimal (and also "fits Int").
        return Err(CowrieError::NonCanonical("bigint fits Int/Uint".into()));
    }

    // --- minimality: the top byte must not be a pure sign-extension of the next byte ---
    if raw.len() >= 2 {
        let last = raw[raw.len() - 1];
        let prev = raw[raw.len() - 2];
        let prev_sign = prev & 0x80 != 0;
        if (last == 0x00 && !prev_sign) || (last == 0xFF && prev_sign) {
            return Err(CowrieError::NonCanonical("non-minimal bigint".into()));
        }
    }

    // --- fits Int/Uint check (INT64_MIN ..= UINT64_MAX) ---
    let negative = raw[raw.len() - 1] & 0x80 != 0;
    if negative {
        // Minimal negative two's-complement: values in [-2^63, -1] need <= 8 bytes.
        // -2^63 itself is exactly [00 00 00 00 00 00 00 80] (8 bytes, minimal).
        if raw.len() <= 8 {
            return Err(CowrieError::NonCanonical("bigint fits Int/Uint".into()));
        }
    } else {
        // Non-negative. UINT64_MAX = 2^64-1 has minimal signed LE form
        // [FF*8, 00] (9 bytes, trailing 0x00 sign byte). Strip a single 0x00
        // sign byte, then anything occupying <= 8 magnitude bytes fits u64.
        let mut sig = raw.len();
        if raw[sig - 1] == 0x00 {
            sig -= 1; // drop sign byte
        }
        if sig <= 8 {
            return Err(CowrieError::NonCanonical("bigint fits Int/Uint".into()));
        }
    }

    Ok(())
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
        // canonical 64B tensor-data alignment (§2.5)
        let pad = (64 - (buf.len() % 64)) % 64;
        buf.extend(std::iter::repeat_n(0u8, pad));
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
        // Canonical 64B tensor-data alignment (SPEC-v1 §2.5): zero-pad so data starts on a
        // 64-byte boundary relative to byte 0 of the message.
        let pad = (TENSOR_ALIGN - (buf.len() % TENSOR_ALIGN)) % TENSOR_ALIGN;
        buf.extend(std::iter::repeat_n(0u8, pad));
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
        // canonical 64B tensor-data alignment (§2.5)
        let pad = (64 - (buf.len() % 64)) % 64;
        buf.extend(std::iter::repeat_n(0u8, pad));
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
