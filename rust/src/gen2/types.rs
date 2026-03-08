//! Cowrie value types.

use std::collections::BTreeMap;
use std::fmt;

/// Cowrie error type.
#[derive(Debug)]
pub enum CowrieError {
    InvalidMagic,
    InvalidVersion(u8),
    InvalidTag(u8),
    InvalidData(String),
    Truncated,
    InvalidUtf8,
    Io(std::io::Error),
    TooDeep,
    TooLarge,
    TrailingData { pos: usize, remaining: usize },
    InvalidDictIndex { index: usize, dict_len: usize },
    RankExceeded { rank: usize, max: usize },
}

impl fmt::Display for CowrieError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CowrieError::InvalidMagic => write!(f, "invalid Cowrie magic"),
            CowrieError::InvalidVersion(v) => write!(f, "invalid Cowrie version: {}", v),
            CowrieError::InvalidTag(t) => write!(f, "invalid tag: 0x{:02x}", t),
            CowrieError::InvalidData(msg) => write!(f, "invalid data: {}", msg),
            CowrieError::Truncated => write!(f, "truncated data"),
            CowrieError::InvalidUtf8 => write!(f, "invalid UTF-8"),
            CowrieError::Io(e) => write!(f, "I/O error: {}", e),
            CowrieError::TooDeep => write!(f, "nesting too deep"),
            CowrieError::TooLarge => write!(f, "data too large"),
            CowrieError::TrailingData { pos, remaining } => write!(f, "trailing data after root value: {} unconsumed bytes at position {}", remaining, pos),
            CowrieError::InvalidDictIndex { index, dict_len } => write!(f, "dictionary index {} out of range (dict size: {})", index, dict_len),
            CowrieError::RankExceeded { rank, max } => write!(f, "tensor rank {} exceeds maximum {}", rank, max),
        }
    }
}

impl std::error::Error for CowrieError {}

impl From<std::io::Error> for CowrieError {
    fn from(e: std::io::Error) -> Self {
        CowrieError::Io(e)
    }
}

/// Data type for tensors - aligned with Go reference implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DType {
    Float32 = 0x01,
    Float16 = 0x02,
    BFloat16 = 0x03,
    Int8 = 0x04,
    Int16 = 0x05,
    Int32 = 0x06,
    Int64 = 0x07,
    Uint8 = 0x08,
    Uint16 = 0x09,
    Uint32 = 0x0A,
    Uint64 = 0x0B,
    Float64 = 0x0C,
    Bool = 0x0D,
    // Quantized types
    QINT4 = 0x10,    // 4-bit quantized integer
    QINT2 = 0x11,    // 2-bit quantized integer
    QINT3 = 0x12,    // 3-bit quantized integer
    Ternary = 0x13,  // Ternary (-1, 0, 1)
    Binary = 0x14,   // Binary (0, 1)
}

impl TryFrom<u8> for DType {
    type Error = CowrieError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(DType::Float32),
            0x02 => Ok(DType::Float16),
            0x03 => Ok(DType::BFloat16),
            0x04 => Ok(DType::Int8),
            0x05 => Ok(DType::Int16),
            0x06 => Ok(DType::Int32),
            0x07 => Ok(DType::Int64),
            0x08 => Ok(DType::Uint8),
            0x09 => Ok(DType::Uint16),
            0x0A => Ok(DType::Uint32),
            0x0B => Ok(DType::Uint64),
            0x0C => Ok(DType::Float64),
            0x0D => Ok(DType::Bool),
            0x10 => Ok(DType::QINT4),
            0x11 => Ok(DType::QINT2),
            0x12 => Ok(DType::QINT3),
            0x13 => Ok(DType::Ternary),
            0x14 => Ok(DType::Binary),
            _ => Err(CowrieError::InvalidTag(value)),
        }
    }
}

/// Tensor data with zero-copy view support.
#[derive(Debug, Clone, PartialEq)]
pub struct TensorData {
    pub dtype: DType,
    pub shape: Vec<u64>,
    pub data: Vec<u8>,
}

impl TensorData {
    /// Create a new tensor.
    pub fn new(dtype: DType, shape: Vec<u64>, data: Vec<u8>) -> Self {
        TensorData { dtype, shape, data }
    }

    /// Zero-copy view as f32 slice. Returns None if dtype mismatch or alignment issue.
    pub fn view_float32(&self) -> Option<&[f32]> {
        if self.dtype != DType::Float32 {
            return None;
        }
        if self.data.is_empty() {
            return Some(&[]);
        }
        if self.data.len() % 4 != 0 {
            return None;
        }
        // Check alignment
        let ptr = self.data.as_ptr();
        if ptr as usize % std::mem::align_of::<f32>() != 0 {
            return None;
        }
        // SAFETY: We checked dtype, alignment, and length divisibility
        unsafe {
            Some(std::slice::from_raw_parts(
                ptr as *const f32,
                self.data.len() / 4,
            ))
        }
    }

    /// Zero-copy view as f64 slice. Returns None if dtype mismatch or alignment issue.
    pub fn view_float64(&self) -> Option<&[f64]> {
        if self.dtype != DType::Float64 {
            return None;
        }
        if self.data.is_empty() {
            return Some(&[]);
        }
        if self.data.len() % 8 != 0 {
            return None;
        }
        let ptr = self.data.as_ptr();
        if ptr as usize % std::mem::align_of::<f64>() != 0 {
            return None;
        }
        unsafe {
            Some(std::slice::from_raw_parts(
                ptr as *const f64,
                self.data.len() / 8,
            ))
        }
    }

    /// Zero-copy view as i32 slice.
    pub fn view_int32(&self) -> Option<&[i32]> {
        if self.dtype != DType::Int32 {
            return None;
        }
        if self.data.is_empty() {
            return Some(&[]);
        }
        if self.data.len() % 4 != 0 {
            return None;
        }
        let ptr = self.data.as_ptr();
        if ptr as usize % std::mem::align_of::<i32>() != 0 {
            return None;
        }
        unsafe {
            Some(std::slice::from_raw_parts(
                ptr as *const i32,
                self.data.len() / 4,
            ))
        }
    }

    /// Zero-copy view as i64 slice.
    pub fn view_int64(&self) -> Option<&[i64]> {
        if self.dtype != DType::Int64 {
            return None;
        }
        if self.data.is_empty() {
            return Some(&[]);
        }
        if self.data.len() % 8 != 0 {
            return None;
        }
        let ptr = self.data.as_ptr();
        if ptr as usize % std::mem::align_of::<i64>() != 0 {
            return None;
        }
        unsafe {
            Some(std::slice::from_raw_parts(
                ptr as *const i64,
                self.data.len() / 8,
            ))
        }
    }

    /// Copy as f32 Vec. Always succeeds for Float32 tensors.
    pub fn copy_float32(&self) -> Option<Vec<f32>> {
        if self.dtype != DType::Float32 {
            return None;
        }
        let count = self.data.len() / 4;
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let bytes = [
                self.data[i * 4],
                self.data[i * 4 + 1],
                self.data[i * 4 + 2],
                self.data[i * 4 + 3],
            ];
            out.push(f32::from_le_bytes(bytes));
        }
        Some(out)
    }

    /// Copy as f64 Vec. Always succeeds for Float64 tensors.
    pub fn copy_float64(&self) -> Option<Vec<f64>> {
        if self.dtype != DType::Float64 {
            return None;
        }
        let count = self.data.len() / 8;
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let bytes = [
                self.data[i * 8],
                self.data[i * 8 + 1],
                self.data[i * 8 + 2],
                self.data[i * 8 + 3],
                self.data[i * 8 + 4],
                self.data[i * 8 + 5],
                self.data[i * 8 + 6],
                self.data[i * 8 + 7],
            ];
            out.push(f64::from_le_bytes(bytes));
        }
        Some(out)
    }

    /// Get f32 slice, preferring zero-copy view, falling back to copy.
    pub fn float32_slice(&self) -> Option<Vec<f32>> {
        if let Some(view) = self.view_float32() {
            return Some(view.to_vec());
        }
        self.copy_float32()
    }

    /// Get f64 slice, preferring zero-copy view, falling back to copy.
    pub fn float64_slice(&self) -> Option<Vec<f64>> {
        if let Some(view) = self.view_float64() {
            return Some(view.to_vec());
        }
        self.copy_float64()
    }
}

/// Tensor reference (external storage).
#[derive(Debug, Clone, PartialEq)]
pub struct TensorRef {
    pub store_id: u8,
    pub key: Vec<u8>,
}

/// Image format - aligned with Go reference implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ImageFormat {
    Jpeg = 0x01,
    Png = 0x02,
    Webp = 0x03,
    Avif = 0x04,
    Bmp = 0x05,
}

impl TryFrom<u8> for ImageFormat {
    type Error = CowrieError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(ImageFormat::Jpeg),
            0x02 => Ok(ImageFormat::Png),
            0x03 => Ok(ImageFormat::Webp),
            0x04 => Ok(ImageFormat::Avif),
            0x05 => Ok(ImageFormat::Bmp),
            _ => Err(CowrieError::InvalidData(format!(
                "invalid image format: 0x{:02x}",
                value
            ))),
        }
    }
}

/// Audio encoding - aligned with Go reference implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AudioEncoding {
    PcmInt16 = 0x01,
    PcmFloat32 = 0x02,
    Opus = 0x03,
    Aac = 0x04,
}

impl TryFrom<u8> for AudioEncoding {
    type Error = CowrieError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(AudioEncoding::PcmInt16),
            0x02 => Ok(AudioEncoding::PcmFloat32),
            0x03 => Ok(AudioEncoding::Opus),
            0x04 => Ok(AudioEncoding::Aac),
            _ => Err(CowrieError::InvalidData(format!(
                "invalid audio encoding: 0x{:02x}",
                value
            ))),
        }
    }
}

/// Image data.
#[derive(Debug, Clone, PartialEq)]
pub struct ImageData {
    pub format: ImageFormat,
    pub width: u16,
    pub height: u16,
    pub data: Vec<u8>,
}

/// Audio data.
#[derive(Debug, Clone, PartialEq)]
pub struct AudioData {
    pub encoding: AudioEncoding,
    pub sample_rate: u32,
    pub channels: u8,
    pub data: Vec<u8>,
}

/// Adjacency list data for graph types.
#[derive(Debug, Clone, PartialEq)]
pub struct AdjlistData {
    pub id_width: u8,      // 1=int32, 2=int64
    pub node_count: u64,
    pub edge_count: u64,
    pub row_offsets: Vec<u64>,
    pub col_indices: Vec<u8>,
}

/// Rich text span.
#[derive(Debug, Clone, PartialEq)]
pub struct RichTextSpan {
    pub start: u64,
    pub end: u64,
    pub kind_id: u64,
}

/// Rich text data with optional tokens and spans.
#[derive(Debug, Clone, PartialEq)]
pub struct RichTextData {
    pub text: String,
    pub tokens: Option<Vec<i32>>,
    pub spans: Option<Vec<RichTextSpan>>,
}

/// Delta operation codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DeltaOpCode {
    SetField = 0x01,
    DeleteField = 0x02,
    AppendArray = 0x03,
}

/// Delta operation.
#[derive(Debug, Clone, PartialEq)]
pub struct DeltaOp {
    pub op_code: DeltaOpCode,
    pub field_id: u64,
    pub value: Option<Box<Value>>,
}

/// Delta data representing a semantic diff/patch.
#[derive(Debug, Clone, PartialEq)]
pub struct DeltaData {
    pub base_id: u64,
    pub ops: Vec<DeltaOp>,
}

/// Extension type (unknown extension).
#[derive(Debug, Clone, PartialEq)]
pub struct ExtData {
    pub type_id: u64,
    pub payload: Vec<u8>,
}

// ============================================================
// Graph Types (v2.1)
// ============================================================

/// Graph node with ID, labels, and properties.
/// Properties are dictionary-coded for efficient encoding.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeData {
    pub id: String,
    pub labels: Vec<String>,
    pub props: BTreeMap<String, Value>,
}

impl NodeData {
    /// Create a new node.
    pub fn new(id: impl Into<String>, labels: Vec<String>, props: BTreeMap<String, Value>) -> Self {
        NodeData {
            id: id.into(),
            labels,
            props,
        }
    }
}

/// Graph edge with source, destination, type, and properties.
/// Properties are dictionary-coded for efficient encoding.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeData {
    pub from: String,
    pub to: String,
    pub edge_type: String,
    pub props: BTreeMap<String, Value>,
}

impl EdgeData {
    /// Create a new edge.
    pub fn new(
        from: impl Into<String>,
        to: impl Into<String>,
        edge_type: impl Into<String>,
        props: BTreeMap<String, Value>,
    ) -> Self {
        EdgeData {
            from: from.into(),
            to: to.into(),
            edge_type: edge_type.into(),
            props,
        }
    }
}

/// Batch of nodes for streaming.
/// Useful for GNN mini-batches and bulk graph loading.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeBatchData {
    pub nodes: Vec<NodeData>,
}

/// Batch of edges for streaming.
/// Useful for GNN mini-batches and bulk graph loading.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeBatchData {
    pub edges: Vec<EdgeData>,
}

/// Self-contained subgraph with nodes, edges, and metadata.
/// Useful for distributed graph processing and checkpointing.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphShardData {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
    pub metadata: BTreeMap<String, Value>,
}

/// Cowrie Value type.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Decimal(Vec<u8>), // 17 bytes: scale + 16-byte coef
    String(String),
    Bytes(Vec<u8>),
    DateTime(i64),
    Uuid([u8; 16]),
    BigInt(Vec<u8>),
    Array(Vec<Value>),
    Object(BTreeMap<String, Value>),
    Tensor(TensorData),
    TensorRef(TensorRef),
    Image(ImageData),
    Audio(AudioData),
    Adjlist(AdjlistData),
    RichText(RichTextData),
    Delta(DeltaData),
    Ext(ExtData),
    // Graph types (v2.1)
    Node(NodeData),
    Edge(EdgeData),
    NodeBatch(NodeBatchData),
    EdgeBatch(EdgeBatchData),
    GraphShard(GraphShardData),
}

impl Value {
    /// Create an object from key-value pairs.
    pub fn object<K: Into<String>, V: Into<Value>>(pairs: Vec<(K, V)>) -> Value {
        let mut map = BTreeMap::new();
        for (k, v) in pairs {
            map.insert(k.into(), v.into());
        }
        Value::Object(map)
    }

    /// Check if value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Get as bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Get as i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Get as u64.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::Uint(u) => Some(*u),
            Value::Int(i) if *i >= 0 => Some(*i as u64),
            _ => None,
        }
    }

    /// Get as f64.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Get as string slice.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get as bytes slice.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Get as array slice.
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    /// Get as object reference.
    pub fn as_object(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Value::Object(o) => Some(o),
            _ => None,
        }
    }

    /// Get a field from object.
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Object(o) => o.get(key),
            _ => None,
        }
    }

    /// Get tensor data.
    pub fn as_tensor(&self) -> Option<&TensorData> {
        match self {
            Value::Tensor(t) => Some(t),
            _ => None,
        }
    }

    /// Get node data.
    pub fn as_node(&self) -> Option<&NodeData> {
        match self {
            Value::Node(n) => Some(n),
            _ => None,
        }
    }

    /// Get edge data.
    pub fn as_edge(&self) -> Option<&EdgeData> {
        match self {
            Value::Edge(e) => Some(e),
            _ => None,
        }
    }

    /// Get node batch data.
    pub fn as_node_batch(&self) -> Option<&NodeBatchData> {
        match self {
            Value::NodeBatch(nb) => Some(nb),
            _ => None,
        }
    }

    /// Get edge batch data.
    pub fn as_edge_batch(&self) -> Option<&EdgeBatchData> {
        match self {
            Value::EdgeBatch(eb) => Some(eb),
            _ => None,
        }
    }

    /// Get graph shard data.
    pub fn as_graph_shard(&self) -> Option<&GraphShardData> {
        match self {
            Value::GraphShard(gs) => Some(gs),
            _ => None,
        }
    }
}

// Convenience conversions
impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Int(i)
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Value::Int(i as i64)
    }
}

impl From<u64> for Value {
    fn from(u: u64) -> Self {
        Value::Uint(u)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Value::Bytes(b)
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(v: Vec<T>) -> Self {
        Value::Array(v.into_iter().map(|x| x.into()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tensor_view_float32() {
        // Aligned data
        let data: Vec<u8> = vec![
            0x00, 0x00, 0x80, 0x3f, // 1.0f32
            0x00, 0x00, 0x00, 0x40, // 2.0f32
            0x00, 0x00, 0x40, 0x40, // 3.0f32
        ];
        let tensor = TensorData::new(DType::Float32, vec![3], data);

        // Try zero-copy view (may fail on unaligned allocations)
        if let Some(view) = tensor.view_float32() {
            assert_eq!(view.len(), 3);
            assert!((view[0] - 1.0).abs() < 1e-6);
            assert!((view[1] - 2.0).abs() < 1e-6);
            assert!((view[2] - 3.0).abs() < 1e-6);
        }

        // Copy always works
        let copied = tensor.copy_float32().unwrap();
        assert_eq!(copied.len(), 3);
        assert!((copied[0] - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_tensor_view_wrong_dtype() {
        let tensor = TensorData::new(DType::Float64, vec![2], vec![0u8; 16]);
        assert!(tensor.view_float32().is_none());
        assert!(tensor.view_int32().is_none());
    }

    #[test]
    fn test_image_format_try_from() {
        assert_eq!(ImageFormat::try_from(0x01).unwrap(), ImageFormat::Jpeg);
        assert_eq!(ImageFormat::try_from(0x02).unwrap(), ImageFormat::Png);
        assert_eq!(ImageFormat::try_from(0x03).unwrap(), ImageFormat::Webp);
        assert_eq!(ImageFormat::try_from(0x04).unwrap(), ImageFormat::Avif);
        assert_eq!(ImageFormat::try_from(0x05).unwrap(), ImageFormat::Bmp);
        assert!(ImageFormat::try_from(0x00).is_err());
        assert!(ImageFormat::try_from(0x06).is_err());
        assert!(ImageFormat::try_from(0xFF).is_err());
    }

    #[test]
    fn test_image_format_roundtrip_u8() {
        let formats = [
            ImageFormat::Jpeg,
            ImageFormat::Png,
            ImageFormat::Webp,
            ImageFormat::Avif,
            ImageFormat::Bmp,
        ];
        for fmt in formats {
            let byte = fmt as u8;
            let back = ImageFormat::try_from(byte).unwrap();
            assert_eq!(fmt, back);
        }
    }

    #[test]
    fn test_audio_encoding_try_from() {
        assert_eq!(AudioEncoding::try_from(0x01).unwrap(), AudioEncoding::PcmInt16);
        assert_eq!(AudioEncoding::try_from(0x02).unwrap(), AudioEncoding::PcmFloat32);
        assert_eq!(AudioEncoding::try_from(0x03).unwrap(), AudioEncoding::Opus);
        assert_eq!(AudioEncoding::try_from(0x04).unwrap(), AudioEncoding::Aac);
        assert!(AudioEncoding::try_from(0x00).is_err());
        assert!(AudioEncoding::try_from(0x05).is_err());
        assert!(AudioEncoding::try_from(0xFF).is_err());
    }

    #[test]
    fn test_audio_encoding_roundtrip_u8() {
        let encodings = [
            AudioEncoding::PcmInt16,
            AudioEncoding::PcmFloat32,
            AudioEncoding::Opus,
            AudioEncoding::Aac,
        ];
        for enc in encodings {
            let byte = enc as u8;
            let back = AudioEncoding::try_from(byte).unwrap();
            assert_eq!(enc, back);
        }
    }

    #[test]
    fn test_value_object() {
        let obj = Value::object(vec![
            ("name", Value::String("test".into())),
            ("count", Value::Int(42)),
        ]);

        assert_eq!(obj.get("name").and_then(|v| v.as_str()), Some("test"));
        assert_eq!(obj.get("count").and_then(|v| v.as_i64()), Some(42));
    }
}
