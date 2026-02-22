// Package cowrie implements Cowrie v2, a binary "JSON++" codec with extended types.
//
// Cowrie v2 extends JSON with:
//   - Explicit integer types (int64, uint64)
//   - Decimal128 for high-precision decimals
//   - Native binary data (no base64)
//   - Datetime64 (nanosecond timestamps)
//   - UUID128 (native UUIDs)
//   - BigInt (arbitrary precision)
//   - Dictionary-coded object keys
//
// Wire format:
//
//	Magic:   'S' 'J'     (2 bytes)
//	Version: 0x02        (1 byte)
//	Flags:   0x00        (1 byte)
//	DictLen: uvarint
//	Dict:    DictLen × [len:uvarint][utf8 bytes]
//	RootVal: encoded value
package cowrie

import "unsafe"

// Wire format constants
const (
	Magic0  = 'S'
	Magic1  = 'J'
	Version = 2
)

// Flag bits
const (
	FlagCompressed       = 0x01 // bit0: data is compressed
	FlagCompressionType1 = 0x02 // bit1: compression type bit 1
	FlagCompressionType2 = 0x04 // bit2: compression type bit 2
	FlagHasColumnHints   = 0x08 // bit3: column hints present after flags
)

// Compression types
type Compression uint8

const (
	CompressionNone Compression = 0
	CompressionGzip Compression = 1
	CompressionZstd Compression = 2
)

// Type tags for wire format
const (
	TagNull       = 0x00
	TagFalse      = 0x01
	TagTrue       = 0x02
	TagInt64      = 0x03
	TagFloat64    = 0x04
	TagString     = 0x05
	TagArray      = 0x06
	TagObject     = 0x07
	TagBytes      = 0x08
	TagUint64     = 0x09
	TagDecimal128 = 0x0A
	TagDatetime64 = 0x0B
	TagUUID128    = 0x0C
	TagBigInt     = 0x0D

	// TagExt is the extension envelope for forward compatibility.
	// Wire format: TagExt | ExtType:uvarint | Len:uvarint | Payload:Len bytes
	// Unknown extensions can be preserved (UnknownExt) or skipped based on DecodeOptions.
	TagExt = 0x0E

	// v2.1 ML/Multimodal extensions (0x20-0x2F)
	TagTensor    = 0x20
	TagTensorRef = 0x21
	TagImage     = 0x22
	TagAudio     = 0x23

	// v2.1 Graph/Delta extensions (0x30-0x3F)
	TagAdjlist  = 0x30
	TagRichText = 0x31
	TagDelta    = 0x32

	// v2.1 Graph types (0x35-0x39)
	TagNode       = 0x35
	TagEdge       = 0x36
	TagNodeBatch  = 0x37
	TagEdgeBatch  = 0x38
	TagGraphShard = 0x39
)

// Type represents the type of an Cowrie value.
type Type uint8

const (
	TypeNull Type = iota
	TypeBool
	TypeInt64
	TypeUint64
	TypeFloat64
	TypeDecimal128
	TypeString
	TypeBytes
	TypeDatetime64
	TypeUUID128
	TypeBigInt
	TypeArray
	TypeObject
	// v2.1 extension types
	TypeTensor
	TypeTensorRef
	TypeImage
	TypeAudio
	TypeAdjlist
	TypeRichText
	TypeDelta
	// v2.1 Graph types
	TypeNode
	TypeEdge
	TypeNodeBatch
	TypeEdgeBatch
	TypeGraphShard
	// Forward compatibility
	TypeUnknownExt // Preserved unknown extension (see UnknownExtData)
)

// String returns the type name.
func (t Type) String() string {
	switch t {
	case TypeNull:
		return "null"
	case TypeBool:
		return "bool"
	case TypeInt64:
		return "int64"
	case TypeUint64:
		return "uint64"
	case TypeFloat64:
		return "float64"
	case TypeDecimal128:
		return "decimal128"
	case TypeString:
		return "string"
	case TypeBytes:
		return "bytes"
	case TypeDatetime64:
		return "datetime64"
	case TypeUUID128:
		return "uuid128"
	case TypeBigInt:
		return "bigint"
	case TypeArray:
		return "array"
	case TypeObject:
		return "object"
	case TypeTensor:
		return "tensor"
	case TypeTensorRef:
		return "tensor_ref"
	case TypeImage:
		return "image"
	case TypeAudio:
		return "audio"
	case TypeAdjlist:
		return "adjlist"
	case TypeRichText:
		return "rich_text"
	case TypeDelta:
		return "delta"
	case TypeNode:
		return "node"
	case TypeEdge:
		return "edge"
	case TypeNodeBatch:
		return "node_batch"
	case TypeEdgeBatch:
		return "edge_batch"
	case TypeGraphShard:
		return "graph_shard"
	case TypeUnknownExt:
		return "unknown_ext"
	default:
		return "unknown"
	}
}

// Decimal128 represents a 128-bit decimal number.
// Value = Coef * 10^(-Scale)
type Decimal128 struct {
	Scale int8     // -127 to +127
	Coef  [16]byte // Two's complement big-endian
}

// ============================================================
// v2.1 Extension Types - Enums
// ============================================================

// DType represents tensor data types.
type DType uint8

const (
	DTypeFloat32  DType = 0x01
	DTypeFloat16  DType = 0x02
	DTypeBFloat16 DType = 0x03
	DTypeFloat64  DType = 0x0C // Float64 for high-precision tensors
	DTypeInt8     DType = 0x04
	DTypeInt16    DType = 0x05
	DTypeInt32    DType = 0x06
	DTypeInt64    DType = 0x07
	DTypeUint8    DType = 0x08
	DTypeUint16   DType = 0x09
	DTypeUint32   DType = 0x0A
	DTypeUint64   DType = 0x0B
	DTypeBool     DType = 0x0D // Boolean tensor elements
	// Quantized types (0x10-0x1F)
	DTypeQINT4   DType = 0x10 // 4-bit quantized integer
	DTypeQINT2   DType = 0x11 // 2-bit quantized integer
	DTypeQINT3   DType = 0x12 // 3-bit quantized integer
	DTypeTernary DType = 0x13 // Ternary (-1, 0, 1)
	DTypeBinary  DType = 0x14 // Binary (0, 1)
)

// ImageFormat represents image formats.
type ImageFormat uint8

const (
	ImageFormatJPEG ImageFormat = 0x01
	ImageFormatPNG  ImageFormat = 0x02
	ImageFormatWEBP ImageFormat = 0x03
	ImageFormatAVIF ImageFormat = 0x04
	ImageFormatBMP  ImageFormat = 0x05
)

// AudioEncoding represents audio encodings.
type AudioEncoding uint8

const (
	AudioEncodingPCMInt16   AudioEncoding = 0x01
	AudioEncodingPCMFloat32 AudioEncoding = 0x02
	AudioEncodingOPUS       AudioEncoding = 0x03
	AudioEncodingAAC        AudioEncoding = 0x04
)

// IDWidth represents adjacency list node ID width.
type IDWidth uint8

const (
	IDWidthInt32 IDWidth = 0x01
	IDWidthInt64 IDWidth = 0x02
)

// DeltaOpCode represents delta operation types.
type DeltaOpCode uint8

const (
	DeltaOpSetField    DeltaOpCode = 0x01
	DeltaOpDeleteField DeltaOpCode = 0x02
	DeltaOpAppendArray DeltaOpCode = 0x03
)

// ============================================================
// v2.1 Extension Types - Structs
// ============================================================

// TensorData represents a tensor value.
type TensorData struct {
	DType DType    // Data type
	Dims  []uint64 // Shape dimensions
	Data  []byte   // Raw tensor bytes, row-major
}

// ViewFloat32 returns a zero-copy view of the tensor data as []float32.
// Returns (nil, false) if dtype mismatch, alignment or size is invalid.
// Returns (nil, true) for empty data with correct dtype.
func (td *TensorData) ViewFloat32() ([]float32, bool) {
	if td == nil || td.DType != DTypeFloat32 {
		return nil, false
	}
	if len(td.Data) == 0 {
		return nil, true // Empty is valid
	}
	if len(td.Data)%4 != 0 {
		return nil, false
	}
	ptr := unsafe.Pointer(&td.Data[0])
	if uintptr(ptr)%4 != 0 {
		return nil, false
	}
	count := len(td.Data) / 4
	return unsafe.Slice((*float32)(ptr), count), true
}

// ViewFloat64 returns a zero-copy view of the tensor data as []float64.
// Returns (nil, false) if dtype mismatch, alignment or size is invalid.
// Returns (nil, true) for empty data with correct dtype.
func (td *TensorData) ViewFloat64() ([]float64, bool) {
	if td == nil || td.DType != DTypeFloat64 {
		return nil, false
	}
	if len(td.Data) == 0 {
		return nil, true // Empty is valid
	}
	if len(td.Data)%8 != 0 {
		return nil, false
	}
	ptr := unsafe.Pointer(&td.Data[0])
	if uintptr(ptr)%8 != 0 {
		return nil, false
	}
	count := len(td.Data) / 8
	return unsafe.Slice((*float64)(ptr), count), true
}

// ViewInt32 returns a zero-copy view of the tensor data as []int32.
// Returns (nil, false) if dtype mismatch, alignment or size is invalid.
// Returns (nil, true) for empty data with correct dtype.
func (td *TensorData) ViewInt32() ([]int32, bool) {
	if td == nil || td.DType != DTypeInt32 {
		return nil, false
	}
	if len(td.Data) == 0 {
		return nil, true // Empty is valid
	}
	if len(td.Data)%4 != 0 {
		return nil, false
	}
	ptr := unsafe.Pointer(&td.Data[0])
	if uintptr(ptr)%4 != 0 {
		return nil, false
	}
	count := len(td.Data) / 4
	return unsafe.Slice((*int32)(ptr), count), true
}

// ViewInt64 returns a zero-copy view of the tensor data as []int64.
// Returns (nil, false) if dtype mismatch, alignment or size is invalid.
// Returns (nil, true) for empty data with correct dtype.
func (td *TensorData) ViewInt64() ([]int64, bool) {
	if td == nil || td.DType != DTypeInt64 {
		return nil, false
	}
	if len(td.Data) == 0 {
		return nil, true // Empty is valid
	}
	if len(td.Data)%8 != 0 {
		return nil, false
	}
	ptr := unsafe.Pointer(&td.Data[0])
	if uintptr(ptr)%8 != 0 {
		return nil, false
	}
	count := len(td.Data) / 8
	return unsafe.Slice((*int64)(ptr), count), true
}

// ViewUint8 returns a zero-copy view of the tensor data as []uint8.
// Always succeeds since byte alignment is always valid.
func (td *TensorData) ViewUint8() ([]uint8, bool) {
	if td == nil {
		return nil, false
	}
	return td.Data, true
}

// TensorRefData represents a reference to a stored tensor.
type TensorRefData struct {
	StoreID uint8  // Which store/shard
	Key     []byte // Lookup key (UUID, hash, etc.)
}

// ImageData represents an image value.
type ImageData struct {
	Format ImageFormat // Image format
	Width  uint16      // Width in pixels
	Height uint16      // Height in pixels
	Data   []byte      // Encoded image bytes
}

// AudioData represents an audio value.
type AudioData struct {
	Encoding   AudioEncoding // Audio encoding
	SampleRate uint32        // Sample rate in Hz
	Channels   uint8         // Number of channels
	Data       []byte        // Audio data bytes
}

// AdjlistData represents a CSR adjacency list for graphs.
type AdjlistData struct {
	IDWidth    IDWidth  // 1=int32, 2=int64
	NodeCount  uint64   // Number of nodes
	EdgeCount  uint64   // Number of edges
	RowOffsets []uint64 // [NodeCount + 1] offsets
	ColIndices []byte   // Edge destinations (int32/int64 LE based on IDWidth)
}

// RichTextSpan represents an annotated span within rich text.
type RichTextSpan struct {
	Start  uint64 // Byte offset start
	End    uint64 // Byte offset end
	KindID uint64 // Application-defined kind
}

// RichTextData represents text with optional tokens and spans.
type RichTextData struct {
	Text   string         // UTF-8 text
	Tokens []int32        // Token IDs (optional, nil if not present)
	Spans  []RichTextSpan // Annotated spans (optional, nil if not present)
}

// DeltaOp represents a single delta operation.
type DeltaOp struct {
	OpCode  DeltaOpCode // Operation type
	FieldID uint64      // Dictionary-coded field ID
	Value   *Value      // For SetField and AppendArray
}

// DeltaData represents a semantic diff/patch.
type DeltaData struct {
	BaseID uint64    // Reference to base object
	Ops    []DeltaOp // Operations
}

// UnknownExtData represents a preserved unknown extension.
// When the decoder encounters a TagExt with an unrecognized ExtType,
// and OnUnknownExt is set to Keep (default), the extension is preserved
// as an UnknownExtData so it can be round-tripped without data loss.
type UnknownExtData struct {
	ExtType uint64 // Extension type identifier
	Payload []byte // Raw extension payload
}

// ============================================================
// v2.1 Graph Types
// ============================================================

// NodeData represents a graph node with ID, labels, and properties.
// Properties are dictionary-coded for efficient encoding.
type NodeData struct {
	ID     string         // Unique node identifier
	Labels []string       // Node labels/types
	Props  map[string]any // Node properties (keys are dictionary-coded)
}

// EdgeData represents a graph edge with source, destination, type, and properties.
// Properties are dictionary-coded for efficient encoding.
type EdgeData struct {
	From  string         // Source node ID
	To    string         // Destination node ID
	Type  string         // Edge type/label
	Props map[string]any // Edge properties (keys are dictionary-coded)
}

// NodeBatchData represents a batch of nodes for streaming.
// Useful for GNN mini-batches and bulk graph loading.
type NodeBatchData struct {
	Nodes []NodeData
}

// EdgeBatchData represents a batch of edges for streaming.
// Useful for GNN mini-batches and bulk graph loading.
type EdgeBatchData struct {
	Edges []EdgeData
}

// GraphShardData represents a self-contained subgraph.
// Includes nodes, edges, and optional metadata.
// Useful for distributed graph processing and checkpointing.
type GraphShardData struct {
	Nodes    []NodeData     // Nodes in this shard
	Edges    []EdgeData     // Edges in this shard
	Metadata map[string]any // Shard metadata (keys are dictionary-coded)
}

// Member represents an object member (key-value pair).
type Member struct {
	Key   string
	Value *Value
}

// Value represents an Cowrie value of any type.
type Value struct {
	typ Type

	// Data fields (only one is valid based on typ)
	boolVal    bool
	int64Val   int64
	uint64Val  uint64
	float64Val float64
	decimal128 Decimal128
	stringVal  string
	bytesVal   []byte
	datetime64 int64 // nanoseconds since Unix epoch
	uuid128    [16]byte
	bigintVal  []byte // Two's complement big-endian
	arrayVal   []*Value
	objectVal  []Member

	// v2.1 extension type fields
	tensorVal    TensorData
	tensorRefVal TensorRefData
	imageVal     ImageData
	audioVal     AudioData
	adjlistVal   AdjlistData
	richTextVal  RichTextData
	deltaVal     DeltaData

	// v2.1 graph type fields
	nodeVal       NodeData
	edgeVal       EdgeData
	nodeBatchVal  NodeBatchData
	edgeBatchVal  EdgeBatchData
	graphShardVal GraphShardData

	// Forward compatibility
	unknownExtVal UnknownExtData
}

// Type returns the type of this value.
func (v *Value) Type() Type {
	if v == nil {
		return TypeNull
	}
	return v.typ
}

// IsNull returns true if this value is null.
func (v *Value) IsNull() bool {
	return v == nil || v.typ == TypeNull
}

// Bool returns the boolean value. Panics if not a bool.
func (v *Value) Bool() bool {
	if v.typ != TypeBool {
		panic("cowrie: not a bool")
	}
	return v.boolVal
}

// Int64 returns the int64 value. Panics if not an int64.
func (v *Value) Int64() int64 {
	if v.typ != TypeInt64 {
		panic("cowrie: not an int64")
	}
	return v.int64Val
}

// Uint64 returns the uint64 value. Panics if not a uint64.
func (v *Value) Uint64() uint64 {
	if v.typ != TypeUint64 {
		panic("cowrie: not a uint64")
	}
	return v.uint64Val
}

// Float64 returns the float64 value. Panics if not a float64.
func (v *Value) Float64() float64 {
	if v.typ != TypeFloat64 {
		panic("cowrie: not a float64")
	}
	return v.float64Val
}

// Decimal128 returns the decimal128 value. Panics if not a decimal128.
func (v *Value) Decimal128() Decimal128 {
	if v.typ != TypeDecimal128 {
		panic("cowrie: not a decimal128")
	}
	return v.decimal128
}

// String returns the string value. Panics if not a string.
func (v *Value) String() string {
	if v.typ != TypeString {
		panic("cowrie: not a string")
	}
	return v.stringVal
}

// Bytes returns the bytes value. Panics if not bytes.
func (v *Value) Bytes() []byte {
	if v.typ != TypeBytes {
		panic("cowrie: not bytes")
	}
	return v.bytesVal
}

// Datetime64 returns the datetime value as nanoseconds since Unix epoch.
// Panics if not a datetime64.
func (v *Value) Datetime64() int64 {
	if v.typ != TypeDatetime64 {
		panic("cowrie: not a datetime64")
	}
	return v.datetime64
}

// UUID128 returns the UUID value. Panics if not a uuid128.
func (v *Value) UUID128() [16]byte {
	if v.typ != TypeUUID128 {
		panic("cowrie: not a uuid128")
	}
	return v.uuid128
}

// BigInt returns the bigint value as two's complement big-endian bytes.
// Panics if not a bigint.
func (v *Value) BigInt() []byte {
	if v.typ != TypeBigInt {
		panic("cowrie: not a bigint")
	}
	return v.bigintVal
}

// Len returns the length of an array or object. Returns 0 for other types.
func (v *Value) Len() int {
	switch v.typ {
	case TypeArray:
		return len(v.arrayVal)
	case TypeObject:
		return len(v.objectVal)
	default:
		return 0
	}
}

// Index returns the i-th element of an array. Panics if not an array or out of bounds.
func (v *Value) Index(i int) *Value {
	if v.typ != TypeArray {
		panic("cowrie: not an array")
	}
	return v.arrayVal[i]
}

// Get returns the value for a key in an object, or nil if not found.
// Panics if not an object.
func (v *Value) Get(key string) *Value {
	if v.typ != TypeObject {
		panic("cowrie: not an object")
	}
	for _, m := range v.objectVal {
		if m.Key == key {
			return m.Value
		}
	}
	return nil
}

// Members returns all members of an object. Panics if not an object.
func (v *Value) Members() []Member {
	if v.typ != TypeObject {
		panic("cowrie: not an object")
	}
	return v.objectVal
}

// Array returns all elements of an array. Panics if not an array.
func (v *Value) Array() []*Value {
	if v.typ != TypeArray {
		panic("cowrie: not an array")
	}
	return v.arrayVal
}

// Set sets a key-value pair on an object. Panics if not an object.
func (v *Value) Set(key string, val *Value) {
	if v.typ != TypeObject {
		panic("cowrie: not an object")
	}
	// Check if key exists
	for i := range v.objectVal {
		if v.objectVal[i].Key == key {
			v.objectVal[i].Value = val
			return
		}
	}
	// Add new member
	v.objectVal = append(v.objectVal, Member{Key: key, Value: val})
}

// Append adds a value to an array. Panics if not an array.
func (v *Value) Append(val *Value) {
	if v.typ != TypeArray {
		panic("cowrie: not an array")
	}
	v.arrayVal = append(v.arrayVal, val)
}

// ============================================================
// v2.1 Extension Type Accessors
// ============================================================

// Tensor returns the tensor data. Panics if not a tensor.
func (v *Value) Tensor() TensorData {
	if v.typ != TypeTensor {
		panic("cowrie: not a tensor")
	}
	return v.tensorVal
}

// TensorRef returns the tensor reference data. Panics if not a tensor_ref.
func (v *Value) TensorRef() TensorRefData {
	if v.typ != TypeTensorRef {
		panic("cowrie: not a tensor_ref")
	}
	return v.tensorRefVal
}

// Image returns the image data. Panics if not an image.
func (v *Value) Image() ImageData {
	if v.typ != TypeImage {
		panic("cowrie: not an image")
	}
	return v.imageVal
}

// Audio returns the audio data. Panics if not an audio.
func (v *Value) Audio() AudioData {
	if v.typ != TypeAudio {
		panic("cowrie: not an audio")
	}
	return v.audioVal
}

// Adjlist returns the adjacency list data. Panics if not an adjlist.
func (v *Value) Adjlist() AdjlistData {
	if v.typ != TypeAdjlist {
		panic("cowrie: not an adjlist")
	}
	return v.adjlistVal
}

// RichText returns the rich text data. Panics if not a rich_text.
func (v *Value) RichText() RichTextData {
	if v.typ != TypeRichText {
		panic("cowrie: not a rich_text")
	}
	return v.richTextVal
}

// Delta returns the delta data. Panics if not a delta.
func (v *Value) Delta() DeltaData {
	if v.typ != TypeDelta {
		panic("cowrie: not a delta")
	}
	return v.deltaVal
}

// UnknownExt returns the unknown extension data. Panics if not an unknown_ext.
func (v *Value) UnknownExt() UnknownExtData {
	if v.typ != TypeUnknownExt {
		panic("cowrie: not an unknown_ext")
	}
	return v.unknownExtVal
}

// TryUnknownExt returns the unknown extension data and true if this is an unknown_ext,
// or (UnknownExtData{}, false) otherwise.
func (v *Value) TryUnknownExt() (UnknownExtData, bool) {
	if v == nil || v.typ != TypeUnknownExt {
		return UnknownExtData{}, false
	}
	return v.unknownExtVal, true
}

// ============================================================
// Safe Accessor Methods (non-panicking alternatives)
// ============================================================

// TryBool returns the boolean value and true if this is a bool, or (false, false) otherwise.
func (v *Value) TryBool() (bool, bool) {
	if v == nil || v.typ != TypeBool {
		return false, false
	}
	return v.boolVal, true
}

// BoolOr returns the boolean value if this is a bool, or the default value otherwise.
func (v *Value) BoolOr(defaultVal bool) bool {
	if val, ok := v.TryBool(); ok {
		return val
	}
	return defaultVal
}

// TryInt64 returns the int64 value and true if this is an int64, or (0, false) otherwise.
func (v *Value) TryInt64() (int64, bool) {
	if v == nil || v.typ != TypeInt64 {
		return 0, false
	}
	return v.int64Val, true
}

// Int64Or returns the int64 value if this is an int64, or the default value otherwise.
func (v *Value) Int64Or(defaultVal int64) int64 {
	if val, ok := v.TryInt64(); ok {
		return val
	}
	return defaultVal
}

// TryUint64 returns the uint64 value and true if this is a uint64, or (0, false) otherwise.
func (v *Value) TryUint64() (uint64, bool) {
	if v == nil || v.typ != TypeUint64 {
		return 0, false
	}
	return v.uint64Val, true
}

// Uint64Or returns the uint64 value if this is a uint64, or the default value otherwise.
func (v *Value) Uint64Or(defaultVal uint64) uint64 {
	if val, ok := v.TryUint64(); ok {
		return val
	}
	return defaultVal
}

// TryFloat64 returns the float64 value and true if this is a float64, or (0, false) otherwise.
func (v *Value) TryFloat64() (float64, bool) {
	if v == nil || v.typ != TypeFloat64 {
		return 0, false
	}
	return v.float64Val, true
}

// Float64Or returns the float64 value if this is a float64, or the default value otherwise.
func (v *Value) Float64Or(defaultVal float64) float64 {
	if val, ok := v.TryFloat64(); ok {
		return val
	}
	return defaultVal
}

// TryString returns the string value and true if this is a string, or ("", false) otherwise.
func (v *Value) TryString() (string, bool) {
	if v == nil || v.typ != TypeString {
		return "", false
	}
	return v.stringVal, true
}

// StringOr returns the string value if this is a string, or the default value otherwise.
func (v *Value) StringOr(defaultVal string) string {
	if val, ok := v.TryString(); ok {
		return val
	}
	return defaultVal
}

// TryBytes returns the bytes value and true if this is bytes, or (nil, false) otherwise.
func (v *Value) TryBytes() ([]byte, bool) {
	if v == nil || v.typ != TypeBytes {
		return nil, false
	}
	return v.bytesVal, true
}

// TryDatetime64 returns the datetime value and true if this is a datetime64, or (0, false) otherwise.
func (v *Value) TryDatetime64() (int64, bool) {
	if v == nil || v.typ != TypeDatetime64 {
		return 0, false
	}
	return v.datetime64, true
}

// TryUUID128 returns the UUID value and true if this is a uuid128, or ([16]byte{}, false) otherwise.
func (v *Value) TryUUID128() ([16]byte, bool) {
	if v == nil || v.typ != TypeUUID128 {
		return [16]byte{}, false
	}
	return v.uuid128, true
}

// TryArray returns the array elements and true if this is an array, or (nil, false) otherwise.
func (v *Value) TryArray() ([]*Value, bool) {
	if v == nil || v.typ != TypeArray {
		return nil, false
	}
	return v.arrayVal, true
}

// TryObject returns the object members and true if this is an object, or (nil, false) otherwise.
func (v *Value) TryObject() ([]Member, bool) {
	if v == nil || v.typ != TypeObject {
		return nil, false
	}
	return v.objectVal, true
}

// TryTensor returns the tensor data and true if this is a tensor, or (TensorData{}, false) otherwise.
func (v *Value) TryTensor() (TensorData, bool) {
	if v == nil || v.typ != TypeTensor {
		return TensorData{}, false
	}
	return v.tensorVal, true
}

// GetOr returns the value for a key in an object, or defaultVal if not found or not an object.
func (v *Value) GetOr(key string, defaultVal *Value) *Value {
	if v == nil || v.typ != TypeObject {
		return defaultVal
	}
	for _, m := range v.objectVal {
		if m.Key == key {
			return m.Value
		}
	}
	return defaultVal
}

// IndexOr returns the i-th element of an array, or defaultVal if out of bounds or not an array.
func (v *Value) IndexOr(i int, defaultVal *Value) *Value {
	if v == nil || v.typ != TypeArray || i < 0 || i >= len(v.arrayVal) {
		return defaultVal
	}
	return v.arrayVal[i]
}

// ============================================================
// v2.1 Graph Type Accessors
// ============================================================

// Node returns the node data. Panics if not a node.
func (v *Value) Node() NodeData {
	if v.typ != TypeNode {
		panic("cowrie: not a node")
	}
	return v.nodeVal
}

// TryNode returns the node data and true if this is a node, or (NodeData{}, false) otherwise.
func (v *Value) TryNode() (NodeData, bool) {
	if v == nil || v.typ != TypeNode {
		return NodeData{}, false
	}
	return v.nodeVal, true
}

// Edge returns the edge data. Panics if not an edge.
func (v *Value) Edge() EdgeData {
	if v.typ != TypeEdge {
		panic("cowrie: not an edge")
	}
	return v.edgeVal
}

// TryEdge returns the edge data and true if this is an edge, or (EdgeData{}, false) otherwise.
func (v *Value) TryEdge() (EdgeData, bool) {
	if v == nil || v.typ != TypeEdge {
		return EdgeData{}, false
	}
	return v.edgeVal, true
}

// NodeBatch returns the node batch data. Panics if not a node_batch.
func (v *Value) NodeBatch() NodeBatchData {
	if v.typ != TypeNodeBatch {
		panic("cowrie: not a node_batch")
	}
	return v.nodeBatchVal
}

// TryNodeBatch returns the node batch data and true if this is a node_batch, or (NodeBatchData{}, false) otherwise.
func (v *Value) TryNodeBatch() (NodeBatchData, bool) {
	if v == nil || v.typ != TypeNodeBatch {
		return NodeBatchData{}, false
	}
	return v.nodeBatchVal, true
}

// EdgeBatch returns the edge batch data. Panics if not an edge_batch.
func (v *Value) EdgeBatch() EdgeBatchData {
	if v.typ != TypeEdgeBatch {
		panic("cowrie: not an edge_batch")
	}
	return v.edgeBatchVal
}

// TryEdgeBatch returns the edge batch data and true if this is an edge_batch, or (EdgeBatchData{}, false) otherwise.
func (v *Value) TryEdgeBatch() (EdgeBatchData, bool) {
	if v == nil || v.typ != TypeEdgeBatch {
		return EdgeBatchData{}, false
	}
	return v.edgeBatchVal, true
}

// GraphShard returns the graph shard data. Panics if not a graph_shard.
func (v *Value) GraphShard() GraphShardData {
	if v.typ != TypeGraphShard {
		panic("cowrie: not a graph_shard")
	}
	return v.graphShardVal
}

// TryGraphShard returns the graph shard data and true if this is a graph_shard, or (GraphShardData{}, false) otherwise.
func (v *Value) TryGraphShard() (GraphShardData, bool) {
	if v == nil || v.typ != TypeGraphShard {
		return GraphShardData{}, false
	}
	return v.graphShardVal, true
}
