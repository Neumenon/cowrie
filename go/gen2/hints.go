package gen2

import (
	"encoding/binary"
)

// HintType specifies the expected data type for a column hint.
type HintType byte

const (
	HintInt64    HintType = 0x03 // Maps to TagInt64
	HintFloat64  HintType = 0x04 // Maps to TagFloat64
	HintString   HintType = 0x05 // Maps to TagString
	HintBytes    HintType = 0x08 // Maps to TagBytes
	HintUint64   HintType = 0x09 // Maps to TagUint64
	HintDatetime HintType = 0x0B // Maps to TagDatetime64
	HintUUID     HintType = 0x0C // Maps to TagUUID128
	HintFloat32  HintType = 0x14 // For tensor data (not a base Cowrie type)
)

// String returns the hint type name.
func (h HintType) String() string {
	switch h {
	case HintInt64:
		return "int64"
	case HintFloat64:
		return "float64"
	case HintString:
		return "string"
	case HintBytes:
		return "bytes"
	case HintUint64:
		return "uint64"
	case HintDatetime:
		return "datetime"
	case HintUUID:
		return "uuid"
	case HintFloat32:
		return "float32"
	default:
		return "unknown"
	}
}

// HintFlags provides additional metadata about a column.
type HintFlags byte

const (
	HintFlagRequired  HintFlags = 0x01 // Field appears in most objects
	HintFlagColumnar  HintFlags = 0x02 // Suitable for columnar reading
	HintFlagFixedSize HintFlags = 0x04 // Array has fixed element size
	HintFlagSorted    HintFlags = 0x08 // Values are sorted
)

// Has checks if a flag is set.
func (f HintFlags) Has(flag HintFlags) bool {
	return f&flag != 0
}

// ColumnHint provides optional metadata about a field for columnar optimization.
// Wire format:
//
//	[fieldLen: uvarint][field: utf8]
//	[type: 1 byte]
//	[shapeLen: uvarint][shape: uvarint...]
//	[flags: 1 byte]
type ColumnHint struct {
	Field string    // Field path (e.g., "id", "props.embedding")
	Type  HintType  // Expected Cowrie type
	Shape []int     // For arrays/tensors (e.g., [128] for float32[128])
	Flags HintFlags // Additional hints
}

// NewHint creates a simple column hint without shape.
func NewHint(field string, typ HintType, flags HintFlags) ColumnHint {
	return ColumnHint{
		Field: field,
		Type:  typ,
		Flags: flags,
	}
}

// NewTensorHint creates a hint for tensor/array data with shape.
func NewTensorHint(field string, typ HintType, shape []int, flags HintFlags) ColumnHint {
	return ColumnHint{
		Field: field,
		Type:  typ,
		Shape: shape,
		Flags: flags | HintFlagFixedSize,
	}
}

// encodeHint writes a single column hint to the buffer.
func encodeHint(buf *buffer, h ColumnHint) {
	// Field name
	buf.writeString(h.Field)
	// Type
	buf.writeByte(byte(h.Type))
	// Shape
	buf.writeUvarint(uint64(len(h.Shape)))
	for _, dim := range h.Shape {
		buf.writeUvarint(uint64(dim))
	}
	// Flags
	buf.writeByte(byte(h.Flags))
}

// encodeHints writes all column hints to the buffer.
func encodeHints(buf *buffer, hints []ColumnHint) {
	buf.writeUvarint(uint64(len(hints)))
	for _, h := range hints {
		encodeHint(buf, h)
	}
}

// decodeHint reads a single column hint from the reader.
func decodeHint(r *reader) (ColumnHint, error) {
	var h ColumnHint

	// Field name
	field, err := r.readString()
	if err != nil {
		return h, err
	}
	h.Field = field

	// Type
	typ, err := r.readByte()
	if err != nil {
		return h, err
	}
	h.Type = HintType(typ)

	// Shape
	shapeLen, err := r.readUvarint()
	if err != nil {
		return h, err
	}
	if shapeLen > 0 {
		h.Shape = make([]int, shapeLen)
		for i := uint64(0); i < shapeLen; i++ {
			dim, err := r.readUvarint()
			if err != nil {
				return h, err
			}
			h.Shape[i] = int(dim)
		}
	}

	// Flags
	flags, err := r.readByte()
	if err != nil {
		return h, err
	}
	h.Flags = HintFlags(flags)

	return h, nil
}

// decodeHints reads all column hints from the reader.
func decodeHints(r *reader) ([]ColumnHint, error) {
	count, err := r.readUvarint()
	if err != nil {
		return nil, err
	}

	hints := make([]ColumnHint, count)
	for i := uint64(0); i < count; i++ {
		h, err := decodeHint(r)
		if err != nil {
			return nil, err
		}
		hints[i] = h
	}
	return hints, nil
}

// HintResult holds the decoded value along with any column hints.
type HintResult struct {
	Value *Value
	Hints []ColumnHint
}

// EncodeWithHints encodes a value with column hints.
func EncodeWithHints(v *Value, hints []ColumnHint) ([]byte, error) {
	var buf buffer

	// Build dictionary
	d := newDict()
	collectKeys(v, d)

	// Write header
	buf.writeByte(Magic0)
	buf.writeByte(Magic1)
	buf.writeByte(Version)

	// Flags - set HAS_COLUMN_HINTS bit if hints present
	flags := byte(0)
	if len(hints) > 0 {
		flags |= FlagHasColumnHints
	}
	buf.writeByte(flags)

	// Write hints (if present)
	if len(hints) > 0 {
		encodeHints(&buf, hints)
	}

	// Write dictionary
	buf.writeUvarint(uint64(len(d.keys)))
	for _, key := range d.keys {
		buf.writeString(key)
	}

	// Write root value
	if err := encodeValue(&buf, v, d); err != nil {
		return nil, err
	}

	return buf.bytes(), nil
}

// DecodeWithHints decodes Cowrie data and returns both the value and any hints.
func DecodeWithHints(data []byte) (*HintResult, error) {
	r := &reader{data: data}

	// Read header
	magic0, err := r.readByte()
	if err != nil {
		return nil, err
	}
	magic1, err := r.readByte()
	if err != nil {
		return nil, err
	}
	if magic0 != Magic0 || magic1 != Magic1 {
		return nil, ErrInvalidMagic
	}

	version, err := r.readByte()
	if err != nil {
		return nil, err
	}
	if version != Version {
		return nil, ErrInvalidVersion
	}

	// Read flags
	flags, err := r.readByte()
	if err != nil {
		return nil, err
	}

	result := &HintResult{}

	// Read hints if present
	if flags&FlagHasColumnHints != 0 {
		hints, err := decodeHints(r)
		if err != nil {
			return nil, err
		}
		result.Hints = hints
	}

	// Read dictionary
	dictLen, err := r.readUvarint()
	if err != nil {
		return nil, err
	}

	dict := make([]string, dictLen)
	for i := uint64(0); i < dictLen; i++ {
		s, err := r.readString()
		if err != nil {
			return nil, err
		}
		dict[i] = s
	}

	// Decode root value
	v, err := decodeValue(r, dict)
	if err != nil {
		return nil, err
	}
	result.Value = v

	return result, nil
}

// ByteSize returns the estimated byte size of a hint type.
func (h HintType) ByteSize() int {
	switch h {
	case HintInt64, HintUint64, HintFloat64, HintDatetime:
		return 8
	case HintFloat32:
		return 4
	case HintUUID:
		return 16
	default:
		return -1 // Variable size
	}
}

// TotalBytes returns the total bytes for a tensor with given shape.
// Returns -1 if the type has variable size.
func (h ColumnHint) TotalBytes() int {
	elemSize := h.Type.ByteSize()
	if elemSize < 0 {
		return -1
	}
	total := elemSize
	for _, dim := range h.Shape {
		total *= dim
	}
	return total
}

// Helper to encode uvarint to bytes (used by tests)
func uvarintBytes(v uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], v)
	return buf[:n]
}
