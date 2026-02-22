package gen2

import (
	"encoding/binary"
	"errors"
	"math"
)

// Errors for column reader
var (
	ErrFieldNotFound    = errors.New("cowrie: field not found")
	ErrIncompatibleType = errors.New("cowrie: field type incompatible with hint")
	ErrNoHints          = errors.New("cowrie: no column hints available")
	ErrArrayRequired    = errors.New("cowrie: root value must be an array for columnar access")
)

// ColumnReader provides columnar access to Cowrie data using hints.
// It enables partial decoding of specific fields without fully decoding the data.
type ColumnReader struct {
	data   []byte
	hints  []ColumnHint
	dict   []string
	root   *Value // Lazily decoded
	offset int    // Position after header, hints, dict
}

// NewColumnReader creates a reader from Cowrie data.
// Returns an error if the data has no column hints.
func NewColumnReader(data []byte) (*ColumnReader, error) {
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

	flags, err := r.readByte()
	if err != nil {
		return nil, err
	}

	cr := &ColumnReader{data: data}

	// Read hints
	if flags&FlagHasColumnHints != 0 {
		hints, err := decodeHints(r)
		if err != nil {
			return nil, err
		}
		cr.hints = hints
	}

	// Read dictionary
	dictLen, err := r.readUvarint()
	if err != nil {
		return nil, err
	}
	cr.dict = make([]string, dictLen)
	for i := uint64(0); i < dictLen; i++ {
		s, err := r.readString()
		if err != nil {
			return nil, err
		}
		cr.dict[i] = s
	}

	cr.offset = r.pos
	return cr, nil
}

// Hints returns the column hints, or nil if none.
func (cr *ColumnReader) Hints() []ColumnHint {
	return cr.hints
}

// GetHint returns the hint for a specific field, or nil if not found.
func (cr *ColumnReader) GetHint(field string) *ColumnHint {
	for i := range cr.hints {
		if cr.hints[i].Field == field {
			return &cr.hints[i]
		}
	}
	return nil
}

// decodeRoot lazily decodes the root value.
func (cr *ColumnReader) decodeRoot() error {
	if cr.root != nil {
		return nil
	}
	r := &reader{data: cr.data, pos: cr.offset}
	root, err := decodeValue(r, cr.dict)
	if err != nil {
		return err
	}
	cr.root = root
	return nil
}

// Root returns the fully decoded root value.
func (cr *ColumnReader) Root() (*Value, error) {
	if err := cr.decodeRoot(); err != nil {
		return nil, err
	}
	return cr.root, nil
}

// ReadColumn extracts a column of values for a given field.
// The root must be an array of objects.
// Returns a slice of values, one per array element.
func (cr *ColumnReader) ReadColumn(field string) ([]*Value, error) {
	if err := cr.decodeRoot(); err != nil {
		return nil, err
	}

	if cr.root.Type() != TypeArray {
		return nil, ErrArrayRequired
	}

	items := cr.root.Array()
	result := make([]*Value, len(items))

	for i, item := range items {
		if item.Type() != TypeObject {
			result[i] = nil
			continue
		}
		result[i] = item.Get(field)
	}

	return result, nil
}

// ReadInt64Column extracts a column of int64 values.
// Returns a slice of int64 and a parallel slice of booleans indicating valid values.
func (cr *ColumnReader) ReadInt64Column(field string) ([]int64, []bool, error) {
	values, err := cr.ReadColumn(field)
	if err != nil {
		return nil, nil, err
	}

	result := make([]int64, len(values))
	valid := make([]bool, len(values))

	for i, v := range values {
		if v != nil && v.Type() == TypeInt64 {
			result[i] = v.Int64()
			valid[i] = true
		}
	}

	return result, valid, nil
}

// ReadFloat64Column extracts a column of float64 values.
func (cr *ColumnReader) ReadFloat64Column(field string) ([]float64, []bool, error) {
	values, err := cr.ReadColumn(field)
	if err != nil {
		return nil, nil, err
	}

	result := make([]float64, len(values))
	valid := make([]bool, len(values))

	for i, v := range values {
		if v != nil && v.Type() == TypeFloat64 {
			result[i] = v.Float64()
			valid[i] = true
		}
	}

	return result, valid, nil
}

// ReadStringColumn extracts a column of string values.
func (cr *ColumnReader) ReadStringColumn(field string) ([]string, []bool, error) {
	values, err := cr.ReadColumn(field)
	if err != nil {
		return nil, nil, err
	}

	result := make([]string, len(values))
	valid := make([]bool, len(values))

	for i, v := range values {
		if v != nil && v.Type() == TypeString {
			result[i] = v.String()
			valid[i] = true
		}
	}

	return result, valid, nil
}

// ReadDatetimeColumn extracts a column of datetime values (as nanoseconds since epoch).
func (cr *ColumnReader) ReadDatetimeColumn(field string) ([]int64, []bool, error) {
	values, err := cr.ReadColumn(field)
	if err != nil {
		return nil, nil, err
	}

	result := make([]int64, len(values))
	valid := make([]bool, len(values))

	for i, v := range values {
		if v != nil && v.Type() == TypeDatetime64 {
			result[i] = v.Datetime64()
			valid[i] = true
		}
	}

	return result, valid, nil
}

// ReadBytesColumn extracts a column of bytes values.
func (cr *ColumnReader) ReadBytesColumn(field string) ([][]byte, []bool, error) {
	values, err := cr.ReadColumn(field)
	if err != nil {
		return nil, nil, err
	}

	result := make([][]byte, len(values))
	valid := make([]bool, len(values))

	for i, v := range values {
		if v != nil && v.Type() == TypeBytes {
			result[i] = v.Bytes()
			valid[i] = true
		}
	}

	return result, valid, nil
}

// ReadFloat32Tensor extracts float32 tensor data from a bytes field.
// Uses the shape from the hint to interpret the data.
// Each row in the array should have a bytes field containing float32 data.
func (cr *ColumnReader) ReadFloat32Tensor(field string) ([][]float32, error) {
	hint := cr.GetHint(field)
	if hint == nil {
		return nil, ErrFieldNotFound
	}
	if hint.Type != HintFloat32 {
		return nil, ErrIncompatibleType
	}

	values, err := cr.ReadColumn(field)
	if err != nil {
		return nil, err
	}

	// Calculate elements per row from shape
	elemsPerRow := 1
	for _, dim := range hint.Shape {
		elemsPerRow *= dim
	}

	result := make([][]float32, len(values))

	for i, v := range values {
		if v == nil || v.Type() != TypeBytes {
			continue
		}
		raw := v.Bytes()

		// Decode float32 values
		expected := elemsPerRow * 4
		if len(raw) < expected {
			continue
		}

		row := make([]float32, elemsPerRow)
		for j := 0; j < elemsPerRow; j++ {
			bits := binary.LittleEndian.Uint32(raw[j*4:])
			row[j] = math.Float32frombits(bits)
		}
		result[i] = row
	}

	return result, nil
}

// ReadFloat64Tensor extracts float64 tensor data from a bytes field.
func (cr *ColumnReader) ReadFloat64Tensor(field string) ([][]float64, error) {
	hint := cr.GetHint(field)
	if hint == nil {
		return nil, ErrFieldNotFound
	}
	if hint.Type != HintFloat64 {
		return nil, ErrIncompatibleType
	}

	values, err := cr.ReadColumn(field)
	if err != nil {
		return nil, err
	}

	// Calculate elements per row from shape
	elemsPerRow := 1
	for _, dim := range hint.Shape {
		elemsPerRow *= dim
	}

	result := make([][]float64, len(values))

	for i, v := range values {
		if v == nil || v.Type() != TypeBytes {
			continue
		}
		raw := v.Bytes()

		expected := elemsPerRow * 8
		if len(raw) < expected {
			continue
		}

		row := make([]float64, elemsPerRow)
		for j := 0; j < elemsPerRow; j++ {
			bits := binary.LittleEndian.Uint64(raw[j*8:])
			row[j] = math.Float64frombits(bits)
		}
		result[i] = row
	}

	return result, nil
}

// ColumnStats holds statistics about a column.
type ColumnStats struct {
	Count      int // Total rows
	ValidCount int // Non-null values
	NullCount  int // Null or missing values
}

// Stats returns statistics about a column.
func (cr *ColumnReader) Stats(field string) (*ColumnStats, error) {
	values, err := cr.ReadColumn(field)
	if err != nil {
		return nil, err
	}

	stats := &ColumnStats{Count: len(values)}
	for _, v := range values {
		if v != nil && !v.IsNull() {
			stats.ValidCount++
		} else {
			stats.NullCount++
		}
	}

	return stats, nil
}

// Fields returns all unique field names from the hints.
func (cr *ColumnReader) Fields() []string {
	fields := make([]string, len(cr.hints))
	for i, h := range cr.hints {
		fields[i] = h.Field
	}
	return fields
}

// Len returns the number of rows if the root is an array, or -1 otherwise.
func (cr *ColumnReader) Len() (int, error) {
	if err := cr.decodeRoot(); err != nil {
		return -1, err
	}
	if cr.root.Type() != TypeArray {
		return -1, nil
	}
	return cr.root.Len(), nil
}
