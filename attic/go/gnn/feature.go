package gnn

import (
	"encoding/binary"
	"errors"
	"math"
)

var (
	ErrInvalidFeatureMode = errors.New("gnn: invalid feature mode")
	ErrShapeMismatch      = errors.New("gnn: shape mismatch")
)

// FeatureWriter writes feature tensors.
type FeatureWriter struct {
	enc         *SectionEncoder
	featureName string
	mode        FeatureMode
	dtype       DType
	shape       []int // Per-element shape (e.g., [128] for embeddings)
}

// NewFeatureWriter creates a blocked feature writer.
// For blocked mode, data is written as one contiguous tensor.
func NewFeatureWriter(featureName string, dtype DType, shape []int) *FeatureWriter {
	w := &FeatureWriter{
		enc:         NewSectionEncoder(),
		featureName: featureName,
		mode:        FeatureModeBlocked,
		dtype:       dtype,
		shape:       shape,
	}
	return w
}

// NewRowWiseFeatureWriter creates a row-wise feature writer.
// For row-wise mode, each row is written separately.
func NewRowWiseFeatureWriter(featureName string, dtype DType, shape []int) *FeatureWriter {
	w := &FeatureWriter{
		enc:         NewSectionEncoder(),
		featureName: featureName,
		mode:        FeatureModeRowWise,
		dtype:       dtype,
		shape:       shape,
	}
	return w
}

// WriteHeader writes the feature header.
func (w *FeatureWriter) WriteHeader(numRows int64) {
	// Mode
	w.enc.buf.writeByte(byte(w.mode))
	// Feature name
	w.enc.WriteString(w.featureName)
	// DType
	w.enc.WriteDType(w.dtype)
	// Shape (per-element for blocked, or full shape including N for row-wise)
	w.enc.WriteShape(w.shape)
	// Num rows (for blocked mode)
	if w.mode == FeatureModeBlocked {
		w.enc.WriteUvarint(uint64(numRows))
	}
}

// WriteFloat32Tensor writes a contiguous float32 tensor (blocked mode).
func (w *FeatureWriter) WriteFloat32Tensor(data []float32) {
	for _, v := range data {
		w.enc.WriteFloat32(v)
	}
}

// WriteFloat64Tensor writes a contiguous float64 tensor (blocked mode).
func (w *FeatureWriter) WriteFloat64Tensor(data []float64) {
	for _, v := range data {
		w.enc.WriteFloat64(v)
	}
}

// WriteInt64Tensor writes a contiguous int64 tensor (blocked mode).
func (w *FeatureWriter) WriteInt64Tensor(data []int64) {
	for _, v := range data {
		w.enc.WriteInt64(v)
	}
}

// WriteFloat32Row writes a single float32 row (row-wise mode).
func (w *FeatureWriter) WriteFloat32Row(id int64, data []float32) {
	// Row ID
	w.enc.WriteUvarint(uint64(id))
	// Data
	for _, v := range data {
		w.enc.WriteFloat32(v)
	}
}

// WriteFloat64Row writes a single float64 row (row-wise mode).
func (w *FeatureWriter) WriteFloat64Row(id int64, data []float64) {
	w.enc.WriteUvarint(uint64(id))
	for _, v := range data {
		w.enc.WriteFloat64(v)
	}
}

// WriteInt64Row writes a single int64 row (row-wise mode).
func (w *FeatureWriter) WriteInt64Row(id int64, data []int64) {
	w.enc.WriteUvarint(uint64(id))
	for _, v := range data {
		w.enc.WriteInt64(v)
	}
}

// Bytes returns the encoded section body.
func (w *FeatureWriter) Bytes() []byte {
	return w.enc.Bytes()
}

// ToSection returns this as a Section.
func (w *FeatureWriter) ToSection(name string) Section {
	return Section{
		Kind: SectionFeature,
		Name: name,
		Body: w.Bytes(),
	}
}

// FeatureReader reads feature tensors.
type FeatureReader struct {
	dec         *SectionDecoder
	featureName string
	mode        FeatureMode
	dtype       DType
	shape       []int
	numRows     int64 // For blocked mode
}

// NewFeatureReader creates a reader for a feature section.
func NewFeatureReader(data []byte) (*FeatureReader, error) {
	dec := NewSectionDecoder(data)

	// Read mode
	modeByte, err := dec.r.readByte()
	if err != nil {
		return nil, err
	}

	// Read feature name
	featureName, err := dec.ReadString()
	if err != nil {
		return nil, err
	}

	// Read dtype
	dtype, err := dec.ReadDType()
	if err != nil {
		return nil, err
	}

	// Read shape
	shape, err := dec.ReadShape()
	if err != nil {
		return nil, err
	}

	r := &FeatureReader{
		dec:         dec,
		featureName: featureName,
		mode:        FeatureMode(modeByte),
		dtype:       dtype,
		shape:       shape,
	}

	// Read num rows for blocked mode
	if r.mode == FeatureModeBlocked {
		numRows, err := dec.ReadUvarint()
		if err != nil {
			return nil, err
		}
		r.numRows = int64(numRows)
	}

	return r, nil
}

// FeatureName returns the feature name.
func (r *FeatureReader) FeatureName() string {
	return r.featureName
}

// Mode returns the feature mode.
func (r *FeatureReader) Mode() FeatureMode {
	return r.mode
}

// DType returns the data type.
func (r *FeatureReader) DType() DType {
	return r.dtype
}

// Shape returns the per-element shape.
func (r *FeatureReader) Shape() []int {
	return r.shape
}

// NumRows returns the number of rows (blocked mode only).
func (r *FeatureReader) NumRows() int64 {
	return r.numRows
}

// elementsPerRow calculates total elements per row from shape.
func (r *FeatureReader) elementsPerRow() int {
	total := 1
	for _, dim := range r.shape {
		total *= dim
	}
	return total
}

// ReadFloat32Tensor reads the entire float32 tensor (blocked mode).
func (r *FeatureReader) ReadFloat32Tensor() ([]float32, error) {
	if r.mode != FeatureModeBlocked {
		return nil, ErrInvalidFeatureMode
	}

	totalElements := int(r.numRows) * r.elementsPerRow()
	data := make([]float32, totalElements)

	for i := 0; i < totalElements; i++ {
		v, err := r.dec.ReadFloat32()
		if err != nil {
			return nil, err
		}
		data[i] = v
	}

	return data, nil
}

// ReadFloat64Tensor reads the entire float64 tensor (blocked mode).
func (r *FeatureReader) ReadFloat64Tensor() ([]float64, error) {
	if r.mode != FeatureModeBlocked {
		return nil, ErrInvalidFeatureMode
	}

	totalElements := int(r.numRows) * r.elementsPerRow()
	data := make([]float64, totalElements)

	for i := 0; i < totalElements; i++ {
		v, err := r.dec.ReadFloat64()
		if err != nil {
			return nil, err
		}
		data[i] = v
	}

	return data, nil
}

// ReadInt64Tensor reads the entire int64 tensor (blocked mode).
func (r *FeatureReader) ReadInt64Tensor() ([]int64, error) {
	if r.mode != FeatureModeBlocked {
		return nil, ErrInvalidFeatureMode
	}

	totalElements := int(r.numRows) * r.elementsPerRow()
	data := make([]int64, totalElements)

	for i := 0; i < totalElements; i++ {
		v, err := r.dec.ReadInt64()
		if err != nil {
			return nil, err
		}
		data[i] = v
	}

	return data, nil
}

// ReadFloat32Row reads a single float32 row (row-wise mode).
func (r *FeatureReader) ReadFloat32Row() (id int64, data []float32, err error) {
	if r.mode != FeatureModeRowWise {
		return 0, nil, ErrInvalidFeatureMode
	}

	idVal, err := r.dec.ReadUvarint()
	if err != nil {
		return 0, nil, err
	}

	elemCount := r.elementsPerRow()
	data = make([]float32, elemCount)
	for i := 0; i < elemCount; i++ {
		v, err := r.dec.ReadFloat32()
		if err != nil {
			return 0, nil, err
		}
		data[i] = v
	}

	return int64(idVal), data, nil
}

// ReadFloat64Row reads a single float64 row (row-wise mode).
func (r *FeatureReader) ReadFloat64Row() (id int64, data []float64, err error) {
	if r.mode != FeatureModeRowWise {
		return 0, nil, ErrInvalidFeatureMode
	}

	idVal, err := r.dec.ReadUvarint()
	if err != nil {
		return 0, nil, err
	}

	elemCount := r.elementsPerRow()
	data = make([]float64, elemCount)
	for i := 0; i < elemCount; i++ {
		v, err := r.dec.ReadFloat64()
		if err != nil {
			return 0, nil, err
		}
		data[i] = v
	}

	return int64(idVal), data, nil
}

// ReadInt64Row reads a single int64 row (row-wise mode).
func (r *FeatureReader) ReadInt64Row() (id int64, data []int64, err error) {
	if r.mode != FeatureModeRowWise {
		return 0, nil, ErrInvalidFeatureMode
	}

	idVal, err := r.dec.ReadUvarint()
	if err != nil {
		return 0, nil, err
	}

	elemCount := r.elementsPerRow()
	data = make([]int64, elemCount)
	for i := 0; i < elemCount; i++ {
		v, err := r.dec.ReadInt64()
		if err != nil {
			return 0, nil, err
		}
		data[i] = v
	}

	return int64(idVal), data, nil
}

// HasMoreRows returns true if there is more data to read (row-wise mode).
func (r *FeatureReader) HasMoreRows() bool {
	return r.dec.Remaining() > 0
}

// Float32ToBytes converts float32 slice to raw bytes (for zero-copy scenarios).
func Float32ToBytes(data []float32) []byte {
	result := make([]byte, len(data)*4)
	for i, v := range data {
		binary.LittleEndian.PutUint32(result[i*4:], math.Float32bits(v))
	}
	return result
}

// BytesToFloat32 converts raw bytes to float32 slice.
func BytesToFloat32(data []byte) []float32 {
	count := len(data) / 4
	result := make([]float32, count)
	for i := 0; i < count; i++ {
		result[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[i*4:]))
	}
	return result
}

// Float64ToBytes converts float64 slice to raw bytes.
func Float64ToBytes(data []float64) []byte {
	result := make([]byte, len(data)*8)
	for i, v := range data {
		binary.LittleEndian.PutUint64(result[i*8:], math.Float64bits(v))
	}
	return result
}

// BytesToFloat64 converts raw bytes to float64 slice.
func BytesToFloat64(data []byte) []float64 {
	count := len(data) / 8
	result := make([]float64, count)
	for i := 0; i < count; i++ {
		result[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[i*8:]))
	}
	return result
}

// Int64ToBytes converts int64 slice to raw bytes.
func Int64ToBytes(data []int64) []byte {
	result := make([]byte, len(data)*8)
	for i, v := range data {
		binary.LittleEndian.PutUint64(result[i*8:], uint64(v))
	}
	return result
}

// BytesToInt64 converts raw bytes to int64 slice.
func BytesToInt64(data []byte) []int64 {
	count := len(data) / 8
	result := make([]int64, count)
	for i := 0; i < count; i++ {
		result[i] = int64(binary.LittleEndian.Uint64(data[i*8:]))
	}
	return result
}
