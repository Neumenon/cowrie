package cowrie

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestViewFloat32Success(t *testing.T) {
	// Create properly aligned float32 data
	values := []float32{1.0, 2.0, 3.0, 4.0}
	data := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(v))
	}

	td := &TensorData{
		DType: DTypeFloat32,
		Dims:  []uint64{uint64(len(values))},
		Data:  data,
	}

	view, ok := td.ViewFloat32()
	if !ok {
		t.Fatal("ViewFloat32 failed unexpectedly")
	}

	if len(view) != len(values) {
		t.Errorf("View length mismatch: got %d, want %d", len(view), len(values))
	}

	for i, v := range values {
		if view[i] != v {
			t.Errorf("Value mismatch at %d: got %f, want %f", i, view[i], v)
		}
	}
}

func TestViewFloat32WrongDType(t *testing.T) {
	td := &TensorData{
		DType: DTypeFloat64, // Wrong dtype
		Dims:  []uint64{4},
		Data:  make([]byte, 32),
	}

	view, ok := td.ViewFloat32()
	if ok {
		t.Error("ViewFloat32 should fail for Float64 dtype")
	}
	if view != nil {
		t.Error("ViewFloat32 should return nil for wrong dtype")
	}
}

func TestViewFloat32InvalidSize(t *testing.T) {
	// Data size not divisible by 4
	td := &TensorData{
		DType: DTypeFloat32,
		Dims:  []uint64{4},
		Data:  make([]byte, 13), // Not divisible by 4
	}

	view, ok := td.ViewFloat32()
	if ok {
		t.Error("ViewFloat32 should fail for invalid size")
	}
	if view != nil {
		t.Error("ViewFloat32 should return nil for invalid size")
	}
}

func TestViewFloat32Empty(t *testing.T) {
	td := &TensorData{
		DType: DTypeFloat32,
		Dims:  []uint64{0},
		Data:  nil,
	}

	view, ok := td.ViewFloat32()
	if !ok {
		t.Error("ViewFloat32 should succeed for empty data")
	}
	if view != nil {
		t.Error("ViewFloat32 should return nil for empty data")
	}
}

func TestViewFloat64Success(t *testing.T) {
	values := []float64{1.1, 2.2, 3.3, 4.4}
	data := make([]byte, len(values)*8)
	for i, v := range values {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(v))
	}

	td := &TensorData{
		DType: DTypeFloat64,
		Dims:  []uint64{uint64(len(values))},
		Data:  data,
	}

	view, ok := td.ViewFloat64()
	if !ok {
		t.Fatal("ViewFloat64 failed unexpectedly")
	}

	for i, v := range values {
		if view[i] != v {
			t.Errorf("Value mismatch at %d: got %f, want %f", i, view[i], v)
		}
	}
}

func TestViewInt32Success(t *testing.T) {
	values := []int32{-100, 0, 100, 1000}
	data := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(data[i*4:], uint32(v))
	}

	td := &TensorData{
		DType: DTypeInt32,
		Dims:  []uint64{uint64(len(values))},
		Data:  data,
	}

	view, ok := td.ViewInt32()
	if !ok {
		t.Fatal("ViewInt32 failed unexpectedly")
	}

	for i, v := range values {
		if view[i] != v {
			t.Errorf("Value mismatch at %d: got %d, want %d", i, view[i], v)
		}
	}
}

func TestViewInt64Success(t *testing.T) {
	values := []int64{-1000000, 0, 1000000, math.MaxInt64}
	data := make([]byte, len(values)*8)
	for i, v := range values {
		binary.LittleEndian.PutUint64(data[i*8:], uint64(v))
	}

	td := &TensorData{
		DType: DTypeInt64,
		Dims:  []uint64{uint64(len(values))},
		Data:  data,
	}

	view, ok := td.ViewInt64()
	if !ok {
		t.Fatal("ViewInt64 failed unexpectedly")
	}

	for i, v := range values {
		if view[i] != v {
			t.Errorf("Value mismatch at %d: got %d, want %d", i, view[i], v)
		}
	}
}

func TestViewUint8Success(t *testing.T) {
	values := []uint8{0, 127, 255, 1}

	td := &TensorData{
		DType: DTypeUint8,
		Dims:  []uint64{uint64(len(values))},
		Data:  values,
	}

	view, ok := td.ViewUint8()
	if !ok {
		t.Fatal("ViewUint8 failed unexpectedly")
	}

	for i, v := range values {
		if view[i] != v {
			t.Errorf("Value mismatch at %d: got %d, want %d", i, view[i], v)
		}
	}
}

func TestFloat32SliceFallback(t *testing.T) {
	// Create unaligned data by prepending a byte
	values := []float32{1.0, 2.0, 3.0}
	aligned := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(aligned[i*4:], math.Float32bits(v))
	}

	td := &TensorData{
		DType: DTypeFloat32,
		Dims:  []uint64{uint64(len(values))},
		Data:  aligned,
	}

	// Float32Slice should work regardless of alignment
	result := td.Float32Slice()
	if result == nil {
		t.Fatal("Float32Slice returned nil")
	}

	for i, v := range values {
		if result[i] != v {
			t.Errorf("Value mismatch at %d: got %f, want %f", i, result[i], v)
		}
	}
}

func TestCopyFloat32(t *testing.T) {
	values := []float32{1.5, 2.5, 3.5}
	data := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(v))
	}

	td := &TensorData{
		DType: DTypeFloat32,
		Dims:  []uint64{uint64(len(values))},
		Data:  data,
	}

	result := CopyFloat32(td)
	if result == nil {
		t.Fatal("CopyFloat32 returned nil")
	}

	for i, v := range values {
		if result[i] != v {
			t.Errorf("Value mismatch at %d: got %f, want %f", i, result[i], v)
		}
	}

	// Verify it's a copy by modifying and checking original
	result[0] = 999.0
	originalValue := math.Float32frombits(binary.LittleEndian.Uint32(data[0:]))
	if originalValue == 999.0 {
		t.Error("CopyFloat32 did not create a copy")
	}
}

func TestCopyFloat64(t *testing.T) {
	values := []float64{1.5, 2.5, 3.5}
	data := make([]byte, len(values)*8)
	for i, v := range values {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(v))
	}

	td := &TensorData{
		DType: DTypeFloat64,
		Dims:  []uint64{uint64(len(values))},
		Data:  data,
	}

	result := CopyFloat64(td)
	if result == nil {
		t.Fatal("CopyFloat64 returned nil")
	}

	for i, v := range values {
		if result[i] != v {
			t.Errorf("Value mismatch at %d: got %f, want %f", i, result[i], v)
		}
	}
}

func BenchmarkViewFloat32(b *testing.B) {
	data := make([]byte, 4096)
	td := &TensorData{
		DType: DTypeFloat32,
		Dims:  []uint64{1024},
		Data:  data,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = td.ViewFloat32()
	}
}

func BenchmarkCopyFloat32(b *testing.B) {
	data := make([]byte, 4096)
	td := &TensorData{
		DType: DTypeFloat32,
		Dims:  []uint64{1024},
		Data:  data,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CopyFloat32(td)
	}
}

func BenchmarkFloat32Slice(b *testing.B) {
	data := make([]byte, 4096)
	td := &TensorData{
		DType: DTypeFloat32,
		Dims:  []uint64{1024},
		Data:  data,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = td.Float32Slice()
	}
}
