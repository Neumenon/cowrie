package cowrie

import (
	"encoding/binary"
	"math"
	"testing"
	"time"
)

func TestEncodeAny_Primitives(t *testing.T) {
	tests := []struct {
		name string
		val  any
	}{
		{"nil", nil},
		{"bool_true", true},
		{"bool_false", false},
		{"string", "hello"},
		{"int64", int64(42)},
		{"float64", 3.14},
		{"int", 99},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := EncodeAny(tt.val)
			if err != nil {
				t.Fatalf("EncodeAny failed: %v", err)
			}
			result, err := DecodeAny(data)
			if err != nil {
				t.Fatalf("DecodeAny failed: %v", err)
			}
			// Nil stays nil
			if tt.val == nil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}
		})
	}
}

func TestEncodeAny_Float32Slice(t *testing.T) {
	input := []float32{1.0, 2.0, 3.0}
	data, err := EncodeAny(input)
	if err != nil {
		t.Fatalf("EncodeAny failed: %v", err)
	}
	result, err := DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}
	floats, ok := result.([]float32)
	if !ok {
		t.Fatalf("expected []float32, got %T", result)
	}
	if len(floats) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(floats))
	}
	for i, v := range input {
		if math.Abs(float64(floats[i])-float64(v)) > 0.0001 {
			t.Errorf("element %d: got %v, want %v", i, floats[i], v)
		}
	}
}

func TestEncodeAny_Float64Slice(t *testing.T) {
	// Default (non high-precision) should downcast to float32
	input := []float64{1.0, 2.0, 3.0}
	data, err := EncodeAny(input)
	if err != nil {
		t.Fatalf("EncodeAny failed: %v", err)
	}
	result, err := DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}
	// Default downcasts to float32
	floats, ok := result.([]float32)
	if !ok {
		t.Fatalf("expected []float32, got %T", result)
	}
	if len(floats) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(floats))
	}
}

func TestEncodeAnyWithOptions_HighPrecision(t *testing.T) {
	input := []float64{1.23456789012345, 2.34567890123456}
	opts := AnyOptions{TensorizeSlices: true, HighPrecision: true}
	data, err := EncodeAnyWithOptions(input, opts)
	if err != nil {
		t.Fatalf("EncodeAnyWithOptions failed: %v", err)
	}
	result, err := DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}
	floats, ok := result.([]float64)
	if !ok {
		t.Fatalf("expected []float64, got %T", result)
	}
	if len(floats) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(floats))
	}
	for i, v := range input {
		if floats[i] != v {
			t.Errorf("element %d: got %v, want %v", i, floats[i], v)
		}
	}
}

func TestEncodeAny_Int32Slice(t *testing.T) {
	input := []int32{10, 20, 30}
	data, err := EncodeAny(input)
	if err != nil {
		t.Fatalf("EncodeAny failed: %v", err)
	}
	result, err := DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}
	ints, ok := result.([]int32)
	if !ok {
		t.Fatalf("expected []int32, got %T", result)
	}
	if len(ints) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(ints))
	}
	for i, v := range input {
		if ints[i] != v {
			t.Errorf("element %d: got %v, want %v", i, ints[i], v)
		}
	}
}

func TestEncodeAny_Int64Slice(t *testing.T) {
	input := []int64{100, 200, 300}
	data, err := EncodeAny(input)
	if err != nil {
		t.Fatalf("EncodeAny failed: %v", err)
	}
	result, err := DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}
	ints, ok := result.([]int64)
	if !ok {
		t.Fatalf("expected []int64, got %T", result)
	}
	for i, v := range input {
		if ints[i] != v {
			t.Errorf("element %d: got %v, want %v", i, ints[i], v)
		}
	}
}

func TestEncodeAny_Float32Matrix(t *testing.T) {
	input := [][]float32{{1.0, 2.0}, {3.0, 4.0}}
	data, err := EncodeAny(input)
	if err != nil {
		t.Fatalf("EncodeAny failed: %v", err)
	}
	result, err := DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}
	// Multi-dim tensor comes back as TensorData
	td, ok := result.(*TensorData)
	if !ok {
		// May also be TensorData (non-pointer)
		if td2, ok2 := result.(TensorData); ok2 {
			td = &td2
		} else {
			t.Fatalf("expected TensorData, got %T", result)
		}
	}
	if len(td.Dims) != 2 || td.Dims[0] != 2 || td.Dims[1] != 2 {
		t.Errorf("unexpected dims: %v", td.Dims)
	}
}

func TestEncodeAny_Float64Matrix(t *testing.T) {
	input := [][]float64{{1.0, 2.0}, {3.0, 4.0}}
	data, err := EncodeAny(input)
	if err != nil {
		t.Fatalf("EncodeAny failed: %v", err)
	}
	_, err = DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}
}

func TestEncodeAny_Float64MatrixHighPrecision(t *testing.T) {
	input := [][]float64{{1.0, 2.0}, {3.0, 4.0}}
	opts := AnyOptions{TensorizeSlices: true, HighPrecision: true}
	data, err := EncodeAnyWithOptions(input, opts)
	if err != nil {
		t.Fatalf("EncodeAnyWithOptions failed: %v", err)
	}
	result, err := DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}
	td, ok := result.(TensorData)
	if !ok {
		t.Fatalf("expected TensorData, got %T", result)
	}
	if td.DType != DTypeFloat64 {
		t.Errorf("expected DTypeFloat64, got %v", td.DType)
	}
}

func TestEncodeAny_Float32MatrixJagged(t *testing.T) {
	// Jagged matrix should not tensorize
	input := [][]float32{{1.0, 2.0}, {3.0}}
	data, err := EncodeAny(input)
	if err != nil {
		t.Fatalf("EncodeAny failed: %v", err)
	}
	_, err = DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}
}

func TestEncodeAny_Float32MatrixEmpty(t *testing.T) {
	// Empty matrix should not tensorize
	input := [][]float32{}
	data, err := EncodeAny(input)
	if err != nil {
		t.Fatalf("EncodeAny failed: %v", err)
	}
	_, err = DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}
}

func TestEncodeAny_Float32MatrixEmptyCols(t *testing.T) {
	// Matrix with zero cols should not tensorize
	input := [][]float32{{}}
	data, err := EncodeAny(input)
	if err != nil {
		t.Fatalf("EncodeAny failed: %v", err)
	}
	_, err = DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}
}

func TestTryTensorizeAnySlice(t *testing.T) {
	// Array of float64 values (as from JSON)
	arr := make([]any, 10)
	for i := range arr {
		arr[i] = float64(i)
	}
	opts := AnyOptions{TensorizeSlices: true}
	result := tryTensorizeAnySlice(arr, opts)
	if result == nil {
		t.Fatal("expected tensor, got nil")
	}
	if result.Type() != TypeTensor {
		t.Errorf("expected TypeTensor, got %v", result.Type())
	}
}

func TestTryTensorizeAnySlice_Mixed(t *testing.T) {
	// Mix of int64 and float64
	arr := []any{float64(1.0), int64(2), int(3), float64(4.0), float64(5.0), float64(6.0), float64(7.0), float64(8.0)}
	opts := AnyOptions{TensorizeSlices: true}
	result := tryTensorizeAnySlice(arr, opts)
	if result == nil {
		t.Fatal("expected tensor for mixed numeric slice")
	}
}

func TestTryTensorizeAnySlice_HighPrecision(t *testing.T) {
	arr := make([]any, 10)
	for i := range arr {
		arr[i] = float64(i) * 0.123456789012345
	}
	opts := AnyOptions{TensorizeSlices: true, HighPrecision: true}
	result := tryTensorizeAnySlice(arr, opts)
	if result == nil {
		t.Fatal("expected tensor")
	}
	td := result.Tensor()
	if td.DType != DTypeFloat64 {
		t.Errorf("expected DTypeFloat64, got %v", td.DType)
	}
}

func TestTryTensorizeAnySlice_TooShort(t *testing.T) {
	arr := []any{float64(1.0), float64(2.0)}
	opts := AnyOptions{TensorizeSlices: true}
	result := tryTensorizeAnySlice(arr, opts)
	if result != nil {
		t.Error("expected nil for short array")
	}
}

func TestTryTensorizeAnySlice_NonNumeric(t *testing.T) {
	arr := make([]any, 10)
	for i := range arr {
		arr[i] = "not a number"
	}
	opts := AnyOptions{TensorizeSlices: true}
	result := tryTensorizeAnySlice(arr, opts)
	if result != nil {
		t.Error("expected nil for non-numeric slice")
	}
}

func TestDefaultAnyOptions(t *testing.T) {
	opts := DefaultAnyOptions()
	if !opts.TensorizeSlices {
		t.Error("TensorizeSlices should default to true")
	}
	if opts.HighPrecision {
		t.Error("HighPrecision should default to false")
	}
	if opts.Enriched {
		t.Error("Enriched should default to false")
	}
}

func TestEncodeAny_Map(t *testing.T) {
	input := map[string]any{
		"name": "test",
		"val":  int64(42),
	}
	data, err := EncodeAny(input)
	if err != nil {
		t.Fatalf("EncodeAny failed: %v", err)
	}
	result, err := DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	if m["name"] != "test" {
		t.Errorf("name: got %v, want test", m["name"])
	}
}

func TestToGoAny_AllTypes(t *testing.T) {
	now := time.Now().UTC()
	uuid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	tests := []struct {
		name string
		val  *Value
	}{
		{"nil", nil},
		{"null", Null()},
		{"bool", Bool(true)},
		{"int64", Int64(42)},
		{"uint64", Uint64(99)},
		{"float64", Float64(3.14)},
		{"decimal128", NewDecimal128(2, [16]byte{})},
		{"string", String("hello")},
		{"bytes", Bytes([]byte{0xDE, 0xAD})},
		{"datetime64", Datetime(now)},
		{"uuid128", UUID128(uuid)},
		{"bigint", BigInt([]byte{0x01, 0x02})},
		{"array", Array(Int64(1), Int64(2))},
		{"object", Object(Member{Key: "k", Value: String("v")})},
		{"tensor_f32_1d", Tensor(DTypeFloat32, []uint64{2}, encodeFloat32LE([]float32{1.0, 2.0}))},
		{"tensor_f64_1d", Tensor(DTypeFloat64, []uint64{2}, encodeFloat64LE([]float64{1.0, 2.0}))},
		{"tensor_i32_1d", Tensor(DTypeInt32, []uint64{2}, encodeInt32LE([]int32{1, 2}))},
		{"tensor_i64_1d", Tensor(DTypeInt64, []uint64{2}, encodeInt64LE([]int64{1, 2}))},
		{"tensor_u8_1d", Tensor(DTypeUint8, []uint64{3}, []byte{1, 2, 3})},
		{"tensor_2d", Tensor(DTypeFloat32, []uint64{2, 2}, encodeFloat32LE([]float32{1, 2, 3, 4}))},
		{"tensor_ref", TensorRef(1, []byte{0xAA})},
		{"image", Image(0, 10, 10, []byte{0xFF})},
		{"audio", Audio(0, 44100, 2, []byte{0x00})},
		{"adjlist", Adjlist(0, 3, 2, []uint64{0, 1, 2}, []byte{1, 2})},
		{"richtext", RichText("hello", nil, nil)},
		{"delta", Delta(0, []DeltaOp{{OpCode: DeltaOpSetField, FieldID: 0, Value: Int64(1)}})},
		{"unknown_ext", UnknownExtension(999, []byte{0x01})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToGoAny(tt.val)
			// Just ensure it doesn't panic
			_ = result
		})
	}
}

func TestDecodeTensorHelpers(t *testing.T) {
	// float32
	f32data := make([]byte, 8)
	binary.LittleEndian.PutUint32(f32data[0:], math.Float32bits(1.5))
	binary.LittleEndian.PutUint32(f32data[4:], math.Float32bits(2.5))
	f32s := decodeTensorFloat32(f32data)
	if len(f32s) != 2 || f32s[0] != 1.5 || f32s[1] != 2.5 {
		t.Errorf("decodeTensorFloat32: got %v", f32s)
	}

	// float64
	f64data := make([]byte, 16)
	binary.LittleEndian.PutUint64(f64data[0:], math.Float64bits(1.5))
	binary.LittleEndian.PutUint64(f64data[8:], math.Float64bits(2.5))
	f64s := decodeTensorFloat64(f64data)
	if len(f64s) != 2 || f64s[0] != 1.5 || f64s[1] != 2.5 {
		t.Errorf("decodeTensorFloat64: got %v", f64s)
	}

	// int32
	i32data := make([]byte, 8)
	binary.LittleEndian.PutUint32(i32data[0:], uint32(42))
	binary.LittleEndian.PutUint32(i32data[4:], uint32(99))
	i32s := decodeTensorInt32(i32data)
	if len(i32s) != 2 || i32s[0] != 42 || i32s[1] != 99 {
		t.Errorf("decodeTensorInt32: got %v", i32s)
	}

	// int64
	i64data := make([]byte, 16)
	binary.LittleEndian.PutUint64(i64data[0:], uint64(100))
	binary.LittleEndian.PutUint64(i64data[8:], uint64(200))
	i64s := decodeTensorInt64(i64data)
	if len(i64s) != 2 || i64s[0] != 100 || i64s[1] != 200 {
		t.Errorf("decodeTensorInt64: got %v", i64s)
	}
}

func TestEncodeAny_Enriched(t *testing.T) {
	input := map[string]any{"key": "value"}
	opts := AnyOptions{TensorizeSlices: false, Enriched: true}
	data, err := EncodeAnyWithOptions(input, opts)
	if err != nil {
		t.Fatalf("EncodeAnyWithOptions failed: %v", err)
	}
	result, err := DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}
	if m["key"] != "value" {
		t.Errorf("expected 'value', got %v", m["key"])
	}
}

func TestFallbackViaJSON(t *testing.T) {
	// Test with a struct that needs JSON bridge
	type customStruct struct {
		Name string `json:"name"`
		Val  int    `json:"val"`
	}
	input := customStruct{Name: "test", Val: 42}
	opts := AnyOptions{TensorizeSlices: true}
	result := fallbackViaJSON(input, opts)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Type() != TypeObject {
		t.Errorf("expected object, got %v", result.Type())
	}
}

func TestEncodeAny_NoTensorize(t *testing.T) {
	input := []float32{1.0, 2.0, 3.0}
	opts := AnyOptions{TensorizeSlices: false}
	data, err := EncodeAnyWithOptions(input, opts)
	if err != nil {
		t.Fatalf("EncodeAnyWithOptions failed: %v", err)
	}
	_, err = DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}
}

func TestDecodeAny_InvalidData(t *testing.T) {
	_, err := DecodeAny([]byte{0xFF, 0xFF})
	if err == nil {
		t.Error("expected error for invalid data")
	}
}

func TestEncodeLeHelpers(t *testing.T) {
	// Test encode helpers produce correct bytes
	f32 := encodeFloat32LE([]float32{1.0})
	if len(f32) != 4 {
		t.Errorf("encodeFloat32LE: expected 4 bytes, got %d", len(f32))
	}
	if math.Float32frombits(binary.LittleEndian.Uint32(f32)) != 1.0 {
		t.Error("encodeFloat32LE roundtrip failed")
	}

	f64 := encodeFloat64LE([]float64{2.0})
	if len(f64) != 8 {
		t.Errorf("encodeFloat64LE: expected 8 bytes, got %d", len(f64))
	}
	if math.Float64frombits(binary.LittleEndian.Uint64(f64)) != 2.0 {
		t.Error("encodeFloat64LE roundtrip failed")
	}

	f64as32 := encodeFloat64AsFloat32LE([]float64{3.0})
	if len(f64as32) != 4 {
		t.Errorf("encodeFloat64AsFloat32LE: expected 4 bytes, got %d", len(f64as32))
	}

	i32 := encodeInt32LE([]int32{42})
	if len(i32) != 4 {
		t.Errorf("encodeInt32LE: expected 4 bytes, got %d", len(i32))
	}

	i64 := encodeInt64LE([]int64{99})
	if len(i64) != 8 {
		t.Errorf("encodeInt64LE: expected 8 bytes, got %d", len(i64))
	}
}
