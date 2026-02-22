package ucodec

import (
	"math"
	"testing"
)

func TestShouldEncodeSparse(t *testing.T) {
	tests := []struct {
		name     string
		data     []float32
		expected bool
	}{
		{"too_small", []float32{0, 0, 0, 0, 0}, false},
		{"no_zeros", make50NonZero(), false},
		{"all_zeros", make([]float32, 20), true},
		{"40_percent_zeros", make40PercentZeros(), false}, // At threshold
		{"50_percent_zeros", make50PercentZeros(), true},
		{"80_percent_zeros", make80PercentZeros(), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := ShouldEncodeSparse(tc.data)
			if result != tc.expected {
				t.Errorf("ShouldEncodeSparse() = %v, expected %v", result, tc.expected)
			}
		})
	}
}

func make50NonZero() []float32 {
	data := make([]float32, 50)
	for i := range data {
		data[i] = float32(i + 1)
	}
	return data
}

func make40PercentZeros() []float32 {
	data := make([]float32, 20)
	for i := range data {
		if i%5 < 2 { // 40% zeros
			data[i] = 0
		} else {
			data[i] = float32(i + 1)
		}
	}
	return data
}

func make50PercentZeros() []float32 {
	data := make([]float32, 20)
	for i := range data {
		if i%2 == 0 {
			data[i] = 0
		} else {
			data[i] = float32(i)
		}
	}
	return data
}

func make80PercentZeros() []float32 {
	data := make([]float32, 20)
	for i := range data {
		if i%5 == 0 {
			data[i] = float32(i + 1)
		} // else 0
	}
	return data
}

func TestShouldEncodeSparseFloat64(t *testing.T) {
	// 60% zeros
	data := make([]float64, 20)
	for i := range data {
		if i%5 < 2 {
			data[i] = float64(i + 1)
		}
	}
	if !ShouldEncodeSparseFloat64(data) {
		t.Error("expected true for 60% sparse data")
	}
}

func TestShouldEncodeSparseInt8(t *testing.T) {
	// Quantized weights often have many zeros
	data := make([]int8, 100)
	for i := range data {
		if i%10 == 0 {
			data[i] = int8(i % 127)
		}
	}
	if !ShouldEncodeSparseInt8(data) {
		t.Error("expected true for 90% sparse int8 data")
	}
}

func TestEncodeCOO(t *testing.T) {
	data := []float32{0, 1.5, 0, 0, 2.5, 0, 3.5, 0, 0, 4.5}

	indices, values := EncodeCOO(data)

	expectedIndices := []uint32{1, 4, 6, 9}
	expectedValues := []float32{1.5, 2.5, 3.5, 4.5}

	if len(indices) != len(expectedIndices) {
		t.Fatalf("indices length mismatch: %d vs %d", len(indices), len(expectedIndices))
	}
	if len(values) != len(expectedValues) {
		t.Fatalf("values length mismatch: %d vs %d", len(values), len(expectedValues))
	}

	for i := range indices {
		if indices[i] != expectedIndices[i] {
			t.Errorf("index mismatch at %d: %d vs %d", i, indices[i], expectedIndices[i])
		}
		if values[i] != expectedValues[i] {
			t.Errorf("value mismatch at %d: %f vs %f", i, values[i], expectedValues[i])
		}
	}
}

func TestDecodeCOO(t *testing.T) {
	indices := []uint32{1, 4, 6, 9}
	values := []float32{1.5, 2.5, 3.5, 4.5}

	result := DecodeCOO(indices, values, 10)

	expected := []float32{0, 1.5, 0, 0, 2.5, 0, 3.5, 0, 0, 4.5}

	if len(result) != len(expected) {
		t.Fatalf("length mismatch: %d vs %d", len(result), len(expected))
	}

	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("mismatch at %d: %f vs %f", i, result[i], expected[i])
		}
	}
}

func TestDecodeCOO_OutOfBounds(t *testing.T) {
	// Indices beyond size should be ignored
	indices := []uint32{1, 100, 200} // 100, 200 are out of bounds
	values := []float32{1.0, 2.0, 3.0}

	result := DecodeCOO(indices, values, 10)

	if result[1] != 1.0 {
		t.Errorf("expected result[1]=1.0, got %f", result[1])
	}
	// Out of bounds indices should not cause panic
}

func TestEncodeRLE(t *testing.T) {
	data := []int8{0, 0, 0, 1, 2, 3, 0, 0, 0, 0, 4, 5}

	segments := EncodeRLE(data)

	// Expected segments:
	// 1. Zeros: count=3
	// 2. Values: [1, 2, 3]
	// 3. Zeros: count=4
	// 4. Values: [4, 5]

	if len(segments) != 4 {
		t.Fatalf("expected 4 segments, got %d", len(segments))
	}

	// Check segment 0: zeros
	if !segments[0].IsZero || segments[0].Count != 3 {
		t.Errorf("segment 0: expected zeros count=3, got isZero=%v count=%d",
			segments[0].IsZero, segments[0].Count)
	}

	// Check segment 1: values
	if segments[1].IsZero || segments[1].Count != 3 {
		t.Errorf("segment 1: expected values count=3, got isZero=%v count=%d",
			segments[1].IsZero, segments[1].Count)
	}

	// Check segment 2: zeros
	if !segments[2].IsZero || segments[2].Count != 4 {
		t.Errorf("segment 2: expected zeros count=4, got isZero=%v count=%d",
			segments[2].IsZero, segments[2].Count)
	}

	// Check segment 3: values
	if segments[3].IsZero || segments[3].Count != 2 {
		t.Errorf("segment 3: expected values count=2, got isZero=%v count=%d",
			segments[3].IsZero, segments[3].Count)
	}
}

func TestDecodeRLE(t *testing.T) {
	segments := []RLESegment{
		{IsZero: true, Count: 3},
		{IsZero: false, Count: 3, Values: []byte{1, 2, 3}},
		{IsZero: true, Count: 4},
		{IsZero: false, Count: 2, Values: []byte{4, 5}},
	}

	result := DecodeRLE(segments)
	expected := []int8{0, 0, 0, 1, 2, 3, 0, 0, 0, 0, 4, 5}

	if len(result) != len(expected) {
		t.Fatalf("length mismatch: %d vs %d", len(result), len(expected))
	}

	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("mismatch at %d: %d vs %d", i, result[i], expected[i])
		}
	}
}

func TestRLE_RoundTrip(t *testing.T) {
	original := []int8{0, 0, 5, -3, 0, 0, 0, 127, -128, 0, 0, 0, 0, 0, 1}

	segments := EncodeRLE(original)
	decoded := DecodeRLE(segments)

	if len(decoded) != len(original) {
		t.Fatalf("length mismatch: %d vs %d", len(decoded), len(original))
	}

	for i := range original {
		if decoded[i] != original[i] {
			t.Errorf("mismatch at %d: %d vs %d", i, decoded[i], original[i])
		}
	}
}

func TestHasLongZeroRuns(t *testing.T) {
	tests := []struct {
		name     string
		data     []float32
		expected bool
	}{
		{"no_zeros", []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, false},
		{"short_run", []float32{1, 0, 0, 0, 2, 3, 4, 5, 6, 7}, false},
		{"exactly_8", []float32{1, 0, 0, 0, 0, 0, 0, 0, 0, 2}, true},
		{"long_run", []float32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, true},
		{"multiple_short", []float32{0, 0, 1, 0, 0, 2, 0, 0, 3}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := hasLongZeroRuns(tc.data)
			if result != tc.expected {
				t.Errorf("hasLongZeroRuns() = %v, expected %v", result, tc.expected)
			}
		})
	}
}

func TestEncodeSparseTensor_COO(t *testing.T) {
	// Data with short zero runs -> should use COO
	data := []float32{1.0, 0, 2.0, 0, 3.0, 0, 4.0, 0, 5.0, 0}
	dims := []uint64{10}

	encoded, err := EncodeSparseTensor(data, dims)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Verify tag
	if encoded[0] != TagTensorSparse {
		t.Errorf("wrong tag: %02x", encoded[0])
	}

	// Decode
	decoded, decodedDims, err := DecodeSparseTensor(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(decodedDims) != len(dims) {
		t.Fatalf("dims length mismatch: %d vs %d", len(decodedDims), len(dims))
	}

	if len(decoded) != len(data) {
		t.Fatalf("data length mismatch: %d vs %d", len(decoded), len(data))
	}

	for i := range data {
		if decoded[i] != data[i] {
			t.Errorf("mismatch at %d: %f vs %f", i, decoded[i], data[i])
		}
	}
}

func TestEncodeSparseTensor_Hybrid(t *testing.T) {
	// Data with long zero runs -> should use Hybrid
	data := make([]float32, 100)
	data[0] = 1.0
	data[50] = 2.0
	data[99] = 3.0
	// Has runs of 49 and 48 zeros

	dims := []uint64{100}

	encoded, err := EncodeSparseTensor(data, dims)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoded, decodedDims, err := DecodeSparseTensor(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decodedDims[0] != dims[0] {
		t.Errorf("dims mismatch: %v vs %v", decodedDims, dims)
	}

	for i := range data {
		if decoded[i] != data[i] {
			t.Errorf("mismatch at %d: %f vs %f", i, decoded[i], data[i])
		}
	}
}

func TestEncodeSparseTensor_Multidimensional(t *testing.T) {
	// 2x3x4 tensor
	data := make([]float32, 24)
	data[0] = 1.0
	data[5] = 2.0
	data[10] = 3.0
	data[23] = 4.0

	dims := []uint64{2, 3, 4}

	encoded, err := EncodeSparseTensor(data, dims)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoded, decodedDims, err := DecodeSparseTensor(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(decodedDims) != 3 {
		t.Fatalf("dims count mismatch: %d vs 3", len(decodedDims))
	}

	for i, d := range dims {
		if decodedDims[i] != d {
			t.Errorf("dim %d mismatch: %d vs %d", i, decodedDims[i], d)
		}
	}

	for i := range data {
		if decoded[i] != data[i] {
			t.Errorf("mismatch at %d: %f vs %f", i, decoded[i], data[i])
		}
	}
}

func TestEncodeSparseTensor_Empty(t *testing.T) {
	_, err := EncodeSparseTensor([]float32{}, []uint64{0})
	if err == nil {
		t.Error("expected error for empty data")
	}
}

func TestDecodeSparseTensor_Invalid(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"too_short", []byte{0x24, 0x00}},
		{"wrong_tag", []byte{0x00, 0x00, 0x01, 0x0A, 0x00}},
		{"wrong_dtype", []byte{0x24, 0x01, 0x01, 0x0A, 0x00}}, // float64 not supported
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := DecodeSparseTensor(tc.data)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestCompressionRatio(t *testing.T) {
	// 90% zeros
	data := make([]float32, 1000)
	for i := range data {
		if i%10 == 0 {
			data[i] = float32(i)
		}
	}

	original, sparse, ratio := CompressionRatio(data)

	t.Logf("Original: %d bytes, Sparse: %d bytes, Ratio: %.2f", original, sparse, ratio)

	// 90% zeros means 100 non-zeros
	// Sparse: 100 * 8 (index + value) + 32 overhead = ~832 bytes
	// Original: 1000 * 4 = 4000 bytes
	// Ratio should be < 0.3

	if ratio > 0.3 {
		t.Errorf("compression ratio %.2f worse than expected (<0.3)", ratio)
	}
}

func TestDTypeSize(t *testing.T) {
	tests := []struct {
		dtype DType
		size  int
	}{
		{DTypeFloat32, 4},
		{DTypeFloat64, 8},
		{DTypeInt8, 1},
		{DTypeInt16, 2},
		{DTypeInt32, 4},
		{DTypeInt64, 8},
		{DTypeUint8, 1},
		{DTypeUint16, 2},
		{DTypeUint32, 4},
		{DTypeUint64, 8},
	}

	for _, tc := range tests {
		if tc.dtype.Size() != tc.size {
			t.Errorf("DType(%d).Size() = %d, expected %d", tc.dtype, tc.dtype.Size(), tc.size)
		}
	}
}

func TestEncodeCOO_AllZeros(t *testing.T) {
	data := make([]float32, 100)
	indices, values := EncodeCOO(data)

	if len(indices) != 0 || len(values) != 0 {
		t.Errorf("expected empty arrays for all-zeros, got %d indices, %d values",
			len(indices), len(values))
	}
}

func TestEncodeCOO_NoZeros(t *testing.T) {
	data := []float32{1, 2, 3, 4, 5}
	indices, values := EncodeCOO(data)

	if len(indices) != 5 || len(values) != 5 {
		t.Errorf("expected 5 entries, got %d indices, %d values",
			len(indices), len(values))
	}

	for i := uint32(0); i < 5; i++ {
		if indices[i] != i {
			t.Errorf("index %d mismatch", i)
		}
		if values[i] != float32(i+1) {
			t.Errorf("value %d mismatch", i)
		}
	}
}

func TestSparseTensor_SpecialValues(t *testing.T) {
	// Test with NaN and Inf
	data := []float32{0, float32(math.Inf(1)), 0, float32(math.Inf(-1)), 0, float32(math.NaN())}
	dims := []uint64{6}

	encoded, err := EncodeSparseTensor(data, dims)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoded, _, err := DecodeSparseTensor(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Check Inf values
	if !math.IsInf(float64(decoded[1]), 1) {
		t.Errorf("expected +Inf at index 1, got %f", decoded[1])
	}
	if !math.IsInf(float64(decoded[3]), -1) {
		t.Errorf("expected -Inf at index 3, got %f", decoded[3])
	}
	// Note: NaN comparison is tricky
	if !math.IsNaN(float64(decoded[5])) {
		t.Errorf("expected NaN at index 5, got %f", decoded[5])
	}
}

// Benchmarks

func BenchmarkEncodeCOO_Sparse90(b *testing.B) {
	data := make([]float32, 10000)
	for i := range data {
		if i%10 == 0 {
			data[i] = float32(i)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeCOO(data)
	}
}

func BenchmarkDecodeCOO_Sparse90(b *testing.B) {
	data := make([]float32, 10000)
	for i := range data {
		if i%10 == 0 {
			data[i] = float32(i)
		}
	}
	indices, values := EncodeCOO(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeCOO(indices, values, 10000)
	}
}

func BenchmarkEncodeRLE_Sparse90(b *testing.B) {
	data := make([]int8, 10000)
	for i := range data {
		if i%10 == 0 {
			data[i] = int8(i % 127)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeRLE(data)
	}
}

func BenchmarkEncodeSparseTensor(b *testing.B) {
	data := make([]float32, 10000)
	for i := range data {
		if i%10 == 0 {
			data[i] = float32(i)
		}
	}
	dims := []uint64{100, 100}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeSparseTensor(data, dims)
	}
}

func BenchmarkDecodeSparseTensor(b *testing.B) {
	data := make([]float32, 10000)
	for i := range data {
		if i%10 == 0 {
			data[i] = float32(i)
		}
	}
	dims := []uint64{100, 100}
	encoded, _ := EncodeSparseTensor(data, dims)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeSparseTensor(encoded)
	}
}
