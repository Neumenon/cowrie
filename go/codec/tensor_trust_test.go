package codec

import (
	"math"
	"math/rand"
	"testing"

	"github.com/Neumenon/cowrie/go"
)

// TestTensor_Float32_RoundTrip tests float32 tensor encoding/decoding.
func TestTensor_Float32_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []float32
	}{
		{"empty", []float32{}},
		{"single", []float32{3.14}},
		{"small", []float32{1.0, 2.0, 3.0}},
		{"with_negatives", []float32{-1.0, 0.0, 1.0, -3.14, 2.71}},
		{"with_denormals", []float32{
			math.SmallestNonzeroFloat32,
			-math.SmallestNonzeroFloat32,
			math.MaxFloat32,
			-math.MaxFloat32,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			tensor := EncodeFloat32Tensor(tt.values)
			if tensor == nil {
				if len(tt.values) == 0 {
					// Empty returns array, not tensor
					return
				}
				t.Fatal("EncodeFloat32Tensor returned nil")
			}

			// Decode
			decoded := DecodeFloat32Tensor(tensor)
			if decoded == nil && len(tt.values) > 0 {
				t.Fatal("DecodeFloat32Tensor returned nil")
			}

			// Verify exact match
			if len(decoded) != len(tt.values) {
				t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(tt.values))
			}

			for i, want := range tt.values {
				got := decoded[i]
				if math.IsNaN(float64(want)) {
					if !math.IsNaN(float64(got)) {
						t.Errorf("[%d] want NaN, got %v", i, got)
					}
				} else if got != want {
					t.Errorf("[%d] got %v, want %v", i, got, want)
				}
			}
		})
	}
}

// TestTensor_Float32_Random tests random float32 values.
func TestTensor_Float32_Random(t *testing.T) {
	sizes := []int{1, 10, 100, 1000, 4096}

	for _, size := range sizes {
		t.Run("size_"+string(rune('0'+size%10)), func(t *testing.T) {
			values := make([]float32, size)
			for i := range values {
				values[i] = rand.Float32()*2000 - 1000 // Random in [-1000, 1000]
			}

			tensor := EncodeFloat32Tensor(values)
			decoded := DecodeFloat32Tensor(tensor)

			if len(decoded) != len(values) {
				t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(values))
			}

			for i := range values {
				if decoded[i] != values[i] {
					t.Errorf("[%d] got %v, want %v", i, decoded[i], values[i])
				}
			}
		})
	}
}

// TestTensor_Float64_RoundTrip tests float64 tensor encoding/decoding.
func TestTensor_Float64_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
	}{
		{"empty", []float64{}},
		{"single", []float64{3.141592653589793}},
		{"high_precision", []float64{
			1.2345678901234567,
			9.8765432109876543,
			0.0000000000000001,
		}},
		{"with_negatives", []float64{-1.0, 0.0, 1.0, -3.14159265358979, 2.71828182845904}},
		{"with_denormals", []float64{
			math.SmallestNonzeroFloat64,
			-math.SmallestNonzeroFloat64,
			math.MaxFloat64,
			-math.MaxFloat64,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			tensor := EncodeFloat64Tensor(tt.values)
			if tensor == nil {
				if len(tt.values) == 0 {
					return
				}
				t.Fatal("EncodeFloat64Tensor returned nil")
			}

			// Decode
			decoded := DecodeFloat64Tensor(tensor)
			if decoded == nil && len(tt.values) > 0 {
				t.Fatal("DecodeFloat64Tensor returned nil")
			}

			// Verify exact bit-for-bit match (float64 should be lossless)
			if len(decoded) != len(tt.values) {
				t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(tt.values))
			}

			for i, want := range tt.values {
				got := decoded[i]
				if math.IsNaN(want) {
					if !math.IsNaN(got) {
						t.Errorf("[%d] want NaN, got %v", i, got)
					}
				} else if math.Float64bits(got) != math.Float64bits(want) {
					t.Errorf("[%d] bit mismatch: got %v (0x%x), want %v (0x%x)",
						i, got, math.Float64bits(got), want, math.Float64bits(want))
				}
			}
		})
	}
}

// TestTensor_SpecialFloats tests special float values.
func TestTensor_SpecialFloats(t *testing.T) {
	t.Run("float32", func(t *testing.T) {
		// Document the policy: special values ARE preserved in tensors
		values := []float32{
			float32(math.NaN()),
			float32(math.Inf(1)),
			float32(math.Inf(-1)),
			float32(0),
			float32(math.Copysign(0, -1)), // -0
		}

		tensor := EncodeFloat32Tensor(values)
		decoded := DecodeFloat32Tensor(tensor)

		// NaN
		if !math.IsNaN(float64(decoded[0])) {
			t.Errorf("NaN not preserved: got %v", decoded[0])
		}

		// +Inf
		if !math.IsInf(float64(decoded[1]), 1) {
			t.Errorf("+Inf not preserved: got %v", decoded[1])
		}

		// -Inf
		if !math.IsInf(float64(decoded[2]), -1) {
			t.Errorf("-Inf not preserved: got %v", decoded[2])
		}

		// +0
		if decoded[3] != 0 || math.Signbit(float64(decoded[3])) {
			t.Errorf("+0 not preserved: got %v (signbit=%v)", decoded[3], math.Signbit(float64(decoded[3])))
		}

		// -0 (sign bit should be preserved)
		if decoded[4] != 0 || !math.Signbit(float64(decoded[4])) {
			t.Errorf("-0 not preserved: got %v (signbit=%v)", decoded[4], math.Signbit(float64(decoded[4])))
		}
	})

	t.Run("float64", func(t *testing.T) {
		values := []float64{
			math.NaN(),
			math.Inf(1),
			math.Inf(-1),
			0,
			math.Copysign(0, -1), // -0
		}

		tensor := EncodeFloat64Tensor(values)
		decoded := DecodeFloat64Tensor(tensor)

		// NaN
		if !math.IsNaN(decoded[0]) {
			t.Errorf("NaN not preserved: got %v", decoded[0])
		}

		// +Inf
		if !math.IsInf(decoded[1], 1) {
			t.Errorf("+Inf not preserved: got %v", decoded[1])
		}

		// -Inf
		if !math.IsInf(decoded[2], -1) {
			t.Errorf("-Inf not preserved: got %v", decoded[2])
		}

		// +0
		if decoded[3] != 0 || math.Signbit(decoded[3]) {
			t.Errorf("+0 not preserved")
		}

		// -0
		if decoded[4] != 0 || !math.Signbit(decoded[4]) {
			t.Errorf("-0 not preserved")
		}
	})
}

// TestTensor_AutoTensorization tests automatic tensorization of []any.
func TestTensor_AutoTensorization(t *testing.T) {
	t.Run("all_float64_becomes_tensor", func(t *testing.T) {
		input := []any{1.0, 2.0, 3.0, 4.0, 5.0}
		tensor := TryEncodeNumericSlice(input)

		if tensor == nil {
			t.Fatal("TryEncodeNumericSlice returned nil for all-float array")
		}

		if tensor.Type() != cowrie.TypeTensor {
			t.Errorf("expected tensor type, got %v", tensor.Type())
		}
	})

	t.Run("mixed_types_not_tensorized", func(t *testing.T) {
		input := []any{1.0, "two", 3.0}
		tensor := TryEncodeNumericSlice(input)

		if tensor != nil {
			t.Error("TryEncodeNumericSlice should return nil for mixed types")
		}
	})

	t.Run("all_ints_becomes_tensor", func(t *testing.T) {
		input := []any{int64(1), int64(2), int64(3)}
		tensor := TryEncodeNumericSlice(input)

		if tensor == nil {
			t.Fatal("TryEncodeNumericSlice returned nil for all-int array")
		}

		if tensor.Type() != cowrie.TypeTensor {
			t.Errorf("expected tensor type, got %v", tensor.Type())
		}
	})

	t.Run("empty_not_tensorized", func(t *testing.T) {
		input := []any{}
		tensor := TryEncodeNumericSlice(input)

		if tensor != nil {
			t.Error("TryEncodeNumericSlice should return nil for empty array")
		}
	})

	t.Run("mixed_int_float_becomes_tensor", func(t *testing.T) {
		input := []any{int64(1), float64(2.5), int32(3)}
		tensor := TryEncodeNumericSlice(input)

		if tensor == nil {
			t.Fatal("TryEncodeNumericSlice returned nil for mixed numeric array")
		}
	})
}

// TestTensor_HighPrecision tests high-precision mode preserves float64.
func TestTensor_HighPrecision(t *testing.T) {
	// High-precision values that would lose precision in float32
	values := []float64{
		1.2345678901234567,
		9.8765432109876543,
		3.141592653589793,
		2.718281828459045,
	}

	// Encode with high precision
	tensor := EncodeFloat64Tensor(values)
	if tensor == nil {
		t.Fatal("EncodeFloat64Tensor returned nil")
	}

	// Verify dtype is float64
	td := tensor.Tensor()
	if td.DType != cowrie.DTypeFloat64 {
		t.Errorf("DType = %v, want DTypeFloat64", td.DType)
	}

	// Decode and verify exact match
	decoded := DecodeFloat64Tensor(tensor)
	for i, want := range values {
		if decoded[i] != want {
			t.Errorf("[%d] precision loss: got %v, want %v", i, decoded[i], want)
		}
	}
}

// TestTensor_Float64AsFloat32 tests compaction with precision loss.
func TestTensor_Float64AsFloat32(t *testing.T) {
	values := []float64{1.0, 2.0, 3.0}

	tensor := EncodeFloat64AsFloat32Tensor(values)
	if tensor == nil {
		t.Fatal("EncodeFloat64AsFloat32Tensor returned nil")
	}

	// Verify dtype is float32
	td := tensor.Tensor()
	if td.DType != cowrie.DTypeFloat32 {
		t.Errorf("DType = %v, want DTypeFloat32", td.DType)
	}

	// Decode as float32
	decoded := DecodeFloat32Tensor(tensor)
	for i, want := range values {
		if float64(decoded[i]) != want {
			t.Errorf("[%d] got %v, want %v", i, decoded[i], want)
		}
	}
}

// TestTensor_DTypeAwareDecode tests dtype-aware decoding.
func TestTensor_DTypeAwareDecode(t *testing.T) {
	tests := []struct {
		name     string
		tensor   *cowrie.Value
		wantType string
	}{
		{
			"float32",
			EncodeFloat32Tensor([]float32{1, 2, 3}),
			"[]float32",
		},
		{
			"float64",
			EncodeFloat64Tensor([]float64{1, 2, 3}),
			"[]float64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DecodeTensorAuto(tt.tensor)
			if result == nil {
				t.Fatal("DecodeTensorAuto returned nil")
			}

			switch tt.wantType {
			case "[]float32":
				if _, ok := result.([]float32); !ok {
					t.Errorf("expected []float32, got %T", result)
				}
			case "[]float64":
				if _, ok := result.([]float64); !ok {
					t.Errorf("expected []float64, got %T", result)
				}
			}
		})
	}
}

// TestTensor_ShapePreservation tests tensor shape/dims are preserved.
func TestTensor_ShapePreservation(t *testing.T) {
	// Create 2D tensor (3x4)
	data := make([]byte, 3*4*4) // 3x4 float32
	for i := 0; i < 12; i++ {
		// Fill with dummy data
		data[i*4] = byte(i)
	}

	tensor := cowrie.Tensor(cowrie.DTypeFloat32, []uint64{3, 4}, data)

	// Encode and decode
	encoded, err := cowrie.Encode(tensor)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoded, err := cowrie.Decode(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	td := decoded.Tensor()
	if len(td.Dims) != 2 {
		t.Fatalf("dims length = %d, want 2", len(td.Dims))
	}
	if td.Dims[0] != 3 || td.Dims[1] != 4 {
		t.Errorf("dims = %v, want [3, 4]", td.Dims)
	}
}

// TestTensor_WrongDTypeDecodeReturnsNil tests type safety.
func TestTensor_WrongDTypeDecodeReturnsNil(t *testing.T) {
	f32Tensor := EncodeFloat32Tensor([]float32{1, 2, 3})
	f64Tensor := EncodeFloat64Tensor([]float64{1, 2, 3})

	// Trying to decode float32 tensor as float64 should fail
	if DecodeFloat64Tensor(f32Tensor) != nil {
		t.Error("DecodeFloat64Tensor should return nil for float32 tensor")
	}

	// Trying to decode float64 tensor as float32 should fail
	if DecodeFloat32Tensor(f64Tensor) != nil {
		t.Error("DecodeFloat32Tensor should return nil for float64 tensor")
	}
}

// BenchmarkTensor_Float32_Encode benchmarks float32 tensor encoding.
func BenchmarkTensor_Float32_Encode(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		values := make([]float32, size)
		for i := range values {
			values[i] = rand.Float32()
		}

		b.Run(string(rune('0'+size/1000))+"k", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = EncodeFloat32Tensor(values)
			}
		})
	}
}

// BenchmarkTensor_Float32_Decode benchmarks float32 tensor decoding.
func BenchmarkTensor_Float32_Decode(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		values := make([]float32, size)
		for i := range values {
			values[i] = rand.Float32()
		}
		tensor := EncodeFloat32Tensor(values)

		b.Run(string(rune('0'+size/1000))+"k", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = DecodeFloat32Tensor(tensor)
			}
		})
	}
}
