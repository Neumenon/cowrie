package cowrie

import (
	"testing"
)

// TestEncodeAnyFloat64DefaultPrecision verifies that []float64 encodes as
// DTypeFloat64 by default (no silent precision loss). This is the behavior
// change introduced by the Float32Tensors opt-in: the zero-value AnyOptions
// must preserve float64.
func TestEncodeAnyFloat64DefaultPrecision(t *testing.T) {
	input := []float64{1.1, 2.2, 3.3}

	// Default path: EncodeAny uses DefaultAnyOptions (TensorizeSlices=true,
	// Float32Tensors=false). Expect DTypeFloat64 tensor output.
	data, err := EncodeAny(input)
	if err != nil {
		t.Fatalf("EncodeAny failed: %v", err)
	}

	decoded, err := DecodeAny(data)
	if err != nil {
		t.Fatalf("DecodeAny failed: %v", err)
	}

	// ToGoAny flattens rank-1 float64 tensors to []float64.
	got, ok := decoded.([]float64)
	if !ok {
		t.Fatalf("expected []float64 from DecodeAny, got %T", decoded)
	}
	if len(got) != len(input) {
		t.Fatalf("length mismatch: want %d, got %d", len(input), len(got))
	}
	for i := range input {
		if got[i] != input[i] {
			t.Errorf("element %d: want %v, got %v (precision lost)", i, input[i], got[i])
		}
	}

	// Also confirm the wire DType directly via Value path.
	val, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if val.typ != TypeTensor {
		t.Fatalf("expected TypeTensor, got %v", val.typ)
	}
	if val.tensorVal.DType != DTypeFloat64 {
		t.Errorf("default []float64 encoding: want DTypeFloat64, got %v (precision was lost)", val.tensorVal.DType)
	}
}

// TestEncodeAnyFloat64Float32Opt verifies that setting Float32Tensors=true
// produces DTypeFloat32 output (the old pre-fix default, now an explicit opt-in).
func TestEncodeAnyFloat64Float32Opt(t *testing.T) {
	input := []float64{1.1, 2.2, 3.3}

	opts := AnyOptions{
		TensorizeSlices: true,
		Float32Tensors:  true, // explicit opt-in to lossy downcast
	}
	data, err := EncodeAnyWithOptions(input, opts)
	if err != nil {
		t.Fatalf("EncodeAnyWithOptions failed: %v", err)
	}

	val, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if val.typ != TypeTensor {
		t.Fatalf("expected TypeTensor, got %v", val.typ)
	}
	if val.tensorVal.DType != DTypeFloat32 {
		t.Errorf("Float32Tensors=true: want DTypeFloat32, got %v", val.tensorVal.DType)
	}
}

// TestEncodeAnyFloat64HighPrecisionBackcompat verifies that the retained
// HighPrecision field does not break anything (it is now a no-op since float64
// is the default, but callers who set it must still get DTypeFloat64).
func TestEncodeAnyFloat64HighPrecisionBackcompat(t *testing.T) {
	input := []float64{1.1, 2.2, 3.3}

	opts := AnyOptions{
		TensorizeSlices: true,
		HighPrecision:   true, // legacy field — must not cause float32 output
	}
	data, err := EncodeAnyWithOptions(input, opts)
	if err != nil {
		t.Fatalf("EncodeAnyWithOptions failed: %v", err)
	}

	val, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if val.typ != TypeTensor {
		t.Fatalf("expected TypeTensor, got %v", val.typ)
	}
	if val.tensorVal.DType != DTypeFloat64 {
		t.Errorf("HighPrecision=true: want DTypeFloat64, got %v", val.tensorVal.DType)
	}
}
