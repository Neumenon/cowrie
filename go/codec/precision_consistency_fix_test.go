package codec

import (
	"math"
	"reflect"
	"testing"

	cowrie "github.com/Neumenon/cowrie/go/v2"
)

// TestFloat64SlicePreservesFullPrecision verifies that a struct []float64 field round-trips
// as a DTypeFloat64 tensor (not downcast to float32), matching the any-encoder default.
func TestFloat64SlicePreservesFullPrecision(t *testing.T) {
	type Payload struct {
		Name   string    `json:"name"`
		Scores []float64 `json:"scores"`
	}

	// Values chosen to be NOT exactly representable as float32.
	// If the codec silently downcasts to float32, these will differ on round-trip.
	original := Payload{
		Name: "precision-check",
		Scores: []float64{
			1.0000000000000002, // smallest float64 > 1.0, not representable in float32
			math.Pi,            // 3.141592653589793... — float32 loses digits after ~7 sig figs
			1.234567890123456789,
			-9.876543210987654321e100,
		},
	}

	// Encode via the struct encoder (FastEncode uses the cached struct encoder path).
	encoded, err := FastEncode(original)
	if err != nil {
		t.Fatalf("FastEncode failed: %v", err)
	}

	// Verify the wire representation uses a DTypeFloat64 tensor, not float32.
	cowrieVal, err := cowrie.Decode(encoded)
	if err != nil {
		t.Fatalf("cowrie.Decode failed: %v", err)
	}
	scoresVal := cowrieVal.Get("scores")
	if scoresVal == nil {
		t.Fatal("encoded object missing 'scores' key")
	}
	if scoresVal.Type() != cowrie.TypeTensor {
		t.Errorf("expected scores to be encoded as TypeTensor, got %s", scoresVal.Type())
	}
	td := scoresVal.Tensor()
	if td.DType != cowrie.DTypeFloat64 {
		t.Errorf("expected DTypeFloat64 tensor, got dtype=%v — []float64 was downcast to float32", td.DType)
	}

	// Decode back and verify bit-identical round-trip.
	var got Payload
	if err := DecodeBytes(encoded, &got); err != nil {
		t.Fatalf("DecodeBytes failed: %v", err)
	}

	if got.Name != original.Name {
		t.Errorf("Name mismatch: got %q want %q", got.Name, original.Name)
	}
	if len(got.Scores) != len(original.Scores) {
		t.Fatalf("Scores length mismatch: got %d want %d", len(got.Scores), len(original.Scores))
	}
	for i, want := range original.Scores {
		got64 := got.Scores[i]
		if math.Float64bits(got64) != math.Float64bits(want) {
			t.Errorf("Scores[%d] precision loss: got %v (%016x), want %v (%016x)",
				i, got64, math.Float64bits(got64), want, math.Float64bits(want))
		}
	}
}

// TestFloat32SliceStaysFloat32 verifies that []float32 fields are still encoded as
// DTypeFloat32 tensors and round-trip correctly.
func TestFloat32SliceStaysFloat32(t *testing.T) {
	type Payload struct {
		Embeddings []float32 `json:"embeddings"`
	}

	original := Payload{
		Embeddings: []float32{0.1, 0.2, 0.3, -1.5, math.MaxFloat32},
	}

	encoded, err := FastEncode(original)
	if err != nil {
		t.Fatalf("FastEncode failed: %v", err)
	}

	// Verify DTypeFloat32 on wire.
	cowrieVal, err := cowrie.Decode(encoded)
	if err != nil {
		t.Fatalf("cowrie.Decode failed: %v", err)
	}
	embVal := cowrieVal.Get("embeddings")
	if embVal == nil {
		t.Fatal("encoded object missing 'embeddings' key")
	}
	if embVal.Type() != cowrie.TypeTensor {
		t.Errorf("expected embeddings to be TypeTensor, got %s", embVal.Type())
	}
	td := embVal.Tensor()
	if td.DType != cowrie.DTypeFloat32 {
		t.Errorf("expected DTypeFloat32 tensor, got dtype=%v", td.DType)
	}

	// Decode back and verify bit-identical round-trip.
	var got Payload
	if err := DecodeBytes(encoded, &got); err != nil {
		t.Fatalf("DecodeBytes failed: %v", err)
	}

	if !reflect.DeepEqual(got.Embeddings, original.Embeddings) {
		t.Errorf("Embeddings mismatch: got %v, want %v", got.Embeddings, original.Embeddings)
	}
}

// TestFloat64TensorToFloat32Slice verifies that decoding a DTypeFloat64 tensor into
// a []float32 target narrows correctly (precision loss is expected and accepted by caller).
func TestFloat64TensorToFloat32Slice(t *testing.T) {
	// Encode a []float64 tensor directly.
	f64s := []float64{1.0, 2.0, 3.0}
	tensorVal := EncodeFloat64Tensor(f64s)

	// Wrap it in an object and encode to bytes.
	obj := cowrie.Object(cowrie.Member{Key: "data", Value: tensorVal})
	encoded, err := cowrie.Encode(obj)
	if err != nil {
		t.Fatalf("cowrie.Encode failed: %v", err)
	}

	type Payload struct {
		Data []float32 `json:"data"`
	}
	var got Payload
	if err := DecodeBytes(encoded, &got); err != nil {
		t.Fatalf("DecodeBytes float64-tensor -> []float32 failed: %v", err)
	}
	// Values 1.0, 2.0, 3.0 are exactly representable in float32.
	expected := []float32{1.0, 2.0, 3.0}
	if !reflect.DeepEqual(got.Data, expected) {
		t.Errorf("narrowing decode failed: got %v, want %v", got.Data, expected)
	}
}

// TestFloat32TensorToFloat64Slice verifies that decoding a DTypeFloat32 tensor into
// a []float64 target widens correctly (no precision loss).
func TestFloat32TensorToFloat64Slice(t *testing.T) {
	f32s := []float32{1.5, 2.5, 3.5}
	tensorVal := EncodeFloat32Tensor(f32s)

	obj := cowrie.Object(cowrie.Member{Key: "data", Value: tensorVal})
	encoded, err := cowrie.Encode(obj)
	if err != nil {
		t.Fatalf("cowrie.Encode failed: %v", err)
	}

	type Payload struct {
		Data []float64 `json:"data"`
	}
	var got Payload
	if err := DecodeBytes(encoded, &got); err != nil {
		t.Fatalf("DecodeBytes float32-tensor -> []float64 failed: %v", err)
	}
	// Widening: float32 -> float64 is exact.
	for i, f32 := range f32s {
		want := float64(f32)
		if got.Data[i] != want {
			t.Errorf("Data[%d]: got %v, want %v", i, got.Data[i], want)
		}
	}
}
