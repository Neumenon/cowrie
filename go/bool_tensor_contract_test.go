package cowrie

import "testing"

// TestBoolTensorShapeValidation pins the new contract: DTypeBool stores one byte
// per element (bit-packed booleans use the separate Bitmask type), so a bool
// tensor's payload length must equal its element count — validated on encode.
func TestBoolTensorShapeValidation(t *testing.T) {
	// Valid: bool[3] carries exactly 3 bytes.
	if _, err := Encode(Tensor(DTypeBool, []uint64{3}, []byte{1, 0, 1})); err != nil {
		t.Fatalf("valid bool[3] encode failed: %v", err)
	}
	// Invalid: bool[3] with 2 bytes is rejected (was silently accepted before).
	if _, err := Encode(Tensor(DTypeBool, []uint64{3}, []byte{1, 0})); err == nil {
		t.Fatal("bool[3] with 2 bytes should be rejected on encode")
	}
	// Round-trip a valid bool tensor through decode.
	enc, err := Encode(Tensor(DTypeBool, []uint64{2}, []byte{1, 0}))
	if err != nil {
		t.Fatalf("bool[2] encode failed: %v", err)
	}
	if _, err := Decode(enc); err != nil {
		t.Fatalf("bool[2] decode failed: %v", err)
	}
}
