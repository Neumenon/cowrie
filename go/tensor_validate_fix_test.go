package cowrie

import (
	"encoding/binary"
	"testing"
)

// TestTensorShapeValidation_Valid confirms that a well-formed tensor encodes
// and decodes without error.
func TestTensorShapeValidation_Valid(t *testing.T) {
	// 2×3 float32 tensor: 6 elements × 4 bytes = 24 bytes
	dims := []uint64{2, 3}
	data := make([]byte, 24)
	for i := 0; i < 6; i++ {
		binary.LittleEndian.PutUint32(data[i*4:], uint32(i+1))
	}
	v := Tensor(DTypeFloat32, dims, data)
	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	got, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode of valid tensor failed: %v", err)
	}
	td, ok := got.TryTensor()
	if !ok {
		t.Fatal("decoded value is not a tensor")
	}
	if td.DType != DTypeFloat32 {
		t.Errorf("dtype: got %v, want float32", td.DType)
	}
	if len(td.Dims) != 2 || td.Dims[0] != 2 || td.Dims[1] != 3 {
		t.Errorf("dims: got %v, want [2 3]", td.Dims)
	}
}

// TestTensorShapeValidation_MismatchedDataLen confirms that a tensor whose
// declared dataLen does not equal product(dims)*elemSize is rejected.
func TestTensorShapeValidation_MismatchedDataLen(t *testing.T) {
	// Build a valid 2×3 float32 tensor, then tamper the encoded bytes so that
	// the payload length appears correct in the wire encoding but the actual
	// dataLen field claims a different size than shape*4.

	// We'll construct malformed bytes directly so we can set dataLen ≠ 24.
	// Wire layout (after Cowrie header + empty dict):
	//   header: Magic0 Magic1 Version Flags (4 bytes)
	//   dict:   0x00 (empty dict, uvarint 0)
	//   tag:    TagTensor (0x20)
	//   dtype:  DTypeFloat32 (0x01)
	//   rank:   0x02
	//   dim0:   uvarint(2)
	//   dim1:   uvarint(3)
	//   dataLen: uvarint(WRONG) — we put 8 instead of 24
	//   data:   8 bytes of payload (matches the claimed wrong dataLen so the reader
	//           won't hit ErrMalformedLength from the remaining-bytes check)

	buf := []byte{
		Magic0, Magic1, Version, 0x00, // header
		0x00,                   // dict length = 0
		TagTensor,              // 0x20
		byte(DTypeFloat32),     // 0x01
		0x02,                   // rank = 2
		0x02,                   // dim[0] = 2 (uvarint)
		0x03,                   // dim[1] = 3 (uvarint)
		0x08,                   // dataLen = 8 (uvarint) — WRONG: should be 24
		0, 0, 0, 0, 0, 0, 0, 0, // 8 bytes of payload
	}

	_, err := Decode(buf)
	if err == nil {
		t.Fatal("Decode of tensor with mismatched dataLen should have returned an error, got nil")
	}
}

// TestTensorShapeValidation_ZeroDim confirms that a tensor with a zero
// dimension decodes correctly (expected byte count is 0).
func TestTensorShapeValidation_ZeroDim(t *testing.T) {
	// Shape [0, 4] float32 → 0 elements → 0 bytes.
	v := Tensor(DTypeFloat32, []uint64{0, 4}, nil)
	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if _, err := Decode(encoded); err != nil {
		t.Fatalf("Decode of zero-dim tensor failed: %v", err)
	}
}

// TestTensorShapeValidation_Rank0 confirms a rank-0 tensor is a SCALAR: exactly
// one element (elemSize bytes). A valid scalar round-trips; a rank-0 with the
// wrong byte length is rejected on encode (symmetric with decode).
func TestTensorShapeValidation_Rank0(t *testing.T) {
	// Valid scalar: float64 rank-0 carries exactly 8 bytes (one element).
	scalar := Tensor(DTypeFloat64, []uint64{}, make([]byte, 8))
	encoded, err := Encode(scalar)
	if err != nil {
		t.Fatalf("Encode of valid rank-0 scalar failed: %v", err)
	}
	if _, err := Decode(encoded); err != nil {
		t.Fatalf("Decode of valid rank-0 scalar failed: %v", err)
	}

	// Invalid: a rank-0 tensor with 0 payload bytes (the old "empty" contract)
	// is now rejected on encode — a scalar must carry exactly one element.
	if _, err := Encode(Tensor(DTypeFloat64, []uint64{}, nil)); err == nil {
		t.Fatal("Encode of rank-0 tensor with 0 bytes should fail (scalar needs 8 bytes)")
	}
}

// TestTensorShapeValidation_SubByteType confirms that sub-byte quantized
// types (QINT4) are not rejected by the shape/size check — we can't know
// their exact packing without more spec detail.
func TestTensorShapeValidation_SubByteType(t *testing.T) {
	// QINT4 with 8 elements packed into 4 bytes; we just use arbitrary data
	// and verify no spurious error.
	v := Tensor(DTypeQINT4, []uint64{8}, []byte{0xAB, 0xCD, 0xEF, 0x01})
	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if _, err := Decode(encoded); err != nil {
		t.Fatalf("Decode of QINT4 tensor failed: %v", err)
	}
}
