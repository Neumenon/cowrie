package cowrie

import (
	"bytes"
	"errors"
	"testing"
)

func TestTensorRoundTrip(t *testing.T) {
	// Create a 2x3 float32 tensor
	dims := []uint64{2, 3}
	data := make([]byte, 24) // 6 float32s = 24 bytes
	for i := range data {
		data[i] = byte(i)
	}

	v := Tensor(DTypeFloat32, dims, data)

	// Verify type
	if v.Type() != TypeTensor {
		t.Fatalf("expected TypeTensor, got %v", v.Type())
	}

	// Encode
	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Decode
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify
	if decoded.Type() != TypeTensor {
		t.Fatalf("decoded type: expected TypeTensor, got %v", decoded.Type())
	}
	tensor := decoded.Tensor()
	if tensor.DType != DTypeFloat32 {
		t.Errorf("dtype: expected %v, got %v", DTypeFloat32, tensor.DType)
	}
	if len(tensor.Dims) != 2 || tensor.Dims[0] != 2 || tensor.Dims[1] != 3 {
		t.Errorf("dims: expected [2, 3], got %v", tensor.Dims)
	}
	if !bytes.Equal(tensor.Data, data) {
		t.Errorf("data mismatch")
	}
}

// SPEC-v1 §2.3: TensorRef(0x21), Image(0x22), Audio(0x23) are RESERVED tags.
// The encoder can still emit them (forward-preservation), but the decoder MUST
// reject them with ERR_RESERVED_TAG (a *TagError). These tests assert the
// freeze-blocker #2 reject behavior, not the old round-trip-as-Null/keep.
func assertReservedTagDecode(t *testing.T, v *Value) {
	t.Helper()
	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	if _, err := Decode(encoded); err == nil {
		t.Fatalf("decode of reserved tag should fail, got nil error")
	} else {
		var te *TagError
		if !errors.As(err, &te) {
			t.Fatalf("decode error should be *TagError (ERR_RESERVED_TAG), got %v", err)
		}
	}
}

func TestTensorRefRoundTrip(t *testing.T) {
	assertReservedTagDecode(t, TensorRef(42, []byte{0xDE, 0xAD, 0xBE, 0xEF}))
}

func TestImageRoundTrip(t *testing.T) {
	assertReservedTagDecode(t, Image(ImageFormatPNG, 1920, 1080, []byte("fake png data")))
}

func TestAudioRoundTrip(t *testing.T) {
	assertReservedTagDecode(t, Audio(AudioEncodingOPUS, 48000, 2, []byte("fake opus data")))
}

func TestV21TypeStrings(t *testing.T) {
	tests := []struct {
		typ      Type
		expected string
	}{
		{TypeTensor, "tensor"},
		{TypeTensorRef, "tensor_ref"},
		{TypeImage, "image"},
		{TypeAudio, "audio"},
	}

	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.expected {
			t.Errorf("Type(%d).String() = %q, want %q", tt.typ, got, tt.expected)
		}
	}
}

func TestV21InArray(t *testing.T) {
	// Tensor is a v1 core type and round-trips; the reserved Image(0x22) nested in
	// the array makes the whole message reject on decode with ERR_RESERVED_TAG.
	arr := Array(
		Tensor(DTypeInt8, []uint64{4}, []byte{1, 2, 3, 4}),
		Image(ImageFormatJPEG, 100, 100, []byte("jpeg")),
		String("mixed"),
		Int64(42),
	)
	assertReservedTagDecode(t, arr)
}

func TestV21InObject(t *testing.T) {
	// The reserved Audio(0x23) nested in the object makes decode reject with
	// ERR_RESERVED_TAG (Tensor is core, but a single reserved tag fails the stream).
	obj := Object(
		Member{Key: "embedding", Value: Tensor(DTypeFloat32, []uint64{768}, make([]byte, 768*4))},
		Member{Key: "audio_clip", Value: Audio(AudioEncodingPCMInt16, 16000, 1, []byte("audio"))},
		Member{Key: "name", Value: String("test")},
	)
	assertReservedTagDecode(t, obj)
}
