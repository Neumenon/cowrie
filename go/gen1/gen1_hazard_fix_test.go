package gen1

import (
	"errors"
	"math"
	"testing"
)

// TestHazard1_TrailingBytesRejected verifies that Decode returns ErrTrailingData
// when the input contains bytes after the encoded value.
// Why it matters: silently ignoring trailing bytes lets an attacker smuggle
// extra data through a parser that believes the message ended.
func TestHazard1_TrailingBytesRejected(t *testing.T) {
	encoded, err := Encode("hello")
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Append a junk trailing byte
	withJunk := append(encoded, 0xFF)

	_, err = Decode(withJunk)
	if !errors.Is(err, ErrTrailingData) {
		t.Errorf("expected ErrTrailingData, got: %v", err)
	}
}

// TestHazard1_ExactDataSucceeds verifies normal single-value decode still works
// (regression guard for the trailing-bytes fix).
func TestHazard1_ExactDataSucceeds(t *testing.T) {
	encoded, err := Encode(int64(42))
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	v, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if v != int64(42) {
		t.Errorf("expected 42, got %v", v)
	}
}

// TestHazard1_StreamDecoderUnaffected verifies StreamDecoder is NOT broken by
// the trailing-bytes fix: it legitimately reads multiple values from a stream
// and must NOT see ErrTrailingData on the first value when more follow.
func TestHazard1_StreamDecoderUnaffected(t *testing.T) {
	enc1, _ := Encode(int64(1))
	enc2, _ := Encode(int64(2))
	stream := append(enc1, enc2...)

	// StreamDecoder reads value-at-a-time from the concatenated stream.
	// It calls readValue internally and manages consumed offsets itself —
	// it does NOT go through Decode(), so ErrTrailingData must not appear.
	buf := stream
	v1, consumed1, err := readValue(buf, 0, DefaultDecodeOptions())
	if err != nil {
		t.Fatalf("readValue[0] failed: %v", err)
	}
	if v1 != int64(1) {
		t.Errorf("expected 1, got %v", v1)
	}
	v2, _, err := readValue(buf, consumed1, DefaultDecodeOptions())
	if err != nil {
		t.Fatalf("readValue[1] failed: %v", err)
	}
	if v2 != int64(2) {
		t.Errorf("expected 2, got %v", v2)
	}
}

// TestHazard2_Uint64BelowMaxInt64RoundTrips verifies uint64 values within
// signed int64 range encode/decode correctly (tagInt64 path).
func TestHazard2_Uint64BelowMaxInt64RoundTrips(t *testing.T) {
	values := []uint64{0, 1, 100, math.MaxInt64}
	for _, v := range values {
		encoded, err := Encode(v)
		if err != nil {
			t.Fatalf("Encode(%d) failed: %v", v, err)
		}
		got, err := Decode(encoded)
		if err != nil {
			t.Fatalf("Decode(%d) failed: %v", v, err)
		}
		// Decoder returns int64 for tagInt64; compare as int64
		gotI, ok := got.(int64)
		if !ok {
			t.Fatalf("expected int64, got %T for input %d", got, v)
		}
		if uint64(gotI) != v {
			t.Errorf("round-trip mismatch: input=%d got=%d", v, gotI)
		}
	}
}

// TestHazard2_Uint64AboveMaxInt64UsesUint64Tag verifies that uint64 values
// exceeding math.MaxInt64 are encoded with tagUint64 (not truncated to negative
// int64) and decode back to the correct uint64 value.
func TestHazard2_Uint64AboveMaxInt64UsesUint64Tag(t *testing.T) {
	large := uint64(math.MaxInt64) + 1 // 0x8000000000000000 — wraps to negative as int64
	encoded, err := Encode(large)
	if err != nil {
		t.Fatalf("Encode(%d) failed: %v", large, err)
	}

	// Verify the emitted tag is tagUint64 (0x09), not tagInt64 (0x03)
	if len(encoded) == 0 {
		t.Fatal("encoded output is empty")
	}
	if encoded[0] != tagUint64 {
		t.Errorf("expected tag 0x%02X (tagUint64), got 0x%02X", tagUint64, encoded[0])
	}

	got, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	gotU, ok := got.(uint64)
	if !ok {
		t.Fatalf("expected uint64, got %T (%v)", got, got)
	}
	if gotU != large {
		t.Errorf("round-trip mismatch: input=%d got=%d", large, gotU)
	}
}

// TestHazard2_Uint64MaxValue verifies math.MaxUint64 round-trips cleanly.
func TestHazard2_Uint64MaxValue(t *testing.T) {
	v := uint64(math.MaxUint64)
	encoded, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode(MaxUint64) failed: %v", err)
	}
	got, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode(MaxUint64) failed: %v", err)
	}
	gotU, ok := got.(uint64)
	if !ok {
		t.Fatalf("expected uint64, got %T (%v)", got, got)
	}
	if gotU != v {
		t.Errorf("round-trip mismatch: input=%d got=%d", v, gotU)
	}
}
