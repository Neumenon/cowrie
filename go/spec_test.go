package cowrie

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

// =============================================================================
// Spec Alignment Tests - Wire Format Conformance
// =============================================================================

// TestSpecMagicBytes verifies the wire format magic bytes are correct.
// SPEC-v1 §2.2: 4-byte magic "COWR" (0x43 0x4F 0x57 0x52).
func TestSpecMagicBytes(t *testing.T) {
	v := String("test")
	data, err := Encode(v)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Magic: 'C' 'O' 'W' 'R' (4 bytes)
	if len(data) < 4 {
		t.Fatal("encoded data too short")
	}
	if data[0] != 'C' || data[1] != 'O' || data[2] != 'W' || data[3] != 'R' {
		t.Errorf("magic bytes: got %c%c%c%c, want COWR", data[0], data[1], data[2], data[3])
	}
}

// TestSpecVersion verifies the version byte is correct.
// SPEC-v1 §2.2: version byte 0x01 at offset 4 (after the 4-byte magic).
func TestSpecVersion(t *testing.T) {
	v := String("test")
	data, err := Encode(v)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Version: 0x01 (1 byte at offset 4)
	if len(data) < 5 {
		t.Fatal("encoded data too short")
	}
	if data[4] != 0x01 {
		t.Errorf("version: got 0x%02x, want 0x01", data[4])
	}
}

// TestSpecFlags verifies the flags byte encoding.
func TestSpecFlags(t *testing.T) {
	tests := []struct {
		name      string
		wantFlags byte
	}{
		{"uncompressed", 0x00},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := String("test")
			data, err := Encode(v)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			// Flags: offset 5 (after 4-byte magic + 1-byte version)
			if len(data) < 6 {
				t.Fatal("encoded data too short")
			}
			if data[5] != tt.wantFlags {
				t.Errorf("flags: got 0x%02x, want 0x%02x", data[5], tt.wantFlags)
			}
		})
	}
}

// TestSpecCoreTags verifies all 14 core type tags.
func TestSpecCoreTags(t *testing.T) {
	tests := []struct {
		name    string
		value   *Value
		wantTag byte
	}{
		{"null", Null(), 0x00},
		{"false", Bool(false), 0x01},
		{"true", Bool(true), 0x02},
		{"int64", Int64(200), 0x03},         // 200 > 127, falls back to TagInt64
		{"int64_fixint", Int64(42), 0x6A},   // v3: fixint 42 = 0x40 + 42
		{"int64_fixneg", Int64(-1), 0xE0},   // v3: fixneg -1 = 0xE0
		{"float64", Float64(3.14), 0x04},
		{"string", String("test"), 0x05},
		{"array_fixarray", Array(Int64(1)), 0xC1}, // v3: fixarray len=1
		{"object_fixmap", Object(Member{Key: "k", Value: Int64(1)}), 0xD1}, // v3: fixmap count=1
		{"bytes", Bytes([]byte{1, 2, 3}), 0x08},
		{"uint64", Uint64(42), 0x09},
		{"decimal128", NewDecimal128(2, [16]byte{}), 0x0A},
		{"datetime64", Datetime64(0), 0x0B},
		{"uuid128", UUID128([16]byte{}), 0x0C},
		{"bigint", BigInt([]byte{1}), 0x0D},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := Encode(tt.value)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			// Find the root value tag (after header and dictionary)
			tag := findRootTag(data)
			if tag != tt.wantTag {
				t.Errorf("tag: got 0x%02x, want 0x%02x", tag, tt.wantTag)
			}
		})
	}
}

// TestSpecV21Tags verifies the v2.1 extension type tags for kept types.
func TestSpecV21Tags(t *testing.T) {
	tests := []struct {
		name    string
		value   *Value
		wantTag byte
	}{
		{"tensor", Tensor(DTypeFloat32, []uint64{2, 3}, make([]byte, 24)), 0x20},
		{"tensorRef", TensorRef(0, []byte("key")), 0x21},
		{"image", Image(ImageFormatJPEG, 640, 480, make([]byte, 10)), 0x22},
		{"audio", Audio(AudioEncodingPCMInt16, 44100, 2, make([]byte, 10)), 0x23},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := Encode(tt.value)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			// Find the root value tag
			tag := findRootTag(data)
			if tag != tt.wantTag {
				t.Errorf("tag: got 0x%02x, want 0x%02x", tag, tt.wantTag)
			}
		})
	}
}

// TestSpecUnknownTagRejection verifies unknown tags are rejected during decode.
func TestSpecUnknownTagRejection(t *testing.T) {
	// Test reserved/unknown tags
	// Note: 0x0E is TagExt (extension envelope), not unknown
	unknownTags := []byte{
		0x10, // Reserved in 0x10 range
		0x13, 0x14, 0x15, // Reserved in 0x10 range (except LD tags 0x11, 0x12)
		0x25, 0x2F, // Reserved after bitmask (0x24), before 0x30
		0x33, 0x34, 0x3F, // Reserved after delta
		// 0x40-0xEF are now valid inline types (fixint/fixarray/fixmap/fixneg)
		0xF0, 0xF5, 0xFF, // 0xF0-0xFF reserved
	}

	for _, tag := range unknownTags {
		t.Run("tag_0x"+hexByte(tag), func(t *testing.T) {
			// Build minimal valid header + unknown tag
			data := buildMinimalWithTag(tag)

			_, err := Decode(data)
			if err == nil {
				t.Errorf("expected error for unknown tag 0x%02x", tag)
			}

			// Check it's a TagError
			if _, ok := err.(*TagError); !ok {
				t.Errorf("expected TagError, got %T: %v", err, err)
			}
		})
	}
}

// TestSpecInvalidEnumValues verifies invalid enum values are rejected.
func TestSpecInvalidEnumValues(t *testing.T) {
	t.Run("invalid_dtype", func(t *testing.T) {
		// Build a tensor with invalid dtype (0xFF)
		data := buildTensorWithDType(0xFF)
		_, err := Decode(data)
		if err != ErrInvalidDType {
			t.Errorf("expected ErrInvalidDType, got: %v", err)
		}
	})

	t.Run("invalid_image_format", func(t *testing.T) {
		// Image(0x22) is now a RESERVED tag (SPEC-v1 §2.3): the decoder rejects on
		// the tag byte itself (ERR_RESERVED_TAG) before ever reading the format
		// byte, so the format value is irrelevant.
		data := buildImageWithFormat(0xFF)
		_, err := Decode(data)
		if !errors.As(err, new(*TagError)) {
			t.Errorf("expected ERR_RESERVED_TAG (*TagError), got: %v", err)
		}
	})

	t.Run("invalid_audio_encoding", func(t *testing.T) {
		// Audio(0x23) is now a RESERVED tag (SPEC-v1 §2.3): decode rejects on the
		// tag byte (ERR_RESERVED_TAG) before reaching the encoding byte.
		data := buildAudioWithEncoding(0xFF)
		_, err := Decode(data)
		if !errors.As(err, new(*TagError)) {
			t.Errorf("expected ERR_RESERVED_TAG (*TagError), got: %v", err)
		}
	})

}

// TestSpecInt64ZigZagEncoding verifies int64 uses zigzag encoding.
func TestSpecInt64ZigZagEncoding(t *testing.T) {
	tests := []struct {
		value      int64
		wantZigzag uint64
	}{
		{0, 0},
		{-1, 1},
		{1, 2},
		{-2, 3},
		{2, 4},
		{-128, 255},
		{127, 254},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			// Verify zigzag encoding formula
			got := zigzagEncode(tt.value)
			if got != tt.wantZigzag {
				t.Errorf("zigzag(%d): got %d, want %d", tt.value, got, tt.wantZigzag)
			}

			// Verify round-trip
			decoded := zigzagDecode(got)
			if decoded != tt.value {
				t.Errorf("zigzag roundtrip: got %d, want %d", decoded, tt.value)
			}
		})
	}
}

// TestSpecFloat64LittleEndian verifies float64 uses little-endian encoding.
func TestSpecFloat64LittleEndian(t *testing.T) {
	testVal := 3.14159265358979
	v := Float64(testVal)
	data, err := Encode(v)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Find float64 data (after header, dict=0, tag)
	// Header (§2.2): 6 bytes, dict length: 1 byte (0), tag: 1 byte -> data at offset 8
	if len(data) < 16 {
		t.Fatal("encoded data too short")
	}

	// Extract the 8 float bytes
	floatBytes := data[8:16]

	// Verify little-endian by decoding and comparing
	decoded := binary.LittleEndian.Uint64(floatBytes)
	decodedFloat := math.Float64frombits(decoded)
	if decodedFloat != testVal {
		t.Errorf("float64 decode: got %v, want %v", decodedFloat, testVal)
	}

	// Verify the encoding matches what binary.LittleEndian would produce
	expected := make([]byte, 8)
	binary.LittleEndian.PutUint64(expected, math.Float64bits(testVal))
	if !bytes.Equal(floatBytes, expected) {
		t.Errorf("float64 bytes: got %v, want %v", floatBytes, expected)
	}
}

// TestSpecDatetime64Nanoseconds verifies datetime64 stores nanoseconds.
func TestSpecDatetime64Nanoseconds(t *testing.T) {
	// Unix epoch = 0 nanoseconds
	v := Datetime64(0)
	decoded := v.Datetime64()
	if decoded != 0 {
		t.Errorf("datetime64(0): got %d, want 0", decoded)
	}

	// 1 second = 1,000,000,000 nanoseconds
	v2 := Datetime64(1_000_000_000)
	decoded2 := v2.Datetime64()
	if decoded2 != 1_000_000_000 {
		t.Errorf("datetime64(1e9): got %d, want 1000000000", decoded2)
	}
}

// TestSpecUUID128FixedSize verifies UUID128 is exactly 16 bytes.
func TestSpecUUID128FixedSize(t *testing.T) {
	uuid := [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
	v := UUID128(uuid)

	data, err := Encode(v)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Verify the UUID bytes are in the encoded data
	// After header (6) + dict len (1) + tag (1) = offset 8
	if len(data) < 24 {
		t.Fatal("encoded data too short for UUID")
	}

	uuidBytes := data[8:24]
	if !bytes.Equal(uuidBytes, uuid[:]) {
		t.Errorf("UUID bytes mismatch: got %v, want %v", uuidBytes, uuid[:])
	}
}

// TestSpecRoundTrip verifies all types survive encode/decode round-trip.
func TestSpecRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		value    *Value
		reserved bool // SPEC-v1 §2.3: decode rejects with ERR_RESERVED_TAG
	}{
		{"null", Null(), false},
		{"false", Bool(false), false},
		{"true", Bool(true), false},
		{"int64_positive", Int64(12345), false},
		{"int64_negative", Int64(-12345), false},
		{"uint64", Uint64(18446744073709551615), false},
		{"float64", Float64(3.14159265358979), false},
		{"string", String("hello world 🌍"), false},
		{"bytes", Bytes([]byte{0x00, 0xFF, 0x42}), false},
		{"datetime64", Datetime64(1732905600000000000), false},
		{"uuid128", UUID128([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}), false},
		{"bigint", BigInt([]byte{0x01, 0x02, 0x03}), false},
		{"array", Array(Int64(1), String("two"), Bool(true)), false},
		{"object", Object(Member{Key: "a", Value: Int64(1)}, Member{Key: "b", Value: String("two")}), false},
		{"tensor", Tensor(DTypeFloat32, []uint64{2, 3}, make([]byte, 24)), false},
		{"tensorRef", TensorRef(5, []byte("model-key")), true},
		{"image", Image(ImageFormatPNG, 100, 200, []byte{0x89, 0x50, 0x4E, 0x47}), true},
		{"audio", Audio(AudioEncodingOPUS, 48000, 1, []byte{0x4F, 0x67, 0x67, 0x53}), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := Encode(tt.value)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			decoded, err := Decode(data)
			if tt.reserved {
				if !errors.As(err, new(*TagError)) {
					t.Fatalf("expected ERR_RESERVED_TAG (*TagError), got: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}

			// Verify type matches
			if decoded.Type() != tt.value.Type() {
				t.Errorf("type mismatch: got %v, want %v", decoded.Type(), tt.value.Type())
			}
		})
	}
}

// =============================================================================
// Helper Functions
// =============================================================================

// findRootTag finds the root value tag in encoded data.
func findRootTag(data []byte) byte {
	if len(data) < 6 {
		return 0
	}
	// Header (§2.2): magic (4) + version (1) + flags (1) = 6 bytes
	// Then dictionary length (uvarint)
	pos := 6
	dictLen, n := binary.Uvarint(data[pos:])
	pos += n

	// Skip dictionary entries
	for i := uint64(0); i < dictLen; i++ {
		strLen, n := binary.Uvarint(data[pos:])
		pos += n
		pos += int(strLen)
	}

	// Now we're at the root value tag
	if pos >= len(data) {
		return 0
	}
	return data[pos]
}

// buildMinimalWithTag builds minimal valid Cowrie with a specific root tag.
func buildMinimalWithTag(tag byte) []byte {
	// Magic(COWR) + Version(0x01) + Flags(0x00) + DictLen(0) + Tag
	return []byte{'C', 'O', 'W', 'R', 0x01, 0x00, 0x00, tag}
}

// buildTensorWithDType builds a tensor value with a specific dtype.
func buildTensorWithDType(dtype byte) []byte {
	buf := []byte{'C', 'O', 'W', 'R', 0x01, 0x00, 0x00} // Header + dict len
	buf = append(buf, 0x20)                   // Tensor tag
	buf = append(buf, dtype)                  // DType
	buf = append(buf, 0x01)                   // Rank = 1
	buf = append(buf, 0x02)                   // Dim[0] = 2
	buf = append(buf, 0x08)                   // Data length = 8
	buf = append(buf, make([]byte, 8)...)     // Data
	return buf
}

// buildImageWithFormat builds an image value with a specific format.
func buildImageWithFormat(format byte) []byte {
	buf := []byte{'C', 'O', 'W', 'R', 0x01, 0x00, 0x00} // Header + dict len
	buf = append(buf, 0x22)                   // Image tag
	buf = append(buf, format)                 // Format
	buf = append(buf, 0x00, 0x01)             // Width = 256 LE
	buf = append(buf, 0x00, 0x01)             // Height = 256 LE
	buf = append(buf, 0x04)                   // Data length = 4
	buf = append(buf, 0x00, 0x00, 0x00, 0x00) // Data
	return buf
}

// buildAudioWithEncoding builds an audio value with a specific encoding.
func buildAudioWithEncoding(encoding byte) []byte {
	buf := []byte{'C', 'O', 'W', 'R', 0x01, 0x00, 0x00} // Header + dict len
	buf = append(buf, 0x23)                   // Audio tag
	buf = append(buf, encoding)               // Encoding
	buf = append(buf, 0x44, 0xAC, 0x00, 0x00) // Sample rate = 44100 LE
	buf = append(buf, 0x02)                   // Channels = 2
	buf = append(buf, 0x04)                   // Data length = 4
	buf = append(buf, 0x00, 0x00, 0x00, 0x00) // Data
	return buf
}

// =============================================================================
// Security Tests - Memory Exhaustion Protection
// =============================================================================

// TestSecurityMalformedStringLength tests that strings with lengths exceeding
// remaining data are rejected.
func TestSecurityMalformedStringLength(t *testing.T) {
	// Build a string claiming to be 1GB but only has 4 bytes of data
	buf := []byte{'C', 'O', 'W', 'R', 0x01, 0x00, 0x00}       // Header + dict len
	buf = append(buf, 0x05)                         // String tag
	buf = append(buf, 0x80, 0x80, 0x80, 0x80, 0x04) // Varint: 1GB (way too big)
	buf = append(buf, 't', 'e', 's', 't')           // Only 4 bytes of actual data

	_, err := Decode(buf)
	if err != ErrMalformedLength {
		t.Errorf("expected ErrMalformedLength, got %v", err)
	}
}

// TestSecurityMalformedBytesLength tests that bytes with lengths exceeding
// remaining data are rejected.
func TestSecurityMalformedBytesLength(t *testing.T) {
	buf := []byte{'C', 'O', 'W', 'R', 0x01, 0x00, 0x00}       // Header + dict len
	buf = append(buf, 0x08)                         // Bytes tag
	buf = append(buf, 0x80, 0x80, 0x80, 0x80, 0x04) // Varint: 1GB
	buf = append(buf, 0x00, 0x01, 0x02, 0x03)       // Only 4 bytes

	_, err := Decode(buf)
	if err != ErrMalformedLength {
		t.Errorf("expected ErrMalformedLength, got %v", err)
	}
}

// TestSecurityMalformedArrayCount tests that arrays claiming too many elements
// are rejected.
func TestSecurityMalformedArrayCount(t *testing.T) {
	buf := []byte{'C', 'O', 'W', 'R', 0x01, 0x00, 0x00}       // Header + dict len
	buf = append(buf, 0x06)                         // Array tag
	buf = append(buf, 0x80, 0x80, 0x80, 0x80, 0x04) // Varint: 1B elements

	_, err := Decode(buf)
	if err != ErrMalformedLength {
		t.Errorf("expected ErrMalformedLength, got %v", err)
	}
}

// TestSecurityMalformedObjectCount tests that objects claiming too many fields
// are rejected.
func TestSecurityMalformedObjectCount(t *testing.T) {
	buf := []byte{'C', 'O', 'W', 'R', 0x01, 0x00, 0x00}       // Header + dict len
	buf = append(buf, 0x07)                         // Object tag
	buf = append(buf, 0x80, 0x80, 0x80, 0x80, 0x04) // Varint: 1B fields

	_, err := Decode(buf)
	if err != ErrMalformedLength {
		t.Errorf("expected ErrMalformedLength, got %v", err)
	}
}

// TestSecurityDepthLimit tests that deeply nested structures are rejected.
func TestSecurityDepthLimit(t *testing.T) {
	// Create a payload with 2000 nested arrays
	buf := []byte{'C', 'O', 'W', 'R', 0x01, 0x00, 0x00} // Header + dict len
	for i := 0; i < 2000; i++ {
		buf = append(buf, 0x06, 0x01) // Array tag + count=1
	}
	buf = append(buf, 0x00) // Null at the center

	_, err := Decode(buf)
	if err != ErrDepthExceeded {
		t.Errorf("expected ErrDepthExceeded, got %v", err)
	}
}

// TestSecurityCustomLimits tests DecodeWithOptions with custom limits.
func TestSecurityCustomLimits(t *testing.T) {
	// Build a valid small array
	arr := Array(String("a"), String("b"), String("c"))
	data, _ := Encode(arr)

	// Should work with default limits
	_, err := Decode(data)
	if err != nil {
		t.Fatalf("default decode failed: %v", err)
	}

	// Should fail with very restrictive array limit
	opts := DecodeOptions{MaxArrayLen: 2}
	_, err = DecodeWithOptions(data, opts)
	if err != ErrArrayTooLarge {
		t.Errorf("expected ErrArrayTooLarge with MaxArrayLen=2, got %v", err)
	}

	// Should fail with very restrictive string limit
	strOpts := DecodeOptions{MaxStringLen: 0} // Use default
	_, err = DecodeWithOptions(data, strOpts)
	if err != nil {
		t.Errorf("expected success with default string limit, got %v", err)
	}
}

// TestSecurityTensorMalformedLength tests tensor data length validation.
func TestSecurityTensorMalformedLength(t *testing.T) {
	buf := []byte{'C', 'O', 'W', 'R', 0x01, 0x00, 0x00} // Header + dict len
	buf = append(buf, 0x20)             // Tensor tag
	buf = append(buf, 0x01)             // dtype = float32
	buf = append(buf, 0x01)             // rank = 1
	buf = append(buf, 0x02)             // dim[0] = 2
	buf = append(buf, 0x80, 0x80, 0x04) // Data length = 65536 (way too big)
	// §2.5: tensor data is 64-byte aligned. Pad with zeros up to the next 64-byte
	// boundary so the decoder reaches the dataLen-vs-remaining check (rather than
	// failing earlier while reading alignment padding). r.pos is now len(buf).
	pad := (-len(buf)) & (TensorAlign - 1)
	buf = append(buf, make([]byte, pad)...)
	buf = append(buf, 0x00, 0x00, 0x00, 0x00) // Only 4 data bytes (<< 65536)

	_, err := Decode(buf)
	if err != ErrMalformedLength {
		t.Errorf("expected ErrMalformedLength, got %v", err)
	}
}
