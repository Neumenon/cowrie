package cowrie

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"testing"
)

// =============================================================================
// Test 1: Quantized DType round-trip
// Regression: isValidDType must accept quantized types (QINT4, QINT2, etc.)
// =============================================================================

func TestSecurity_QuantizedDTypeRoundTrip(t *testing.T) {
	quantizedTypes := []struct {
		name  string
		dtype DType
	}{
		{"QINT4", DTypeQINT4},
		{"QINT2", DTypeQINT2},
		{"QINT3", DTypeQINT3},
		{"Ternary", DTypeTernary},
		{"Binary", DTypeBinary},
	}

	for _, tt := range quantizedTypes {
		t.Run(tt.name, func(t *testing.T) {
			// Create a tensor with a quantized dtype and some opaque data bytes
			data := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04}
			original := Tensor(tt.dtype, []uint64{8}, data)

			// Encode
			encoded, err := Encode(original)
			if err != nil {
				t.Fatalf("Encode failed for %s tensor: %v", tt.name, err)
			}

			// Decode
			decoded, err := Decode(encoded)
			if err != nil {
				t.Fatalf("Decode failed for %s tensor: %v", tt.name, err)
			}

			// Verify type
			if decoded.Type() != TypeTensor {
				t.Fatalf("expected TypeTensor, got %v", decoded.Type())
			}

			td := decoded.Tensor()
			if td.DType != tt.dtype {
				t.Errorf("dtype mismatch: want 0x%02x, got 0x%02x", tt.dtype, td.DType)
			}
			if len(td.Dims) != 1 || td.Dims[0] != 8 {
				t.Errorf("dims mismatch: want [8], got %v", td.Dims)
			}
			if !bytes.Equal(td.Data, data) {
				t.Errorf("data mismatch: want %x, got %x", data, td.Data)
			}
		})
	}
}

// =============================================================================
// Test 2: DecodeWithHints respects depth limits
// Regression: deeply nested structures must be rejected, not cause stack overflow
// =============================================================================

func TestSecurity_DecodeWithHintsDepthLimit(t *testing.T) {
	// Build a deeply nested object (depth 2000 > DefaultMaxDepth of 1000).
	// We encode it by constructing the binary payload directly to avoid
	// any encoder-side depth limits.

	var buf buffer
	// Header
	buf.writeByte(Magic0)
	buf.writeByte(Magic1)
	buf.writeByte(Version)
	buf.writeByte(0) // no flags

	// Dictionary with one key: "x"
	buf.writeUvarint(1)          // 1 dictionary entry
	buf.writeString("x")        // key "x"

	// Emit 2000 nested objects, each with 1 field using dict key 0 ("x")
	depth := 2000
	for i := 0; i < depth; i++ {
		buf.writeByte(TagObject) // object tag
		buf.writeUvarint(1)     // 1 field
		buf.writeUvarint(0)     // field ID 0 = "x"
	}
	// Innermost value: null
	buf.writeByte(TagNull)

	data := buf.bytes()

	// DecodeWithHints uses DefaultDecodeOptions (MaxDepth=1000)
	_, err := DecodeWithHints(data)
	if err == nil {
		t.Fatal("expected ErrDepthExceeded for depth=2000, got nil")
	}
	if !errors.Is(err, ErrDepthExceeded) {
		t.Fatalf("expected ErrDepthExceeded, got: %v", err)
	}

	// Also verify that Decode (with defaults) catches this
	_, err = Decode(data)
	if err == nil {
		t.Fatal("expected ErrDepthExceeded from Decode, got nil")
	}
	if !errors.Is(err, ErrDepthExceeded) {
		t.Fatalf("expected ErrDepthExceeded from Decode, got: %v", err)
	}
}

// =============================================================================
// Test 3: Adjlist nodeCount overflow (MaxUint64)
// Regression: nodeCount = MaxUint64 must return ErrMalformedLength, not panic
// or wrap around on nodeCount+1 overflow
// =============================================================================

func TestSecurity_AdjlistNodeCountOverflow(t *testing.T) {
	var buf bytes.Buffer

	// Cowrie v2 header
	buf.Write([]byte{Magic0, Magic1, Version, 0x00})

	// Empty dictionary
	buf.WriteByte(0x00)

	// Adjlist tag
	buf.WriteByte(TagAdjlist)

	// IDWidth = int32
	buf.WriteByte(byte(IDWidthInt32))

	// nodeCount = MaxUint64 (varint encoding: 10 bytes of 0xFF, final 0x01)
	var varintBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(varintBuf[:], math.MaxUint64)
	buf.Write(varintBuf[:n])

	// edgeCount = 0
	buf.WriteByte(0x00)

	data := buf.Bytes()

	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for nodeCount=MaxUint64, got nil")
	}
	if !errors.Is(err, ErrMalformedLength) {
		t.Fatalf("expected ErrMalformedLength, got: %v", err)
	}
}

// Also test that a legitimately large (but still overflowing) nodeCount is caught.
func TestSecurity_AdjlistNodeCountExceedsRemaining(t *testing.T) {
	var buf bytes.Buffer

	// Cowrie v2 header
	buf.Write([]byte{Magic0, Magic1, Version, 0x00})

	// Empty dictionary
	buf.WriteByte(0x00)

	// Adjlist tag
	buf.WriteByte(TagAdjlist)

	// IDWidth = int64
	buf.WriteByte(byte(IDWidthInt64))

	// nodeCount = 1 billion (way more than the remaining bytes)
	var varintBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(varintBuf[:], 1_000_000_000)
	buf.Write(varintBuf[:n])

	// edgeCount = 0
	buf.WriteByte(0x00)

	data := buf.Bytes()

	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for huge nodeCount, got nil")
	}
	// Should be ErrMalformedLength (nodeCount+1 > remaining)
	if !errors.Is(err, ErrMalformedLength) {
		t.Fatalf("expected ErrMalformedLength, got: %v", err)
	}
}

// =============================================================================
// Test 4: RichText MaxStringLen enforcement
// Regression: RichText text field must be subject to MaxStringLen limits
// =============================================================================

func TestSecurity_RichTextMaxStringLen(t *testing.T) {
	// Create a RichText with a text string of known length
	longText := strings.Repeat("A", 10000) // 10KB text
	original := RichText(longText, nil, nil)

	// Encode it normally (no string length limit on encode)
	encoded, err := Encode(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode with a restrictive MaxStringLen (smaller than the text)
	opts := DefaultDecodeOptions()
	opts.MaxStringLen = 5000 // 5KB limit, text is 10KB

	_, err = DecodeWithOptions(encoded, opts)
	if err == nil {
		t.Fatal("expected ErrStringTooLarge for RichText text exceeding MaxStringLen, got nil")
	}
	if !errors.Is(err, ErrStringTooLarge) {
		t.Fatalf("expected ErrStringTooLarge, got: %v", err)
	}

	// Verify it works when MaxStringLen is large enough
	opts.MaxStringLen = 20000
	decoded, err := DecodeWithOptions(encoded, opts)
	if err != nil {
		t.Fatalf("Decode with sufficient MaxStringLen failed: %v", err)
	}
	if decoded.Type() != TypeRichText {
		t.Fatalf("expected TypeRichText, got %v", decoded.Type())
	}
	rt := decoded.RichText()
	if rt.Text != longText {
		t.Error("RichText text round-trip mismatch")
	}
}

// =============================================================================
// Test 5: Default limits are sane
// Regression: DefaultDecodeOptions must return non-zero values for all limits
// =============================================================================

func TestSecurity_DefaultLimitsSane(t *testing.T) {
	opts := DefaultDecodeOptions()

	checks := []struct {
		name  string
		value int
	}{
		{"MaxDepth", opts.MaxDepth},
		{"MaxArrayLen", opts.MaxArrayLen},
		{"MaxObjectLen", opts.MaxObjectLen},
		{"MaxStringLen", opts.MaxStringLen},
		{"MaxBytesLen", opts.MaxBytesLen},
		{"MaxExtLen", opts.MaxExtLen},
		{"MaxDictLen", opts.MaxDictLen},
		{"MaxHintCount", opts.MaxHintCount},
		{"MaxRank", opts.MaxRank},
	}

	for _, c := range checks {
		if c.value <= 0 {
			t.Errorf("%s is %d, expected a positive non-zero default", c.name, c.value)
		}
	}

	// Verify specific known defaults
	if opts.MaxDepth != DefaultMaxDepth {
		t.Errorf("MaxDepth: got %d, want %d", opts.MaxDepth, DefaultMaxDepth)
	}
	if opts.MaxArrayLen != DefaultMaxArrayLen {
		t.Errorf("MaxArrayLen: got %d, want %d", opts.MaxArrayLen, DefaultMaxArrayLen)
	}
	if opts.MaxObjectLen != DefaultMaxObjectLen {
		t.Errorf("MaxObjectLen: got %d, want %d", opts.MaxObjectLen, DefaultMaxObjectLen)
	}
	if opts.MaxStringLen != DefaultMaxStringLen {
		t.Errorf("MaxStringLen: got %d, want %d", opts.MaxStringLen, DefaultMaxStringLen)
	}
	if opts.MaxBytesLen != DefaultMaxBytesLen {
		t.Errorf("MaxBytesLen: got %d, want %d", opts.MaxBytesLen, DefaultMaxBytesLen)
	}
	if opts.MaxExtLen != DefaultMaxExtLen {
		t.Errorf("MaxExtLen: got %d, want %d", opts.MaxExtLen, DefaultMaxExtLen)
	}
	if opts.MaxDictLen != DefaultMaxDictLen {
		t.Errorf("MaxDictLen: got %d, want %d", opts.MaxDictLen, DefaultMaxDictLen)
	}
	if opts.MaxHintCount != DefaultMaxHintCount {
		t.Errorf("MaxHintCount: got %d, want %d", opts.MaxHintCount, DefaultMaxHintCount)
	}
	if opts.MaxRank != DefaultMaxRank {
		t.Errorf("MaxRank: got %d, want %d", opts.MaxRank, DefaultMaxRank)
	}

	// Verify zero-value DecodeOptions get populated by DecodeWithOptions
	zeroOpts := DecodeOptions{}
	// Encode a simple value
	encoded, err := Encode(String("test"))
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	// Should succeed because DecodeWithOptions fills in defaults for zero values
	_, err = DecodeWithOptions(encoded, zeroOpts)
	if err != nil {
		t.Fatalf("DecodeWithOptions with zero opts should use defaults: %v", err)
	}
}

// =============================================================================
// Test 6: Decompression bomb (root package compress.go)
// Regression: DecodeFramedWithLimit must reject payloads that claim to
// decompress to more than the specified limit
// =============================================================================

func TestSecurity_DecompressionBomb(t *testing.T) {
	// Create a legitimate value, encode it with gzip compression
	largeString := strings.Repeat("ABCDEFGH", 10000) // 80KB string, compresses well
	original := String(largeString)

	encoded, err := EncodeFramed(original, CompressionGzip)
	if err != nil {
		t.Fatalf("EncodeFramed failed: %v", err)
	}

	// Verify it decodes normally with a generous limit
	decoded, err := DecodeFramedWithLimit(encoded, 1<<20) // 1MB limit
	if err != nil {
		t.Fatalf("DecodeFramedWithLimit failed with generous limit: %v", err)
	}
	if decoded.Type() != TypeString {
		t.Fatalf("expected TypeString, got %v", decoded.Type())
	}

	// Now try with a very small limit (smaller than the decompressed size)
	_, err = DecodeFramedWithLimit(encoded, 1000) // 1KB limit, payload is ~80KB decompressed
	if err == nil {
		t.Fatal("expected ErrDecompressedTooLarge for small limit, got nil")
	}
	if !errors.Is(err, ErrDecompressedTooLarge) {
		t.Fatalf("expected ErrDecompressedTooLarge, got: %v", err)
	}

	// Test with zstd compression too
	encodedZstd, err := EncodeFramed(original, CompressionZstd)
	if err != nil {
		t.Fatalf("EncodeFramed zstd failed: %v", err)
	}

	_, err = DecodeFramedWithLimit(encodedZstd, 1000)
	if err == nil {
		t.Fatal("expected ErrDecompressedTooLarge for zstd with small limit, got nil")
	}
	if !errors.Is(err, ErrDecompressedTooLarge) {
		t.Fatalf("expected ErrDecompressedTooLarge for zstd, got: %v", err)
	}
}

// =============================================================================
// Test 6b: Hand-crafted decompression bomb header
// Craft a frame that claims a huge decompressed size in the header
// =============================================================================

func TestSecurity_DecompressionBombCraftedHeader(t *testing.T) {
	var buf bytes.Buffer

	// Cowrie v2 header with compression flag
	// Compression type = gzip (1), encoded in flags as: FlagCompressed | (comp << 1)
	flags := byte(FlagCompressed) | byte((CompressionGzip&0x03)<<1)
	buf.WriteByte(Magic0)
	buf.WriteByte(Magic1)
	buf.WriteByte(Version)
	buf.WriteByte(flags)

	// Claimed original length: 1GB (way too large)
	var varintBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(varintBuf[:], 1<<30)
	buf.Write(varintBuf[:n])

	// Tiny gzip payload (valid gzip header but won't produce 1GB)
	buf.Write([]byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

	data := buf.Bytes()

	// Decode with a 1MB limit -- the claimed size (1GB) should be rejected immediately
	_, err := DecodeFramedWithLimit(data, 1<<20)
	if err == nil {
		t.Fatal("expected error for crafted decompression bomb header, got nil")
	}
	if !errors.Is(err, ErrDecompressedTooLarge) {
		t.Fatalf("expected ErrDecompressedTooLarge, got: %v", err)
	}
}

// =============================================================================
// Additional regression tests for completeness
// =============================================================================

// TestSecurity_VarintOverflow verifies that a varint that overflows uint64 is rejected.
func TestSecurity_VarintOverflow(t *testing.T) {
	var buf bytes.Buffer

	// Cowrie v2 header
	buf.Write([]byte{Magic0, Magic1, Version, 0x00})

	// Dictionary length as overflowing varint (10 continuation bytes + 1 terminator > 10 bytes)
	// binary.Uvarint returns n<0 for overflow
	buf.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02})

	data := buf.Bytes()
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for varint overflow, got nil")
	}
	// Should be ErrInvalidVarint (overflow detected by binary.Uvarint returning n<0)
	if !errors.Is(err, ErrInvalidVarint) {
		t.Fatalf("expected ErrInvalidVarint, got: %v", err)
	}
}

// TestSecurity_StringLenExceedsRemaining verifies that a string whose declared
// length exceeds remaining bytes is rejected as ErrMalformedLength.
func TestSecurity_StringLenExceedsRemaining(t *testing.T) {
	var buf bytes.Buffer

	// Cowrie v2 header
	buf.Write([]byte{Magic0, Magic1, Version, 0x00})

	// Empty dictionary
	buf.WriteByte(0x00)

	// String tag
	buf.WriteByte(TagString)

	// String length = 1000 (way more than remaining data)
	var varintBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(varintBuf[:], 1000)
	buf.Write(varintBuf[:n])

	// Only a few actual bytes follow
	buf.Write([]byte("short"))

	data := buf.Bytes()
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for string length exceeding remaining data, got nil")
	}
	if !errors.Is(err, ErrMalformedLength) {
		t.Fatalf("expected ErrMalformedLength, got: %v", err)
	}
}

// TestSecurity_ArrayCountExceedsRemaining verifies that an array claiming more
// elements than there are bytes is rejected.
func TestSecurity_ArrayCountExceedsRemaining(t *testing.T) {
	var buf bytes.Buffer

	// Cowrie v2 header
	buf.Write([]byte{Magic0, Magic1, Version, 0x00})

	// Empty dictionary
	buf.WriteByte(0x00)

	// Array tag
	buf.WriteByte(TagArray)

	// Count = 10 million (way more than remaining data)
	var varintBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(varintBuf[:], 10_000_000)
	buf.Write(varintBuf[:n])

	data := buf.Bytes()
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for array count exceeding remaining data, got nil")
	}
	if !errors.Is(err, ErrMalformedLength) {
		t.Fatalf("expected ErrMalformedLength, got: %v", err)
	}
}

// TestSecurity_DictTooLarge verifies that an enormous dictionary is rejected.
func TestSecurity_DictTooLarge(t *testing.T) {
	var buf bytes.Buffer

	// Cowrie v2 header
	buf.Write([]byte{Magic0, Magic1, Version, 0x00})

	// Dictionary length = 100 million (exceeds DefaultMaxDictLen of 10M)
	var varintBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(varintBuf[:], 100_000_000)
	buf.Write(varintBuf[:n])

	data := buf.Bytes()

	// With default limits (MaxDictLen = 10M), this should fail
	_, err := Decode(data)
	if err == nil {
		t.Fatal("expected error for oversized dictionary, got nil")
	}
	if !errors.Is(err, ErrDictTooLarge) {
		t.Fatalf("expected ErrDictTooLarge, got: %v", err)
	}
}
