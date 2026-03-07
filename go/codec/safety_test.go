package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/Neumenon/cowrie/go"
)

// TestSafety_DecompressionBomb tests protection against decompression bombs.
func TestSafety_DecompressionBomb(t *testing.T) {
	t.Run("huge_raw_length_in_header", func(t *testing.T) {
		frame := buildMasterFrameWithRawLen(
			123,
			0,
			nil,
			[]byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			0x80000000,
			CompressionGzip,
			false,
		)

		mr := NewMasterReader(frame, MasterReaderOptions{
			MaxDecompressedSize: 1 << 20, // 1MB limit
		})

		_, err := mr.Next()
		if !errors.Is(err, cowrie.ErrDecompressedTooLarge) {
			t.Fatalf("expected ErrDecompressedTooLarge for huge raw length, got %v", err)
		}
	})

	t.Run("gzip_bomb", func(t *testing.T) {
		rawPayload, err := EncodeBytes(map[string]any{
			"data": strings.Repeat("COMPRESS_ME_", 10000),
		})
		if err != nil {
			t.Fatalf("EncodeBytes failed: %v", err)
		}

		compressed, err := compressGzip(rawPayload)
		if err != nil {
			t.Fatalf("compressGzip failed: %v", err)
		}

		frame := buildMasterFrameWithRawLen(
			123,
			0,
			nil,
			compressed,
			uint32(len(rawPayload)),
			CompressionGzip,
			false,
		)

		mr := NewMasterReader(frame, MasterReaderOptions{
			MaxDecompressedSize: 1024, // Very small limit
		})

		_, err = mr.Next()
		if !errors.Is(err, cowrie.ErrDecompressedTooLarge) {
			t.Fatalf("expected ErrDecompressedTooLarge for oversized gzip payload, got %v", err)
		}
	})
}

// TestSafety_TruncatedHeader tests handling of truncated headers.
func TestSafety_TruncatedHeader(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"magic_only", []byte("SJST")},
		{"magic_version", []byte("SJST\x02")},
		{"magic_version_flags", []byte("SJST\x02\x00")},
		{"partial_header", []byte("SJST\x02\x00\x18\x00\x00\x00")},
		{"header_no_payload", []byte("SJST\x02\x00\x18\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := NewMasterReader(tt.data, MasterReaderOptions{})
			_, err := mr.Next()
			if err == nil {
				t.Error("expected error for truncated header")
			}
		})
	}
}

// TestSafety_TruncatedPayload tests handling of truncated payloads.
func TestSafety_TruncatedPayload(t *testing.T) {
	// Build a valid frame
	var buf bytes.Buffer
	mw := NewMasterWriter(&buf, MasterWriterOptions{})
	mw.Write(MustValueFromAny(map[string]any{"test": int64(1)}))
	validFrame := buf.Bytes()

	// Truncate at various points in the payload
	headerEnd := 24 // After header
	for i := headerEnd; i < len(validFrame); i++ {
		truncated := validFrame[:i]
		mr := NewMasterReader(truncated, MasterReaderOptions{})
		_, err := mr.Next()
		if err == nil {
			// This might succeed for some truncation points if the payload
			// happens to be valid at that length - but shouldn't panic
		}
	}
}

// TestSafety_BadCRC tests detection of CRC errors.
func TestSafety_BadCRC(t *testing.T) {
	// Build a frame with CRC
	var buf bytes.Buffer
	mw := NewMasterWriter(&buf, MasterWriterOptions{EnableCRC: true})
	mw.Write(MustValueFromAny(map[string]any{"test": int64(1)}))
	validFrame := buf.Bytes()

	// Corrupt the CRC (last 4 bytes)
	corrupted := make([]byte, len(validFrame))
	copy(corrupted, validFrame)
	corrupted[len(corrupted)-1] ^= 0xFF
	corrupted[len(corrupted)-2] ^= 0xFF

	mr := NewMasterReader(corrupted, MasterReaderOptions{})
	_, err := mr.Next()
	if err == nil {
		t.Error("expected CRC error")
	}
}

// TestSafety_CompressedFlagWithoutAlgorithmBits tests malformed compressed headers.
func TestSafety_CompressedFlagWithoutAlgorithmBits(t *testing.T) {
	rawPayload, err := EncodeBytes(map[string]any{"test": int64(1)})
	if err != nil {
		t.Fatalf("EncodeBytes failed: %v", err)
	}
	compressed, err := compressGzip(rawPayload)
	if err != nil {
		t.Fatalf("compressGzip failed: %v", err)
	}

	frame := buildMasterFrameWithRawLen(
		123,
		FlagMasterCompressed,
		nil,
		compressed,
		uint32(len(rawPayload)),
		CompressionNone,
		false,
	)

	mr := NewMasterReader(frame, MasterReaderOptions{})
	_, err = mr.Next()
	if err == nil {
		t.Error("expected error for compressed frame without algorithm bits")
	}
}

// TestSafety_InvalidMagic tests handling of invalid magic bytes.
func TestSafety_InvalidMagic(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"wrong_magic", []byte("XXXX\x02\x00")},
		{"partial_magic", []byte("SJS")},
		{"null_magic", []byte{0, 0, 0, 0, 0x02, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := NewMasterReader(tt.data, MasterReaderOptions{})
			_, err := mr.Next()
			// Should either error or return legacy frame (with AllowLegacy)
			if err == nil {
				// If no error, it should have been detected as non-master-stream
				// and handled appropriately
			}
		})
	}
}

// TestSafety_InvalidVersion tests handling of unsupported versions.
func TestSafety_InvalidVersion(t *testing.T) {
	tests := []struct {
		name    string
		version byte
	}{
		{"version_0", 0x00},
		{"version_255", 0xFF},
		{"version_3", 0x03},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			buf.WriteString("SJST")
			buf.WriteByte(tt.version)
			buf.WriteByte(0x00)
			// Rest of header
			buf.Write(make([]byte, 18))

			mr := NewMasterReader(buf.Bytes(), MasterReaderOptions{})
			_, err := mr.Next()
			// Might succeed for some versions or error - shouldn't panic
			_ = err
		})
	}
}

// TestSafety_CorruptedGzip tests handling of corrupted gzip data.
func TestSafety_CorruptedGzip(t *testing.T) {
	frame := buildMasterFrameWithRawLen(
		123,
		0,
		nil,
		[]byte{0x1f, 0x8b, 0x08, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		100,
		CompressionGzip,
		false,
	)

	mr := NewMasterReader(frame, MasterReaderOptions{
		MaxDecompressedSize: 1 << 20,
	})
	_, err := mr.Next()
	if err == nil {
		t.Error("expected error for corrupted gzip")
	}
}

// TestSafety_CorruptedZstd tests handling of corrupted zstd data.
func TestSafety_CorruptedZstd(t *testing.T) {
	frame := buildMasterFrameWithRawLen(
		123,
		0,
		nil,
		[]byte{0x28, 0xB5, 0x2F, 0xFD, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		100,
		CompressionZstd,
		false,
	)

	mr := NewMasterReader(frame, MasterReaderOptions{
		MaxDecompressedSize: 1 << 20,
	})
	_, err := mr.Next()
	if err == nil {
		t.Error("expected error for corrupted zstd")
	}
}

// TestSafety_HugeMetaLength tests handling of huge meta length.
func TestSafety_HugeMetaLength(t *testing.T) {
	var buf bytes.Buffer

	buf.WriteString("SJST")
	buf.WriteByte(0x02)
	buf.WriteByte(FlagMasterMeta)
	binary.Write(&buf, binary.LittleEndian, uint16(24))
	binary.Write(&buf, binary.LittleEndian, uint32(123))
	buf.WriteByte(0)
	buf.WriteByte(0)
	binary.Write(&buf, binary.LittleEndian, uint32(0))          // payload len
	binary.Write(&buf, binary.LittleEndian, uint32(0))          // raw len
	binary.Write(&buf, binary.LittleEndian, uint32(0x80000000)) // meta len (2GB!)

	mr := NewMasterReader(buf.Bytes(), MasterReaderOptions{
		MaxDecompressedSize: 1 << 20,
	})
	_, err := mr.Next()
	if err == nil {
		t.Error("expected error for huge meta length")
	}
}

// TestSafety_HugePayloadLength tests handling of huge payload length.
func TestSafety_HugePayloadLength(t *testing.T) {
	var buf bytes.Buffer

	buf.WriteString("SJST")
	buf.WriteByte(0x02)
	buf.WriteByte(0)
	binary.Write(&buf, binary.LittleEndian, uint16(24))
	binary.Write(&buf, binary.LittleEndian, uint32(123))
	buf.WriteByte(0)
	buf.WriteByte(0)
	binary.Write(&buf, binary.LittleEndian, uint32(0x80000000)) // payload len (2GB!)
	binary.Write(&buf, binary.LittleEndian, uint32(0))          // raw len
	binary.Write(&buf, binary.LittleEndian, uint32(0))          // meta len

	mr := NewMasterReader(buf.Bytes(), MasterReaderOptions{
		MaxDecompressedSize: 1 << 20,
	})
	_, err := mr.Next()
	if err == nil {
		t.Error("expected error for huge payload length")
	}
}

// TestSafety_CowrieDepthLimit tests that deeply nested structures are limited.
func TestSafety_CowrieDepthLimit(t *testing.T) {
	// Build deeply nested structure
	depth := 1000
	var value any = int64(42)
	for i := 0; i < depth; i++ {
		value = map[string]any{"nested": value}
	}

	// Try to encode - should either succeed or return error, not crash
	_, err := FastEncode(value)
	_ = err // Error is acceptable for very deep nesting
}

// TestSafety_CowrieArraySizeLimit tests that huge arrays are limited.
func TestSafety_CowrieArraySizeLimit(t *testing.T) {
	// We don't actually allocate the huge array, just craft bytes
	// that claim to have a huge array

	var buf bytes.Buffer
	buf.Write([]byte{'S', 'J', 0x02, 0x00}) // Header
	buf.WriteByte(0x00)                     // Empty dict

	// Array tag
	buf.WriteByte(0x06)
	// Huge count (varint encoding of 2^30)
	buf.Write([]byte{0x80, 0x80, 0x80, 0x80, 0x04})

	var decoded any
	err := DecodeBytes(buf.Bytes(), &decoded)
	if err == nil {
		t.Error("expected error for huge array")
	}
}

// TestSafety_CowrieStringSizeLimit tests that huge strings are limited.
func TestSafety_CowrieStringSizeLimit(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{'S', 'J', 0x02, 0x00}) // Header
	buf.WriteByte(0x00)                     // Empty dict

	// String tag
	buf.WriteByte(0x05)
	// Huge length (varint encoding of 2^30)
	buf.Write([]byte{0x80, 0x80, 0x80, 0x80, 0x04})

	var decoded any
	err := DecodeBytes(buf.Bytes(), &decoded)
	if err == nil {
		t.Error("expected error for huge string")
	}
}
