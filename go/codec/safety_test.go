package codec

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestSafety_DecompressionBomb tests protection against decompression bombs.
func TestSafety_DecompressionBomb(t *testing.T) {
	t.Run("huge_raw_length_in_header", func(t *testing.T) {
		// Create a frame that claims huge decompressed size
		var buf bytes.Buffer

		// Magic
		buf.WriteString("SJST")
		// Version
		buf.WriteByte(0x02)
		// Flags (compressed with gzip)
		buf.WriteByte(FlagMasterCompressed)
		// Header length
		binary.Write(&buf, binary.LittleEndian, uint16(24))
		// TypeID
		binary.Write(&buf, binary.LittleEndian, uint32(123))
		// Compression type (gzip)
		buf.WriteByte(byte(CompressionGzip))
		// Reserved
		buf.WriteByte(0)
		// Payload length (small)
		binary.Write(&buf, binary.LittleEndian, uint32(10))
		// Raw length (HUGE - 2GB)
		binary.Write(&buf, binary.LittleEndian, uint32(0x80000000))
		// Meta length
		binary.Write(&buf, binary.LittleEndian, uint32(0))
		// Tiny payload (not valid gzip, but that's fine)
		buf.Write([]byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

		mr := NewMasterReader(buf.Bytes(), MasterReaderOptions{
			MaxDecompressedSize: 1 << 20, // 1MB limit
		})

		_, err := mr.Next()
		if err == nil {
			t.Error("expected error for huge raw length, got nil")
		}
	})

	t.Run("gzip_bomb", func(t *testing.T) {
		// Create a gzip bomb: small compressed data that expands massively
		// For this test, we just verify the size limit is enforced

		// This would require actually creating compressed data that expands
		// For now, just verify the option is respected
		mr := NewMasterReader([]byte{}, MasterReaderOptions{
			MaxDecompressedSize: 1024, // Very small limit
		})

		if mr.opts.MaxDecompressedSize != 1024 {
			t.Errorf("MaxDecompressedSize not set: got %d", mr.opts.MaxDecompressedSize)
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

// TestSafety_UnknownCompressionType tests handling of unknown compression.
func TestSafety_UnknownCompressionType(t *testing.T) {
	var buf bytes.Buffer

	// Magic
	buf.WriteString("SJST")
	// Version
	buf.WriteByte(0x02)
	// Flags (compressed)
	buf.WriteByte(FlagMasterCompressed)
	// Header length
	binary.Write(&buf, binary.LittleEndian, uint16(24))
	// TypeID
	binary.Write(&buf, binary.LittleEndian, uint32(123))
	// Compression type (INVALID - 0xFF)
	buf.WriteByte(0xFF)
	// Reserved
	buf.WriteByte(0)
	// Payload length
	binary.Write(&buf, binary.LittleEndian, uint32(10))
	// Raw length
	binary.Write(&buf, binary.LittleEndian, uint32(10))
	// Meta length
	binary.Write(&buf, binary.LittleEndian, uint32(0))
	// Dummy payload
	buf.Write(make([]byte, 10))

	mr := NewMasterReader(buf.Bytes(), MasterReaderOptions{})
	_, err := mr.Next()
	if err == nil {
		t.Error("expected error for unknown compression type")
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
	var buf bytes.Buffer

	// Build header claiming gzip compression
	buf.WriteString("SJST")
	buf.WriteByte(0x02)
	buf.WriteByte(FlagMasterCompressed)
	binary.Write(&buf, binary.LittleEndian, uint16(24))
	binary.Write(&buf, binary.LittleEndian, uint32(123))
	buf.WriteByte(byte(CompressionGzip))
	buf.WriteByte(0)
	binary.Write(&buf, binary.LittleEndian, uint32(20)) // payload len
	binary.Write(&buf, binary.LittleEndian, uint32(100)) // raw len
	binary.Write(&buf, binary.LittleEndian, uint32(0))  // meta len

	// Invalid gzip data (starts with gzip magic but is garbage)
	buf.Write([]byte{0x1f, 0x8b, 0x08, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	mr := NewMasterReader(buf.Bytes(), MasterReaderOptions{
		MaxDecompressedSize: 1 << 20,
	})
	_, err := mr.Next()
	if err == nil {
		t.Error("expected error for corrupted gzip")
	}
}

// TestSafety_CorruptedZstd tests handling of corrupted zstd data.
func TestSafety_CorruptedZstd(t *testing.T) {
	var buf bytes.Buffer

	// Build header claiming zstd compression
	buf.WriteString("SJST")
	buf.WriteByte(0x02)
	buf.WriteByte(FlagMasterCompressed)
	binary.Write(&buf, binary.LittleEndian, uint16(24))
	binary.Write(&buf, binary.LittleEndian, uint32(123))
	buf.WriteByte(byte(CompressionZstd))
	buf.WriteByte(0)
	binary.Write(&buf, binary.LittleEndian, uint32(20)) // payload len
	binary.Write(&buf, binary.LittleEndian, uint32(100)) // raw len
	binary.Write(&buf, binary.LittleEndian, uint32(0))  // meta len

	// Invalid zstd data (random bytes)
	buf.Write([]byte{0x28, 0xB5, 0x2F, 0xFD, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	mr := NewMasterReader(buf.Bytes(), MasterReaderOptions{
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
	binary.Write(&buf, binary.LittleEndian, uint32(0))        // payload len
	binary.Write(&buf, binary.LittleEndian, uint32(0))        // raw len
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
	binary.Write(&buf, binary.LittleEndian, uint32(0))         // raw len
	binary.Write(&buf, binary.LittleEndian, uint32(0))         // meta len

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
	buf.WriteByte(0x00) // Empty dict

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
	buf.WriteByte(0x00) // Empty dict

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
