package codec

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Neumenon/cowrie/go/v2"
)

// Compression type alias for convenience in tests
type Compression = cowrie.Compression

const (
	CompressionNone = cowrie.CompressionNone
	CompressionGzip = cowrie.CompressionGzip
	CompressionZstd = cowrie.CompressionZstd
)

// MustValueFromAny converts any Go value to *cowrie.Value, panicking on error.
func MustValueFromAny(v any) *cowrie.Value {
	return toCowrieValue(v)
}

// MustEncodeBytes encodes a value to Cowrie bytes, panicking on error.
func MustEncodeBytes(v any) []byte {
	data, err := EncodeBytes(v)
	if err != nil {
		panic(err)
	}
	return data
}

// writeGoldenFixture writes test fixture data to testdata directory.
func writeGoldenFixture(t *testing.T, name string, data []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("failed to write fixture %s: %v", name, err)
	}
}

// readGoldenFixture reads test fixture data from testdata directory.
func readGoldenFixture(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("testdata", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read fixture %s: %v", name, err)
	}
	return data
}

// fixtureExists checks if a golden fixture file exists.
func fixtureExists(name string) bool {
	path := filepath.Join("testdata", name)
	_, err := os.Stat(path)
	return err == nil
}

// writePreambleBytes appends the 8-byte file preamble to buf.
func writePreambleBytes(buf *bytes.Buffer) {
	buf.WriteString("SJST")
	binary.Write(buf, binary.LittleEndian, uint16(FormatVersion))
	binary.Write(buf, binary.LittleEndian, uint16(0)) // file_flags
}

// buildMasterFrame builds a master stream stream (preamble + one frame).
func buildMasterFrame(typeID uint32, flags uint8, meta, payload []byte, compress Compression, enableCRC bool) []byte {
	rawLen := uint32(0)
	if compress != CompressionNone {
		rawLen = uint32(len(payload))
	}
	return buildMasterFrameWithRawLen(typeID, flags, meta, payload, rawLen, compress, enableCRC)
}

// buildMasterFrameWithRawLen builds a master stream frame with an explicit raw length.
func buildMasterFrameWithRawLen(typeID uint32, flags uint8, meta, payload []byte, rawLen uint32, compress Compression, enableCRC bool) []byte {
	var buf bytes.Buffer

	// File preamble (written once at stream start)
	writePreambleBytes(&buf)

	// crcStart marks the first byte of the frame (CRC covers the frame only,
	// not the preamble).
	crcStart := buf.Len()

	// Magic "SJST"
	buf.WriteString("SJST")

	// frame_kind (0 = data)
	buf.WriteByte(FrameKindData)

	// Flags
	var frameFlags uint8 = flags
	frameFlags |= compressionFlags(compress)
	if enableCRC {
		frameFlags |= FlagMasterCRC
	}
	if len(meta) > 0 {
		frameFlags |= FlagMasterMeta
	}
	buf.WriteByte(frameFlags)

	// Header length (fixed 24 bytes for v2)
	binary.Write(&buf, binary.LittleEndian, uint16(24))

	// TypeID
	binary.Write(&buf, binary.LittleEndian, typeID)

	// Payload length
	binary.Write(&buf, binary.LittleEndian, uint32(len(payload)))

	// Raw length (0 if not compressed)
	if compress == CompressionNone {
		rawLen = 0
	}
	binary.Write(&buf, binary.LittleEndian, rawLen)

	// Meta length
	binary.Write(&buf, binary.LittleEndian, uint32(len(meta)))

	// Meta
	buf.Write(meta)

	// Payload
	buf.Write(payload)

	// CRC32 if enabled (covers the frame bytes only, not the preamble)
	if enableCRC {
		crc := crc32IEEE(buf.Bytes()[crcStart:])
		binary.Write(&buf, binary.LittleEndian, crc)
	}

	return buf.Bytes()
}

func compressionFlags(comp Compression) uint8 {
	switch comp {
	case CompressionGzip:
		return FlagMasterCompressed | FlagMasterCompGzip
	case CompressionZstd:
		return FlagMasterCompressed | FlagMasterCompZstd
	default:
		return 0
	}
}

// crc32IEEE computes CRC32-IEEE checksum.
func crc32IEEE(data []byte) uint32 {
	var crc uint32 = 0xFFFFFFFF
	for _, b := range data {
		crc ^= uint32(b)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0xEDB88320
			} else {
				crc >>= 1
			}
		}
	}
	return ^crc
}

// makeRepeatedBytes creates a byte slice with repeated pattern.
func makeRepeatedBytes(pattern []byte, count int) []byte {
	result := make([]byte, len(pattern)*count)
	for i := 0; i < count; i++ {
		copy(result[i*len(pattern):], pattern)
	}
	return result
}

// truncateAt returns data truncated at position n.
func truncateAt(data []byte, n int) []byte {
	if n > len(data) {
		return data
	}
	return data[:n]
}

// corruptByteAt returns data with byte at position n XORed with 0xFF.
func corruptByteAt(data []byte, n int) []byte {
	if n >= len(data) {
		return data
	}
	result := make([]byte, len(data))
	copy(result, data)
	result[n] ^= 0xFF
	return result
}
