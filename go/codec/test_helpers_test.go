package codec

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Neumenon/cowrie/go"
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

// buildLegacyStreamFrame builds a legacy length-prefixed Cowrie stream frame.
func buildLegacyStreamFrame(payload []byte) []byte {
	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	copy(buf[4:], payload)
	return buf
}

// buildMasterFrame builds a master stream frame with given parameters.
func buildMasterFrame(typeID uint32, flags uint8, meta, payload []byte, compress Compression, enableCRC bool) []byte {
	var buf bytes.Buffer

	// Magic "SJST"
	buf.WriteString("SJST")

	// Version
	buf.WriteByte(0x02)

	// Flags
	var frameFlags uint8 = flags
	if compress != CompressionNone {
		frameFlags |= FlagMasterCompressed
	}
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

	// Compression type
	buf.WriteByte(byte(compress))

	// Reserved
	buf.WriteByte(0)

	// Payload length
	binary.Write(&buf, binary.LittleEndian, uint32(len(payload)))

	// Raw length (0 if not compressed)
	var rawLen uint32
	if compress != CompressionNone {
		rawLen = uint32(len(payload)) // In real usage this would be decompressed size
	}
	binary.Write(&buf, binary.LittleEndian, rawLen)

	// Meta length
	binary.Write(&buf, binary.LittleEndian, uint32(len(meta)))

	// Meta
	buf.Write(meta)

	// Payload
	buf.Write(payload)

	// CRC32 if enabled
	if enableCRC {
		data := buf.Bytes()
		crc := crc32IEEE(data)
		binary.Write(&buf, binary.LittleEndian, crc)
	}

	return buf.Bytes()
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
