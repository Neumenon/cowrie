package gen2

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Minimum size to consider compression (bytes)
const compressThreshold = 256

// EncodeFramed encodes with optional compression.
func EncodeFramed(v *Value, comp Compression) ([]byte, error) {
	// First encode to raw SJSON
	raw, err := Encode(v)
	if err != nil {
		return nil, err
	}

	// Decide whether to compress
	if comp == CompressionNone || len(raw) < compressThreshold {
		return raw, nil
	}

	// Extract payload (skip 4-byte header)
	payload := raw[4:]

	// Compress
	var compressed []byte
	switch comp {
	case CompressionGzip:
		compressed, err = compressGzip(payload)
	case CompressionZstd:
		compressed, err = compressZstd(payload)
	default:
		return raw, nil
	}

	if err != nil {
		// Compression failed, return raw
		return raw, nil
	}

	// Check if compression helped
	if len(compressed) >= len(payload) {
		return raw, nil
	}

	// Build framed output
	var buf buffer

	// Header with compression flag
	flags := byte(FlagCompressed) | byte((comp&0x03)<<1)
	buf.writeByte(Magic0)
	buf.writeByte(Magic1)
	buf.writeByte(Version)
	buf.writeByte(flags)

	// Original uncompressed length
	buf.writeUvarint(uint64(len(payload)))

	// Compressed data
	buf.write(compressed)

	return buf.bytes(), nil
}

// ErrDecompressedTooLarge is returned when decompressed size exceeds limits.
var ErrDecompressedTooLarge = errors.New("sjson: decompressed size exceeds limit")

// DecodeFramed decodes with automatic decompression.
func DecodeFramed(data []byte) (*Value, error) {
	return DecodeFramedWithLimit(data, DefaultMaxBytesLen)
}

// DecodeFramedWithLimit decodes with automatic decompression and custom size limit.
func DecodeFramedWithLimit(data []byte, maxDecompressedSize int) (*Value, error) {
	if len(data) < 4 {
		return nil, ErrUnexpectedEOF
	}

	// Check magic
	if data[0] != Magic0 || data[1] != Magic1 {
		return nil, ErrInvalidMagic
	}

	// Check version
	if data[2] != Version {
		return nil, ErrInvalidVersion
	}

	flags := data[3]

	// Not compressed? Use regular decode
	if flags&FlagCompressed == 0 {
		return Decode(data)
	}

	// Extract compression type
	compType := Compression((flags >> 1) & 0x03)

	// Read original length
	origLen, n := binary.Uvarint(data[4:])
	if n <= 0 {
		return nil, ErrUnexpectedEOF
	}

	// Security check: validate claimed decompressed size against limit
	// This prevents decompression bombs where a small compressed payload
	// claims to decompress to a huge size
	if maxDecompressedSize > 0 && origLen > uint64(maxDecompressedSize) {
		return nil, ErrDecompressedTooLarge
	}

	compressed := data[4+n:]

	// Decompress
	var decompressed []byte
	var err error
	switch compType {
	case CompressionGzip:
		decompressed, err = decompressGzip(compressed, maxDecompressedSize)
	case CompressionZstd:
		decompressed, err = decompressZstd(compressed, maxDecompressedSize)
	default:
		return nil, ErrInvalidTag
	}

	if err != nil {
		return nil, err
	}

	// Verify actual size matches claimed size
	if uint64(len(decompressed)) != origLen {
		return nil, ErrUnexpectedEOF
	}

	// Reconstruct full message: header + decompressed payload
	full := make([]byte, 4+len(decompressed))
	full[0] = Magic0
	full[1] = Magic1
	full[2] = Version
	full[3] = 0 // no compression
	copy(full[4:], decompressed)

	return Decode(full)
}

// compressGzip compresses data using gzip.
func compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decompressGzip decompresses gzip data with a size limit.
func decompressGzip(data []byte, maxSize int) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	if maxSize <= 0 {
		return io.ReadAll(r)
	}

	limited := io.LimitReader(r, int64(maxSize)+1)
	out, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(out) > maxSize {
		return nil, ErrDecompressedTooLarge
	}
	return out, nil
}

// zstd encoder/decoder (reusable for performance)
var (
	zstdEncoder *zstd.Encoder
)

func init() {
	var err error
	zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		panic(err)
	}
}

// compressZstd compresses data using zstd.
func compressZstd(data []byte) ([]byte, error) {
	return zstdEncoder.EncodeAll(data, nil), nil
}

// decompressZstd decompresses zstd data.
func decompressZstd(data []byte, maxSize int) ([]byte, error) {
	dec, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer dec.Close()

	if maxSize <= 0 {
		return io.ReadAll(dec)
	}

	limited := io.LimitReader(dec, int64(maxSize)+1)
	out, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(out) > maxSize {
		return nil, ErrDecompressedTooLarge
	}
	return out, nil
}
