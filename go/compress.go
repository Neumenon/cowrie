package cowrie

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
	// First encode to raw Cowrie
	raw, err := Encode(v)
	if err != nil {
		return nil, err
	}

	// Decide whether to compress
	if comp == CompressionNone || len(raw) < compressThreshold {
		return raw, nil
	}

	// Extract payload (skip 6-byte header: COWR + version + compression byte)
	payload := raw[6:]

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

	// Header (§2.2): COWR + version + single compression byte (1=gzip, 2=zstd)
	buf.writeByte(Magic0)
	buf.writeByte(Magic1)
	buf.writeByte(Magic2)
	buf.writeByte(Magic3)
	buf.writeByte(Version)
	buf.writeByte(byte(comp))

	// Original uncompressed length
	buf.writeUvarint(uint64(len(payload)))

	// Compressed data
	buf.write(compressed)

	return buf.bytes(), nil
}

// ErrDecompressedTooLarge is returned when decompressed size exceeds limits.
var ErrDecompressedTooLarge = errors.New("cowrie: decompressed size exceeds limit")

// DecodeFramed decodes with automatic decompression.
func DecodeFramed(data []byte) (*Value, error) {
	return DecodeFramedWithLimit(data, DefaultMaxBytesLen)
}

// DecodeFramedStrict decodes with automatic decompression in strict mode
// (SPEC-v1 §5.3): well-formed but non-canonical input is rejected with
// ErrNonCanonical. The post-decompression body is decoded with Strict=true.
func DecodeFramedStrict(data []byte) (*Value, error) {
	opts := DefaultDecodeOptions()
	opts.Strict = true
	return DecodeFramedWithLimitAndOptions(data, DefaultMaxBytesLen, opts)
}

// DecodeFramedWithLimit decodes with automatic decompression and custom size limit.
func DecodeFramedWithLimit(data []byte, maxDecompressedSize int) (*Value, error) {
	return DecodeFramedWithLimitAndOptions(data, maxDecompressedSize, DefaultDecodeOptions())
}

// DecodeFramedWithLimitAndOptions decodes with automatic decompression, a custom
// size limit, and explicit DecodeOptions (notably opts.Strict for §5.3
// strict-decode). The post-decompression body is decoded with the same options.
func DecodeFramedWithLimitAndOptions(data []byte, maxDecompressedSize int, opts DecodeOptions) (*Value, error) {
	if len(data) < 6 {
		return nil, ErrUnexpectedEOF
	}

	// Check magic (COWR) + version + compression byte (§2.2)
	if data[0] != Magic0 || data[1] != Magic1 || data[2] != Magic2 || data[3] != Magic3 {
		return nil, ErrInvalidMagic
	}
	if data[4] != Version {
		return nil, ErrInvalidVersion
	}

	compByte := data[5]

	// Not compressed? Use regular decode
	if compByte == 0 {
		return DecodeWithOptions(data, opts)
	}

	compType := Compression(compByte)

	// Read original length
	origLen, n := binary.Uvarint(data[6:])
	if n <= 0 {
		return nil, ErrUnexpectedEOF
	}

	// Security check: validate claimed decompressed size against limit
	// This prevents decompression bombs where a small compressed payload
	// claims to decompress to a huge size
	if maxDecompressedSize > 0 && origLen > uint64(maxDecompressedSize) {
		return nil, ErrDecompressedTooLarge
	}

	compressed := data[6+n:]

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
	full := make([]byte, 6+len(decompressed))
	full[0] = Magic0
	full[1] = Magic1
	full[2] = Magic2
	full[3] = Magic3
	full[4] = Version
	full[5] = 0 // no compression
	copy(full[6:], decompressed)

	return DecodeWithOptions(full, opts)
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
