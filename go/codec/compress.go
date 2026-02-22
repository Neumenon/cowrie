package codec

import (
	"bytes"
	"compress/gzip"
	"io"

	"github.com/Neumenon/cowrie"
	"github.com/klauspost/compress/zstd"
)

// MaxDecompressedSize is the maximum allowed decompressed payload size (256 MB).
// This prevents decompression bombs where a small compressed payload expands
// into gigabytes of RAM.
const MaxDecompressedSize = 256 * 1024 * 1024

// zstd encoder/decoder (reusable for performance)
var (
	zstdEnc *zstd.Encoder
	zstdDec *zstd.Decoder
)

func init() {
	var err error
	zstdEnc, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		panic(err)
	}
	zstdDec, err = zstd.NewReader(nil)
	if err != nil {
		panic(err)
	}
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

// decompressGzip decompresses gzip data with a size limit to prevent decompression bombs.
func decompressGzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	limited := io.LimitReader(r, MaxDecompressedSize+1)
	out, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(out)) > MaxDecompressedSize {
		return nil, cowrie.ErrDecompressedTooLarge
	}
	return out, nil
}

// compressZstd compresses data using zstd.
func compressZstd(data []byte) ([]byte, error) {
	return zstdEnc.EncodeAll(data, nil), nil
}

// decompressZstd decompresses zstd data with a size limit to prevent decompression bombs.
func decompressZstd(data []byte) ([]byte, error) {
	// Use a streaming reader with LimitReader instead of DecodeAll
	// to enforce the decompression size limit.
	dec, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer dec.Close()

	limited := io.LimitReader(dec, MaxDecompressedSize+1)
	out, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(out)) > MaxDecompressedSize {
		return nil, cowrie.ErrDecompressedTooLarge
	}
	return out, nil
}
