package codec

import (
	"bytes"
	"compress/gzip"
	"io"

	"github.com/klauspost/compress/zstd"
)

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

// decompressGzip decompresses gzip data.
func decompressGzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// compressZstd compresses data using zstd.
func compressZstd(data []byte) ([]byte, error) {
	return zstdEnc.EncodeAll(data, nil), nil
}

// decompressZstd decompresses zstd data.
func decompressZstd(data []byte) ([]byte, error) {
	return zstdDec.DecodeAll(data, nil)
}
