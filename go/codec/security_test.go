package codec

import (
	"errors"
	"strings"
	"testing"

	"github.com/Neumenon/cowrie/go"
)

// =============================================================================
// Test 6 (codec package): Decompression bomb protection
// Regression: MasterStream decompression must enforce MaxDecompressedSize,
// rejecting payloads whose decompressed size exceeds the configured limit.
// =============================================================================

func TestSecurity_GzipDecompressionBomb(t *testing.T) {
	// Build a MasterStream frame with gzip compression using a large payload.
	// Pass a raw Go map (not *cowrie.Value) so the MasterWriter encodes it properly.
	largePayload := map[string]any{
		"data": strings.Repeat("COMPRESS_ME_", 10000), // ~120KB string
	}

	var frameBuf safeBuffer
	mw := NewMasterWriter(&frameBuf, MasterWriterOptions{
		Compression: cowrie.CompressionGzip,
	})
	if err := mw.Write(largePayload); err != nil {
		t.Fatalf("MasterWriter.Write failed: %v", err)
	}
	frameData := frameBuf.Bytes()

	// Verify that compression was actually applied
	if len(frameData) < 6 {
		t.Fatalf("frame too small: %d bytes", len(frameData))
	}
	flags := frameData[5]
	if flags&FlagMasterCompressed == 0 {
		t.Fatal("frame was NOT compressed -- test is invalid (payload too small?)")
	}
	t.Logf("frame size: %d bytes, flags: 0x%02x (compressed)", len(frameData), flags)

	// Read with a very small decompression limit (1KB, payload is ~120KB decompressed)
	mr := NewMasterReader(frameData, MasterReaderOptions{
		MaxDecompressedSize: 1024,
	})

	_, err := mr.Next()
	if !errors.Is(err, cowrie.ErrDecompressedTooLarge) {
		t.Fatalf("expected ErrDecompressedTooLarge for oversized gzip payload, got %v", err)
	}
}

func TestSecurity_ZstdDecompressionBomb(t *testing.T) {
	// Same test with zstd compression
	largePayload := map[string]any{
		"data": strings.Repeat("COMPRESS_ME_", 10000),
	}

	var frameBuf safeBuffer
	mw := NewMasterWriter(&frameBuf, MasterWriterOptions{
		Compression: cowrie.CompressionZstd,
	})
	if err := mw.Write(largePayload); err != nil {
		t.Fatalf("MasterWriter.Write failed: %v", err)
	}
	frameData := frameBuf.Bytes()

	if len(frameData) < 6 {
		t.Fatalf("frame too small: %d bytes", len(frameData))
	}
	flags := frameData[5]
	if flags&FlagMasterCompressed == 0 {
		t.Fatal("frame was NOT compressed -- test is invalid (payload too small?)")
	}
	t.Logf("frame size: %d bytes, flags: 0x%02x (compressed)", len(frameData), flags)

	mr := NewMasterReader(frameData, MasterReaderOptions{
		MaxDecompressedSize: 1024,
	})

	_, err := mr.Next()
	if !errors.Is(err, cowrie.ErrDecompressedTooLarge) {
		t.Fatalf("expected ErrDecompressedTooLarge for oversized zstd payload, got %v", err)
	}
}

func TestSecurity_PerReaderDecompressionLimitAppliedDuringDecompression(t *testing.T) {
	payload := []byte(strings.Repeat("COMPRESS_ME_", 200))

	tests := []struct {
		name        string
		compression cowrie.Compression
		compress    func([]byte) ([]byte, error)
	}{
		{
			name:        "gzip",
			compression: cowrie.CompressionGzip,
			compress:    compressGzip,
		},
		{
			name:        "zstd",
			compression: cowrie.CompressionZstd,
			compress:    compressZstd,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := tt.compress(payload)
			if err != nil {
				t.Fatalf("compress failed: %v", err)
			}

			_, err = decompressPayload(compressed, tt.compression, 1024)
			if !errors.Is(err, cowrie.ErrDecompressedTooLarge) {
				t.Fatalf("expected ErrDecompressedTooLarge, got %v", err)
			}
		})
	}
}

// TestSecurity_DecompressionLimitConstant verifies the MaxDecompressedSize constant.
func TestSecurity_DecompressionLimitConstant(t *testing.T) {
	// MaxDecompressedSize should be 256MB
	if MaxDecompressedSize != 256*1024*1024 {
		t.Errorf("MaxDecompressedSize: got %d, want %d", MaxDecompressedSize, 256*1024*1024)
	}
}

// TestSecurity_DecompressionLimitRespected verifies that the MaxDecompressedSize
// option is stored and consulted by the reader.
func TestSecurity_DecompressionLimitRespected(t *testing.T) {
	mr := NewMasterReader([]byte{}, MasterReaderOptions{
		MaxDecompressedSize: 42,
	})
	if mr.opts.MaxDecompressedSize != 42 {
		t.Errorf("MaxDecompressedSize not stored: got %d, want 42", mr.opts.MaxDecompressedSize)
	}
}

// safeBuffer wraps bytes for the io.Writer interface used by MasterWriter.
type safeBuffer struct {
	data []byte
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *safeBuffer) Bytes() []byte {
	return b.data
}
