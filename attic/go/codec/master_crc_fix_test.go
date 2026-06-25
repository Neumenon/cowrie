package codec

import (
	"bytes"
	"testing"

	"github.com/Neumenon/cowrie/go/v2"
)

// TestMasterCRCVerifiedBeforeMetaDecode asserts that CRC is checked BEFORE
// metadata bytes are decoded into a cowrie.Value. A corrupted frame must return
// ErrMasterCRCMismatch with nil frame/metadata, not a partially-decoded result.
func TestMasterCRCVerifiedBeforeMetaDecode(t *testing.T) {
	meta := cowrie.Object(
		cowrie.Member{Key: "source", Value: cowrie.String("integrity-test")},
		cowrie.Member{Key: "seq", Value: cowrie.Int64(42)},
	)
	payload := map[string]any{"data": "sensitive"}

	var buf bytes.Buffer
	opts := MasterWriterOptions{
		EnableCRC:     true,
		Deterministic: true,
		Compression:   cowrie.CompressionNone,
	}
	writer := NewMasterWriter(&buf, opts)

	if err := writer.WriteWithMeta(payload, meta); err != nil {
		t.Fatalf("WriteWithMeta failed: %v", err)
	}

	// Verify the untampered frame round-trips correctly.
	t.Run("untampered_roundtrip", func(t *testing.T) {
		good := make([]byte, buf.Len())
		copy(good, buf.Bytes())

		reader := NewMasterReader(good, DefaultMasterReaderOptions())
		frame, err := reader.Next()
		if err != nil {
			t.Fatalf("Next() on untampered frame failed: %v", err)
		}
		if frame == nil {
			t.Fatal("Next() returned nil frame for untampered data")
		}
		if frame.Meta == nil {
			t.Fatal("Meta is nil on untampered frame")
		}
		sourceVal := frame.Meta.Get("source")
		if sourceVal == nil || sourceVal.String() != "integrity-test" {
			t.Errorf("meta.source mismatch: got %v", sourceVal)
		}
		if frame.Payload == nil {
			t.Fatal("Payload is nil on untampered frame")
		}
	})

	// Flip a byte inside the metadata region of the frame.
	// Header is 24 bytes; the metadata bytes begin immediately after.
	// Flipping byte 25 (0-indexed: byte 24) corrupts the metadata bytes while
	// leaving the header intact so the reader can still parse lengths.
	t.Run("tampered_meta_region_returns_crc_error", func(t *testing.T) {
		corrupted := make([]byte, buf.Len())
		copy(corrupted, buf.Bytes())

		const metaByteOffset = 24 // first byte of metadata (after 24-byte header)
		if len(corrupted) <= metaByteOffset {
			t.Skip("frame too short to corrupt metadata region")
		}
		corrupted[metaByteOffset] ^= 0xFF

		reader := NewMasterReader(corrupted, DefaultMasterReaderOptions())
		frame, err := reader.Next()

		if err != ErrMasterCRCMismatch {
			t.Errorf("expected ErrMasterCRCMismatch, got: %v", err)
		}
		if frame != nil {
			t.Errorf("expected nil frame on CRC failure, got non-nil (Meta=%v, Payload=%v)",
				frame.Meta, frame.Payload)
		}
	})

	// Flip a byte in the payload region to confirm CRC also catches payload corruption.
	t.Run("tampered_payload_region_returns_crc_error", func(t *testing.T) {
		corrupted := make([]byte, buf.Len())
		copy(corrupted, buf.Bytes())

		// Flip the last byte before the 4-byte CRC trailer.
		lastPayloadByte := len(corrupted) - 5
		if lastPayloadByte < 24 {
			t.Skip("frame too short to corrupt payload region")
		}
		corrupted[lastPayloadByte] ^= 0x01

		reader := NewMasterReader(corrupted, DefaultMasterReaderOptions())
		frame, err := reader.Next()

		if err != ErrMasterCRCMismatch {
			t.Errorf("expected ErrMasterCRCMismatch, got: %v", err)
		}
		if frame != nil {
			t.Errorf("expected nil frame on CRC failure, got non-nil (Meta=%v, Payload=%v)",
				frame.Meta, frame.Payload)
		}
	})
}
