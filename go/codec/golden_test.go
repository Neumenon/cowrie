package codec

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Neumenon/cowrie"
)

// TestMasterFrame_GoldenGenerate generates golden fixtures if they don't exist.
// Run with: go test -run TestMasterFrame_GoldenGenerate -v
func TestMasterFrame_GoldenGenerate(t *testing.T) {
	if os.Getenv("GENERATE_GOLDEN") != "1" {
		t.Skip("Set GENERATE_GOLDEN=1 to regenerate fixtures")
	}

	// Ensure testdata directory exists
	os.MkdirAll("testdata", 0755)

	// Test payload
	payload := MustValueFromAny(map[string]any{
		"id":    int64(42),
		"name":  "test",
		"score": float64(3.14),
	})

	meta := MustValueFromAny(map[string]any{
		"version": int64(1),
		"source":  "golden_test",
	})

	// 1) Uncompressed frame
	{
		var buf bytes.Buffer
		mw := NewMasterWriter(&buf, MasterWriterOptions{
			Deterministic: true,
		})
		if err := mw.Write(payload); err != nil {
			t.Fatal(err)
		}
		writeGoldenFixture(t, "frame_v1_uncompressed.bin", buf.Bytes())
		t.Logf("Generated frame_v1_uncompressed.bin: %d bytes", buf.Len())
	}

	// 2) Gzip compressed frame
	{
		var buf bytes.Buffer
		mw := NewMasterWriter(&buf, MasterWriterOptions{
			Compression:   CompressionGzip,
			Deterministic: true,
		})
		if err := mw.Write(payload); err != nil {
			t.Fatal(err)
		}
		writeGoldenFixture(t, "frame_v1_gzip.bin", buf.Bytes())
		t.Logf("Generated frame_v1_gzip.bin: %d bytes", buf.Len())
	}

	// 3) Zstd compressed frame
	{
		var buf bytes.Buffer
		mw := NewMasterWriter(&buf, MasterWriterOptions{
			Compression:   CompressionZstd,
			Deterministic: true,
		})
		if err := mw.Write(payload); err != nil {
			t.Fatal(err)
		}
		writeGoldenFixture(t, "frame_v1_zstd.bin", buf.Bytes())
		t.Logf("Generated frame_v1_zstd.bin: %d bytes", buf.Len())
	}

	// 4) Frame with metadata
	{
		var buf bytes.Buffer
		mw := NewMasterWriter(&buf, MasterWriterOptions{
			Deterministic: true,
		})
		if err := mw.WriteWithMeta(payload, meta); err != nil {
			t.Fatal(err)
		}
		writeGoldenFixture(t, "frame_v1_with_meta.bin", buf.Bytes())
		t.Logf("Generated frame_v1_with_meta.bin: %d bytes", buf.Len())
	}

	// 5) Frame with CRC enabled
	{
		var buf bytes.Buffer
		mw := NewMasterWriter(&buf, MasterWriterOptions{
			EnableCRC:     true,
			Deterministic: true,
		})
		if err := mw.Write(payload); err != nil {
			t.Fatal(err)
		}
		writeGoldenFixture(t, "frame_v1_with_crc.bin", buf.Bytes())
		t.Logf("Generated frame_v1_with_crc.bin: %d bytes", buf.Len())
	}

	// 6) Generate bad CRC frame (corrupt the CRC bytes)
	{
		var buf bytes.Buffer
		mw := NewMasterWriter(&buf, MasterWriterOptions{
			EnableCRC:     true,
			Deterministic: true,
		})
		if err := mw.Write(payload); err != nil {
			t.Fatal(err)
		}
		data := buf.Bytes()
		// Corrupt last 4 bytes (CRC)
		data[len(data)-1] ^= 0xFF
		data[len(data)-2] ^= 0xFF
		writeGoldenFixture(t, "frame_v1_crc_bad.bin", data)
		t.Logf("Generated frame_v1_crc_bad.bin: %d bytes", len(data))
	}

	// 7) Truncated frame (header only, no payload)
	{
		var buf bytes.Buffer
		mw := NewMasterWriter(&buf, MasterWriterOptions{
			Deterministic: true,
		})
		if err := mw.Write(payload); err != nil {
			t.Fatal(err)
		}
		data := buf.Bytes()
		truncated := data[:24] // Just the header
		writeGoldenFixture(t, "frame_v1_truncated.bin", truncated)
		t.Logf("Generated frame_v1_truncated.bin: %d bytes", len(truncated))
	}
}

// TestMasterFrame_GoldenDecode tests decoding of golden fixtures.
func TestMasterFrame_GoldenDecode(t *testing.T) {
	tests := []struct {
		name      string
		fixture   string
		wantTypeID uint32
		wantMeta  bool
		wantError bool
	}{
		{"uncompressed", "frame_v1_uncompressed.bin", 123, false, false},
		{"gzip", "frame_v1_gzip.bin", 123, false, false},
		{"zstd", "frame_v1_zstd.bin", 123, false, false},
		{"with_meta", "frame_v1_with_meta.bin", 456, true, false},
		{"with_crc", "frame_v1_with_crc.bin", 789, false, false},
		{"bad_crc", "frame_v1_crc_bad.bin", 0, false, true},
		{"truncated", "frame_v1_truncated.bin", 0, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join("testdata", tt.fixture)
			if _, err := os.Stat(path); os.IsNotExist(err) {
				t.Skipf("Fixture %s not found (run with GENERATE_GOLDEN=1 to create)", tt.fixture)
			}

			data := readGoldenFixture(t, tt.fixture)
			mr := NewMasterReader(data, MasterReaderOptions{})

			frame, err := mr.Next()
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if frame.TypeID != tt.wantTypeID {
				t.Errorf("TypeID = %d, want %d", frame.TypeID, tt.wantTypeID)
			}

			if tt.wantMeta && frame.Meta == nil {
				t.Error("expected meta, got nil")
			}

			if !tt.wantMeta && frame.Meta != nil {
				t.Error("expected no meta, got some")
			}

			if frame.Payload == nil {
				t.Error("payload is nil")
			}
		})
	}
}

// TestMasterFrame_GoldenEncode verifies encoding produces exact bytes.
func TestMasterFrame_GoldenEncode(t *testing.T) {
	payload := MustValueFromAny(map[string]any{
		"id":    int64(42),
		"name":  "test",
		"score": float64(3.14),
	})

	tests := []struct {
		name    string
		fixture string
		opts    MasterWriterOptions
		typeID  uint32
		meta    *cowrie.Value
	}{
		{
			name:    "uncompressed",
			fixture: "frame_v1_uncompressed.bin",
			opts:    MasterWriterOptions{Deterministic: true},
			typeID:  123,
			meta:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join("testdata", tt.fixture)
			if _, err := os.Stat(path); os.IsNotExist(err) {
				t.Skipf("Fixture %s not found", tt.fixture)
			}

			golden := readGoldenFixture(t, tt.fixture)

			var buf bytes.Buffer
			mw := NewMasterWriter(&buf, tt.opts)
			if err := mw.WriteWithMeta(payload, tt.meta); err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(buf.Bytes(), golden) {
				t.Errorf("encoded bytes differ from golden fixture")
				t.Logf("got:    %x", buf.Bytes()[:min(50, buf.Len())])
				t.Logf("golden: %x", golden[:min(50, len(golden))])
			}
		})
	}
}

// TestMasterFrame_VersionStability ensures version byte is stable.
func TestMasterFrame_VersionStability(t *testing.T) {
	var buf bytes.Buffer
	mw := NewMasterWriter(&buf, MasterWriterOptions{})
	if err := mw.Write(cowrie.Int64(42)); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()

	// Check magic
	if string(data[0:4]) != "SJST" {
		t.Errorf("magic = %q, want SJST", string(data[0:4]))
	}

	// Check version
	if data[4] != 0x02 {
		t.Errorf("version = %#x, want 0x02", data[4])
	}
}

// TestLegacyCowrie_GoldenRoundTrip ensures legacy Cowrie documents are stable.
// Note: Large integers may be decoded as strings for precision, affecting byte stability.
func TestLegacyCowrie_GoldenRoundTrip(t *testing.T) {
	testCases := []struct {
		name           string
		value          any
		byteStable     bool // whether encode→decode→encode should produce identical bytes
	}{
		{"null", nil, true},
		{"bool_true", true, true},
		{"bool_false", false, true},
		{"int_positive", int64(42), true},
		{"int_negative", int64(-42), true},
		{"int_max", int64(9223372036854775807), false}, // May become string
		{"uint_max", uint64(18446744073709551615), false}, // May become string
		{"float_pi", float64(3.141592653589793), true},
		{"string_simple", "hello, world!", true},
		{"string_unicode", "你好世界 🌍", true},
		{"array_empty", []any{}, true},
		{"array_mixed", []any{int64(1), "two", true, nil}, true},
		{"object_empty", map[string]any{}, true},
		{"object_nested", map[string]any{
			"a": int64(1),
			"b": map[string]any{"c": int64(2)},
		}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Encode
			encoded, err := EncodeBytes(tc.value)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			// Verify Cowrie magic
			if len(encoded) < 4 {
				t.Fatal("encoded too short")
			}
			if encoded[0] != 'S' || encoded[1] != 'J' {
				t.Errorf("missing Cowrie magic: got %q", encoded[0:2])
			}

			// Decode and verify round-trip
			var decoded any
			if err := DecodeBytes(encoded, &decoded); err != nil {
				t.Fatalf("decode error: %v", err)
			}

			// Re-encode to verify stability
			reencoded, err := EncodeBytes(decoded)
			if err != nil {
				t.Fatalf("re-encode error: %v", err)
			}

			if tc.byteStable {
				if !bytes.Equal(encoded, reencoded) {
					t.Errorf("re-encoded bytes differ (expected byte stability)")
				}
			} else {
				// For non-byte-stable values, just verify the decode succeeded
				t.Logf("Note: Large integer may have changed representation (encoded %d bytes → %d bytes)", len(encoded), len(reencoded))
			}
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestMasterStream_MultipleFrames tests reading multiple frames in sequence.
func TestMasterStream_MultipleFrames(t *testing.T) {
	var buf bytes.Buffer
	mw := NewMasterWriter(&buf, MasterWriterOptions{Deterministic: true})

	// Write 10 frames with same schema (TypeID is schema fingerprint, not frame index)
	for i := 0; i < 10; i++ {
		payload := MustValueFromAny(map[string]any{"index": int64(i)})
		if err := mw.Write(payload); err != nil {
			t.Fatalf("write frame %d: %v", i, err)
		}
	}

	// Read back
	mr := NewMasterReader(buf.Bytes(), MasterReaderOptions{})
	count := 0
	var firstTypeID uint32
	for {
		frame, err := mr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read frame %d: %v", count, err)
		}

		// All frames have the same schema, so TypeID should be consistent
		if count == 0 {
			firstTypeID = frame.TypeID
			t.Logf("Schema TypeID: %#x", firstTypeID)
		} else if frame.TypeID != firstTypeID {
			t.Errorf("frame %d: TypeID = %#x, want %#x (same schema)", count, frame.TypeID, firstTypeID)
		}

		// Verify payload was decoded
		if frame.Payload == nil {
			t.Errorf("frame %d: payload is nil", count)
		}
		count++
	}

	if count != 10 {
		t.Errorf("read %d frames, want 10", count)
	}
}
