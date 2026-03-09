package codec

import (
	"bytes"
	"testing"
	"time"

	cowrie "github.com/Neumenon/cowrie/go"
)

// FuzzMasterStreamReader_Next fuzzes the master stream reader.
// Run with: go test -fuzz=FuzzMasterStreamReader_Next -fuzztime=30s
func FuzzMasterStreamReader_Next(f *testing.F) {
	// Seed with various inputs
	seeds := [][]byte{
		// Valid master stream prefix
		[]byte("SJST\x02\x00\x18\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
		// Valid Cowrie document prefix
		[]byte{'S', 'J', 0x02, 0x00},
		// Truncated header
		[]byte("SJST"),
		// Empty
		[]byte{},
		// Random bytes
		[]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05},
		// Large length values (potential bombs)
		[]byte("SJST\x02\x00\x18\x00\x00\x00\x00\x00\x00\x00\xFF\xFF\xFF\xFF\x00\x00\x00\x00\x00\x00\x00\x00"),
		// Invalid compression type
		[]byte("SJST\x02\x01\x18\x00\x00\x00\x00\x00\xFF\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		mr := NewMasterReader(data, MasterReaderOptions{
			MaxDecompressedSize: 1 << 20, // 1MB cap
			AllowLegacy:         true,
		})

		// Try reading up to 10 frames to catch infinite loops
		for i := 0; i < 10; i++ {
			frame, err := mr.Next()
			if err != nil {
				// Error is expected for malformed input
				break
			}
			// If we got a frame, basic sanity check
			if frame != nil {
				_ = frame.TypeID
				_ = frame.Payload
				_ = frame.Meta
			}
		}
	})
}

// FuzzDecodeBytes fuzzes the generic decode path.
// Run with: go test -fuzz=FuzzDecodeBytes -fuzztime=30s
func FuzzDecodeBytes(f *testing.F) {
	seeds := [][]byte{
		// Valid Cowrie null
		{'S', 'J', 0x02, 0x00, 0x00, 0x00},
		// Valid Cowrie true
		{'S', 'J', 0x02, 0x00, 0x00, 0x02},
		// Valid Cowrie int
		{'S', 'J', 0x02, 0x00, 0x00, 0x03, 0x2A},
		// Valid Cowrie string "hi"
		{'S', 'J', 0x02, 0x00, 0x01, 0x02, 'h', 'i', 0x05, 0x00},
		// Truncated
		{'S', 'J', 0x02},
		// Wrong magic
		{'X', 'X', 0x02, 0x00},
		// Empty
		{},
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		var decoded any
		// Should never panic
		_ = DecodeBytes(data, &decoded)
	})
}

// FuzzFastEncode fuzzes the fast encoder with various Go values.
// Run with: go test -fuzz=FuzzFastEncode -fuzztime=30s
func FuzzFastEncode(f *testing.F) {
	// Seed with string inputs that become various structures
	f.Add("simple string")
	f.Add("")
	f.Add("unicode: 你好")
	f.Add("with\nnewlines\tand\ttabs")
	f.Add("null byte: \x00")

	f.Fuzz(func(t *testing.T, s string) {
		// Test encoding various structures containing the fuzzed string
		testCases := []any{
			s,
			map[string]any{"key": s},
			[]any{s, s, s},
			struct {
				Name string `json:"name"`
			}{Name: s},
		}

		for _, tc := range testCases {
			encoded, err := FastEncode(tc)
			if err != nil {
				// Encoding error is acceptable for some inputs
				continue
			}

			// If encoding succeeded, decoding should also succeed
			var decoded any
			if err := DecodeBytes(encoded, &decoded); err != nil {
				t.Errorf("encode succeeded but decode failed: %v", err)
			}
		}
	})
}

// FuzzLegacyStreamFrame fuzzes legacy length-prefixed frames.
// Run with: go test -fuzz=FuzzLegacyStreamFrame -fuzztime=30s
func FuzzLegacyStreamFrame(f *testing.F) {
	seeds := [][]byte{
		// Valid frame: length=4, payload=SJSJ
		{0x00, 0x00, 0x00, 0x04, 'S', 'J', 0x02, 0x00},
		// Zero length
		{0x00, 0x00, 0x00, 0x00},
		// Huge length (bomb attempt)
		{0xFF, 0xFF, 0xFF, 0xFF},
		// Truncated length
		{0x00, 0x00},
		// Empty
		{},
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		mr := NewMasterReader(data, MasterReaderOptions{
			AllowLegacy:         true,
			MaxDecompressedSize: 1 << 20,
		})

		for i := 0; i < 5; i++ {
			_, err := mr.Next()
			if err != nil {
				break
			}
		}
	})
}

// FuzzTensorDecode fuzzes tensor decoding.
// Run with: go test -fuzz=FuzzTensorDecode -fuzztime=30s
func FuzzTensorDecode(f *testing.F) {
	// Seed with various tensor-like data
	seeds := [][]byte{
		// Minimal tensor header
		{0x20, 0x01, 0x01, 0x04, 0x00, 0x00, 0x00, 0x80, 0x3F},
		// Empty tensor
		{0x20, 0x01, 0x00},
		// Large dimensions
		{0x20, 0x01, 0xFF, 0xFF, 0xFF, 0xFF},
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Wrap fuzzed data as a cowrie document and try to decode
		var decoded any
		err := DecodeBytes(data, &decoded)
		if err != nil {
			return // errors expected for malformed input
		}

		// If decode succeeded with a Value, try tensor extraction (should not panic)
		val, ok := decoded.(*cowrie.Value)
		if !ok {
			return
		}
		DecodeFloat32Tensor(val)
	})
}

// TestFuzz_NoPanicOnTruncated ensures truncated inputs don't panic.
func TestFuzz_NoPanicOnTruncated(t *testing.T) {
	// Generate truncated versions of valid data
	validFrame := func() []byte {
		var buf bytes.Buffer
		mw := NewMasterWriter(&buf, MasterWriterOptions{})
		mw.Write(MustValueFromAny(map[string]any{"test": int64(1)}))
		return buf.Bytes()
	}()

	for i := 0; i < len(validFrame); i++ {
		truncated := validFrame[:i]
		t.Run("truncated_at_"+string(rune('0'+i%10)), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic on truncated input at %d: %v", i, r)
				}
			}()

			mr := NewMasterReader(truncated, MasterReaderOptions{})
			mr.Next()
		})
	}
}

// TestFuzz_NoPanicOnCorrupted ensures corrupted inputs don't panic.
func TestFuzz_NoPanicOnCorrupted(t *testing.T) {
	validFrame := func() []byte {
		var buf bytes.Buffer
		mw := NewMasterWriter(&buf, MasterWriterOptions{EnableCRC: true})
		mw.Write(MustValueFromAny(map[string]any{"test": int64(1)}))
		return buf.Bytes()
	}()

	for i := 0; i < len(validFrame); i++ {
		corrupted := corruptByteAt(validFrame, i)
		t.Run("corrupted_at_"+string(rune('0'+i%10)), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic on corrupted input at %d: %v", i, r)
				}
			}()

			mr := NewMasterReader(corrupted, MasterReaderOptions{})
			mr.Next()
		})
	}
}

// TestFuzz_NoInfiniteLoop ensures reader doesn't hang on pathological input.
func TestFuzz_NoInfiniteLoop(t *testing.T) {
	// Inputs that might cause infinite loops
	inputs := [][]byte{
		// Self-referential length (length byte points to itself)
		{0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04},
		// Zero-length frames repeated
		makeRepeatedBytes([]byte{0x00, 0x00, 0x00, 0x00}, 100),
		// All zeros
		make([]byte, 1000),
		// All 0xFF
		func() []byte {
			b := make([]byte, 1000)
			for i := range b {
				b[i] = 0xFF
			}
			return b
		}(),
	}

	for i, input := range inputs {
		t.Run("input_"+string(rune('0'+i)), func(t *testing.T) {
			done := make(chan struct{})
			go func() {
				defer close(done)
				mr := NewMasterReader(input, MasterReaderOptions{
					AllowLegacy:         true,
					MaxDecompressedSize: 1 << 20,
				})
				for j := 0; j < 100; j++ {
					_, err := mr.Next()
					if err != nil {
						break
					}
				}
			}()

			select {
			case <-done:
				// Good - completed
			case <-time.After(1 * time.Second):
				t.Error("reader appears to be stuck in infinite loop")
			}
		})
	}
}

