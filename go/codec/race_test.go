package codec

import (
	"bytes"
	"sync"
	"testing"

	"github.com/Neumenon/cowrie"
)

// Value is an alias for cowrie.Value
type Value = cowrie.Value

// TestRace_ParallelEncode tests concurrent encoding doesn't race.
// Run with: go test -race -run TestRace_ParallelEncode
func TestRace_ParallelEncode(t *testing.T) {
	type TestStruct struct {
		ID    int64   `json:"id"`
		Name  string  `json:"name"`
		Score float64 `json:"score"`
	}

	numGoroutines := 100
	iterations := 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*iterations)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				data := TestStruct{
					ID:    int64(id*1000 + i),
					Name:  "test_" + string(rune('0'+id%10)),
					Score: float64(i) * 0.01,
				}

				encoded, err := FastEncode(data)
				if err != nil {
					errors <- err
					return
				}

				// Also verify it can be decoded
				var decoded TestStruct
				if err := DecodeBytes(encoded, &decoded); err != nil {
					errors <- err
					return
				}

				if decoded.ID != data.ID {
					t.Errorf("goroutine %d iter %d: ID mismatch", id, i)
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("error during parallel encode: %v", err)
	}
}

// TestRace_ParallelDecode tests concurrent decoding doesn't race.
// Run with: go test -race -run TestRace_ParallelDecode
func TestRace_ParallelDecode(t *testing.T) {
	type TestStruct struct {
		ID    int64   `json:"id"`
		Name  string  `json:"name"`
		Score float64 `json:"score"`
	}

	// Pre-encode test data
	original := TestStruct{ID: 42, Name: "test", Score: 3.14}
	encoded, err := FastEncode(original)
	if err != nil {
		t.Fatalf("setup encode failed: %v", err)
	}

	numGoroutines := 100
	iterations := 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*iterations)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				var decoded TestStruct
				if err := DecodeBytes(encoded, &decoded); err != nil {
					errors <- err
					return
				}

				if decoded.ID != original.ID {
					t.Errorf("goroutine %d iter %d: ID mismatch", id, i)
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("error during parallel decode: %v", err)
	}
}

// TestRace_ParallelMasterStreamWrite tests concurrent master stream writing.
// Run with: go test -race -run TestRace_ParallelMasterStreamWrite
func TestRace_ParallelMasterStreamWrite(t *testing.T) {
	numGoroutines := 50
	framesPerGoroutine := 50

	var wg sync.WaitGroup
	results := make(chan []byte, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			var buf bytes.Buffer
			mw := NewMasterWriter(&buf, MasterWriterOptions{
				Deterministic: true,
			})

			for i := 0; i < framesPerGoroutine; i++ {
				payload := MustValueFromAny(map[string]any{
					"goroutine": int64(id),
					"frame":     int64(i),
				})
				if err := mw.Write(payload); err != nil {
					t.Errorf("goroutine %d frame %d: write error: %v", id, i, err)
					return
				}
			}

			results <- buf.Bytes()
		}(g)
	}

	wg.Wait()
	close(results)

	// Verify each result is readable
	count := 0
	for data := range results {
		mr := NewMasterReader(data, MasterReaderOptions{})
		frameCount := 0
		for {
			_, err := mr.Next()
			if err != nil {
				break
			}
			frameCount++
		}
		if frameCount != framesPerGoroutine {
			t.Errorf("result %d: got %d frames, want %d", count, frameCount, framesPerGoroutine)
		}
		count++
	}
}

// TestRace_ParallelMasterStreamRead tests concurrent master stream reading.
// Run with: go test -race -run TestRace_ParallelMasterStreamRead
func TestRace_ParallelMasterStreamRead(t *testing.T) {
	// Build test data
	var buf bytes.Buffer
	mw := NewMasterWriter(&buf, MasterWriterOptions{})
	for i := 0; i < 100; i++ {
		payload := MustValueFromAny(map[string]any{"index": int64(i)})
		mw.Write(payload)
	}
	testData := buf.Bytes()

	numGoroutines := 50

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Each goroutine reads its own copy
			mr := NewMasterReader(testData, MasterReaderOptions{})
			count := 0
			var firstTypeID uint32
			for {
				frame, err := mr.Next()
				if err != nil {
					break
				}
				// All frames have the same schema, so TypeID should be consistent
				if count == 0 {
					firstTypeID = frame.TypeID
				} else if frame.TypeID != firstTypeID {
					t.Errorf("goroutine %d: frame %d TypeID = %#x, want %#x (same schema)",
						id, count, frame.TypeID, firstTypeID)
				}
				// Verify payload exists
				if frame.Payload == nil {
					t.Errorf("goroutine %d: frame %d has nil payload", id, count)
				}
				count++
			}
			if count != 100 {
				t.Errorf("goroutine %d: read %d frames, want 100", id, count)
			}
		}(g)
	}

	wg.Wait()
}

// TestRace_ParallelRegistration tests concurrent type registration.
// Run with: go test -race -run TestRace_ParallelRegistration
func TestRace_ParallelRegistration(t *testing.T) {
	numGoroutines := 20
	iterations := 10

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				// Register and unregister a type
				type DynamicType struct {
					Value int64 `json:"value"`
				}

				Register[DynamicType](func(v *Value, ptr *DynamicType) error {
					ptr.Value = int64(id*1000 + i)
					return nil
				})

				// Check registration
				_ = IsRegistered[DynamicType]()

				// Unregister
				Unregister[DynamicType]()
			}
		}(g)
	}

	wg.Wait()
}

// TestRace_EncoderCacheAccess tests concurrent encoder cache access.
// Run with: go test -race -run TestRace_EncoderCacheAccess
func TestRace_EncoderCacheAccess(t *testing.T) {
	// Define multiple struct types to stress the cache
	type Type1 struct{ A int64 }
	type Type2 struct{ B string }
	type Type3 struct{ C float64 }
	type Type4 struct{ D bool }

	numGoroutines := 100
	iterations := 50

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				switch id % 4 {
				case 0:
					FastEncode(Type1{A: int64(i)})
				case 1:
					FastEncode(Type2{B: "test"})
				case 2:
					FastEncode(Type3{C: float64(i)})
				case 3:
					FastEncode(Type4{D: i%2 == 0})
				}
			}
		}(g)
	}

	wg.Wait()
}

// TestRace_TensorEncoding tests concurrent tensor encoding.
// Run with: go test -race -run TestRace_TensorEncoding
func TestRace_TensorEncoding(t *testing.T) {
	numGoroutines := 50
	iterations := 100

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				// Create unique data for each iteration
				f32 := make([]float32, 100)
				f64 := make([]float64, 100)
				for j := range f32 {
					f32[j] = float32(id*1000 + i + j)
					f64[j] = float64(id*1000 + i + j)
				}

				// Encode
				t32 := EncodeFloat32Tensor(f32)
				t64 := EncodeFloat64Tensor(f64)

				// Decode
				d32 := DecodeFloat32Tensor(t32)
				d64 := DecodeFloat64Tensor(t64)

				// Verify
				if len(d32) != len(f32) {
					t.Errorf("goroutine %d iter %d: f32 length mismatch", id, i)
				}
				if len(d64) != len(f64) {
					t.Errorf("goroutine %d iter %d: f64 length mismatch", id, i)
				}
			}
		}(g)
	}

	wg.Wait()
}

// TestDeterminism_ConcurrentEncode verifies deterministic encoding under concurrency.
// Run with: go test -race -run TestDeterminism_ConcurrentEncode
func TestDeterminism_ConcurrentEncode(t *testing.T) {
	type TestStruct struct {
		A string  `json:"a"`
		B int64   `json:"b"`
		C float64 `json:"c"`
	}

	data := TestStruct{A: "test", B: 42, C: 3.14}

	// Get reference encoding
	reference, err := FastEncode(data)
	if err != nil {
		t.Fatalf("reference encode failed: %v", err)
	}

	numGoroutines := 100
	iterations := 50

	var wg sync.WaitGroup
	mismatches := make(chan int, numGoroutines*iterations)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				encoded, err := FastEncode(data)
				if err != nil {
					t.Errorf("goroutine %d iter %d: encode error: %v", id, i, err)
					return
				}

				if !bytes.Equal(encoded, reference) {
					mismatches <- id*iterations + i
				}
			}
		}(g)
	}

	wg.Wait()
	close(mismatches)

	mismatchCount := 0
	for range mismatches {
		mismatchCount++
	}

	if mismatchCount > 0 {
		t.Errorf("%d encodings differed from reference (non-deterministic)", mismatchCount)
	}
}

