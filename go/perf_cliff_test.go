package cowrie

import (
	"testing"
	"time"
)

// Suite 7: Performance Cliff Detection
// Time-bounded tests that fail if execution exceeds O(n) expectations.
// These detect quadratic/exponential blowups from pathological inputs.

func TestPerfCliff_DeepNesting(t *testing.T) {
	depths := []int{50, 100, 500}
	var lastDuration time.Duration

	for _, depth := range depths {
		t.Run("", func(t *testing.T) {
			// Build deeply nested array: [[[[...]]]]
			v := Null()
			for i := 0; i < depth; i++ {
				v = Array(v)
			}

			start := time.Now()
			data, err := Encode(v)
			if err != nil {
				t.Fatalf("Encode depth=%d failed: %v", depth, err)
			}

			_, err = Decode(data)
			if err != nil {
				t.Fatalf("Decode depth=%d failed: %v", depth, err)
			}
			dur := time.Since(start)

			// Check for quadratic blowup: if depth doubles, time should
			// roughly double (O(n)), not quadruple (O(n²)).
			if lastDuration > 0 && dur > lastDuration*20 {
				t.Errorf("possible quadratic blowup: depth=%d took %v, previous took %v (ratio %.1fx)",
					depth, dur, lastDuration, float64(dur)/float64(lastDuration))
			}

			lastDuration = dur
		})
	}
}

func TestPerfCliff_WideObject(t *testing.T) {
	sizes := []int{1000, 10000}

	for _, size := range sizes {
		t.Run("", func(t *testing.T) {
			members := make([]Member, size)
			for i := 0; i < size; i++ {
				members[i] = Member{
					Key:   string(rune('a' + (i % 26))),
					Value: Int64(int64(i)),
				}
			}
			v := Object(members...)

			start := time.Now()
			data, err := Encode(v)
			if err != nil {
				t.Fatalf("Encode size=%d failed: %v", size, err)
			}

			_, err = Decode(data)
			if err != nil {
				t.Fatalf("Decode size=%d failed: %v", size, err)
			}
			dur := time.Since(start)

			// 10K keys should complete well under 5 seconds
			if dur > 5*time.Second {
				t.Errorf("wide object size=%d took %v, exceeds 5s threshold", size, dur)
			}
		})
	}
}

func TestPerfCliff_WideArray(t *testing.T) {
	sizes := []int{1000, 10000, 100000}
	var lastDuration time.Duration

	for _, size := range sizes {
		t.Run("", func(t *testing.T) {
			items := make([]*Value, size)
			for i := 0; i < size; i++ {
				items[i] = Int64(int64(i))
			}
			v := Array(items...)

			start := time.Now()
			data, err := Encode(v)
			if err != nil {
				t.Fatalf("Encode size=%d failed: %v", size, err)
			}

			_, err = Decode(data)
			if err != nil {
				t.Fatalf("Decode size=%d failed: %v", size, err)
			}
			dur := time.Since(start)

			if lastDuration > 0 && dur > lastDuration*20 {
				t.Errorf("possible quadratic blowup: size=%d took %v, previous took %v (ratio %.1fx)",
					size, dur, lastDuration, float64(dur)/float64(lastDuration))
			}

			lastDuration = dur
		})
	}
}

func TestPerfCliff_ManyEmptyStrings(t *testing.T) {
	count := 100000
	items := make([]*Value, count)
	for i := 0; i < count; i++ {
		items[i] = String("")
	}
	v := Array(items...)

	start := time.Now()
	data, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	_, err = Decode(data)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	dur := time.Since(start)

	if dur > 5*time.Second {
		t.Errorf("100K empty strings took %v, exceeds 5s threshold", dur)
	}
}
