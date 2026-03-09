package cowrie

import (
	"errors"
	"math"
	"testing"
)

// Invariant #4: Trailing garbage after a valid root value must be rejected.
func TestDecode_RejectsTrailingGarbage(t *testing.T) {
	// Encode map{"a": 42}
	v := Object(Member{Key: "a", Value: Int64(42)})
	data, err := Encode(v)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Append 0xFF garbage byte
	corrupted := make([]byte, len(data)+1)
	copy(corrupted, data)
	corrupted[len(data)] = 0xFF

	// Decode must return an error
	_, err = Decode(corrupted)
	if err == nil {
		t.Fatal("expected error for trailing garbage, got nil")
	}
	if !errors.Is(err, ErrTrailingData) {
		t.Fatalf("expected ErrTrailingData, got: %v", err)
	}
}

// Invariant: NaN and Inf must roundtrip through cowrie binary encoding.
func TestBinaryNaNInfRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		val  float64
	}{
		{"NaN", math.NaN()},
		{"+Inf", math.Inf(1)},
		{"-Inf", math.Inf(-1)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := Float64(tc.val)
			data, err := Encode(v)
			if err != nil {
				t.Fatalf("Encode(%s) failed: %v", tc.name, err)
			}

			decoded, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode(%s) failed: %v", tc.name, err)
			}

			if decoded.Type() != TypeFloat64 {
				t.Fatalf("expected TypeFloat64, got %v", decoded.Type())
			}

			got := decoded.Float64()

			if math.IsNaN(tc.val) {
				if !math.IsNaN(got) {
					t.Fatalf("expected NaN, got %v", got)
				}
			} else if math.IsInf(tc.val, 1) {
				if !math.IsInf(got, 1) {
					t.Fatalf("expected +Inf, got %v", got)
				}
			} else if math.IsInf(tc.val, -1) {
				if !math.IsInf(got, -1) {
					t.Fatalf("expected -Inf, got %v", got)
				}
			}
		})
	}
}
