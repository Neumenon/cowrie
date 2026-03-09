package cowrie

import (
	"testing"
)

func TestEncodeDecodeAny_Coverage(t *testing.T) {
	// Test encoding/decoding various any types
	tests := []struct {
		name string
		v    any
	}{
		{"nil", nil},
		{"bool_true", true},
		{"bool_false", false},
		{"int_fixint", 42},
		{"int_negative", -5},
		{"int_large", 1000},
		{"int64_small", int64(10)},
		{"int64_neg", int64(-10)},
		{"int64_large", int64(99999)},
		{"uint", uint(42)},
		{"uint64", uint64(42)},
		{"float64", float64(3.14)},
		{"string", "hello world"},
		{"bytes", []byte{1, 2, 3, 4}},
		{"array_small", []any{1, 2, 3}},
		{"array_with_mixed", []any{1, "two", true, nil}},
		{"map_simple", map[string]any{"x": 1, "y": "z"}},
		{"map_nested", map[string]any{"outer": map[string]any{"inner": 42}}},
		{"map_with_array", map[string]any{"arr": []any{1, 2, 3}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := FromAny(tt.v)
			data, err := Encode(v)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			decoded, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if decoded == nil && tt.v != nil {
				t.Error("decoded nil for non-nil input")
			}
		})
	}
}

func TestEncodeAppend_Coverage(t *testing.T) {
	v := Object(
		Member{Key: "x", Value: Int64(42)},
	)

	// Encode with pre-allocated buffer
	buf := make([]byte, 0, 256)
	result, err := EncodeAppend(buf, v)
	if err != nil {
		t.Fatalf("EncodeAppend: %v", err)
	}
	if len(result) == 0 {
		t.Error("empty result")
	}

	// Decode to verify
	decoded, err := Decode(result)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.Get("x").Int64() != 42 {
		t.Error("value mismatch")
	}
}
