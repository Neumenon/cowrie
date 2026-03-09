package gen1

import (
	"encoding/json"
	"testing"
)

func TestDecodeWithOptions_Coverage(t *testing.T) {
	// Encode something, then decode with custom options
	data, err := Encode(map[string]any{
		"name": "test",
		"arr":  []any{1, 2, 3},
		"nested": map[string]any{
			"x": 42,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	opts := DecodeOptions{
		MaxDepth:     50,
		MaxArrayLen:  1000,
		MaxObjectLen: 1000,
		MaxStringLen: 10000,
		MaxBytesLen:  10000,
	}

	result, err := DecodeWithOptions(data, opts)
	if err != nil {
		t.Fatalf("DecodeWithOptions: %v", err)
	}

	m, ok := result.(map[string]any)
	if !ok {
		t.Fatal("expected map")
	}
	if m["name"] != "test" {
		t.Error("name mismatch")
	}
}

func TestDecodeWithOptions_ZeroOpts(t *testing.T) {
	data, err := Encode("hello")
	if err != nil {
		t.Fatal(err)
	}

	// Zero opts should use defaults
	result, err := DecodeWithOptions(data, DecodeOptions{})
	if err != nil {
		t.Fatalf("DecodeWithOptions zero: %v", err)
	}
	if result != "hello" {
		t.Error("value mismatch")
	}
}

func TestEncodeAppendValue_AllTypes(t *testing.T) {
	tests := []struct {
		name string
		v    any
	}{
		{"nil", nil},
		{"bool_true", true},
		{"bool_false", false},
		{"float64", float64(3.14)},
		{"float32", float32(2.5)},
		{"json_number_int", json.Number("42")},
		{"json_number_float", json.Number("3.14")},
		{"int", int(42)},
		{"int8", int8(42)},
		{"int16", int16(42)},
		{"int32", int32(42)},
		{"int64", int64(42)},
		{"uint8", uint8(42)},
		{"uint16", uint16(42)},
		{"uint32", uint32(42)},
		{"uint64", uint64(42)},
		{"string", "hello"},
		{"bytes", []byte{1, 2, 3}},
		{"map", map[string]any{"x": 1}},
		{"array_int", []any{1, 2, 3}},
		{"array_float", []any{1.1, 2.2, 3.3}},
		{"array_mixed", []any{1, "two", true}},
		{"array_string", []any{"a", "b", "c"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := Encode(tt.v)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}

			result, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			_ = result
		})
	}
}

func TestCompactFloats_Coverage(t *testing.T) {
	opts := EncodeOptions{CompactFloats: true}

	// Float that can compact
	data, err := EncodeWithOptions(float64(1.5), opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	// Result should be float64 or float32
	switch v := result.(type) {
	case float64:
		if v != 1.5 {
			t.Errorf("expected 1.5, got %v", v)
		}
	case float32:
		if v != 1.5 {
			t.Errorf("expected 1.5, got %v", v)
		}
	default:
		t.Errorf("unexpected type %T", result)
	}
}

func TestEncodeHighPrecision_Coverage(t *testing.T) {
	opts := EncodeOptions{HighPrecision: true}

	data, err := EncodeWithOptions(float64(3.141592653589793), opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := result.(float64)
	if !ok {
		t.Fatal("expected float64")
	}
	if f != 3.141592653589793 {
		t.Error("precision loss")
	}
}

func TestEncodeAppendValue_Float32(t *testing.T) {
	// Test float32 encoding
	data, err := Encode(float32(2.5))
	if err != nil {
		t.Fatal(err)
	}
	result, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	_ = result
}

func TestEncodeDecodeJSON_Complex(t *testing.T) {
	input := `{"users":[{"name":"Alice","age":30},{"name":"Bob","age":25}],"count":2}`

	encoded, err := EncodeJSON([]byte(input))
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}

	// Verify JSON is valid
	var result map[string]any
	if err := json.Unmarshal(decoded, &result); err != nil {
		t.Fatal(err)
	}
	if result["count"] == nil {
		t.Error("missing count")
	}
}

func TestEncodeDecodeRoundTrip_Deep(t *testing.T) {
	// Deep nesting
	inner := map[string]any{"leaf": "value"}
	for i := 0; i < 5; i++ {
		inner = map[string]any{"level": inner}
	}

	data, err := Encode(inner)
	if err != nil {
		t.Fatal(err)
	}

	result, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}

	m, ok := result.(map[string]any)
	if !ok {
		t.Fatal("expected map")
	}
	_ = m
}
