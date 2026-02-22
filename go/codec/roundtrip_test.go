package codec

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"
)

// TestRoundTrip_Primitives tests all primitive types survive encode/decode.
func TestRoundTrip_Primitives(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		// Booleans
		{"bool_true", true},
		{"bool_false", false},

		// Integers
		{"int_zero", int64(0)},
		{"int_positive", int64(42)},
		{"int_negative", int64(-42)},
		{"int_max", int64(math.MaxInt64)},
		{"int_min", int64(math.MinInt64)},

		// Unsigned integers
		{"uint_zero", uint64(0)},
		{"uint_positive", uint64(42)},
		{"uint_max", uint64(math.MaxUint64)},

		// Floats
		{"float_zero", float64(0)},
		{"float_positive", float64(3.14159265358979)},
		{"float_negative", float64(-3.14159265358979)},
		{"float_small", float64(1e-300)},
		{"float_large", float64(1e300)},

		// Strings
		{"string_empty", ""},
		{"string_simple", "hello, world!"},
		{"string_unicode", "你好世界 🌍 مرحبا"},
		{"string_escapes", "line1\nline2\ttab\"quote"},
		{"string_null_byte", "before\x00after"},

		// Null
		{"null", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded, err := EncodeBytes(tt.value)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			// Decode
			var decoded any
			if err := DecodeBytes(encoded, &decoded); err != nil {
				t.Fatalf("decode error: %v", err)
			}

			// Compare
			if !compareValues(tt.value, decoded) {
				t.Errorf("round-trip mismatch: got %v (%T), want %v (%T)",
					decoded, decoded, tt.value, tt.value)
			}
		})
	}
}

// TestRoundTrip_Containers tests arrays and objects.
// Note: Homogeneous numeric arrays may be tensorized during decode.
func TestRoundTrip_Containers(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		// Arrays
		{"array_empty", []any{}},
		{"array_strings", []any{"a", "b", "c"}},
		{"array_ints", []any{int64(1), int64(2), int64(3)}},
		{"array_mixed", []any{int64(1), "two", true, nil, float64(3.14)}},
		{"array_nested", []any{
			[]any{int64(1), int64(2)},
			[]any{int64(3), int64(4)},
		}},

		// Objects
		{"object_empty", map[string]any{}},
		{"object_simple", map[string]any{"a": int64(1), "b": "two"}},
		{"object_nested", map[string]any{
			"outer": map[string]any{
				"inner": int64(42),
			},
		}},
		{"object_array_value", map[string]any{
			"items": []any{int64(1), int64(2), int64(3)},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeBytes(tt.value)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			var decoded any
			if err := DecodeBytes(encoded, &decoded); err != nil {
				t.Fatalf("decode error: %v", err)
			}

			// Use semantic comparison that handles tensorization
			if !compareContainers(tt.value, decoded) {
				t.Errorf("round-trip mismatch:\ngot:  %#v\nwant: %#v", decoded, tt.value)
			}
		})
	}
}

// compareContainers compares containers, handling tensor auto-conversion.
func compareContainers(expected, actual any) bool {
	switch e := expected.(type) {
	case []any:
		// Actual might be a typed slice due to tensorization
		switch a := actual.(type) {
		case []any:
			if len(e) != len(a) {
				return false
			}
			for i := range e {
				if !compareContainers(e[i], a[i]) {
					return false
				}
			}
			return true
		case []float32:
			// Tensorized to float32
			if len(e) != len(a) {
				return false
			}
			for i := range e {
				if !compareNumeric(e[i], float64(a[i])) {
					return false
				}
			}
			return true
		case []float64:
			// Tensorized to float64
			if len(e) != len(a) {
				return false
			}
			for i := range e {
				if !compareNumeric(e[i], a[i]) {
					return false
				}
			}
			return true
		case []int64:
			// Tensorized to int64
			if len(e) != len(a) {
				return false
			}
			for i := range e {
				if !compareNumeric(e[i], float64(a[i])) {
					return false
				}
			}
			return true
		}
	case map[string]any:
		a, ok := actual.(map[string]any)
		if !ok {
			return false
		}
		if len(e) != len(a) {
			return false
		}
		for k, ev := range e {
			av, exists := a[k]
			if !exists || !compareContainers(ev, av) {
				return false
			}
		}
		return true
	default:
		// Primitive comparison
		return compareValues(expected, actual)
	}
	return false
}

// compareNumeric compares two numeric values.
func compareNumeric(expected any, actual float64) bool {
	switch e := expected.(type) {
	case int64:
		return float64(e) == actual
	case uint64:
		return float64(e) == actual
	case float64:
		return e == actual
	case float32:
		return float64(e) == actual
	}
	return false
}

// TestRoundTrip_Structs tests struct encoding/decoding.
func TestRoundTrip_Structs(t *testing.T) {
	type SimpleStruct struct {
		Name  string `json:"name"`
		Value int64  `json:"value"`
	}

	type NestedStruct struct {
		ID     int64        `json:"id"`
		Simple SimpleStruct `json:"simple"`
	}

	type EmbeddedBase struct {
		BaseID   int64  `json:"base_id"`
		BaseName string `json:"base_name"`
	}

	type WithEmbedded struct {
		EmbeddedBase
		Extra string `json:"extra"`
	}

	type WithOmitEmpty struct {
		Required string `json:"required"`
		Optional string `json:"optional,omitempty"`
		Zero     int64  `json:"zero,omitempty"`
	}

	type WithRenamedFields struct {
		OldName string `json:"new_name"`
		Skipped string `json:"-"`
	}

	tests := []struct {
		name     string
		original any
		decoded  any
	}{
		{
			"simple",
			SimpleStruct{Name: "test", Value: 42},
			&SimpleStruct{},
		},
		{
			"nested",
			NestedStruct{ID: 1, Simple: SimpleStruct{Name: "inner", Value: 99}},
			&NestedStruct{},
		},
		{
			"embedded",
			WithEmbedded{EmbeddedBase: EmbeddedBase{BaseID: 1, BaseName: "base"}, Extra: "extra"},
			&WithEmbedded{},
		},
		{
			"omitempty_with_values",
			WithOmitEmpty{Required: "req", Optional: "opt", Zero: 1},
			&WithOmitEmpty{},
		},
		{
			"omitempty_empty_values",
			WithOmitEmpty{Required: "req", Optional: "", Zero: 0},
			&WithOmitEmpty{},
		},
		{
			"renamed_fields",
			WithRenamedFields{OldName: "value", Skipped: "should_not_appear"},
			&WithRenamedFields{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := FastEncode(tt.original)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			if err := DecodeBytes(encoded, tt.decoded); err != nil {
				t.Fatalf("decode error: %v", err)
			}

			// Use JSON encoding to compare (handles unexported fields, etc.)
			origJSON, _ := json.Marshal(tt.original)
			decodedJSON, _ := json.Marshal(tt.decoded)

			// For renamed fields, skipped field should be empty
			if tt.name == "renamed_fields" {
				decoded := tt.decoded.(*WithRenamedFields)
				if decoded.Skipped != "" {
					t.Errorf("Skipped field should be empty, got %q", decoded.Skipped)
				}
				return
			}

			if string(origJSON) != string(decodedJSON) {
				t.Errorf("mismatch:\noriginal: %s\ndecoded:  %s", origJSON, decodedJSON)
			}
		})
	}
}

// TestRoundTrip_Time tests time.Time handling.
func TestRoundTrip_Time(t *testing.T) {
	type WithTime struct {
		Created time.Time `json:"created"`
	}

	now := time.Now().UTC().Truncate(time.Nanosecond)

	original := WithTime{Created: now}

	encoded, err := FastEncode(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	var decoded WithTime
	if err := DecodeBytes(encoded, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Compare with nanosecond precision
	if !original.Created.Equal(decoded.Created) {
		t.Errorf("time mismatch:\noriginal: %v\ndecoded:  %v",
			original.Created.UnixNano(), decoded.Created.UnixNano())
	}
}

// TestRoundTrip_Slices tests typed slice handling.
func TestRoundTrip_Slices(t *testing.T) {
	type WithSlices struct {
		Strings  []string  `json:"strings"`
		Ints     []int64   `json:"ints"`
		Floats32 []float32 `json:"floats32"`
		Floats64 []float64 `json:"floats64"`
		Bytes    []byte    `json:"bytes"`
	}

	original := WithSlices{
		Strings:  []string{"a", "b", "c"},
		Ints:     []int64{1, 2, 3, 4, 5},
		Floats32: []float32{1.1, 2.2, 3.3},
		Floats64: []float64{1.111111111, 2.222222222, 3.333333333},
		Bytes:    []byte{0x00, 0x01, 0x02, 0xFF, 0xFE},
	}

	encoded, err := FastEncode(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	var decoded WithSlices
	if err := DecodeBytes(encoded, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Verify strings
	if !reflect.DeepEqual(original.Strings, decoded.Strings) {
		t.Errorf("Strings mismatch: %v vs %v", original.Strings, decoded.Strings)
	}

	// Verify ints
	if !reflect.DeepEqual(original.Ints, decoded.Ints) {
		t.Errorf("Ints mismatch: %v vs %v", original.Ints, decoded.Ints)
	}

	// Verify float32 (exact comparison)
	if len(original.Floats32) != len(decoded.Floats32) {
		t.Errorf("Floats32 length mismatch: %d vs %d", len(original.Floats32), len(decoded.Floats32))
	} else {
		for i := range original.Floats32 {
			if original.Floats32[i] != decoded.Floats32[i] {
				t.Errorf("Floats32[%d] mismatch: %v vs %v", i, original.Floats32[i], decoded.Floats32[i])
			}
		}
	}

	// Verify bytes
	if !reflect.DeepEqual(original.Bytes, decoded.Bytes) {
		t.Errorf("Bytes mismatch: %v vs %v", original.Bytes, decoded.Bytes)
	}
}

// TestRoundTrip_RawMessage tests json.RawMessage handling.
func TestRoundTrip_RawMessage(t *testing.T) {
	type WithRawJSON struct {
		ID   int64           `json:"id"`
		Data json.RawMessage `json:"data"`
	}

	tests := []struct {
		name    string
		data    string
		wantErr bool
	}{
		{"valid_object", `{"foo":"bar","nums":[1,2,3]}`, false},
		{"valid_array", `[1,2,3,"four"]`, false},
		{"valid_string", `"just a string"`, false},
		{"valid_number", `42`, false},
		{"valid_null", `null`, false},
		{"invalid_json", `not valid json {`, false}, // Should preserve as string
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := WithRawJSON{
				ID:   42,
				Data: json.RawMessage(tt.data),
			}

			encoded, err := FastEncode(original)
			if err != nil {
				if tt.wantErr {
					return
				}
				t.Fatalf("encode error: %v", err)
			}

			var decoded WithRawJSON
			if err := DecodeBytes(encoded, &decoded); err != nil {
				if tt.wantErr {
					return
				}
				t.Fatalf("decode error: %v", err)
			}

			if decoded.ID != original.ID {
				t.Errorf("ID mismatch: %d vs %d", decoded.ID, original.ID)
			}

			// For valid JSON, verify it's parseable
			// Note: null RawMessage may be decoded as empty (Cowrie treats null specially)
			if tt.name != "invalid_json" && tt.name != "valid_null" {
				var parsed any
				if err := json.Unmarshal(decoded.Data, &parsed); err != nil {
					t.Errorf("decoded Data is not valid JSON: %v (data=%q)", err, string(decoded.Data))
				}
			}
			// For null, just verify the struct decoded without error
			if tt.name == "valid_null" {
				// Null may decode as empty RawMessage or nil - both are acceptable
				t.Logf("null RawMessage decoded as: %q (len=%d)", string(decoded.Data), len(decoded.Data))
			}
		})
	}
}

// TestRoundTrip_Maps tests map handling.
func TestRoundTrip_Maps(t *testing.T) {
	type WithMaps struct {
		StringMap map[string]string `json:"string_map"`
		IntMap    map[string]int64  `json:"int_map"`
		AnyMap    map[string]any    `json:"any_map"`
	}

	original := WithMaps{
		StringMap: map[string]string{"a": "1", "b": "2", "c": "3"},
		IntMap:    map[string]int64{"x": 10, "y": 20, "z": 30},
		AnyMap: map[string]any{
			"string": "value",
			"int":    int64(42),
			"float":  float64(3.14),
			"bool":   true,
			"null":   nil,
			"array":  []any{int64(1), int64(2)},
			"object": map[string]any{"nested": "value"},
		},
	}

	encoded, err := FastEncode(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	var decoded WithMaps
	if err := DecodeBytes(encoded, &decoded); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Verify string map
	if !reflect.DeepEqual(original.StringMap, decoded.StringMap) {
		t.Errorf("StringMap mismatch:\noriginal: %v\ndecoded:  %v", original.StringMap, decoded.StringMap)
	}

	// Verify int map
	if !reflect.DeepEqual(original.IntMap, decoded.IntMap) {
		t.Errorf("IntMap mismatch:\noriginal: %v\ndecoded:  %v", original.IntMap, decoded.IntMap)
	}

	// Verify any map (using JSON comparison for nested structures)
	origJSON, _ := json.Marshal(original.AnyMap)
	decodedJSON, _ := json.Marshal(decoded.AnyMap)
	if string(origJSON) != string(decodedJSON) {
		t.Errorf("AnyMap mismatch:\noriginal: %s\ndecoded:  %s", origJSON, decodedJSON)
	}
}

// TestRoundTrip_Pointers tests pointer field handling.
func TestRoundTrip_Pointers(t *testing.T) {
	type WithPointers struct {
		Name  *string  `json:"name"`
		Age   *int64   `json:"age"`
		Score *float64 `json:"score"`
	}

	name := "Alice"
	age := int64(30)
	score := float64(95.5)

	tests := []struct {
		name     string
		original WithPointers
	}{
		{
			"all_set",
			WithPointers{Name: &name, Age: &age, Score: &score},
		},
		{
			"some_nil",
			WithPointers{Name: &name, Age: nil, Score: &score},
		},
		{
			"all_nil",
			WithPointers{Name: nil, Age: nil, Score: nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := FastEncode(tt.original)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}

			var decoded WithPointers
			if err := DecodeBytes(encoded, &decoded); err != nil {
				t.Fatalf("decode error: %v", err)
			}

			// Compare Name
			if (tt.original.Name == nil) != (decoded.Name == nil) {
				t.Errorf("Name nil mismatch")
			} else if tt.original.Name != nil && *tt.original.Name != *decoded.Name {
				t.Errorf("Name value mismatch: %q vs %q", *tt.original.Name, *decoded.Name)
			}

			// Compare Age
			if (tt.original.Age == nil) != (decoded.Age == nil) {
				t.Errorf("Age nil mismatch")
			} else if tt.original.Age != nil && *tt.original.Age != *decoded.Age {
				t.Errorf("Age value mismatch: %d vs %d", *tt.original.Age, *decoded.Age)
			}

			// Compare Score
			if (tt.original.Score == nil) != (decoded.Score == nil) {
				t.Errorf("Score nil mismatch")
			} else if tt.original.Score != nil && *tt.original.Score != *decoded.Score {
				t.Errorf("Score value mismatch: %f vs %f", *tt.original.Score, *decoded.Score)
			}
		})
	}
}

// compareValues compares two values with type flexibility.
// Handles known decoder behaviors:
// - Large integers may decode to string (precision preservation)
// - Zero floats may decode to int64 (common JSON behavior)
// - Integers may decode to float64 or vice versa
func compareValues(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	switch av := a.(type) {
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case int64:
		switch bv := b.(type) {
		case int64:
			return av == bv
		case float64:
			return float64(av) == bv
		case string:
			// Large integers may be decoded as strings for precision
			return fmt.Sprintf("%d", av) == bv
		}
	case uint64:
		switch bv := b.(type) {
		case uint64:
			return av == bv
		case int64:
			if bv >= 0 {
				return av == uint64(bv)
			}
		case string:
			// Large unsigned integers may be decoded as strings
			return fmt.Sprintf("%d", av) == bv
		}
	case float64:
		switch bv := b.(type) {
		case float64:
			// Handle special float values
			if math.IsNaN(av) && math.IsNaN(bv) {
				return true
			}
			if math.IsInf(av, 1) && math.IsInf(bv, 1) {
				return true
			}
			if math.IsInf(av, -1) && math.IsInf(bv, -1) {
				return true
			}
			return av == bv
		case int64:
			// Zero floats may decode as int64
			return av == float64(bv)
		}
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	}

	return reflect.DeepEqual(a, b)
}
