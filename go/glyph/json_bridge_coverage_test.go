package glyph

import (
	"testing"
)

func TestFromJSONLoose_AllTypes(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{"null", "null"},
		{"bool_true", "true"},
		{"bool_false", "false"},
		{"int", "42"},
		{"float", "3.14"},
		{"string", `"hello"`},
		{"array", "[1, 2, 3]"},
		{"object", `{"a": 1, "b": "hello"}`},
		{"nested", `{"items": [{"x": 1}, {"x": 2}]}`},
		{"empty_object", `{}`},
		{"empty_array", "[]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gv, err := FromJSONLoose([]byte(tt.json))
			if err != nil {
				t.Fatalf("FromJSONLoose error: %v", err)
			}
			if gv == nil {
				t.Error("nil result")
			}
		})
	}
}

func TestFromJSONLoose_Invalid(t *testing.T) {
	_, err := FromJSONLoose([]byte("invalid json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestFromJSONValueLoose_Coverage(t *testing.T) {
	gv, err := FromJSONValueLoose(map[string]interface{}{
		"a": float64(1),
		"b": "hello",
	})
	if err != nil {
		t.Fatalf("FromJSONValueLoose error: %v", err)
	}
	if gv.Get("a") == nil {
		t.Error("expected field a")
	}
}

func TestToJSONLoose_AllTypes(t *testing.T) {
	types := []struct {
		name string
		val  *GValue
	}{
		{"null", Null()},
		{"bool", Bool(true)},
		{"int", Int(42)},
		{"float", Float(3.14)},
		{"str", Str("hello")},
		{"list", List(Int(1), Int(2))},
		{"map", Map(MapEntry{Key: "a", Value: Int(1)})},
	}

	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			data, err := ToJSONLoose(tt.val)
			if err != nil {
				t.Fatalf("ToJSONLoose error: %v", err)
			}
			if len(data) == 0 {
				t.Error("empty JSON")
			}
		})
	}
}

func TestToJSONLoose_Nil(t *testing.T) {
	data, err := ToJSONLoose(nil)
	if err != nil {
		t.Fatalf("ToJSONLoose nil error: %v", err)
	}
	if string(data) != "null" {
		t.Errorf("expected null, got %s", data)
	}
}

func TestJSONLoose_RoundTrip(t *testing.T) {
	v := Map(
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "age", Value: Int(30)},
		MapEntry{Key: "items", Value: List(Int(1), Int(2), Int(3))},
	)

	data, err := ToJSONLoose(v)
	if err != nil {
		t.Fatalf("ToJSONLoose error: %v", err)
	}

	back, err := FromJSONLoose(data)
	if err != nil {
		t.Fatalf("FromJSONLoose error: %v", err)
	}

	if back.Get("name") == nil {
		t.Error("expected name field after round-trip")
	}
}

func TestDefaultBridgeOpts(t *testing.T) {
	opts := DefaultBridgeOpts()
	if opts.Extended {
		t.Error("default should not be extended")
	}
}
