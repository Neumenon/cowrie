package glyph

import (
	"testing"
	"time"
)

func TestToJSONValueLoose_AllTypes(t *testing.T) {
	tests := []struct {
		name string
		v    *GValue
	}{
		{"null", Null()},
		{"bool", Bool(true)},
		{"int", Int(42)},
		{"float", Float(3.14)},
		{"str", Str("hello")},
		{"bytes", Bytes([]byte{1, 2, 3})},
		{"time", Time(time.Now())},
		{"id", ID("d", "1")},
		{"list", List(Int(1), Int(2))},
		{"map", Map(MapEntry{Key: "x", Value: Int(1)})},
		{"struct", Struct("T", MapEntry{Key: "x", Value: Int(1)})},
		{"nested_list", List(List(Int(1)), List(Int(2)))},
		{"nested_map", Map(MapEntry{Key: "outer", Value: Map(MapEntry{Key: "inner", Value: Int(1)})})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ToJSONValueLoose(tt.v)
			if err != nil {
				t.Fatalf("ToJSONValueLoose: %v", err)
			}
			_ = result
		})
	}
}

func TestToJSONValueLooseWithOpts_Extended(t *testing.T) {
	opts := BridgeOpts{Extended: true}

	// Bytes with extended
	bv := Bytes([]byte{1, 2, 3})
	result, err := ToJSONValueLooseWithOpts(bv, opts)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatal("expected map for extended bytes")
	}
	if m["$glyph"] != "bytes" {
		t.Error("expected $glyph: bytes")
	}

	// Time with extended
	tv := Time(time.Now())
	result, err = ToJSONValueLooseWithOpts(tv, opts)
	if err != nil {
		t.Fatal(err)
	}
	m, ok = result.(map[string]interface{})
	if !ok {
		t.Fatal("expected map for extended time")
	}
	if m["$glyph"] != "time" {
		t.Error("expected $glyph: time")
	}

	// ID with extended
	idv := ID("d", "1")
	result, err = ToJSONValueLooseWithOpts(idv, opts)
	if err != nil {
		t.Fatal(err)
	}
	m, ok = result.(map[string]interface{})
	if !ok {
		t.Fatal("expected map for extended id")
	}
	if m["$glyph"] != "id" {
		t.Error("expected $glyph: id")
	}
}

func TestToJSONLoose_Coverage(t *testing.T) {
	v := Map(
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "age", Value: Int(30)},
	)
	data, err := ToJSONLoose(v)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Error("empty json")
	}
}

func TestFromJSONLoose_Deep(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"null", `null`},
		{"bool", `true`},
		{"int", `42`},
		{"float", `3.14`},
		{"string", `"hello"`},
		{"array", `[1, 2, 3]`},
		{"object", `{"x": 1, "y": "z"}`},
		{"nested", `{"arr": [1, {"inner": true}]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := FromJSONLoose([]byte(tt.input))
			if err != nil {
				t.Fatalf("FromJSONLoose: %v", err)
			}
			if v == nil {
				t.Error("nil value")
			}
		})
	}
}


func TestFromJSONValueLoose_Deep(t *testing.T) {
	// Various Go types
	tests := []struct {
		name  string
		input interface{}
	}{
		{"nil", nil},
		{"bool", true},
		{"float64", float64(42)},
		{"float64_frac", float64(3.14)},
		{"string", "hello"},
		{"array", []interface{}{1.0, "two", true}},
		{"map", map[string]interface{}{"x": 1.0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := FromJSONValueLoose(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if v == nil && tt.input != nil {
				t.Error("nil value")
			}
		})
	}
}
