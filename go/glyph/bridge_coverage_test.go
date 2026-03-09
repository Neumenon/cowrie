package glyph

import (
	"testing"
	"time"
)

func TestToCowrie_AllTypes(t *testing.T) {
	types := []struct {
		name string
		val  *GValue
	}{
		{"null", Null()},
		{"bool_true", Bool(true)},
		{"bool_false", Bool(false)},
		{"int", Int(42)},
		{"float", Float(3.14)},
		{"str", Str("hello")},
		{"bytes", Bytes([]byte{1, 2, 3})},
		{"time", Time(time.Date(2025, 3, 9, 12, 0, 0, 0, time.UTC))},
		{"id_with_prefix", ID("m", "123")},
		{"id_no_prefix", ID("", "abc")},
		{"list", List(Int(1), Int(2))},
		{"map", Map(MapEntry{Key: "a", Value: Int(1)})},
		{"struct", Struct("Person", MapEntry{Key: "name", Value: Str("Alice")})},
		{"sum", Sum("Ok", Int(42))},
		{"nil", nil},
	}

	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			cv := ToCowrie(tt.val)
			if cv == nil {
				t.Error("ToCowrie returned nil")
			}
		})
	}
}

func TestFromCowrie_AllTypes(t *testing.T) {
	// Round-trip test through ToCowrie -> FromCowrie
	types := []struct {
		name string
		val  *GValue
	}{
		{"null", Null()},
		{"bool", Bool(true)},
		{"int", Int(42)},
		{"float", Float(3.14)},
		{"str", Str("hello")},
		{"bytes", Bytes([]byte{1, 2, 3})},
		{"time", Time(time.Date(2025, 3, 9, 12, 0, 0, 0, time.UTC))},
		{"list", List(Int(1), Int(2))},
		{"map", Map(MapEntry{Key: "a", Value: Int(1)})},
		{"struct", Struct("Person", MapEntry{Key: "name", Value: Str("Alice")})},
		{"sum", Sum("Ok", Int(42))},
	}

	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			cv := ToCowrie(tt.val)
			gv := FromCowrie(cv)
			if gv == nil {
				t.Error("FromCowrie returned nil")
			}
			if gv.Type() != tt.val.Type() {
				t.Errorf("type mismatch: got %v, want %v", gv.Type(), tt.val.Type())
			}
		})
	}
}

func TestFromCowrie_NilInput(t *testing.T) {
	gv := FromCowrie(nil)
	if !gv.IsNull() {
		t.Error("expected null for nil input")
	}
}

func TestToCowrieList(t *testing.T) {
	values := []*GValue{Int(1), Str("hello"), Bool(true)}
	result := ToCowrieList(values)
	if len(result) != 3 {
		t.Errorf("expected 3, got %d", len(result))
	}
}

func TestFromCowrieList(t *testing.T) {
	gvalues := []*GValue{Int(1), Str("hello")}
	cowrieValues := ToCowrieList(gvalues)
	back := FromCowrieList(cowrieValues)
	if len(back) != 2 {
		t.Errorf("expected 2, got %d", len(back))
	}
}

func TestToJSON_Coverage(t *testing.T) {
	v := Map(MapEntry{Key: "x", Value: Int(1)})
	data, err := ToJSON(v)
	if err != nil {
		t.Fatalf("ToJSON error: %v", err)
	}
	if len(data) == 0 {
		t.Error("empty JSON")
	}
}

func TestFromJSON_Coverage(t *testing.T) {
	gv, err := FromJSON([]byte(`{"x": 1}`))
	if err != nil {
		t.Fatalf("FromJSON error: %v", err)
	}
	if gv.Get("x") == nil {
		t.Error("expected field x")
	}
}

func TestToJSONString_Coverage(t *testing.T) {
	v := Map(MapEntry{Key: "a", Value: Int(1)})
	s, err := ToJSONString(v)
	if err != nil {
		t.Fatalf("ToJSONString error: %v", err)
	}
	if s == "" {
		t.Error("empty string")
	}
}

func TestToAny_Coverage(t *testing.T) {
	v := Map(MapEntry{Key: "a", Value: Int(1)})
	a := ToAny(v)
	if a == nil {
		t.Error("expected non-nil")
	}
}

func TestFromAny_Coverage(t *testing.T) {
	v := FromAny(map[string]interface{}{"a": float64(1)})
	if v == nil {
		t.Error("expected non-nil")
	}
}

func TestEncodeBinary_Coverage(t *testing.T) {
	v := Map(MapEntry{Key: "x", Value: Int(42)})
	data, err := EncodeBinary(v)
	if err != nil {
		t.Fatalf("EncodeBinary error: %v", err)
	}
	if len(data) == 0 {
		t.Error("empty binary")
	}
}

func TestDecodeBinary_Coverage(t *testing.T) {
	v := Map(MapEntry{Key: "x", Value: Int(42)})
	data, err := EncodeBinary(v)
	if err != nil {
		t.Fatalf("EncodeBinary error: %v", err)
	}
	gv, err := DecodeBinary(data)
	if err != nil {
		t.Fatalf("DecodeBinary error: %v", err)
	}
	if gv.Get("x") == nil {
		t.Error("expected field x")
	}
}

func TestRoundTrips_Coverage(t *testing.T) {
	v := Map(MapEntry{Key: "a", Value: Int(1)}, MapEntry{Key: "b", Value: Str("hello")})
	if !RoundTrips(v) {
		t.Error("expected round-trip to succeed")
	}
}
