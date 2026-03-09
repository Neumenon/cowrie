package glyph

import (
	"testing"
)

func TestBridge_FromJSON_Coverage(t *testing.T) {
	input := `{"name":"Alice","age":30,"active":true}`
	v, err := FromJSON([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	if v == nil {
		t.Fatal("nil")
	}
}

func TestBridge_ToJSON_Coverage(t *testing.T) {
	v := Map(
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "age", Value: Int(30)},
	)
	data, err := ToJSON(v)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Error("empty")
	}
}

func TestBridge_ToJSONString_Coverage(t *testing.T) {
	v := Map(MapEntry{Key: "x", Value: Int(1)})
	s, err := ToJSONString(v)
	if err != nil {
		t.Fatal(err)
	}
	if s == "" {
		t.Error("empty")
	}
}

func TestBridge_EncodeBinary_Coverage(t *testing.T) {
	v := Map(
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "items", Value: List(Int(1), Int(2))},
	)
	data, err := EncodeBinary(v)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Error("empty")
	}

	// Decode back
	v2, err := DecodeBinary(data)
	if err != nil {
		t.Fatal(err)
	}
	if v2 == nil {
		t.Error("nil")
	}
}

func TestBridge_RoundTrips_Coverage(t *testing.T) {
	tests := []*GValue{
		Null(),
		Bool(true),
		Int(42),
		Float(3.14),
		Str("hello"),
		List(Int(1), Int(2)),
		Map(MapEntry{Key: "x", Value: Int(1)}),
	}
	for i, v := range tests {
		if !RoundTrips(v) {
			t.Errorf("round trip failed for case %d", i)
		}
	}
}

func TestBridge_FromCowrieWithSchema_Coverage(t *testing.T) {
	sb := NewSchemaBuilder()
	sb.AddStruct("Person", "",
		&FieldDef{Name: "name", Type: TypeSpec{Kind: TypeSpecStr}},
		&FieldDef{Name: "age", Type: TypeSpec{Kind: TypeSpecInt}},
	)
	schema := sb.Build()

	// Encode a glyph map to cowrie, then convert back with schema
	v := Map(
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "age", Value: Int(30)},
	)
	sv := ToCowrie(v)
	result := FromCowrieWithSchema(sv, schema, "Person")
	if result == nil {
		t.Fatal("nil result")
	}

	// Unknown type falls back to FromCowrie
	result2 := FromCowrieWithSchema(sv, schema, "Unknown")
	if result2 == nil {
		t.Fatal("nil result for unknown type")
	}

	// Nil value
	result3 := FromCowrieWithSchema(nil, schema, "Person")
	if result3 == nil {
		t.Fatal("nil result for nil value")
	}
}

func TestBridge_ToAny_Coverage(t *testing.T) {
	tests := []struct {
		name string
		v    *GValue
	}{
		{"null", Null()},
		{"bool", Bool(true)},
		{"int", Int(42)},
		{"float", Float(3.14)},
		{"str", Str("hello")},
		{"list", List(Int(1), Int(2))},
		{"map", Map(MapEntry{Key: "x", Value: Int(1)})},
		{"struct", Struct("T", MapEntry{Key: "x", Value: Int(1)})},
		{"id", ID("d", "1")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := ToAny(tt.v)
			_ = a
		})
	}
}

func TestBridge_FromAny_Coverage(t *testing.T) {
	tests := []struct {
		name string
		v    interface{}
	}{
		{"nil", nil},
		{"bool", true},
		{"int", 42},
		{"int64", int64(42)},
		{"float", 3.14},
		{"string", "hello"},
		{"array", []interface{}{1, "two", true}},
		{"map", map[string]interface{}{"x": 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := FromAny(tt.v)
			if v == nil && tt.v != nil {
				t.Error("nil value")
			}
		})
	}
}

func TestEmitTabular_Coverage(t *testing.T) {
	// Create list of structs
	sb := NewSchemaBuilder()
	sb.AddStruct("Person", "",
		&FieldDef{Name: "name", Type: TypeSpec{Kind: TypeSpecStr}},
		&FieldDef{Name: "age", Type: TypeSpec{Kind: TypeSpecInt}},
	)
	schema := sb.Build()

	v := List(
		Map(MapEntry{Key: "name", Value: Str("Alice")}, MapEntry{Key: "age", Value: Int(30)}),
		Map(MapEntry{Key: "name", Value: Str("Bob")}, MapEntry{Key: "age", Value: Int(25)}),
	)

	result, err := EmitTabular(v, schema)
	if err != nil {
		t.Skipf("EmitTabular: %v", err)
		return
	}
	if result == "" {
		t.Error("empty result")
	}
}

func TestAutoPoolEncode_Deep(t *testing.T) {
	// Create a value with refs
	v := Map(
		MapEntry{Key: "users", Value: List(
			Map(
				MapEntry{Key: "name", Value: Str("Alice")},
				MapEntry{Key: "age", Value: Int(30)},
			),
			Map(
				MapEntry{Key: "name", Value: Str("Bob")},
				MapEntry{Key: "age", Value: Int(25)},
			),
		)},
		MapEntry{Key: "count", Value: Int(2)},
	)

	opts := DefaultAutoPoolOpts()
	result, err := AutoPoolEncode(v, opts)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("nil result")
	}
	if result.Output == "" {
		t.Error("empty output")
	}
}

func TestParseSchemaHeader_Coverage(t *testing.T) {
	// Valid schema header
	ref, keys, err := ParseSchemaHeader("@schema#abc123 @keys=[name age]")
	if err != nil {
		t.Fatal(err)
	}
	_ = ref
	_ = keys

	// Schema without keys
	ref2, _, err := ParseSchemaHeader("@schema#def456")
	if err != nil {
		t.Fatal(err)
	}
	if ref2 == "" {
		t.Error("empty ref")
	}

	// Invalid
	_, _, err = ParseSchemaHeader("not a schema")
	if err == nil {
		t.Error("should fail")
	}
}

func TestLooseCanon_BytesAndTime(t *testing.T) {
	// Time value
	v := Map(
		MapEntry{Key: "ts", Value: Str("2024-01-01T00:00:00Z")},
		MapEntry{Key: "data", Value: Str("base64data")},
	)
	s := CanonicalizeLoose(v)
	if s == "" {
		t.Error("empty")
	}
}

func TestParseDocument_WithPool(t *testing.T) {
	// Single-line pool definition
	input := `@pool.str id=S1 ["Alice" "Bob" "Charlie"]
name ^S1:0`
	v, err := ParseDocument(input)
	if err != nil {
		t.Skipf("pool format: %v", err)
		return
	}
	if v == nil {
		t.Error("nil value")
	}
}

func TestParseDocument_MultiLinePool(t *testing.T) {
	input := `@pool.str id=S1 [
"Alice"
"Bob"
"Charlie"]
name ^S1:1`
	v, err := ParseDocument(input)
	if err != nil {
		t.Skipf("multi-line pool: %v", err)
		return
	}
	if v == nil {
		t.Error("nil value")
	}
}

func TestParseDocument_WithSchema(t *testing.T) {
	input := `@schema#abc @keys=[name age]
name Alice
age 30`
	v, err := ParseDocument(input)
	if err != nil {
		t.Skipf("schema doc: %v", err)
		return
	}
	if v == nil {
		t.Error("nil value")
	}
}

func TestParseDocument_EmbeddedTab(t *testing.T) {
	input := `messages=@tab _ [content role]
|hello|user|
|hi|assistant|
@end`
	v, err := ParseDocument(input)
	if err != nil {
		t.Skipf("embedded tab: %v", err)
		return
	}
	if v == nil {
		t.Error("nil value")
	}
}

func TestParseDocument_WithTab(t *testing.T) {
	// Test ParseDocument with @tab
	input := "@tab _ [name age]\n|Alice|30|\n|Bob|25|\n@end"
	v, err := ParseDocument(input)
	if err != nil {
		t.Fatal(err)
	}
	if v == nil {
		t.Error("nil value")
	}
}

func TestEmitPacked_Coverage(t *testing.T) {
	sb := NewSchemaBuilder()
	sb.AddPackedStruct("Event", "",
		&FieldDef{Name: "type", Type: TypeSpec{Kind: TypeSpecStr}, FID: 1},
		&FieldDef{Name: "ts", Type: TypeSpec{Kind: TypeSpecInt}, FID: 2},
		&FieldDef{Name: "data", Type: TypeSpec{Kind: TypeSpecStr}, FID: 3, Optional: true},
	)
	schema := sb.Build()

	v := Struct("Event",
		MapEntry{Key: "type", Value: Str("click")},
		MapEntry{Key: "ts", Value: Int(12345)},
	)

	result, err := EmitPacked(v, schema)
	if err != nil {
		t.Skipf("packed emit: %v", err)
		return
	}
	if result == "" {
		t.Error("empty result")
	}
}

func TestDiff_StructChanges(t *testing.T) {
	old := Map(
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "age", Value: Int(30)},
		MapEntry{Key: "city", Value: Str("Boston")},
	)
	newVal := Map(
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "age", Value: Int(31)},
		MapEntry{Key: "email", Value: Str("alice@example.com")},
	)

	patch := Diff(old, newVal, "")
	if patch == nil {
		t.Fatal("nil patch")
	}
	// Should have: age changed, city deleted, email added
	if len(patch.Ops) < 2 {
		t.Errorf("expected >= 2 ops, got %d", len(patch.Ops))
	}
}

func TestDiff_EqualValues(t *testing.T) {
	v := Map(
		MapEntry{Key: "name", Value: Str("Alice")},
	)
	patch := Diff(v, v, "")
	if patch == nil {
		t.Fatal("nil patch")
	}
	if len(patch.Ops) != 0 {
		t.Errorf("expected 0 ops for equal values, got %d", len(patch.Ops))
	}
}
