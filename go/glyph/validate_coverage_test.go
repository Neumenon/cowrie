package glyph

import (
	"testing"
)

func TestValidateWithSchema_Coverage(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("Person", "1.0",
			Field("name", TypeSpec{Kind: TypeSpecStr}),
			Field("age", TypeSpec{Kind: TypeSpecInt}),
		).Build()

	// Valid value
	v := Struct("Person",
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "age", Value: Int(30)},
	)
	result := ValidateWithSchema(v, schema)
	if result == nil {
		t.Fatal("nil result")
	}
	if !result.Valid {
		t.Errorf("expected valid, got errors: %v", result.Errors)
	}
}

func TestValidateAs_Coverage(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("Item", "1.0",
			Field("id", TypeSpec{Kind: TypeSpecStr}),
		).Build()

	v := Struct("Item", MapEntry{Key: "id", Value: Str("abc")})
	result := ValidateAs(v, schema, "Item")
	if result == nil {
		t.Fatal("nil result")
	}
}

func TestIsValid_Coverage(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("T", "1.0",
			Field("x", TypeSpec{Kind: TypeSpecInt}),
		).Build()

	v := Struct("T", MapEntry{Key: "x", Value: Int(1)})
	if !IsValid(v, schema) {
		t.Error("expected valid")
	}
}

func TestValidateStrict_Coverage(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("T", "1.0",
			Field("x", TypeSpec{Kind: TypeSpecInt}),
		).Build()

	v := Struct("T", MapEntry{Key: "x", Value: Int(1)})
	result := ValidateStrict(v, schema)
	if result == nil {
		t.Fatal("nil result")
	}
}
