package glyph

import (
	"math"
	"testing"
	"time"
)

func TestValidateValue_AllTypeSpecs(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("Inner", "1.0",
			Field("x", TypeSpec{Kind: TypeSpecInt}),
		).
		AddStruct("Outer", "1.0",
			Field("name", TypeSpec{Kind: TypeSpecStr}),
			Field("flag", TypeSpec{Kind: TypeSpecBool}),
			Field("count", TypeSpec{Kind: TypeSpecInt}),
			Field("score", TypeSpec{Kind: TypeSpecFloat}),
			Field("data", TypeSpec{Kind: TypeSpecBytes}),
			Field("tags", TypeSpec{Kind: TypeSpecList, Elem: &TypeSpec{Kind: TypeSpecStr}}),
			Field("meta", TypeSpec{Kind: TypeSpecMap, KeyType: &TypeSpec{Kind: TypeSpecStr}, ValType: &TypeSpec{Kind: TypeSpecStr}}),
			Field("inner", TypeSpec{Kind: TypeSpecRef, Name: "Inner"}),
			Field("ts", TypeSpec{Kind: TypeSpecTime}),
			Field("id", TypeSpec{Kind: TypeSpecID}),
			Field("opt", TypeSpec{Kind: TypeSpecStr}, WithOptional()),
		).
		AddSum("Status", "1.0",
			Variant("Ok", TypeSpec{Kind: TypeSpecStr}),
			Variant("Err", TypeSpec{Kind: TypeSpecStr}),
		).
		Build()

	// Valid struct
	v := Struct("Outer",
		MapEntry{Key: "name", Value: Str("test")},
		MapEntry{Key: "flag", Value: Bool(true)},
		MapEntry{Key: "count", Value: Int(42)},
		MapEntry{Key: "score", Value: Float(3.14)},
		MapEntry{Key: "data", Value: Bytes([]byte{1, 2})},
		MapEntry{Key: "tags", Value: List(Str("a"), Str("b"))},
		MapEntry{Key: "meta", Value: Map(MapEntry{Key: "k", Value: Str("v")})},
		MapEntry{Key: "inner", Value: Struct("Inner", MapEntry{Key: "x", Value: Int(1)})},
		MapEntry{Key: "ts", Value: Time(time.Now())},
		MapEntry{Key: "id", Value: ID("d", "1")},
	)
	result := ValidateWithSchema(v, schema)
	if !result.Valid {
		t.Errorf("expected valid, errors: %v", result.Errors)
	}

	// Type mismatches
	bad := Struct("Outer",
		MapEntry{Key: "name", Value: Int(42)}, // wrong type
		MapEntry{Key: "flag", Value: Str("x")},
		MapEntry{Key: "count", Value: Str("x")},
		MapEntry{Key: "score", Value: Str("x")},
		MapEntry{Key: "data", Value: Int(1)},
		MapEntry{Key: "tags", Value: Int(1)},
		MapEntry{Key: "meta", Value: Int(1)},
		MapEntry{Key: "inner", Value: Int(1)},
		MapEntry{Key: "ts", Value: Int(1)},
		MapEntry{Key: "id", Value: Int(1)},
	)
	result2 := ValidateWithSchema(bad, schema)
	if result2.Valid {
		t.Error("expected invalid")
	}
	if len(result2.Errors) == 0 {
		t.Error("expected errors")
	}
}

func TestValidateConstraints_Coverage(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("T", "1.0",
			Field("age", TypeSpec{Kind: TypeSpecInt},
				WithConstraint(Constraint{Kind: ConstraintMin, Value: float64(0)}),
				WithConstraint(Constraint{Kind: ConstraintMax, Value: float64(150)}),
			),
			Field("name", TypeSpec{Kind: TypeSpecStr},
				WithConstraint(Constraint{Kind: ConstraintMinLen, Value: 1}),
				WithConstraint(Constraint{Kind: ConstraintMaxLen, Value: 100}),
				WithConstraint(Constraint{Kind: ConstraintNonEmpty}),
				WithConstraint(Constraint{Kind: ConstraintRegex, Value: "^[A-Za-z]+$"}),
			),
			Field("code", TypeSpec{Kind: TypeSpecStr},
				WithConstraint(Constraint{Kind: ConstraintLen, Value: 3}),
			),
			Field("score", TypeSpec{Kind: TypeSpecFloat},
				WithConstraint(Constraint{Kind: ConstraintRange, Value: [2]float64{0, 100}}),
			),
			Field("status", TypeSpec{Kind: TypeSpecStr},
				WithConstraint(Constraint{Kind: ConstraintEnum, Value: []string{"active", "inactive"}}),
			),
		).Build()

	// Valid
	v := Struct("T",
		MapEntry{Key: "age", Value: Int(25)},
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "code", Value: Str("ABC")},
		MapEntry{Key: "score", Value: Float(85.5)},
		MapEntry{Key: "status", Value: Str("active")},
	)
	result := ValidateWithSchema(v, schema)
	if !result.Valid {
		t.Errorf("expected valid, errors: %v", result.Errors)
	}

	// Violations
	bad := Struct("T",
		MapEntry{Key: "age", Value: Int(-1)},
		MapEntry{Key: "name", Value: Str("")},
		MapEntry{Key: "code", Value: Str("AB")},
		MapEntry{Key: "score", Value: Float(150)},
		MapEntry{Key: "status", Value: Str("unknown")},
	)
	result2 := ValidateWithSchema(bad, schema)
	if result2.Valid {
		t.Error("expected invalid")
	}
}

func TestValidateAs_SumType(t *testing.T) {
	schema := NewSchemaBuilder().
		AddSum("Result", "1.0",
			Variant("Ok", TypeSpec{Kind: TypeSpecStr}),
			Variant("Err", TypeSpec{Kind: TypeSpecStr}),
		).Build()

	v := Sum("Ok", Str("success"))
	result := ValidateAs(v, schema, "Result")
	if !result.Valid {
		t.Errorf("expected valid, errors: %v", result.Errors)
	}

	// Bad variant
	bad := Sum("Unknown", Str("test"))
	result2 := ValidateAs(bad, schema, "Result")
	if result2.Valid {
		t.Error("expected invalid for unknown variant")
	}

	// Unknown type
	result3 := ValidateAs(v, schema, "NonExistent")
	if result3.Valid {
		t.Error("expected invalid for unknown type")
	}
}

func TestValidateStrict_UnknownFields(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("T", "1.0",
			Field("x", TypeSpec{Kind: TypeSpecInt}),
		).Build()

	// Extra field in strict mode
	v := Struct("T",
		MapEntry{Key: "x", Value: Int(1)},
		MapEntry{Key: "extra", Value: Str("unknown")},
	)
	result := ValidateStrict(v, schema)
	if result.Valid {
		t.Error("strict should reject unknown fields")
	}
}

func TestEmit_SpecialFloats(t *testing.T) {
	// NaN
	v := Float(math.NaN())
	s := Emit(v)
	if s == "" {
		t.Error("NaN emit empty")
	}

	// Inf
	v = Float(math.Inf(1))
	s = Emit(v)
	if s == "" {
		t.Error("Inf emit empty")
	}

	// -Inf
	v = Float(math.Inf(-1))
	s = Emit(v)
	if s == "" {
		t.Error("-Inf emit empty")
	}
}

func TestEmitCompact_Deep(t *testing.T) {
	v := Map(
		MapEntry{Key: "x", Value: Int(1)},
		MapEntry{Key: "y", Value: List(Int(1), Int(2))},
	)
	s := EmitCompact(v)
	if s == "" {
		t.Error("compact emit empty")
	}
}

func TestEmit_Sum(t *testing.T) {
	v := Sum("Ok", Str("success"))
	s := Emit(v)
	if s == "" {
		t.Error("sum emit empty")
	}
}

func TestEmit_MoreTypes(t *testing.T) {
	tests := []struct {
		name string
		v    *GValue
	}{
		{"null", Null()},
		{"bool_true", Bool(true)},
		{"bool_false", Bool(false)},
		{"int", Int(42)},
		{"int_neg", Int(-5)},
		{"float", Float(3.14)},
		{"str", Str("hello")},
		{"str_needs_quote", Str("hello world")},
		{"bytes", Bytes([]byte{1, 2, 3})},
		{"list_empty", List()},
		{"list", List(Int(1), Int(2))},
		{"map_empty", Map()},
		{"map", Map(MapEntry{Key: "x", Value: Int(1)})},
		{"id", ID("d", "1")},
		{"time", Time(time.Now())},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := Emit(tt.v)
			if s == "" {
				t.Error("empty emit")
			}
		})
	}
}

func TestParse_Roundtrip(t *testing.T) {
	tests := []string{
		"42",
		"3.14",
		"true",
		"false",
		"null",
		"hello",
		"\"hello world\"",
		"[1 2 3]",
	}

	for _, input := range tests {
		pr, err := Parse(input)
		if err != nil {
			t.Errorf("Parse(%q): %v", input, err)
			continue
		}
		if pr.Value == nil {
			t.Errorf("Parse(%q): nil value", input)
			continue
		}
		s := Emit(pr.Value)
		if s == "" {
			t.Errorf("Emit for %q is empty", input)
		}
	}
}

func TestParseWithOptions_Tolerant(t *testing.T) {
	// Tolerant mode should handle minor errors
	pr, err := ParseWithOptions("{ x 1 y 2 }", ParseOptions{Tolerant: true})
	if err != nil {
		t.Fatalf("ParseWithOptions: %v", err)
	}
	if pr.Value == nil {
		t.Error("nil value")
	}
}
