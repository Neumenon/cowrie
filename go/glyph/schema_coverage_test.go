package glyph

import (
	"testing"
)

func TestSchemaBuilder_AllMethods(t *testing.T) {
	b := NewSchemaBuilder()

	// AddStruct
	b.AddStruct("Person", "1.0",
		Field("name", TypeSpec{Kind: TypeSpecStr}),
		Field("age", TypeSpec{Kind: TypeSpecInt}, WithOptional()),
	)

	// AddPackedStruct
	b.AddPackedStruct("Event", "1.0",
		Field("type", TypeSpec{Kind: TypeSpecStr}, WithWireKey("t")),
		Field("ts", TypeSpec{Kind: TypeSpecStr}, WithFID(1)),
	)

	// AddOpenStruct
	b.AddOpenStruct("Config", "1.0",
		Field("key", TypeSpec{Kind: TypeSpecStr}),
	)

	// AddOpenPackedStruct
	b.AddOpenPackedStruct("ExtConfig", "1.0",
		Field("key", TypeSpec{Kind: TypeSpecStr}),
	)

	// AddSum
	b.AddSum("Result", "1.0",
		Variant("Ok", TypeSpec{Kind: TypeSpecStr}),
		Variant("Err", TypeSpec{Kind: TypeSpecStr}),
	)

	// WithPack/WithTab/WithOpen by name
	b.WithPack("Person")
	b.WithTab("Person")
	b.WithOpen("Person")

	schema := b.Build()

	if schema == nil {
		t.Fatal("nil schema")
	}
	if schema.GetType("Person") == nil {
		t.Error("missing Person type")
	}
	if schema.GetType("Event") == nil {
		t.Error("missing Event type")
	}
	if schema.GetType("Result") == nil {
		t.Error("missing Result type")
	}
}

func TestFieldOptions(t *testing.T) {
	// WithConstraint
	f := Field("email", TypeSpec{Kind: TypeSpecStr},
		WithConstraint(Constraint{Kind: ConstraintRegex, Value: ".*@.*"}),
		WithDefault(Str("unknown")),
		WithKeepNull(),
		WithCodec("utf8"),
	)
	if f.Name != "email" {
		t.Error("field name")
	}
	if len(f.Constraints) != 1 {
		t.Error("expected 1 constraint")
	}
	if f.Default == nil {
		t.Error("expected default")
	}
	if !f.KeepNull {
		t.Error("expected KeepNull")
	}
	if f.Codec != "utf8" {
		t.Error("expected codec")
	}
}

func TestTypeOptions(t *testing.T) {
	td := &TypeDef{}

	opt := WithPack()
	opt(td)
	if !td.PackEnabled {
		t.Error("WithPack")
	}

	td2 := &TypeDef{}
	opt2 := WithTab()
	opt2(td2)
	if !td2.TabEnabled {
		t.Error("WithTab")
	}

	td3 := &TypeDef{}
	opt3 := WithOpen()
	opt3(td3)
	if !td3.Open {
		t.Error("WithOpen")
	}
}

func TestSchemaComputeHash(t *testing.T) {
	s1 := NewSchemaBuilder().
		AddStruct("A", "1.0", Field("x", TypeSpec{Kind: TypeSpecInt})).
		Build()

	s2 := NewSchemaBuilder().
		AddStruct("A", "1.0", Field("x", TypeSpec{Kind: TypeSpecInt})).
		Build()

	if s1.Hash != s2.Hash {
		t.Error("identical schemas should have same hash")
	}
}

func TestSchemaGetType(t *testing.T) {
	s := NewSchemaBuilder().
		AddStruct("T", "1.0", Field("x", TypeSpec{Kind: TypeSpecInt})).
		Build()

	if s.GetType("T") == nil {
		t.Error("should find type T")
	}
	if s.GetType("missing") != nil {
		t.Error("should not find missing type")
	}
}

func TestConstraint_Compile(t *testing.T) {
	// Regex
	c := Constraint{Kind: ConstraintRegex, Value: "^[a-z]+$"}
	cc, err := c.Compile()
	if err != nil {
		t.Fatalf("Compile regex: %v", err)
	}
	if cc.Regex == nil {
		t.Error("expected compiled regex")
	}

	// Enum
	c2 := Constraint{Kind: ConstraintEnum, Value: []string{"a", "b", "c"}}
	cc2, err := c2.Compile()
	if err != nil {
		t.Fatalf("Compile enum: %v", err)
	}
	if len(cc2.EnumSet) != 3 {
		t.Error("expected 3 enum values")
	}
}

func TestVariant(t *testing.T) {
	v := Variant("Ok", TypeSpec{Kind: TypeSpecStr})
	if v.Tag != "Ok" {
		t.Error("variant tag")
	}
}
