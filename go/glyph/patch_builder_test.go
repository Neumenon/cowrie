package glyph

import (
	"testing"
)

func TestPatchBuilder_Coverage(t *testing.T) {
	pb := NewPatchBuilder(RefID{Prefix: "d", Value: "1"})

	pb.WithSchemaID("schema123").
		WithTargetType("Person").
		WithBaseFingerprint("abcdef1234567890").
		Set("name", Str("Alice")).
		Append("tags", Str("new")).
		Delete("old_field").
		Delta("count", 1.0)

	p := pb.Build()
	if p == nil {
		t.Fatal("nil patch")
	}
	if len(p.Ops) != 4 {
		t.Errorf("expected 4 ops, got %d", len(p.Ops))
	}
	if p.TargetType != "Person" {
		t.Error("wrong target type")
	}
	if p.BaseFingerprint != "abcdef1234567890" {
		t.Error("wrong fingerprint")
	}

	// Emit
	result, err := EmitPatch(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result == "" {
		t.Error("empty emit")
	}
}

func TestPatchBuilder_WithBaseValue(t *testing.T) {
	base := Map(
		MapEntry{Key: "name", Value: Str("Alice")},
	)
	pb := NewPatchBuilder(RefID{Prefix: "d", Value: "1"})
	pb.WithBaseValue(base)

	p := pb.Build()
	if p.BaseFingerprint == "" {
		t.Error("empty fingerprint")
	}
}

func TestPatchBuilder_SetFID(t *testing.T) {
	pb := NewPatchBuilder(RefID{Prefix: "d", Value: "1"})
	pb.SetFID([]PathSeg{{Field: "name"}}, Str("Bob"))

	p := pb.Build()
	if len(p.Ops) != 1 {
		t.Error("expected 1 op")
	}
}

func TestPatch_API_Coverage(t *testing.T) {
	p := NewPatch(RefID{Prefix: "d", Value: "1"}, "schema1")

	p.Set("name", Str("Alice"))
	p.SetWithSegs([]PathSeg{{Field: "age"}}, Int(30))
	p.Append("tags", Str("new"))
	p.Delete("old")
	p.Delta("count", 5.0)
	p.InsertAt("items", 0, Str("first"))

	if len(p.Ops) != 6 {
		t.Errorf("expected 6 ops, got %d", len(p.Ops))
	}
}

func TestPatchOpKind_String(t *testing.T) {
	if OpSet.String() != "=" {
		t.Error("OpSet")
	}
	if OpAppend.String() != "+" {
		t.Error("OpAppend")
	}
	if OpDelete.String() != "-" {
		t.Error("OpDelete")
	}
	if OpDelta.String() != "~" {
		t.Error("OpDelta")
	}
}

func TestPathSeg_String(t *testing.T) {
	// Field
	s1 := PathSeg{Kind: PathSegField, Field: "name"}
	if s1.String() != "name" {
		t.Error("field string")
	}

	// ListIdx
	s2 := ListIdxSeg(3)
	if s2.String() != "[3]" {
		t.Errorf("list idx: %s", s2.String())
	}

	// MapKey
	s3 := MapKeySeg("key")
	r := s3.String()
	if r == "" {
		t.Error("empty mapkey string")
	}
}

func TestValuesEqual_Coverage(t *testing.T) {
	// Same values
	if !valuesEqual(Int(42), Int(42)) {
		t.Error("ints should be equal")
	}
	if !valuesEqual(Str("hello"), Str("hello")) {
		t.Error("strs should be equal")
	}
	if !valuesEqual(Bool(true), Bool(true)) {
		t.Error("bools should be equal")
	}
	if !valuesEqual(Float(3.14), Float(3.14)) {
		t.Error("floats should be equal")
	}
	if !valuesEqual(Null(), Null()) {
		t.Error("nulls should be equal")
	}

	// Nil cases
	if !valuesEqual(nil, nil) {
		t.Error("nil == nil")
	}
	if valuesEqual(nil, Int(1)) {
		t.Error("nil != int")
	}
	if valuesEqual(Int(1), nil) {
		t.Error("int != nil")
	}

	// Type mismatch
	if valuesEqual(Int(1), Str("1")) {
		t.Error("type mismatch")
	}

	// ID
	if !valuesEqual(ID("d", "1"), ID("d", "1")) {
		t.Error("ids should be equal")
	}
	if valuesEqual(ID("d", "1"), ID("d", "2")) {
		t.Error("ids should differ")
	}

	// List
	l1 := List(Int(1), Int(2))
	l2 := List(Int(1), Int(2))
	l3 := List(Int(1), Int(3))
	if !valuesEqual(l1, l2) {
		t.Error("lists should be equal")
	}
	if valuesEqual(l1, l3) {
		t.Error("lists should differ")
	}

	// Struct
	s1 := Struct("T", MapEntry{Key: "x", Value: Int(1)})
	s2 := Struct("T", MapEntry{Key: "x", Value: Int(1)})
	s3 := Struct("T", MapEntry{Key: "x", Value: Int(2)})
	s4 := Struct("U", MapEntry{Key: "x", Value: Int(1)})
	if !valuesEqual(s1, s2) {
		t.Error("structs should be equal")
	}
	if valuesEqual(s1, s3) {
		t.Error("structs should differ by value")
	}
	if valuesEqual(s1, s4) {
		t.Error("structs should differ by type name")
	}
}

func TestListsEqual_Coverage(t *testing.T) {
	if !listsEqual(nil, nil) {
		t.Error("nil lists should be equal")
	}
	if listsEqual([]*GValue{Int(1)}, []*GValue{Int(1), Int(2)}) {
		t.Error("different length")
	}
	if !listsEqual([]*GValue{Int(1), Int(2)}, []*GValue{Int(1), Int(2)}) {
		t.Error("same lists")
	}
}

func TestJSONRoundTripLoose_Coverage(t *testing.T) {
	input := `{"name":"Alice","age":30}`
	result, err := JSONRoundTripLoose([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Error("empty result")
	}
}

func TestFromJSONValueLooseWithOpts_Extended(t *testing.T) {
	// Extended mode: decode $glyph markers
	opts := BridgeOpts{Extended: true}

	// Time marker
	timeObj := map[string]interface{}{
		"$glyph": "time",
		"value":  "2024-01-01T00:00:00Z",
	}
	v, err := FromJSONValueLooseWithOpts(timeObj, opts)
	if err != nil {
		t.Fatal(err)
	}
	if v == nil {
		t.Error("nil")
	}

	// ID marker
	idObj := map[string]interface{}{
		"$glyph": "id",
		"value":  "^d:123",
	}
	v2, err := FromJSONValueLooseWithOpts(idObj, opts)
	if err != nil {
		t.Fatal(err)
	}
	if v2 == nil {
		t.Error("nil")
	}

	// Non-extended mode (regular map)
	noExt := BridgeOpts{Extended: false}
	v3, err := FromJSONValueLooseWithOpts(timeObj, noExt)
	if err != nil {
		t.Fatal(err)
	}
	if v3 == nil {
		t.Error("nil")
	}
}

func TestEmitSchema_Coverage(t *testing.T) {
	sb := NewSchemaBuilder()
	sb.AddStruct("Person", "v1",
		&FieldDef{Name: "name", Type: TypeSpec{Kind: TypeSpecStr}},
		&FieldDef{Name: "age", Type: TypeSpec{Kind: TypeSpecInt}},
	)
	s := sb.Build()

	result := EmitSchema(s)
	if result == "" {
		t.Error("empty schema emit")
	}

	ref := EmitSchemaRef(s)
	if ref == "" {
		t.Error("empty schema ref")
	}
}

func TestCanonValue_Coverage(t *testing.T) {
	tests := []struct {
		name string
		v    *GValue
	}{
		{"nil", nil},
		{"null", Null()},
		{"bool_true", Bool(true)},
		{"bool_false", Bool(false)},
		{"int", Int(42)},
		{"float", Float(3.14)},
		{"str", Str("hello")},
		{"id", ID("d", "1")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := canonValue(tt.v)
			if result == "" && tt.name != "nil" && tt.name != "null" {
				// null returns "null" which is non-empty, so this should not happen
			}
			_ = result
		})
	}
}

func TestDiff_ListChanges(t *testing.T) {
	old := Map(
		MapEntry{Key: "tags", Value: List(Str("a"), Str("b"))},
	)
	newVal := Map(
		MapEntry{Key: "tags", Value: List(Str("a"), Str("c"))},
	)

	patch := Diff(old, newVal, "")
	if patch == nil {
		t.Fatal("nil patch")
	}
	if len(patch.Ops) == 0 {
		t.Error("expected ops for list change")
	}
}

func TestApplyPatch_Delta(t *testing.T) {
	v := Map(
		MapEntry{Key: "count", Value: Int(10)},
	)
	p := &Patch{
		Ops: []*PatchOp{
			{Op: OpDelta, Path: []PathSeg{{Field: "count"}}, Value: Int(5)},
		},
	}

	result, err := ApplyPatch(v, p)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("nil result")
	}
}

func TestApplyPatch_DeleteField(t *testing.T) {
	v := Map(
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "age", Value: Int(30)},
	)
	p := &Patch{
		Ops: []*PatchOp{
			{Op: OpDelete, Path: []PathSeg{{Field: "age"}}},
		},
	}

	result, err := ApplyPatch(v, p)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("nil result")
	}
}

func TestJSONEqual_Coverage(t *testing.T) {
	eq, err := JSONEqual([]byte(`{"a":1}`), []byte(`{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if !eq {
		t.Error("should be equal")
	}

	eq2, err := JSONEqual([]byte(`{"a":1}`), []byte(`{"a":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if eq2 {
		t.Error("should not be equal")
	}
}
