package glyph

import (
	"testing"
	"time"
)

// ============================================================
// canon.go: quoteString edge cases
// ============================================================

func TestQuoteStringControlChars(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`hello`, `"hello"`},
		{`he"llo`, `"he\"llo"`},
		{"he\nllo", `"he\nllo"`},
		{"he\rllo", `"he\rllo"`},
		{"he\tllo", `"he\tllo"`},
		{"he\\llo", `"he\\llo"`},
		{string([]byte{0x01}), `"\u0001"`},
		{string([]byte{0x1f}), `"\u001F"`},
	}
	for _, tt := range tests {
		got := quoteString(tt.input)
		if got != tt.want {
			t.Errorf("quoteString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// ============================================================
// blob.go: AsBlob, unquoteBlobString, ParseBlobRef edge cases
// ============================================================

func TestAsBlobPanicsOnNonBlob(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	v := Int(42)
	v.AsBlob()
}

func TestAsBlobNilBlobVal(t *testing.T) {
	v := &GValue{typ: TypeBlob, blobVal: nil}
	ref := v.AsBlob()
	if ref.CID != "" {
		t.Fatalf("expected empty CID, got %q", ref.CID)
	}
}

func TestUnquoteBlobString(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{`hello`, "hello"},
		{`he\nllo`, "he\nllo"},
		{`he\rllo`, "he\rllo"},
		{`he\tllo`, "he\tllo"},
		{`he\\llo`, "he\\llo"},
		{`he\"llo`, "he\"llo"},
		{`he\xllo`, "hexllo"}, // unknown escape
	}
	for _, tt := range tests {
		got := unquoteBlobString(tt.input)
		if got != tt.want {
			t.Errorf("unquoteBlobString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseBlobRefWithQuotedValues(t *testing.T) {
	input := `@blob cid=abc123 mime=image/png bytes=1024 name="my file.png" caption="a test"`
	ref, err := ParseBlobRef(input)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Name != "my file.png" {
		t.Fatalf("expected 'my file.png', got %q", ref.Name)
	}
	if ref.Caption != "a test" {
		t.Fatalf("expected 'a test', got %q", ref.Caption)
	}
}

func TestParseBlobRefMissingRequired(t *testing.T) {
	_, err := ParseBlobRef("@blob mime=image/png bytes=1024")
	if err == nil {
		t.Fatal("expected error for missing CID")
	}
	_, err = ParseBlobRef("@blob cid=abc123 bytes=1024")
	if err == nil {
		t.Fatal("expected error for missing MIME")
	}
	_, err = ParseBlobRef("not a blob")
	if err == nil {
		t.Fatal("expected error for non-blob prefix")
	}
}

func TestParseBlobRefWithPreview(t *testing.T) {
	input := `@blob cid=abc123 mime=image/png bytes=1024 preview=thumb123`
	ref, err := ParseBlobRef(input)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Preview != "thumb123" {
		t.Fatalf("expected 'thumb123', got %q", ref.Preview)
	}
}

func TestParseBlobRefUnterminatedQuote(t *testing.T) {
	input := `@blob cid=abc123 mime=image/png name="unterminated`
	_, err := ParseBlobRef(input)
	if err == nil {
		t.Fatal("expected error for unterminated quote")
	}
}

// ============================================================
// schema.go: constraint constructors and GetFIDForField
// ============================================================

func TestConstraintConstructors(t *testing.T) {
	c := LenConstraint(10)
	if c.Kind != ConstraintLen || c.Value != 10 {
		t.Fatalf("LenConstraint: %v", c)
	}
	c = MaxLenConstraint(100)
	if c.Kind != ConstraintMaxLen || c.Value != 100 {
		t.Fatalf("MaxLenConstraint: %v", c)
	}
	c = MinLenConstraint(1)
	if c.Kind != ConstraintMinLen || c.Value != 1 {
		t.Fatalf("MinLenConstraint: %v", c)
	}
	c = RegexConstraint(`^[a-z]+$`)
	if c.Kind != ConstraintRegex || c.Value != `^[a-z]+$` {
		t.Fatalf("RegexConstraint: %v", c)
	}
	c = EnumConstraint([]string{"a", "b", "c"})
	if c.Kind != ConstraintEnum {
		t.Fatalf("EnumConstraint: %v", c)
	}
	c = NonEmptyConstraint()
	if c.Kind != ConstraintNonEmpty {
		t.Fatalf("NonEmptyConstraint: %v", c)
	}
	c = OptionalConstraint()
	if c.Kind != ConstraintOptional {
		t.Fatalf("OptionalConstraint: %v", c)
	}
}

func TestGetFIDForField(t *testing.T) {
	td := &TypeDef{
		Kind: TypeDefStruct,
		Struct: &StructDef{
			Fields: []*FieldDef{
				{Name: "alpha", FID: 1},
				{Name: "beta", FID: 2},
			},
		},
	}
	if fid := td.GetFIDForField("alpha"); fid != 1 {
		t.Fatalf("expected FID 1, got %d", fid)
	}
	if fid := td.GetFIDForField("beta"); fid != 2 {
		t.Fatalf("expected FID 2, got %d", fid)
	}
	if fid := td.GetFIDForField("missing"); fid != 0 {
		t.Fatalf("expected FID 0 for missing field, got %d", fid)
	}
}

// ============================================================
// validate.go: ValidationError.Error, isInteger
// ============================================================

func TestValidationErrorString(t *testing.T) {
	e := &ValidationError{Path: "$.name", Message: "required"}
	if got := e.Error(); got != "$.name: required" {
		t.Fatalf("expected '$.name: required', got %q", got)
	}
	e = &ValidationError{Message: "global error"}
	if got := e.Error(); got != "global error" {
		t.Fatalf("expected 'global error', got %q", got)
	}
}

func TestIsInteger(t *testing.T) {
	if !isInteger(42.0) {
		t.Fatal("42.0 should be integer")
	}
	if isInteger(42.5) {
		t.Fatal("42.5 should not be integer")
	}
}

// ============================================================
// stream_validator.go: Error, WithLimits, IsToolAllowed, MaxInt, MinInt
// ============================================================

func TestStreamValidationError(t *testing.T) {
	e := StreamValidationError{Code: "ERR", Message: "test"}
	if e.Error() != "ERR: test" {
		t.Fatalf("unexpected: %q", e.Error())
	}
	e = StreamValidationError{Code: "ERR", Message: "test", Field: "name"}
	if e.Error() != "ERR: test (name)" {
		t.Fatalf("unexpected: %q", e.Error())
	}
}

func TestWithLimits(t *testing.T) {
	reg := NewToolRegistry()
	v := NewStreamingValidator(reg)
	v.WithLimits(1024, 50, 10)
	if v.maxBufferSize != 1024 {
		t.Fatalf("expected maxBufferSize 1024, got %d", v.maxBufferSize)
	}
	if v.maxFieldCount != 50 {
		t.Fatalf("expected maxFieldCount 50, got %d", v.maxFieldCount)
	}
	if v.maxErrorCount != 10 {
		t.Fatalf("expected maxErrorCount 10, got %d", v.maxErrorCount)
	}

	// Zero values should not change limits
	v.WithLimits(0, 0, 0)
	if v.maxBufferSize != 1024 {
		t.Fatal("zero should not change limit")
	}
}

func TestIsToolAllowed(t *testing.T) {
	reg := NewToolRegistry()
	reg.Register(&ToolSchema{Name: "calculator"})
	v := NewStreamingValidator(reg)
	// Feed enough data to detect a tool
	v.toolName = "calculator"
	if !v.IsToolAllowed() {
		t.Fatal("registered tool should be allowed")
	}
	v.toolName = "unknown_tool"
	if v.IsToolAllowed() {
		t.Fatal("unregistered tool should not be allowed")
	}
}

func TestMaxIntMinInt(t *testing.T) {
	p := MaxInt(5)
	if *p != 5 {
		t.Fatalf("expected 5, got %d", *p)
	}
	p = MinInt(3)
	if *p != 3 {
		t.Fatalf("expected 3, got %d", *p)
	}
}

// ============================================================
// token.go: Token.String, PeekN, Position, Reset
// ============================================================

func TestTokenString(t *testing.T) {
	tok := Token{Type: TokenString, Value: "hello"}
	s := tok.String()
	if s == "" {
		t.Fatal("expected non-empty string")
	}
	// Empty value
	tok2 := Token{Type: TokenEOF}
	s2 := tok2.String()
	if s2 == "" {
		t.Fatal("expected non-empty string for EOF")
	}
}

func TestTokenStreamPeekNPositionReset(t *testing.T) {
	lexer := NewLexer(`name = "hello"`)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatal(err)
	}
	ts := NewTokenStream(tokens)

	if ts.Position() != 0 {
		t.Fatalf("expected position 0, got %d", ts.Position())
	}

	tok := ts.PeekN(0)
	if tok.Type == TokenEOF && len(tokens) > 0 {
		t.Fatal("PeekN(0) should not be EOF when tokens exist")
	}

	ts.Advance()
	if ts.Position() != 1 {
		t.Fatalf("expected position 1, got %d", ts.Position())
	}

	ts.Reset(0)
	if ts.Position() != 0 {
		t.Fatalf("expected position 0 after reset, got %d", ts.Position())
	}

	tok = ts.PeekN(9999)
	if tok.Type != TokenEOF {
		t.Fatal("PeekN beyond end should return EOF")
	}
}

// ============================================================
// emit.go: emitSum via EmitWithOptions
// ============================================================

func TestEmitSumNullValue(t *testing.T) {
	sv := Sum("MyTag", Null())
	result := Emit(sv)
	if result == "" {
		t.Fatal("expected non-empty sum emission")
	}
}

func TestEmitSumWithStruct(t *testing.T) {
	st := Struct("Item", MapEntry{Key: "name", Value: Str("test")})
	sv := Sum("MyTag", st)
	result := Emit(sv)
	if result == "" {
		t.Fatal("expected non-empty sum struct emission")
	}
}

// ============================================================
// emit_patch.go: Diff, applyOp, pathSegsToString, needsQuoting,
// PatchBuilder.WithSchema, emitPathSegs
// ============================================================

func TestDiffBasic(t *testing.T) {
	from := Struct("Item",
		MapEntry{Key: "name", Value: Str("old")},
		MapEntry{Key: "count", Value: Int(1)},
	)
	to := Struct("Item",
		MapEntry{Key: "name", Value: Str("new")},
		MapEntry{Key: "count", Value: Int(2)},
	)
	p := Diff(from, to, "Item")
	if len(p.Ops) == 0 {
		t.Fatal("expected diff ops")
	}
}

func TestDiffNilFrom(t *testing.T) {
	to := Str("hello")
	p := Diff(nil, to, "")
	if len(p.Ops) != 1 || p.Ops[0].Op != OpSet {
		t.Fatal("expected set op for nil-to-value diff")
	}
}

func TestDiffNilTo(t *testing.T) {
	from := Str("hello")
	p := Diff(from, nil, "")
	_ = p
}

func TestDiffBothNil(t *testing.T) {
	p := Diff(nil, nil, "")
	if len(p.Ops) != 0 {
		t.Fatal("expected no ops for nil-nil diff")
	}
}

func TestApplyOpRootSet(t *testing.T) {
	v := Str("old")
	op := &PatchOp{Op: OpSet, Path: nil, Value: Str("new")}
	result, err := applyOp(v, op)
	if err != nil {
		t.Fatal(err)
	}
	if result.strVal != "new" {
		t.Fatalf("expected 'new', got %q", result.strVal)
	}
}

func TestApplyOpRootNonSetError(t *testing.T) {
	v := Str("old")
	op := &PatchOp{Op: OpDelete, Path: nil}
	_, err := applyOp(v, op)
	if err == nil {
		t.Fatal("expected error for delete at root")
	}
}

func TestNeedsQuoting(t *testing.T) {
	if needsQuoting("simple") {
		t.Fatal("simple should not need quoting")
	}
	if !needsQuoting("") {
		t.Fatal("empty should need quoting")
	}
	if !needsQuoting("has space") {
		t.Fatal("string with space should need quoting")
	}
	if !needsQuoting("123start") {
		t.Fatal("digit-start should need quoting")
	}
	if needsQuoting("valid_key-name") {
		t.Fatal("underscores and dashes should not need quoting")
	}
}

func TestPatchBuilderWithSchema(t *testing.T) {
	pb := NewPatchBuilder(RefID{Prefix: "item", Value: "1"})
	schema := &Schema{Hash: "abc123"}
	pb.WithSchema(schema)
	if pb.patch.SchemaID != "abc123" {
		t.Fatalf("expected schema hash abc123, got %q", pb.patch.SchemaID)
	}
}

// ============================================================
// json_bridge.go: fromGlyphMarker, JSONEqual
// ============================================================

func TestFromGlyphMarkerTime(t *testing.T) {
	obj := map[string]interface{}{
		"$glyph": "time",
		"value":  "2024-01-15T10:30:00Z",
	}
	v, err := fromGlyphMarker("time", obj)
	if err != nil {
		t.Fatal(err)
	}
	if v.typ != TypeTime {
		t.Fatalf("expected time, got %v", v.typ)
	}
}

func TestFromGlyphMarkerTimeMissingValue(t *testing.T) {
	obj := map[string]interface{}{
		"$glyph": "time",
	}
	_, err := fromGlyphMarker("time", obj)
	if err == nil {
		t.Fatal("expected error for missing value")
	}
}

func TestFromGlyphMarkerTimeInvalid(t *testing.T) {
	obj := map[string]interface{}{
		"$glyph": "time",
		"value":  "not-a-time",
	}
	_, err := fromGlyphMarker("time", obj)
	if err == nil {
		t.Fatal("expected error for invalid time")
	}
}

func TestFromGlyphMarkerID(t *testing.T) {
	obj := map[string]interface{}{
		"$glyph": "id",
		"value":  "^item:42",
	}
	v, err := fromGlyphMarker("id", obj)
	if err != nil {
		t.Fatal(err)
	}
	if v.typ != TypeID {
		t.Fatalf("expected id, got %v", v.typ)
	}
}

func TestFromGlyphMarkerIDMissing(t *testing.T) {
	obj := map[string]interface{}{
		"$glyph": "id",
	}
	_, err := fromGlyphMarker("id", obj)
	if err == nil {
		t.Fatal("expected error for missing value")
	}
}

func TestFromGlyphMarkerBytes(t *testing.T) {
	obj := map[string]interface{}{
		"$glyph": "bytes",
		"base64": "aGVsbG8=",
	}
	v, err := fromGlyphMarker("bytes", obj)
	if err != nil {
		t.Fatal(err)
	}
	if v.typ != TypeBytes {
		t.Fatalf("expected bytes, got %v", v.typ)
	}
}

func TestFromGlyphMarkerBytesMissing(t *testing.T) {
	obj := map[string]interface{}{
		"$glyph": "bytes",
	}
	_, err := fromGlyphMarker("bytes", obj)
	if err == nil {
		t.Fatal("expected error for missing base64")
	}
}

func TestJSONEqual(t *testing.T) {
	eq, err := JSONEqual([]byte(`{"a":1,"b":2}`), []byte(`{"b":2,"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if !eq {
		t.Fatal("equal JSON objects should be JSONEqual")
	}

	eq, err = JSONEqual([]byte(`{"a":1}`), []byte(`{"a":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if eq {
		t.Fatal("different JSON should not be JSONEqual")
	}

	eq, err = JSONEqual([]byte(`[1,2,3]`), []byte(`[1,2,3]`))
	if err != nil {
		t.Fatal(err)
	}
	if !eq {
		t.Fatal("equal JSON arrays should be JSONEqual")
	}
}

// ============================================================
// emit_packed.go: EmitPacked with various types
// ============================================================

func TestEmitPackedValueTypes(t *testing.T) {
	schema := &Schema{
		Types: map[string]*TypeDef{
			"Item": {
				Name: "Item",
				Kind: TypeDefStruct,
				Struct: &StructDef{
					Fields: []*FieldDef{
						{Name: "name", FID: 1, Type: TypeSpec{Kind: TypeSpecStr}},
						{Name: "count", FID: 2, Type: TypeSpec{Kind: TypeSpecInt}},
						{Name: "active", FID: 3, Type: TypeSpec{Kind: TypeSpecBool}},
						{Name: "score", FID: 4, Type: TypeSpec{Kind: TypeSpecFloat}},
					},
				},
				PackEnabled: true,
			},
		},
	}
	v := Struct("Item",
		MapEntry{Key: "name", Value: Str("test")},
		MapEntry{Key: "count", Value: Int(42)},
		MapEntry{Key: "active", Value: Bool(true)},
		MapEntry{Key: "score", Value: Float(3.14)},
	)
	packed, err := EmitPacked(v, schema)
	if err != nil {
		t.Fatal(err)
	}
	if packed == "" {
		t.Fatal("expected non-empty packed output")
	}
}

func TestEmitPackedValueNull(t *testing.T) {
	schema := &Schema{
		Types: map[string]*TypeDef{
			"Item": {
				Name: "Item",
				Kind: TypeDefStruct,
				Struct: &StructDef{
					Fields: []*FieldDef{
						{Name: "name", FID: 1, Type: TypeSpec{Kind: TypeSpecStr}, Optional: true},
					},
				},
				PackEnabled: true,
			},
		},
	}
	v := Struct("Item",
		MapEntry{Key: "name", Value: Null()},
	)
	packed, err := EmitPacked(v, schema)
	if err != nil {
		t.Fatal(err)
	}
	if packed == "" {
		t.Fatal("expected non-empty packed output")
	}
}

func TestEmitPackedNonStruct(t *testing.T) {
	schema := &Schema{}
	_, err := EmitPacked(Int(42), schema)
	if err == nil {
		t.Fatal("expected error for non-struct")
	}
	_, err = EmitPacked(nil, schema)
	if err == nil {
		t.Fatal("expected error for nil")
	}
}

// ============================================================
// emit_tabular.go: TabularWriter.WriteRow
// ============================================================

func TestTabularWriterWriteRow(t *testing.T) {
	td := &TypeDef{
		Name: "Row",
		Kind: TypeDefStruct,
		Struct: &StructDef{
			Fields: []*FieldDef{
				{Name: "a", FID: 1, Type: TypeSpec{Kind: TypeSpecStr}},
				{Name: "b", FID: 2, Type: TypeSpec{Kind: TypeSpecInt}},
			},
		},
	}
	tw := NewTabularWriter(td, TabularOptions{})
	row := Struct("Row",
		MapEntry{Key: "a", Value: Str("hello")},
		MapEntry{Key: "b", Value: Int(42)},
	)
	err := tw.WriteRow(row)
	if err != nil {
		t.Fatal(err)
	}
	result, err := tw.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if result == "" {
		t.Fatal("expected non-empty tabular output")
	}
}

// ============================================================
// incremental.go: NewIncrementalParser
// ============================================================

func TestIncrementalParserBasic(t *testing.T) {
	events := []ParseEvent{}
	handler := func(evt ParseEvent) error {
		events = append(events, evt)
		return nil
	}
	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
	if p == nil {
		t.Fatal("expected non-nil parser")
	}
	_, err := p.Feed([]byte(`{"name": "hello", "count": 42}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Fatal("expected parse events")
	}
}

// ============================================================
// decimal128.go: Add
// ============================================================

func TestDecimal128Add(t *testing.T) {
	a := NewDecimal128FromInt64(100)
	b := NewDecimal128FromInt64(200)
	c, err := a.Add(b)
	if err != nil {
		t.Fatal(err)
	}
	if c.IsZero() {
		t.Fatal("sum should not be zero")
	}
}

// ============================================================
// emit.go: EmitSchemaRef
// ============================================================

func TestEmitSchemaRef(t *testing.T) {
	s := EmitSchemaRef(&Schema{Hash: "abc123"})
	if s != "@schema#abc123" {
		t.Fatalf("expected '@schema#abc123', got %q", s)
	}
}

// ============================================================
// loose.go: various functions via roundtrip
// ============================================================

func TestLooseParseAndEmitTime(t *testing.T) {
	input := `{
  created = 2024-01-15T10:30:00Z
}`
	result, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Value == nil {
		t.Fatal("expected non-nil value")
	}
	output := Emit(result.Value)
	if output == "" {
		t.Fatal("expected non-empty output")
	}
}

func TestLooseParseAndEmitBytes(t *testing.T) {
	input := `{
  data = b64"aGVsbG8="
}`
	result, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Value == nil {
		t.Fatal("expected non-nil value")
	}
}

func TestLooseParseNestedStructs(t *testing.T) {
	input := `{
  user = {
    name = "Alice"
    age = 30
    tags = ["admin" "user"]
    metadata = {
      key1 = "value1"
      key2 = 42
    }
  }
}`
	result, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Value == nil {
		t.Fatal("expected non-nil value")
	}
	output := Emit(result.Value)
	if output == "" {
		t.Fatal("expected non-empty output")
	}
}

func TestLooseParseRef(t *testing.T) {
	input := `{
  owner = ^user:123
}`
	result, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Value == nil {
		t.Fatal("expected non-nil value")
	}
}

func TestLooseParseNullValues(t *testing.T) {
	input := `{
  a = null
  b = null
}`
	result, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Value == nil {
		t.Fatal("expected non-nil value")
	}
}

// ============================================================
// pool.go: ClearAll
// ============================================================

func TestPoolRegistryClearAll(t *testing.T) {
	r := NewPoolRegistry()
	r.Define(&Pool{ID: "p1", Entries: []*GValue{Str("hello")}})
	r.Define(&Pool{ID: "p2", Entries: []*GValue{Str("world")}})
	r.ClearAll()
	// After clearing, pools should be gone
	if r.Get("p1") != nil {
		t.Fatal("expected nil after ClearAll")
	}
}

// ============================================================
// emit_patch.go: applyAtPathSegs (struct navigation, list index)
// ============================================================

func TestApplyPatchToStructField(t *testing.T) {
	v := Struct("Item",
		MapEntry{Key: "name", Value: Str("old")},
		MapEntry{Key: "count", Value: Int(1)},
	)
	op := &PatchOp{
		Op:    OpSet,
		Path:  []PathSeg{{Kind: PathSegField, Field: "name"}},
		Value: Str("new"),
	}
	result, err := applyOp(v, op)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range result.structVal.Fields {
		if f.Key == "name" {
			if f.Value.strVal != "new" {
				t.Fatalf("expected 'new', got %q", f.Value.strVal)
			}
			return
		}
	}
	t.Fatal("name field not found")
}

// TestApplyPatchDelete tests a delete operation on a struct field.
func TestApplyPatchDelete(t *testing.T) {
	v := Struct("Item",
		MapEntry{Key: "name", Value: Str("old")},
		MapEntry{Key: "count", Value: Int(1)},
	)
	op := &PatchOp{
		Op:   OpDelete,
		Path: []PathSeg{{Kind: PathSegField, Field: "name"}},
	}
	result, err := applyOp(v, op)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestApplyPatchDeepNavigation(t *testing.T) {
	v := Struct("Outer",
		MapEntry{Key: "inner", Value: Struct("Inner",
			MapEntry{Key: "value", Value: Str("old")},
		)},
	)
	op := &PatchOp{
		Op: OpSet,
		Path: []PathSeg{
			{Kind: PathSegField, Field: "inner"},
			{Kind: PathSegField, Field: "value"},
		},
		Value: Str("new"),
	}
	result, err := applyOp(v, op)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// ============================================================
// document.go: ParseDocument basic
// ============================================================

func TestParseDocumentBasic(t *testing.T) {
	input := `name = "test"
count = 42`
	doc, err := ParseDocument(input)
	if err != nil {
		t.Fatal(err)
	}
	if doc == nil {
		t.Fatal("expected non-nil document")
	}
}

// ============================================================
// Various GValue type helpers
// ============================================================

func TestGValueHelpers(t *testing.T) {
	v := &GValue{typ: TypeBlob}
	if !v.IsBlob() {
		t.Fatal("expected IsBlob true")
	}
	if Int(1).IsBlob() {
		t.Fatal("expected IsBlob false for int")
	}
	var nilV *GValue
	if nilV.IsBlob() {
		t.Fatal("expected IsBlob false for nil")
	}
}

// ============================================================
// emit_patch.go: pathSegsToString various modes
// ============================================================

func TestPathSegsToString(t *testing.T) {
	path := []PathSeg{
		{Kind: PathSegField, Field: "a"},
		{Kind: PathSegListIdx, ListIdx: 0},
		{Kind: PathSegField, Field: "b"},
	}
	s := pathSegsToString(path, KeyModeName)
	if s == "" {
		t.Fatal("expected non-empty path string")
	}

	path2 := []PathSeg{
		{Kind: PathSegField, Field: "a", FID: 1},
		{Kind: PathSegMapKey, MapKey: "key"},
	}
	s = pathSegsToString(path2, KeyModeFID)
	if s == "" {
		t.Fatal("expected non-empty path string for FID mode")
	}
}

// ============================================================
// auto_pool.go: AutoPoolEncode
// ============================================================

func TestAutoPoolBasic(t *testing.T) {
	v := Struct("Item",
		MapEntry{Key: "name", Value: Str("this is a long enough string to pool")},
		MapEntry{Key: "tag", Value: Str("this is a long enough string to pool")},
		MapEntry{Key: "label", Value: Str("this is a long enough string to pool")},
		MapEntry{Key: "other", Value: Str("world")},
	)
	opts := DefaultAutoPoolOpts()
	result, err := AutoPoolEncode(v, opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output == "" {
		t.Fatal("expected non-empty auto pool result")
	}
}

func TestAutoPoolEncodeNil(t *testing.T) {
	_, err := AutoPoolEncode(nil, DefaultAutoPoolOpts())
	if err == nil {
		t.Fatal("expected error for nil value")
	}
}

// ============================================================
// Time roundtrip
// ============================================================

func TestTimeRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	v := Time(now)
	s := Emit(v)
	if s == "" {
		t.Fatal("expected non-empty time emission")
	}
}

// ============================================================
// emit_patch.go: emitPathSegs with various segment types
// ============================================================

func TestEmitPatchWithMapKeyPath(t *testing.T) {
	from := Map(
		MapEntry{Key: "x", Value: Str("old")},
	)
	to := Map(
		MapEntry{Key: "x", Value: Str("new")},
	)
	p := Diff(from, to, "")
	if len(p.Ops) == 0 {
		t.Fatal("expected diff ops for map change")
	}
}

// ============================================================
// loose.go: writeStructLoose, writeCanonRef via Emit
// ============================================================

func TestEmitStructWithRef(t *testing.T) {
	v := Struct("Item",
		MapEntry{Key: "name", Value: Str("test")},
		MapEntry{Key: "ref", Value: ID("user", "123")},
	)
	s := Emit(v)
	if s == "" {
		t.Fatal("expected non-empty struct emission with ref")
	}
}

func TestEmitListWithMixedTypes(t *testing.T) {
	v := List(
		Str("hello"),
		Int(42),
		Float(3.14),
		Bool(true),
		Null(),
	)
	s := Emit(v)
	if s == "" {
		t.Fatal("expected non-empty list emission")
	}
}

// ============================================================
// loose.go: unquoteString, tryParseNumber
// ============================================================

func TestLooseParseNumbers(t *testing.T) {
	tests := []struct {
		input string
	}{
		{`{ v = 42 }`},
		{`{ v = -42 }`},
		{`{ v = 3.14 }`},
		{`{ v = -3.14 }`},
		{`{ v = 0 }`},
		{`{ v = 1e10 }`},
	}
	for _, tt := range tests {
		result, err := Parse(tt.input)
		if err != nil {
			t.Errorf("Parse(%q): %v", tt.input, err)
			continue
		}
		if result == nil || result.Value == nil {
			t.Errorf("Parse(%q) = nil", tt.input)
		}
	}
}

func TestLooseParseStringsWithEscapes(t *testing.T) {
	input := `{
  a = "hello\nworld"
  b = "tab\there"
  c = "quote\"inside"
  d = "backslash\\"
}`
	result, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Value == nil {
		t.Fatal("expected non-nil value")
	}
}

// ============================================================
// auto_pool.go: deeper coverage of writeValueWithRefs
// ============================================================

func TestAutoPoolEncodeAllTypes(t *testing.T) {
	v := Struct("Data",
		MapEntry{Key: "s1", Value: Str("this is a repeated long string for pool testing")},
		MapEntry{Key: "s2", Value: Str("this is a repeated long string for pool testing")},
		MapEntry{Key: "s3", Value: Str("this is a repeated long string for pool testing")},
		MapEntry{Key: "num", Value: Int(42)},
		MapEntry{Key: "flag", Value: Bool(true)},
		MapEntry{Key: "score", Value: Float(3.14)},
		MapEntry{Key: "nothing", Value: Null()},
		MapEntry{Key: "ref", Value: ID("item", "123")},
		MapEntry{Key: "items", Value: List(
			Str("this is a repeated long string for pool testing"),
			Int(1),
		)},
		MapEntry{Key: "nested", Value: Map(
			MapEntry{Key: "inner", Value: Str("this is a repeated long string for pool testing")},
		)},
	)
	opts := DefaultAutoPoolOpts()
	opts.MinLength = 10
	result, err := AutoPoolEncode(v, opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output == "" {
		t.Fatal("expected non-empty output")
	}
	if result.Stats.PooledStrings == 0 {
		t.Fatal("expected some strings to be pooled")
	}
}

func TestAutoPoolEncodeJSON2(t *testing.T) {
	jsonData := []byte(`{
		"name": "this is a repeated long string for pool testing",
		"label": "this is a repeated long string for pool testing",
		"tag": "this is a repeated long string for pool testing"
	}`)
	opts := DefaultAutoPoolOpts()
	opts.MinLength = 10
	result, err := AutoPoolEncodeJSON(jsonData, opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output == "" {
		t.Fatal("expected non-empty output")
	}
}

// ============================================================
// bridge.go: FromJSON, ToJSONString, DecodeBinary
// ============================================================

func TestFromJSONAndToJSONString(t *testing.T) {
	jsonData := []byte(`{"name":"Alice","age":30}`)
	v, err := FromJSON(jsonData)
	if err != nil {
		t.Fatal(err)
	}
	if v == nil {
		t.Fatal("expected non-nil value")
	}
	s, err := ToJSONString(v)
	if err != nil {
		t.Fatal(err)
	}
	if s == "" {
		t.Fatal("expected non-empty JSON string")
	}
}

func TestDecodeBinary(t *testing.T) {
	v := Struct("Item",
		MapEntry{Key: "name", Value: Str("test")},
	)
	data, err := EncodeBinary(v)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := DecodeBinary(data)
	if err != nil {
		t.Fatal(err)
	}
	if v2 == nil {
		t.Fatal("expected non-nil value")
	}
}

// ============================================================
// canon.go: canonValue more types
// ============================================================

func TestCanonValueAllTypes(t *testing.T) {
	tests := []*GValue{
		Null(),
		Bool(true),
		Bool(false),
		Int(0),
		Int(-42),
		Int(999),
		Float(0.0),
		Float(3.14),
		Float(-0.0),
		Str(""),
		Str("hello"),
		ID("user", "123"),
		List(Int(1), Int(2)),
		Map(MapEntry{Key: "a", Value: Int(1)}),
		Bytes([]byte{1, 2, 3}),
	}
	for _, v := range tests {
		s := canonValue(v)
		_ = s // Just ensure no panic
	}
}

// ============================================================
// emit_packed.go: emitPackedValue more types
// ============================================================

func TestEmitPackedWithList(t *testing.T) {
	schema := &Schema{
		Types: map[string]*TypeDef{
			"Item": {
				Name: "Item",
				Kind: TypeDefStruct,
				Struct: &StructDef{
					Fields: []*FieldDef{
						{Name: "tags", FID: 1, Type: TypeSpec{Kind: TypeSpecList, Elem: &TypeSpec{Kind: TypeSpecStr}}},
						{Name: "ref", FID: 2, Type: TypeSpec{Kind: TypeSpecStr}},
					},
				},
				PackEnabled: true,
			},
		},
	}
	v := Struct("Item",
		MapEntry{Key: "tags", Value: List(Str("a"), Str("b"))},
		MapEntry{Key: "ref", Value: ID("user", "1")},
	)
	packed, err := EmitPacked(v, schema)
	if err != nil {
		t.Fatal(err)
	}
	if packed == "" {
		t.Fatal("expected non-empty packed output")
	}
}

func TestEmitPackedWithMap(t *testing.T) {
	schema := &Schema{
		Types: map[string]*TypeDef{
			"Item": {
				Name: "Item",
				Kind: TypeDefStruct,
				Struct: &StructDef{
					Fields: []*FieldDef{
						{Name: "meta", FID: 1, Type: TypeSpec{Kind: TypeSpecMap}},
					},
				},
				PackEnabled: true,
			},
		},
	}
	v := Struct("Item",
		MapEntry{Key: "meta", Value: Map(
			MapEntry{Key: "key1", Value: Str("val1")},
			MapEntry{Key: "key2", Value: Int(42)},
		)},
	)
	packed, err := EmitPacked(v, schema)
	if err != nil {
		t.Fatal(err)
	}
	if packed == "" {
		t.Fatal("expected non-empty packed output")
	}
}

func TestEmitPackedWithTime(t *testing.T) {
	schema := &Schema{
		Types: map[string]*TypeDef{
			"Item": {
				Name: "Item",
				Kind: TypeDefStruct,
				Struct: &StructDef{
					Fields: []*FieldDef{
						{Name: "created", FID: 1, Type: TypeSpec{Kind: TypeSpecTime}},
					},
				},
				PackEnabled: true,
			},
		},
	}
	v := Struct("Item",
		MapEntry{Key: "created", Value: Time(time.Now().UTC())},
	)
	packed, err := EmitPacked(v, schema)
	if err != nil {
		t.Fatal(err)
	}
	if packed == "" {
		t.Fatal("expected non-empty packed output")
	}
}

func TestEmitPackedWithBytes(t *testing.T) {
	schema := &Schema{
		Types: map[string]*TypeDef{
			"Item": {
				Name: "Item",
				Kind: TypeDefStruct,
				Struct: &StructDef{
					Fields: []*FieldDef{
						{Name: "data", FID: 1, Type: TypeSpec{Kind: TypeSpecBytes}},
					},
				},
				PackEnabled: true,
			},
		},
	}
	v := Struct("Item",
		MapEntry{Key: "data", Value: Bytes([]byte{1, 2, 3})},
	)
	packed, err := EmitPacked(v, schema)
	if err != nil {
		t.Fatal(err)
	}
	if packed == "" {
		t.Fatal("expected non-empty packed output")
	}
}

// ============================================================
// Streaming validator: feed tokens
// ============================================================

func TestStreamingValidatorFeed(t *testing.T) {
	reg := NewToolRegistry()
	reg.Register(&ToolSchema{
		Name: "calculator",
		Args: map[string]ArgSchema{
			"expression": {Type: "string", Required: true},
		},
	})
	v := NewStreamingValidator(reg)
	v.Start()
	// Feed tokens one at a time
	tokens := []string{`{"tool":"calculator","args":{"expression":"2+2"}}`}
	for _, tok := range tokens {
		v.PushToken(tok)
	}
	result := v.GetResult()
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// ============================================================
// Incremental parser: various inputs
// ============================================================

func TestIncrementalParserArray(t *testing.T) {
	events := []ParseEvent{}
	handler := func(evt ParseEvent) error {
		events = append(events, evt)
		return nil
	}
	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
	_, err := p.Feed([]byte(`[1, "hello", true, null]`))
	if err != nil {
		t.Fatal(err)
	}
}

func TestIncrementalParserNested(t *testing.T) {
	events := []ParseEvent{}
	handler := func(evt ParseEvent) error {
		events = append(events, evt)
		return nil
	}
	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
	_, err := p.Feed([]byte(`{"a": {"b": [1,2]}, "c": "hello"}`))
	if err != nil {
		t.Fatal(err)
	}
}

// ============================================================
// Diff with list/map changes
// ============================================================

func TestDiffListChanges(t *testing.T) {
	from := Struct("Item",
		MapEntry{Key: "tags", Value: List(Str("a"), Str("b"))},
	)
	to := Struct("Item",
		MapEntry{Key: "tags", Value: List(Str("a"), Str("c"))},
	)
	p := Diff(from, to, "Item")
	if len(p.Ops) == 0 {
		t.Fatal("expected diff ops for list change")
	}
}

func TestDiffMapChanges(t *testing.T) {
	from := Map(
		MapEntry{Key: "a", Value: Int(1)},
		MapEntry{Key: "b", Value: Int(2)},
	)
	to := Map(
		MapEntry{Key: "a", Value: Int(1)},
		MapEntry{Key: "b", Value: Int(3)},
		MapEntry{Key: "c", Value: Int(4)},
	)
	p := Diff(from, to, "")
	if len(p.Ops) == 0 {
		t.Fatal("expected diff ops for map change")
	}
}

func TestDiffTypeChange(t *testing.T) {
	from := Int(42)
	to := Str("hello")
	p := Diff(from, to, "")
	if len(p.Ops) == 0 {
		t.Fatal("expected diff ops for type change")
	}
}
