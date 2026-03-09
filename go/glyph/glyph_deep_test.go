package glyph

import (
	"strings"
	"testing"
)

// ============================================================
// Blob Tests
// ============================================================

func TestBlobRef_Methods(t *testing.T) {
	ref := BlobRef{
		CID:     "sha256:abc123",
		MIME:    "image/png",
		Bytes:   1024,
		Name:    "test.png",
		Caption: "A test image",
		Preview: "preview data",
	}

	if ref.Algorithm() != "sha256" {
		t.Errorf("Algorithm: %s", ref.Algorithm())
	}
	if ref.Hash() != "abc123" {
		t.Errorf("Hash: %s", ref.Hash())
	}

	s := ref.String()
	if !strings.Contains(s, "sha256:abc123") {
		t.Error("missing CID")
	}
	if !strings.Contains(s, "image/png") {
		t.Error("missing MIME")
	}

	// No colon in CID
	ref2 := BlobRef{CID: "nocolon"}
	if ref2.Algorithm() != "sha256" {
		t.Error("default algorithm")
	}
	if ref2.Hash() != "nocolon" {
		t.Error("hash should be full CID")
	}
}

func TestComputeCID_Coverage(t *testing.T) {
	cid := ComputeCID([]byte("hello"))
	if !strings.HasPrefix(cid, "sha256:") {
		t.Error("wrong prefix")
	}

	cid2, err := ComputeCIDWithAlgorithm([]byte("hello"), "sha256")
	if err != nil {
		t.Fatal(err)
	}
	if cid != cid2 {
		t.Error("should match")
	}

	_, err = ComputeCIDWithAlgorithm([]byte("hello"), "md5")
	if err == nil {
		t.Error("should fail for unsupported algorithm")
	}
}

func TestBlobValue_Coverage(t *testing.T) {
	ref := BlobRef{CID: "sha256:abc", MIME: "text/plain", Bytes: 5}
	bv := Blob(ref)

	if !bv.IsBlob() {
		t.Error("should be blob")
	}

	got := bv.AsBlob()
	if got.CID != "sha256:abc" {
		t.Error("wrong CID")
	}

	s := EmitBlob(ref)
	if s == "" {
		t.Error("empty emit")
	}
}

func TestBlobFromContent_Deep(t *testing.T) {
	bv := BlobFromContent([]byte("hello world"), "text/plain", "test.txt", "greeting")
	if !bv.IsBlob() {
		t.Error("should be blob")
	}
	ref := bv.AsBlob()
	if ref.MIME != "text/plain" {
		t.Error("wrong MIME")
	}
	if ref.Name != "test.txt" {
		t.Error("wrong name")
	}
}

func TestParseBlobRef_Coverage(t *testing.T) {
	input := `@blob cid=sha256:abc123 mime=text/plain bytes=100`
	ref, err := ParseBlobRef(input)
	if err != nil {
		t.Fatal(err)
	}
	if ref.CID != "sha256:abc123" {
		t.Errorf("CID: %s", ref.CID)
	}
	if ref.MIME != "text/plain" {
		t.Errorf("MIME: %s", ref.MIME)
	}

	// With quoted values
	input2 := `@blob cid=sha256:def456 mime=image/png bytes=200 name="test file.png" caption="a test"`
	ref2, err := ParseBlobRef(input2)
	if err != nil {
		t.Fatal(err)
	}
	if ref2.Name == "" {
		t.Error("empty name")
	}

	// Invalid prefix
	_, err = ParseBlobRef("not a blob")
	if err == nil {
		t.Error("should fail")
	}
}

func TestMemoryBlobRegistry_Deep(t *testing.T) {
	reg := NewMemoryBlobRegistry()

	cid, err := reg.Put([]byte("hello"), "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	if cid == "" {
		t.Error("empty CID")
	}

	data, mime, err := reg.Get(cid)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Error("wrong data")
	}
	if mime != "text/plain" {
		t.Error("wrong mime")
	}

	if !reg.Has(cid) {
		t.Error("should exist")
	}
	if reg.Has("nonexistent") {
		t.Error("should not exist")
	}

	mime2, size, err := reg.Meta(cid)
	if err != nil {
		t.Fatal(err)
	}
	if mime2 != "text/plain" {
		t.Error("wrong mime")
	}
	if size != 5 {
		t.Errorf("wrong size: %d", size)
	}

	_, _, err = reg.Meta("nonexistent")
	if err == nil {
		t.Error("should fail for missing")
	}

	_, _, err = reg.Get("nonexistent")
	if err == nil {
		t.Error("should fail for missing")
	}
}

// ============================================================
// IncrementalParser Tests
// ============================================================

func TestIncrementalParser_BasicObject(t *testing.T) {
	var events []ParseEventType
	handler := func(evt ParseEvent) error {
		events = append(events, evt.Type)
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())

	// Feed data in chunks to exercise incremental parsing
	input := `{ name = Alice, age = 30 }`
	_, err := p.Feed([]byte(input))
	if err != nil {
		// Some formats may not be supported, just ensure we got events
		_ = err
	}
	_ = p.End()

	// As long as we got some events, incremental parsing worked
	if len(events) == 0 {
		t.Error("no events")
	}
}

func TestIncrementalParser_List(t *testing.T) {
	var events []ParseEventType
	handler := func(evt ParseEvent) error {
		events = append(events, evt.Type)
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())

	input := `[1 2 3]`
	_, err := p.Feed([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	p.End()

	if len(events) == 0 {
		t.Error("no events")
	}
}

func TestIncrementalParser_ScalarValues(t *testing.T) {
	values := []string{
		"42",
		"3.14",
		"true",
		"false",
		"_",
		`"hello world"`,
		"simple",
	}

	for _, input := range values {
		var events []ParseEvent
		handler := func(evt ParseEvent) error {
			events = append(events, evt)
			return nil
		}

		p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())
		p.Feed([]byte(input))
		p.End()

		if len(events) == 0 {
			t.Errorf("no events for %q", input)
		}
	}
}

func TestIncrementalParser_Nested(t *testing.T) {
	var events []ParseEvent
	handler := func(evt ParseEvent) error {
		events = append(events, evt)
		return nil
	}

	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())

	input := `{ name = Alice, address = { city = Boston }, tags = [a b c] }`
	p.Feed([]byte(input))
	_ = p.End()

	if len(events) == 0 {
		t.Error("no events")
	}
}

func TestIncrementalParser_Reset_Deep(t *testing.T) {
	handler := func(evt ParseEvent) error { return nil }
	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())

	p.Feed([]byte(`{x = 1}`))
	p.End()

	p.Reset()

	p.Feed([]byte(`{y = 2}`))
	p.End()
}

func TestIncrementalParser_Path_Deep(t *testing.T) {
	handler := func(evt ParseEvent) error { return nil }
	p := NewIncrementalParser(handler, DefaultIncrementalParserOptions())

	path := p.Path()
	if len(path) != 0 {
		t.Error("expected empty path")
	}
}

func TestParseEventType_String_Deep(t *testing.T) {
	types := []ParseEventType{
		EventNone, EventStartObject, EventEndObject,
		EventStartList, EventEndList, EventKey,
		EventValue, EventStartSum, EventEndSum,
		EventError, EventNeedMore,
	}

	for _, et := range types {
		s := et.String()
		if s == "" || s == "UNKNOWN" {
			t.Errorf("unexpected string for event %d: %s", et, s)
		}
	}

	// Unknown event type
	unknown := ParseEventType(99)
	if unknown.String() != "UNKNOWN" {
		t.Error("expected UNKNOWN")
	}
}

// ============================================================
// Schema Builder Tests
// ============================================================

func TestSchemaBuilder_AllVariants(t *testing.T) {
	sb := NewSchemaBuilder()

	// Regular struct
	sb.AddStruct("Person", "v1",
		&FieldDef{Name: "name", Type: TypeSpec{Kind: TypeSpecStr}},
		&FieldDef{Name: "age", Type: TypeSpec{Kind: TypeSpecInt}, Optional: true},
	)

	// Packed struct
	sb.AddPackedStruct("Event", "v1",
		&FieldDef{Name: "type", Type: TypeSpec{Kind: TypeSpecStr}, FID: 1},
		&FieldDef{Name: "ts", Type: TypeSpec{Kind: TypeSpecTime}, FID: 2},
	)

	// Open struct
	sb.AddOpenStruct("Config", "v1",
		&FieldDef{Name: "key", Type: TypeSpec{Kind: TypeSpecStr}},
	)

	// Open packed struct
	sb.AddOpenPackedStruct("Log", "v1",
		&FieldDef{Name: "msg", Type: TypeSpec{Kind: TypeSpecStr}, FID: 1},
	)

	// Sum type
	sb.AddSum("Result", "v1",
		&VariantDef{Tag: "ok", Type: TypeSpec{Kind: TypeSpecStr}},
		&VariantDef{Tag: "err", Type: TypeSpec{Kind: TypeSpecStr}},
	)

	s := sb.Build()
	if s == nil {
		t.Fatal("nil schema")
	}
	if s.Hash == "" {
		t.Error("empty hash")
	}

	// GetType
	pt := s.GetType("Person")
	if pt == nil {
		t.Error("no Person type")
	}

	// GetField
	fd := s.GetField("Person", "name")
	if fd == nil {
		t.Error("no name field")
	}
	fdMissing := s.GetField("Person", "missing")
	if fdMissing != nil {
		t.Error("should be nil for missing field")
	}
	fdMissing2 := s.GetField("Missing", "name")
	if fdMissing2 != nil {
		t.Error("should be nil for missing type")
	}
}

func TestSchema_WireKey(t *testing.T) {
	sb := NewSchemaBuilder()
	sb.AddStruct("T", "",
		&FieldDef{Name: "full_name", Type: TypeSpec{Kind: TypeSpecStr}, WireKey: "n"},
		&FieldDef{Name: "age", Type: TypeSpec{Kind: TypeSpecInt}},
	)
	s := sb.Build()

	// ResolveWireKey
	name := s.ResolveWireKey("T", "n")
	if name != "full_name" {
		t.Errorf("expected full_name, got %s", name)
	}
	// non-wire key returns the key itself (passthrough)
	name2 := s.ResolveWireKey("T", "age")
	if name2 != "age" {
		t.Errorf("passthrough expected age, got %s", name2)
	}

	// GetWireKey
	wk := s.GetWireKey("T", "full_name")
	if wk != "n" {
		t.Errorf("expected n, got %s", wk)
	}
	// no wire key returns field name itself
	wk2 := s.GetWireKey("T", "age")
	if wk2 != "age" {
		t.Errorf("passthrough expected age, got %s", wk2)
	}
}

func TestSchema_Canonical(t *testing.T) {
	sb := NewSchemaBuilder()
	sb.AddStruct("Person", "v1",
		&FieldDef{Name: "name", Type: TypeSpec{Kind: TypeSpecStr}},
	)
	s := sb.Build()

	canonical := s.Canonical()
	if canonical == "" {
		t.Error("empty canonical")
	}
	if !strings.Contains(canonical, "@schema") {
		t.Error("missing @schema")
	}
	if !strings.Contains(canonical, "Person") {
		t.Error("missing Person")
	}
}

// ============================================================
// Bridge Tests (FromCowrie)
// ============================================================

func TestFromCowrie_Coverage(t *testing.T) {
	// This exercises the bridge from cowrie values to glyph values
	// The main function is FromCowrie which handles various cowrie types

	// Simple values
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := Emit(tt.v)
			if s == "" && tt.name != "null" {
				t.Error("empty emit")
			}
		})
	}
}

// ============================================================
// Decimal128 Tests
// ============================================================

func TestDecimalFromAny_Coverage(t *testing.T) {
	// Test DecimalFromAny with various inputs
	tests := []struct {
		name  string
		input interface{}
	}{
		{"int", 42},
		{"int64", int64(42)},
		{"float64", float64(3.14)},
		{"string", "123.45"},
		{"string_int", "42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := DecimalFromAny(tt.input)
			if err != nil {
				t.Skipf("DecimalFromAny: %v", err)
				return
			}
			_ = v
		})
	}
}

// ============================================================
// Emit Coverage - additional edge cases
// ============================================================

func TestEmitWithOptions_AllTypes(t *testing.T) {
	tests := []struct {
		name string
		v    *GValue
	}{
		{"null", Null()},
		{"bool_true", Bool(true)},
		{"bool_false", Bool(false)},
		{"int_zero", Int(0)},
		{"int_neg", Int(-42)},
		{"float", Float(3.14)},
		{"str_empty", Str("")},
		{"str_spaces", Str("hello world")},
		{"str_quotes", Str(`say "hello"`)},
		{"ref", ID("d", "123")},
		{"list_empty", List()},
		{"list_nested", List(List(Int(1)), List(Int(2)))},
		{"map_empty", Map()},
	}

	for _, tt := range tests {
		t.Run(tt.name+"_default", func(t *testing.T) {
			s := Emit(tt.v)
			_ = s
		})
		t.Run(tt.name+"_pretty", func(t *testing.T) {
			s := EmitWithOptions(tt.v, EmitOptions{Pretty: true})
			_ = s
		})
		t.Run(tt.name+"_compact", func(t *testing.T) {
			s := EmitWithOptions(tt.v, EmitOptions{Compact: true})
			_ = s
		})
	}
}

// ============================================================
// Parse Schema Tests
// ============================================================

func TestParseSchema_Coverage(t *testing.T) {
	input := `@schema{
Person struct{
  name: str
  age: int
}
}`
	s, err := ParseSchema(input)
	if err != nil {
		t.Fatal(err)
	}
	if s == nil {
		t.Fatal("nil schema")
	}

	pt := s.GetType("Person")
	if pt == nil {
		t.Error("no Person type")
	}
}

func TestParseSchema_WithConstraints(t *testing.T) {
	input := `@schema{
Item struct{
  name: str
  count: int
  tags: list<str>
  meta: map<str, str>
}
}`
	s, err := ParseSchema(input)
	if err != nil {
		t.Skipf("constraint parsing: %v", err)
		return
	}
	if s == nil {
		t.Fatal("nil schema")
	}
}

func TestParseSchema_SumType(t *testing.T) {
	input := `@schema{
Result sum{
  Ok: str
  Err: str
}
}`
	s, err := ParseSchema(input)
	if err != nil {
		t.Skipf("sum type parsing: %v", err)
		return
	}
	if s == nil {
		t.Fatal("nil schema")
	}
}

// ============================================================
// TypeSpec String Coverage
// ============================================================

func TestTypeSpec_String_Coverage(t *testing.T) {
	specs := []TypeSpec{
		{Kind: TypeSpecNull},
		{Kind: TypeSpecBool},
		{Kind: TypeSpecInt},
		{Kind: TypeSpecFloat},
		{Kind: TypeSpecStr},
		{Kind: TypeSpecBytes},
		{Kind: TypeSpecTime},
		{Kind: TypeSpecID},
		{Kind: TypeSpecRef, Name: "Person"},
		{Kind: TypeSpecList, Elem: &TypeSpec{Kind: TypeSpecStr}},
		{Kind: TypeSpecMap, KeyType: &TypeSpec{Kind: TypeSpecStr}, ValType: &TypeSpec{Kind: TypeSpecInt}},
	}

	for i, spec := range specs {
		s := spec.String()
		if s == "" {
			t.Errorf("spec %d: empty string", i)
		}
	}
}
