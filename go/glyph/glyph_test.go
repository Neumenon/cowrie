package glyph

import (
	"testing"
	"time"
)

// ============================================================
// Test Helpers - must* functions for cleaner test assertions
// ============================================================

func mustAsInt(t *testing.T, v *GValue) int64 {
	t.Helper()
	i, err := v.AsInt()
	if err != nil {
		t.Fatalf("AsInt failed: %v", err)
	}
	return i
}

func mustAsStr(t *testing.T, v *GValue) string {
	t.Helper()
	s, err := v.AsStr()
	if err != nil {
		t.Fatalf("AsStr failed: %v", err)
	}
	return s
}

func mustAsFloat(t *testing.T, v *GValue) float64 {
	t.Helper()
	f, err := v.AsFloat()
	if err != nil {
		t.Fatalf("AsFloat failed: %v", err)
	}
	return f
}

func mustAsBool(t *testing.T, v *GValue) bool {
	t.Helper()
	b, err := v.AsBool()
	if err != nil {
		t.Fatalf("AsBool failed: %v", err)
	}
	return b
}

func mustAsList(t *testing.T, v *GValue) []*GValue {
	t.Helper()
	l, err := v.AsList()
	if err != nil {
		t.Fatalf("AsList failed: %v", err)
	}
	return l
}

func mustAsStruct(t *testing.T, v *GValue) *StructValue {
	t.Helper()
	sv, err := v.AsStruct()
	if err != nil {
		t.Fatalf("AsStruct failed: %v", err)
	}
	return sv
}

func mustAsSum(t *testing.T, v *GValue) *SumValue {
	t.Helper()
	sv, err := v.AsSum()
	if err != nil {
		t.Fatalf("AsSum failed: %v", err)
	}
	return sv
}

func mustAsID(t *testing.T, v *GValue) RefID {
	t.Helper()
	id, err := v.AsID()
	if err != nil {
		t.Fatalf("AsID failed: %v", err)
	}
	return id
}

func mustAsTime(t *testing.T, v *GValue) time.Time {
	t.Helper()
	tm, err := v.AsTime()
	if err != nil {
		t.Fatalf("AsTime failed: %v", err)
	}
	return tm
}

// ============================================================
// Lexer Tests
// ============================================================

func TestLexer_BasicTokens(t *testing.T) {
	tests := []struct {
		input    string
		expected []TokenType
	}{
		{"123", []TokenType{TokenInt, TokenEOF}},
		{"-456", []TokenType{TokenInt, TokenEOF}},
		{"3.14", []TokenType{TokenFloat, TokenEOF}},
		{"-2.5e10", []TokenType{TokenFloat, TokenEOF}},
		{"true", []TokenType{TokenTrue, TokenEOF}},
		{"false", []TokenType{TokenFalse, TokenEOF}},
		{"t", []TokenType{TokenTrue, TokenEOF}},
		{"f", []TokenType{TokenFalse, TokenEOF}},
		{"null", []TokenType{TokenNull, TokenEOF}},
		{"none", []TokenType{TokenNull, TokenEOF}},
		{`"hello"`, []TokenType{TokenString, TokenEOF}},
		{"hello_world", []TokenType{TokenIdent, TokenEOF}},
		{"^ref:123", []TokenType{TokenRef, TokenEOF}},
		{"{}", []TokenType{TokenLBrace, TokenRBrace, TokenEOF}},
		{"[]", []TokenType{TokenLBracket, TokenRBracket, TokenEOF}},
		{"()", []TokenType{TokenLParen, TokenRParen, TokenEOF}},
		{"=", []TokenType{TokenEq, TokenEOF}},
		{":", []TokenType{TokenEq, TokenEOF}},
		{"@schema", []TokenType{TokenAt, TokenIdent, TokenEOF}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tokens, err := lexer.Tokenize()
			if err != nil {
				t.Fatalf("Tokenize failed: %v", err)
			}

			if len(tokens) != len(tt.expected) {
				t.Fatalf("Expected %d tokens, got %d", len(tt.expected), len(tokens))
			}

			for i, tok := range tokens {
				if tok.Type != tt.expected[i] {
					t.Errorf("Token %d: expected %s, got %s", i, tt.expected[i], tok.Type)
				}
			}
		})
	}
}

func TestLexer_Comments(t *testing.T) {
	input := `123 // this is a comment
456`
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}

	// Should have: INT(123), INT(456), EOF
	if len(tokens) != 3 {
		t.Fatalf("Expected 3 tokens, got %d", len(tokens))
	}
	if tokens[0].Value != "123" || tokens[1].Value != "456" {
		t.Errorf("Unexpected token values: %v, %v", tokens[0].Value, tokens[1].Value)
	}
}

func TestLexer_NullSymbol(t *testing.T) {
	input := "∅"
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}

	if tokens[0].Type != TokenNull {
		t.Errorf("Expected TokenNull, got %s", tokens[0].Type)
	}
}

// ============================================================
// Parser Tests
// ============================================================

func TestParse_Scalars(t *testing.T) {
	tests := []struct {
		input    string
		expected GType
	}{
		{"null", TypeNull},
		{"∅", TypeNull},
		{"true", TypeBool},
		{"t", TypeBool},
		{"false", TypeBool},
		{"f", TypeBool},
		{"123", TypeInt},
		{"-456", TypeInt},
		{"3.14", TypeFloat},
		{`"hello"`, TypeStr},
		{"bare_string", TypeStr},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			if result.Value.Type() != tt.expected {
				t.Errorf("Expected type %s, got %s", tt.expected, result.Value.Type())
			}
		})
	}
}

func TestParse_List(t *testing.T) {
	result, err := Parse("[1 2 3]")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if result.Value.Type() != TypeList {
		t.Fatalf("Expected list, got %s", result.Value.Type())
	}

	list := mustAsList(t, result.Value)
	if len(list) != 3 {
		t.Fatalf("Expected 3 elements, got %d", len(list))
	}

	if mustAsInt(t, list[0]) != 1 || mustAsInt(t, list[1]) != 2 || mustAsInt(t, list[2]) != 3 {
		t.Errorf("Unexpected list values")
	}
}

func TestParse_Map(t *testing.T) {
	result, err := Parse("{a:1 b:2}")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if result.Value.Type() != TypeMap {
		t.Fatalf("Expected map, got %s", result.Value.Type())
	}

	a := result.Value.Get("a")
	b := result.Value.Get("b")

	if a == nil || mustAsInt(t, a) != 1 {
		t.Errorf("Expected a=1, got %v", a)
	}
	if b == nil || mustAsInt(t, b) != 2 {
		t.Errorf("Expected b=2, got %v", b)
	}
}

func TestParse_Struct(t *testing.T) {
	result, err := Parse(`Team{name="Arsenal" rank=1}`)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if result.Value.Type() != TypeStruct {
		t.Fatalf("Expected struct, got %s", result.Value.Type())
	}

	sv := mustAsStruct(t, result.Value)
	if sv.TypeName != "Team" {
		t.Errorf("Expected type Team, got %s", sv.TypeName)
	}

	name := result.Value.Get("name")
	if name == nil || mustAsStr(t, name) != "Arsenal" {
		t.Errorf("Expected name=Arsenal, got %v", name)
	}
}

func TestParse_Sum(t *testing.T) {
	result, err := Parse("Success(42)")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if result.Value.Type() != TypeSum {
		t.Fatalf("Expected sum, got %s", result.Value.Type())
	}

	sv := mustAsSum(t, result.Value)
	if sv.Tag != "Success" {
		t.Errorf("Expected tag Success, got %s", sv.Tag)
	}
	if mustAsInt(t, sv.Value) != 42 {
		t.Errorf("Expected value 42, got %v", sv.Value)
	}
}

func TestParse_Ref(t *testing.T) {
	result, err := Parse("^m:2025-12-19:ARS-LIV")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if result.Value.Type() != TypeID {
		t.Fatalf("Expected id, got %s", result.Value.Type())
	}

	ref := mustAsID(t, result.Value)
	if ref.Prefix != "m" {
		t.Errorf("Expected prefix 'm', got %s", ref.Prefix)
	}
	if ref.Value != "2025-12-19:ARS-LIV" {
		t.Errorf("Expected value '2025-12-19:ARS-LIV', got %s", ref.Value)
	}
}

func TestParse_NestedStruct(t *testing.T) {
	input := `Match{
		id=^m:ARS-LIV
		home=Team{name="Arsenal" rank=1}
		away=Team{name="Liverpool" rank=2}
		odds=[2.10 3.40 3.25]
	}`

	result, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if result.Value.Type() != TypeStruct {
		t.Fatalf("Expected struct, got %s", result.Value.Type())
	}

	sv := mustAsStruct(t, result.Value)
	if sv.TypeName != "Match" {
		t.Errorf("Expected type Match, got %s", sv.TypeName)
	}

	home := result.Value.Get("home")
	if home == nil || home.Type() != TypeStruct {
		t.Fatalf("Expected home struct")
	}
	if mustAsStruct(t, home).TypeName != "Team" {
		t.Errorf("Expected Team type for home")
	}

	odds := result.Value.Get("odds")
	if odds == nil || odds.Type() != TypeList {
		t.Fatalf("Expected odds list")
	}
	if len(mustAsList(t, odds)) != 3 {
		t.Errorf("Expected 3 odds values")
	}
}

func TestParse_TolerantMode(t *testing.T) {
	// Test tolerance to = vs :
	result1, _ := Parse("{a=1 b:2}")
	if result1.Value.Get("a") == nil || result1.Value.Get("b") == nil {
		t.Error("Should accept both = and :")
	}

	// Test tolerance to optional commas
	result2, _ := Parse("[1, 2, 3]")
	if len(mustAsList(t, result2.Value)) != 3 {
		t.Error("Should accept commas in lists")
	}
}

// ============================================================
// Emit Tests
// ============================================================

func TestEmit_Scalars(t *testing.T) {
	tests := []struct {
		value    *GValue
		expected string
	}{
		{Null(), "∅"},
		{Bool(true), "t"},
		{Bool(false), "f"},
		{Int(42), "42"},
		{Int(-123), "-123"},
		{Float(3.14), "3.14"},
		{Str("hello"), "hello"},
		{Str("hello world"), `"hello world"`},
		{ID("m", "123"), "^m:123"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := Emit(tt.value)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestEmit_List(t *testing.T) {
	v := List(Int(1), Int(2), Int(3))
	result := Emit(v)
	if result != "[1 2 3]" {
		t.Errorf("Expected '[1 2 3]', got %q", result)
	}
}

func TestEmit_Struct(t *testing.T) {
	v := Struct("Team",
		MapEntry{Key: "name", Value: Str("Arsenal")},
		MapEntry{Key: "rank", Value: Int(1)},
	)
	result := Emit(v)
	// Fields should be sorted alphabetically
	expected := `Team{name=Arsenal rank=1}`
	if result != expected {
		t.Errorf("Expected %q, got %q", expected, result)
	}
}

func TestEmit_RoundTrip(t *testing.T) {
	input := `Match{home=Team{name=Arsenal rank=1} id=^m:123 odds=[2.1 3.4 3.25]}`

	result, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	emitted := Emit(result.Value)

	// Parse again and emit again - should be stable
	result2, err := Parse(emitted)
	if err != nil {
		t.Fatalf("Re-parse failed: %v", err)
	}

	emitted2 := Emit(result2.Value)

	if emitted != emitted2 {
		t.Errorf("Round-trip not stable:\n  First:  %s\n  Second: %s", emitted, emitted2)
	}
}

// ============================================================
// Schema Tests
// ============================================================

func TestSchemaBuilder(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("Team", "v1",
			Field("id", PrimitiveType("id"), WithWireKey("t")),
			Field("name", PrimitiveType("str"), WithWireKey("n")),
			Field("league", PrimitiveType("str"), WithWireKey("l"), WithOptional()),
		).
		AddStruct("Match", "v1",
			Field("id", PrimitiveType("id"), WithWireKey("m")),
			Field("home", RefType("Team"), WithWireKey("H")),
			Field("away", RefType("Team"), WithWireKey("A")),
			Field("odds", ListType(PrimitiveType("float")), WithWireKey("O")),
		).
		Build()

	if len(schema.Types) != 2 {
		t.Errorf("Expected 2 types, got %d", len(schema.Types))
	}

	team := schema.GetType("Team")
	if team == nil {
		t.Fatal("Team type not found")
	}
	if team.Version != "v1" {
		t.Errorf("Expected version v1, got %s", team.Version)
	}

	// Test wire key resolution
	resolved := schema.ResolveWireKey("Team", "n")
	if resolved != "name" {
		t.Errorf("Expected 'name', got %s", resolved)
	}

	// Test schema hash
	if schema.Hash == "" {
		t.Error("Schema hash should not be empty")
	}
}

func TestValidator_Basic(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("Team", "",
			Field("name", PrimitiveType("str")),
			Field("rank", PrimitiveType("int"), WithConstraint(MinConstraint(1)), WithConstraint(MaxConstraint(100))),
		).
		Build()

	// Valid value
	validTeam := Struct("Team",
		MapEntry{Key: "name", Value: Str("Arsenal")},
		MapEntry{Key: "rank", Value: Int(1)},
	)

	result := ValidateAs(validTeam, schema, "Team")
	if !result.Valid {
		t.Errorf("Expected valid, got errors: %v", result.Errors)
	}

	// Invalid: rank out of range
	invalidTeam := Struct("Team",
		MapEntry{Key: "name", Value: Str("Arsenal")},
		MapEntry{Key: "rank", Value: Int(200)},
	)

	result = ValidateAs(invalidTeam, schema, "Team")
	if result.Valid {
		t.Error("Expected invalid for rank > 100")
	}
}

func TestValidator_RequiredField(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("Team", "",
			Field("name", PrimitiveType("str")),
			Field("rank", PrimitiveType("int")),
		).
		Build()

	// Missing required field
	incomplete := Struct("Team",
		MapEntry{Key: "name", Value: Str("Arsenal")},
		// rank is missing
	)

	result := ValidateAs(incomplete, schema, "Team")
	if result.Valid {
		t.Error("Expected invalid for missing required field")
	}
}

func TestValidator_OptionalField(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("Team", "",
			Field("name", PrimitiveType("str")),
			Field("league", PrimitiveType("str"), WithOptional()),
		).
		Build()

	// Missing optional field - should be valid
	team := Struct("Team",
		MapEntry{Key: "name", Value: Str("Arsenal")},
	)

	result := ValidateAs(team, schema, "Team")
	if !result.Valid {
		t.Errorf("Expected valid with missing optional field, got: %v", result.Errors)
	}
}

// ============================================================
// Bridge Tests
// ============================================================

func TestBridge_RoundTrip(t *testing.T) {
	original := Struct("Match",
		MapEntry{Key: "id", Value: ID("m", "ARS-LIV")},
		MapEntry{Key: "home", Value: Struct("Team",
			MapEntry{Key: "name", Value: Str("Arsenal")},
			MapEntry{Key: "rank", Value: Int(1)},
		)},
		MapEntry{Key: "odds", Value: List(Float(2.10), Float(3.40), Float(3.25))},
	)

	// Convert to Cowrie and back
	cowrieVal := ToCowrie(original)
	roundTripped := FromCowrie(cowrieVal)

	// Compare canonical forms
	origEmit := Emit(original)
	rtEmit := Emit(roundTripped)

	if origEmit != rtEmit {
		t.Errorf("Round-trip mismatch:\n  Original: %s\n  RoundTrip: %s", origEmit, rtEmit)
	}
}

func TestBridge_TimeValue(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Nanosecond)
	original := Time(now)

	cowrieVal := ToCowrie(original)
	roundTripped := FromCowrie(cowrieVal)

	if roundTripped.Type() != TypeTime {
		t.Fatalf("Expected time type, got %s", roundTripped.Type())
	}

	rtTime := mustAsTime(t, roundTripped)
	if !rtTime.Equal(now) {
		t.Errorf("Time mismatch: %v vs %v", now, rtTime)
	}
}

// ============================================================
// Football Scouting Domain Tests
// ============================================================

func TestFootballSchema(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("Team", "v1",
			Field("id", PrimitiveType("id"), WithWireKey("t"), WithOptional()),
			Field("name", PrimitiveType("str"), WithWireKey("n")),
			Field("league", PrimitiveType("str"), WithWireKey("l"), WithOptional()),
		).
		AddStruct("MarketOdds", "v1",
			Field("h", PrimitiveType("float"), WithConstraint(MinConstraint(1.01))),
			Field("d", PrimitiveType("float"), WithConstraint(MinConstraint(1.01))),
			Field("a", PrimitiveType("float"), WithConstraint(MinConstraint(1.01))),
			Field("src", PrimitiveType("str"), WithOptional()),
		).
		AddStruct("ModelPred", "v1",
			Field("ph", PrimitiveType("float"), WithConstraint(RangeConstraint(0, 1))),
			Field("pd", PrimitiveType("float"), WithConstraint(RangeConstraint(0, 1))),
			Field("pa", PrimitiveType("float"), WithConstraint(RangeConstraint(0, 1))),
			Field("xh", PrimitiveType("float"), WithConstraint(RangeConstraint(0, 10))),
			Field("xa", PrimitiveType("float"), WithConstraint(RangeConstraint(0, 10))),
			Field("ver", PrimitiveType("str"), WithWireKey("v"), WithOptional()),
		).
		AddStruct("Match", "v1",
			Field("id", PrimitiveType("id"), WithWireKey("m")),
			Field("kickoff", PrimitiveType("time"), WithWireKey("k")),
			Field("home", RefType("Team"), WithWireKey("H")),
			Field("away", RefType("Team"), WithWireKey("A")),
			Field("odds", RefType("MarketOdds"), WithWireKey("O"), WithOptional()),
			Field("pred", RefType("ModelPred"), WithWireKey("P"), WithOptional()),
		).
		Build()

	input := `Match{
		m=^m:2025-12-19:ARS-LIV
		k=2025-12-19T20:00:00Z
		H=Team{t=^t:ARS n="Arsenal" l="EPL"}
		A=Team{t=^t:LIV n="Liverpool" l="EPL"}
		O=MarketOdds{h=2.10 d=3.40 a=3.25 src=pinny}
		P=ModelPred{ph=0.45 pd=0.27 pa=0.28 xh=1.72 xa=1.31 v=ens_v1}
	}`

	result, err := ParseWithSchema(input, schema)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if result.HasErrors() {
		t.Errorf("Parse errors: %v", result.Errors)
	}

	// Validate
	validation := ValidateAs(result.Value, schema, "Match")
	if !validation.Valid {
		t.Errorf("Validation errors: %v", validation.Errors)
	}

	// Check structure
	match := result.Value
	if match.Type() != TypeStruct {
		t.Fatal("Expected struct")
	}
	if mustAsStruct(t, match).TypeName != "Match" {
		t.Error("Expected Match type")
	}

	home := match.Get("home")
	if home == nil {
		// Try wire key
		home = match.Get("H")
	}
	if home == nil || home.Type() != TypeStruct {
		t.Error("Expected home team struct")
	}
}

func TestCanonicalHash(t *testing.T) {
	v1 := Struct("Team",
		MapEntry{Key: "name", Value: Str("Arsenal")},
		MapEntry{Key: "rank", Value: Int(1)},
	)

	v2 := Struct("Team",
		MapEntry{Key: "rank", Value: Int(1)},
		MapEntry{Key: "name", Value: Str("Arsenal")},
	)

	hash1 := CanonicalHash(v1)
	hash2 := CanonicalHash(v2)

	// Same content, different field order - should produce same hash
	if hash1 != hash2 {
		t.Errorf("Canonical hash should be same regardless of field order:\n  h1: %s\n  h2: %s", hash1, hash2)
	}
}

// ============================================================
// Wire Key Compression Tests
// ============================================================

func TestEmit_WithWireKeys(t *testing.T) {
	schema := NewSchemaBuilder().
		AddStruct("Team", "",
			Field("name", PrimitiveType("str"), WithWireKey("n")),
			Field("rank", PrimitiveType("int"), WithWireKey("r")),
		).
		Build()

	v := Struct("Team",
		MapEntry{Key: "name", Value: Str("Arsenal")},
		MapEntry{Key: "rank", Value: Int(1)},
	)

	opts := EmitOptions{
		Schema:      schema,
		UseWireKeys: true,
		SortFields:  true,
	}

	result := EmitWithOptions(v, opts)

	// Should use short wire keys
	if result != "Team{n=Arsenal r=1}" {
		t.Errorf("Expected wire keys, got: %s", result)
	}
}
