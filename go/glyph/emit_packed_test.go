package glyph

import (
	"strings"
	"testing"
	"time"
)

// ============================================================
// Test Schema Helpers
// ============================================================

// makeTeamSchema creates a simple Team schema for testing.
func makeTeamSchema() *Schema {
	return NewSchemaBuilder().
		AddPackedStruct("Team", "v2",
			Field("id", PrimitiveType("id"), WithFID(1), WithWireKey("t")),
			Field("name", PrimitiveType("str"), WithFID(2), WithWireKey("n")),
			Field("league", PrimitiveType("str"), WithFID(3), WithWireKey("l")),
		).
		Build()
}

// makeMatchSchema creates a Match schema with nested Team and optional fields.
func makeMatchSchema() *Schema {
	return NewSchemaBuilder().
		AddPackedStruct("Team", "v2",
			Field("id", PrimitiveType("id"), WithFID(1), WithWireKey("t")),
			Field("name", PrimitiveType("str"), WithFID(2), WithWireKey("n")),
			Field("league", PrimitiveType("str"), WithFID(3), WithWireKey("l")),
		).
		AddPackedStruct("Odds", "v2",
			Field("h", PrimitiveType("float"), WithFID(1)),
			Field("d", PrimitiveType("float"), WithFID(2)),
			Field("a", PrimitiveType("float"), WithFID(3)),
		).
		AddPackedStruct("Pred", "v2",
			Field("ph", PrimitiveType("float"), WithFID(1)),
			Field("pd", PrimitiveType("float"), WithFID(2)),
			Field("pa", PrimitiveType("float"), WithFID(3)),
			Field("xh", PrimitiveType("float"), WithFID(4)),
			Field("xa", PrimitiveType("float"), WithFID(5)),
		).
		AddPackedStruct("Match", "v2",
			Field("id", PrimitiveType("id"), WithFID(1), WithWireKey("m")),
			Field("kickoff", PrimitiveType("time"), WithFID(2), WithWireKey("k")),
			Field("home", RefType("Team"), WithFID(3), WithWireKey("H")),
			Field("away", RefType("Team"), WithFID(4), WithWireKey("A")),
			Field("odds", RefType("Odds"), WithFID(5), WithWireKey("O"), WithOptional()),
			Field("pred", RefType("Pred"), WithFID(6), WithWireKey("P"), WithOptional()),
			Field("ft_h", PrimitiveType("int"), WithFID(7), WithWireKey("fh"), WithOptional()),
			Field("ft_a", PrimitiveType("int"), WithFID(8), WithWireKey("fa"), WithOptional()),
		).
		Build()
}

// makeTeamValue creates a Team GValue.
func makeTeamValue(id, name, league string) *GValue {
	return Struct("Team",
		MapEntry{Key: "id", Value: ID("t", id)},
		MapEntry{Key: "name", Value: Str(name)},
		MapEntry{Key: "league", Value: Str(league)},
	)
}

// makeMatchValue creates a Match GValue with optional fields.
func makeMatchValue(id string, kickoff time.Time, home, away *GValue, odds, pred *GValue, ftH, ftA *int) *GValue {
	fields := []MapEntry{
		{Key: "id", Value: ID("m", id)},
		{Key: "kickoff", Value: Time(kickoff)},
		{Key: "home", Value: home},
		{Key: "away", Value: away},
	}

	if odds != nil {
		fields = append(fields, MapEntry{Key: "odds", Value: odds})
	}
	if pred != nil {
		fields = append(fields, MapEntry{Key: "pred", Value: pred})
	}
	if ftH != nil {
		fields = append(fields, MapEntry{Key: "ft_h", Value: Int(int64(*ftH))})
	}
	if ftA != nil {
		fields = append(fields, MapEntry{Key: "ft_a", Value: Int(int64(*ftA))})
	}

	return Struct("Match", fields...)
}

// makeOddsValue creates an Odds GValue.
func makeOddsValue(h, d, a float64) *GValue {
	return Struct("Odds",
		MapEntry{Key: "h", Value: Float(h)},
		MapEntry{Key: "d", Value: Float(d)},
		MapEntry{Key: "a", Value: Float(a)},
	)
}

// makePredValue creates a Pred GValue.
func makePredValue(ph, pd, pa, xh, xa float64) *GValue {
	return Struct("Pred",
		MapEntry{Key: "ph", Value: Float(ph)},
		MapEntry{Key: "pd", Value: Float(pd)},
		MapEntry{Key: "pa", Value: Float(pa)},
		MapEntry{Key: "xh", Value: Float(xh)},
		MapEntry{Key: "xa", Value: Float(xa)},
	)
}

// ============================================================
// Canon Scalar Tests
// ============================================================

func TestCanonNull(t *testing.T) {
	got := canonNull()
	want := "∅"
	if got != want {
		t.Errorf("canonNull() = %q, want %q", got, want)
	}
}

func TestCanonBool(t *testing.T) {
	tests := []struct {
		input bool
		want  string
	}{
		{true, "t"},
		{false, "f"},
	}
	for _, tt := range tests {
		got := canonBool(tt.input)
		if got != tt.want {
			t.Errorf("canonBool(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestCanonInt(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0"},
		{1, "1"},
		{-1, "-1"},
		{42, "42"},
		{-42, "-42"},
		{1000000, "1000000"},
	}
	for _, tt := range tests {
		got := canonInt(tt.input)
		if got != tt.want {
			t.Errorf("canonInt(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestCanonFloat(t *testing.T) {
	tests := []struct {
		input float64
		want  string
	}{
		{0, "0"},
		{1.0, "1"},
		{-1.0, "-1"},
		{3.14, "3.14"},
		{0.5, "0.5"},
		{1e10, "1e+10"},
		{1.23e-5, "1.23e-05"},
	}
	for _, tt := range tests {
		got := canonFloat(tt.input)
		if got != tt.want {
			t.Errorf("canonFloat(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestCanonString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{"hello_world", "hello_world"},
		{"Hello", "Hello"},
		{"my-value", "my-value"},
		{"path/to/file", "path/to/file"},
		{"has space", `"has space"`},
		{"has\"quote", `"has\"quote"`},
		{"t", `"t"`}, // reserved
		{"f", `"f"`}, // reserved
		{"", `""`},   // empty
	}
	for _, tt := range tests {
		got := canonString(tt.input)
		if got != tt.want {
			t.Errorf("canonString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestCanonRef(t *testing.T) {
	tests := []struct {
		prefix string
		value  string
		want   string
	}{
		{"t", "ARS", "^t:ARS"},
		{"m", "2025-12-19:ARS-LIV", "^m:2025-12-19:ARS-LIV"},
		{"", "simple", "^simple"},
	}
	for _, tt := range tests {
		ref := RefID{Prefix: tt.prefix, Value: tt.value}
		got := canonRef(ref)
		if got != tt.want {
			t.Errorf("canonRef(%v) = %q, want %q", ref, got, tt.want)
		}
	}
}

// ============================================================
// Bitmap Tests
// ============================================================

func TestMaskToBinary(t *testing.T) {
	tests := []struct {
		mask []bool
		want string
	}{
		{[]bool{}, "0b0"},
		{[]bool{false}, "0b0"},
		{[]bool{true}, "0b1"},
		{[]bool{false, false}, "0b0"},
		{[]bool{true, false}, "0b1"},
		{[]bool{false, true}, "0b10"},
		{[]bool{true, true}, "0b11"},
		{[]bool{true, false, true}, "0b101"},
		{[]bool{true, true, false, true}, "0b1011"},
		{[]bool{false, false, false, true}, "0b1000"},
	}
	for _, tt := range tests {
		got := maskToBinary(tt.mask)
		if got != tt.want {
			t.Errorf("maskToBinary(%v) = %q, want %q", tt.mask, got, tt.want)
		}
	}
}

func TestBinaryToMask(t *testing.T) {
	tests := []struct {
		input string
		want  []bool
	}{
		{"0b0", []bool{false}},
		{"0b1", []bool{true}},
		{"0b10", []bool{false, true}},
		{"0b11", []bool{true, true}},
		{"0b101", []bool{true, false, true}},
		{"0b1011", []bool{true, true, false, true}},
	}
	for _, tt := range tests {
		got, err := binaryToMask(tt.input)
		if err != nil {
			t.Errorf("binaryToMask(%q) error: %v", tt.input, err)
			continue
		}
		if len(got) != len(tt.want) {
			t.Errorf("binaryToMask(%q) len = %d, want %d", tt.input, len(got), len(tt.want))
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("binaryToMask(%q)[%d] = %v, want %v", tt.input, i, got[i], tt.want[i])
			}
		}
	}
}

func TestBitmapLSBOrdering(t *testing.T) {
	// LSB = lowest fid optional field
	// Mask for optional fields with FID order: [5, 6, 7, 8]
	// If only 5 and 7 are present: mask = [true, false, true, false]
	// Binary: LSB is index 0 (fid 5), so 0b0101

	mask := []bool{true, false, true, false}
	got := maskToBinary(mask)
	want := "0b101" // index 0 is LSB, index 2 is set too

	if got != want {
		t.Errorf("Bitmap LSB ordering: got %q, want %q", got, want)
	}

	// Round-trip
	back, err := binaryToMask(got)
	if err != nil {
		t.Fatalf("binaryToMask error: %v", err)
	}

	// back should be [true, false, true] (trailing false trimmed conceptually but still matches)
	for i := 0; i < len(mask) && i < len(back); i++ {
		if mask[i] != back[i] {
			t.Errorf("Round-trip mask[%d] = %v, want %v", i, back[i], mask[i])
		}
	}
}

// ============================================================
// Packed Dense Tests
// ============================================================

func TestPackedDenseSimple(t *testing.T) {
	schema := makeTeamSchema()
	team := makeTeamValue("ARS", "Arsenal", "EPL")

	got, err := EmitPacked(team, schema)
	if err != nil {
		t.Fatalf("EmitPacked error: %v", err)
	}

	want := `Team@(^t:ARS Arsenal EPL)`
	if got != want {
		t.Errorf("EmitPacked =\n  %q\nwant\n  %q", got, want)
	}
}

func TestPackedDenseWithNulls(t *testing.T) {
	schema := makeMatchSchema()
	kickoff := time.Date(2025, 12, 19, 20, 0, 0, 0, time.UTC)

	home := makeTeamValue("ARS", "Arsenal", "EPL")
	away := makeTeamValue("LIV", "Liverpool", "EPL")

	// Match with no optional fields set
	match := makeMatchValue("ARS-LIV", kickoff, home, away, nil, nil, nil, nil)

	got, err := EmitPacked(match, schema)
	if err != nil {
		t.Fatalf("EmitPacked error: %v", err)
	}

	// Dense form: all fields in fid order, ∅ for missing optionals
	// But with bitmap enabled and optionals missing, it should use bitmap form
	// Since ALL optionals are missing, bitmap is used

	if !strings.Contains(got, "Match@{bm=") {
		t.Errorf("Expected bitmap form for sparse optionals, got: %q", got)
	}
}

func TestPackedDenseAllOptionalsPresent(t *testing.T) {
	schema := makeMatchSchema()
	kickoff := time.Date(2025, 12, 19, 20, 0, 0, 0, time.UTC)

	home := makeTeamValue("ARS", "Arsenal", "EPL")
	away := makeTeamValue("LIV", "Liverpool", "EPL")
	odds := makeOddsValue(2.10, 3.40, 3.25)
	pred := makePredValue(0.45, 0.27, 0.28, 1.72, 1.31)
	ftH, ftA := 2, 1

	// Match with all optional fields set
	match := makeMatchValue("ARS-LIV", kickoff, home, away, odds, pred, &ftH, &ftA)

	got, err := EmitPacked(match, schema)
	if err != nil {
		t.Fatalf("EmitPacked error: %v", err)
	}

	// All optionals present -> should use dense form (no bitmap)
	if strings.Contains(got, "@{bm=") {
		t.Errorf("Expected dense form when all optionals present, got bitmap: %q", got)
	}

	// Should contain Match@(
	if !strings.HasPrefix(got, "Match@(") {
		t.Errorf("Expected Match@(...), got: %q", got)
	}
}

// ============================================================
// Packed Bitmap Tests
// ============================================================

func TestPackedBitmapSparse(t *testing.T) {
	schema := makeMatchSchema()
	kickoff := time.Date(2025, 12, 19, 20, 0, 0, 0, time.UTC)

	home := makeTeamValue("ARS", "Arsenal", "EPL")
	away := makeTeamValue("LIV", "Liverpool", "EPL")
	odds := makeOddsValue(2.10, 3.40, 3.25)
	pred := makePredValue(0.45, 0.27, 0.28, 1.72, 1.31)

	// Match with only odds and pred, no ft_h/ft_a
	match := makeMatchValue("ARS-LIV", kickoff, home, away, odds, pred, nil, nil)

	got, err := EmitPacked(match, schema)
	if err != nil {
		t.Fatalf("EmitPacked error: %v", err)
	}

	// 2 of 4 optionals missing -> bitmap form
	if !strings.Contains(got, "Match@{bm=") {
		t.Errorf("Expected bitmap form for sparse optionals, got: %q", got)
	}

	// Bitmap should be 0b11 (first two optionals present: odds at index 0, pred at index 1)
	if !strings.Contains(got, "bm=0b11") {
		t.Errorf("Expected bm=0b11, got: %q", got)
	}
}

func TestPackedBitmapOnlyLast(t *testing.T) {
	schema := makeMatchSchema()
	kickoff := time.Date(2025, 12, 19, 20, 0, 0, 0, time.UTC)

	home := makeTeamValue("ARS", "Arsenal", "EPL")
	away := makeTeamValue("LIV", "Liverpool", "EPL")
	ftH, ftA := 2, 1

	// Match with only ft_h and ft_a (last two optionals)
	match := makeMatchValue("ARS-LIV", kickoff, home, away, nil, nil, &ftH, &ftA)

	got, err := EmitPacked(match, schema)
	if err != nil {
		t.Fatalf("EmitPacked error: %v", err)
	}

	// First two optionals missing, last two present
	// Optional field order by FID: odds(5), pred(6), ft_h(7), ft_a(8)
	// Mask: [false, false, true, true] = 0b1100

	if !strings.Contains(got, "bm=0b1100") {
		t.Errorf("Expected bm=0b1100, got: %q", got)
	}
}

// ============================================================
// Nested Struct Tests
// ============================================================

func TestPackedNested(t *testing.T) {
	schema := makeMatchSchema()
	kickoff := time.Date(2025, 12, 19, 20, 0, 0, 0, time.UTC)

	home := makeTeamValue("ARS", "Arsenal", "EPL")
	away := makeTeamValue("LIV", "Liverpool", "EPL")
	ftH, ftA := 2, 1

	match := makeMatchValue("ARS-LIV", kickoff, home, away, nil, nil, &ftH, &ftA)

	got, err := EmitPacked(match, schema)
	if err != nil {
		t.Fatalf("EmitPacked error: %v", err)
	}

	// Nested Team structs should also be packed
	if !strings.Contains(got, "Team@(") {
		t.Errorf("Expected nested Team to use packed form, got: %q", got)
	}
}

// ============================================================
// No Trailing Whitespace Tests
// ============================================================

func TestNoTrailingWhitespace(t *testing.T) {
	schema := makeTeamSchema()
	team := makeTeamValue("ARS", "Arsenal", "EPL")

	got, err := EmitPacked(team, schema)
	if err != nil {
		t.Fatalf("EmitPacked error: %v", err)
	}

	if strings.HasSuffix(got, " ") || strings.HasSuffix(got, "\n") || strings.HasSuffix(got, "\t") {
		t.Errorf("Output has trailing whitespace: %q", got)
	}
}

// ============================================================
// Schema FID Helpers Tests
// ============================================================

func TestFieldsByFID(t *testing.T) {
	schema := makeMatchSchema()
	td := schema.GetType("Match")
	if td == nil {
		t.Fatal("Match type not found")
	}

	fields := td.FieldsByFID()

	// Should be sorted by FID: 1, 2, 3, 4, 5, 6, 7, 8
	expectedFIDs := []int{1, 2, 3, 4, 5, 6, 7, 8}
	for i, fd := range fields {
		if fd.FID != expectedFIDs[i] {
			t.Errorf("Field %d has FID %d, want %d", i, fd.FID, expectedFIDs[i])
		}
	}
}

func TestOptionalFieldsByFID(t *testing.T) {
	schema := makeMatchSchema()
	td := schema.GetType("Match")
	if td == nil {
		t.Fatal("Match type not found")
	}

	optFields := td.OptionalFieldsByFID()

	// Should have 4 optional fields: odds(5), pred(6), ft_h(7), ft_a(8)
	if len(optFields) != 4 {
		t.Errorf("Expected 4 optional fields, got %d", len(optFields))
	}

	expectedFIDs := []int{5, 6, 7, 8}
	for i, fd := range optFields {
		if fd.FID != expectedFIDs[i] {
			t.Errorf("Optional field %d has FID %d, want %d", i, fd.FID, expectedFIDs[i])
		}
	}
}

func TestRequiredFieldsByFID(t *testing.T) {
	schema := makeMatchSchema()
	td := schema.GetType("Match")
	if td == nil {
		t.Fatal("Match type not found")
	}

	reqFields := td.RequiredFieldsByFID()

	// Should have 4 required fields: id(1), kickoff(2), home(3), away(4)
	if len(reqFields) != 4 {
		t.Errorf("Expected 4 required fields, got %d", len(reqFields))
	}

	expectedFIDs := []int{1, 2, 3, 4}
	for i, fd := range reqFields {
		if fd.FID != expectedFIDs[i] {
			t.Errorf("Required field %d has FID %d, want %d", i, fd.FID, expectedFIDs[i])
		}
	}
}

// ============================================================
// Mode Selection Tests
// ============================================================

func TestSelectModePacked(t *testing.T) {
	schema := makeMatchSchema()
	kickoff := time.Date(2025, 12, 19, 20, 0, 0, 0, time.UTC)
	home := makeTeamValue("ARS", "Arsenal", "EPL")
	away := makeTeamValue("LIV", "Liverpool", "EPL")
	match := makeMatchValue("ARS-LIV", kickoff, home, away, nil, nil, nil, nil)

	mode := SelectMode(match, schema, 3)
	if mode != ModePacked {
		t.Errorf("SelectMode for packable struct = %v, want ModePacked", mode)
	}
}

func TestSelectModeStruct(t *testing.T) {
	// Create a non-packed schema
	schema := NewSchemaBuilder().
		AddStruct("SimpleTeam", "v1",
			Field("name", PrimitiveType("str")),
		).
		Build()

	team := Struct("SimpleTeam", MapEntry{Key: "name", Value: Str("Arsenal")})

	mode := SelectMode(team, schema, 3)
	if mode != ModeStruct {
		t.Errorf("SelectMode for non-packable struct = %v, want ModeStruct", mode)
	}
}

// ============================================================
// KeepNull Tests
// ============================================================

func TestPackedKeepNull(t *testing.T) {
	// Create schema with KeepNull on an optional field
	schema := NewSchemaBuilder().
		AddPackedStruct("WithKeepNull", "v1",
			Field("required", PrimitiveType("str"), WithFID(1)),
			Field("nullable", PrimitiveType("str"), WithFID(2), WithOptional(), WithKeepNull()),
			Field("normal", PrimitiveType("str"), WithFID(3), WithOptional()),
		).
		Build()

	// Create value with explicit nulls
	v := Struct("WithKeepNull",
		MapEntry{Key: "required", Value: Str("hello")},
		MapEntry{Key: "nullable", Value: Null()},
		// normal not set
	)

	got, err := EmitPacked(v, schema)
	if err != nil {
		t.Fatalf("EmitPacked error: %v", err)
	}

	// nullable has KeepNull so it counts as present
	// normal is missing, so bitmap form with mask [true, false]
	// Which is 0b1

	if !strings.Contains(got, "bm=0b1") {
		t.Errorf("Expected bm=0b1 (KeepNull field present), got: %q", got)
	}

	// The null value should be emitted for nullable field
	if !strings.Contains(got, "∅") {
		t.Errorf("Expected ∅ in output for KeepNull field, got: %q", got)
	}
}
