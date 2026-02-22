package glyph

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func TestTabularReaderBasic(t *testing.T) {
	schema := makeHikeSchemaForTabularTest()

	input := `@tab Hike [i n d e c s]
1 "Blue Lake Trail" 7.5 320 ^p:ana t
2 "Ridge Overlook" 9.2 540 ^p:luis f
3 "Wildflower Loop" 5.1 180 ^p:sam t
@end`

	tr := NewTabularReaderFromString(input, schema)

	// Read header
	typeName, columns, err := tr.ReadHeader()
	if err != nil {
		t.Fatalf("ReadHeader error: %v", err)
	}

	if typeName != "Hike" {
		t.Errorf("typeName = %q, want %q", typeName, "Hike")
	}

	expectedCols := []string{"i", "n", "d", "e", "c", "s"}
	if len(columns) != len(expectedCols) {
		t.Errorf("columns len = %d, want %d", len(columns), len(expectedCols))
	}
	for i, col := range columns {
		if col != expectedCols[i] {
			t.Errorf("columns[%d] = %q, want %q", i, col, expectedCols[i])
		}
	}

	// Read rows
	row1, err := tr.Next()
	if err != nil {
		t.Fatalf("Next row 1 error: %v", err)
	}
	if row1.structVal.TypeName != "Hike" {
		t.Errorf("row1 type = %q, want %q", row1.structVal.TypeName, "Hike")
	}

	// Check first row values
	id := row1.Get("id")
	if id == nil || mustAsInt(t, id) != 1 {
		t.Errorf("row1.id = %v, want 1", id)
	}

	name := row1.Get("name")
	if name == nil || mustAsStr(t, name) != "Blue Lake Trail" {
		t.Errorf("row1.name = %v, want 'Blue Lake Trail'", name)
	}

	distance := row1.Get("distance")
	if distance == nil || mustAsFloat(t, distance) != 7.5 {
		t.Errorf("row1.distance = %v, want 7.5", distance)
	}

	// Read remaining rows
	row2, err := tr.Next()
	if err != nil {
		t.Fatalf("Next row 2 error: %v", err)
	}
	if mustAsStr(t, row2.Get("name")) != "Ridge Overlook" {
		t.Errorf("row2.name = %v, want 'Ridge Overlook'", row2.Get("name"))
	}

	row3, err := tr.Next()
	if err != nil {
		t.Fatalf("Next row 3 error: %v", err)
	}
	if mustAsStr(t, row3.Get("name")) != "Wildflower Loop" {
		t.Errorf("row3.name = %v, want 'Wildflower Loop'", row3.Get("name"))
	}

	// Should get EOF
	_, err = tr.Next()
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}

	if tr.RowNum() != 3 {
		t.Errorf("RowNum() = %d, want 3", tr.RowNum())
	}
}

func TestTabularReaderWithPackedCells(t *testing.T) {
	schema := makeMatchSchema()

	input := `@tab Match [m k H A O P fh fa]
^m:ARS-LIV 2025-12-19T20:00:00Z Team@(^t:ARS Arsenal EPL) Team@(^t:LIV Liverpool EPL) Odds@(2.1 3.4 3.25) Pred@(0.45 0.27 0.28 1.72 1.31) 2 1
^m:MCI-CHE 2025-12-20T15:00:00Z Team@(^t:MCI "Manchester City" EPL) Team@(^t:CHE Chelsea EPL) Odds@(1.8 3.6 4.2) Pred@(0.52 0.24 0.24 1.85 1.15) ∅ ∅
@end`

	tr := NewTabularReaderFromString(input, schema)
	rows, err := tr.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}

	// Check first row
	row1 := rows[0]

	home := row1.Get("home")
	if home == nil || home.typ != TypeStruct {
		t.Fatalf("row1.home not a struct: %v", home)
	}
	if home.structVal.TypeName != "Team" {
		t.Errorf("row1.home.TypeName = %q, want 'Team'", home.structVal.TypeName)
	}

	homeId := home.Get("id")
	if homeId == nil || homeId.idVal.Value != "ARS" {
		t.Errorf("row1.home.id = %v, want 'ARS'", homeId)
	}

	// Check second row with nulls
	row2 := rows[1]
	ftH := row2.Get("ft_h")
	if ftH == nil || !ftH.IsNull() {
		t.Errorf("row2.ft_h = %v, want null", ftH)
	}

	ftA := row2.Get("ft_a")
	if ftA == nil || !ftA.IsNull() {
		t.Errorf("row2.ft_a = %v, want null", ftA)
	}
}

func TestTabularReaderWithComments(t *testing.T) {
	schema := makeTeamSchema()

	input := `# This is a comment
@tab Team [t n l]
# Another comment
^t:ARS Arsenal EPL
^t:LIV Liverpool EPL
# Comment between rows
^t:CHE Chelsea EPL
@end`

	tr := NewTabularReaderFromString(input, schema)
	rows, err := tr.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
}

func TestInlineTabular(t *testing.T) {
	schema := makeTeamSchema()

	input := "@tab Team [t n l] ^t:ARS Arsenal EPL | ^t:LIV Liverpool EPL | ^t:CHE Chelsea EPL @end"

	rows, err := ParseInlineTabular(input, schema)
	if err != nil {
		t.Fatalf("ParseInlineTabular error: %v", err)
	}

	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}

	// Check first row
	if rows[0].structVal.TypeName != "Team" {
		t.Errorf("rows[0].TypeName = %q, want 'Team'", rows[0].structVal.TypeName)
	}

	id := rows[0].Get("id")
	if id == nil || id.idVal.Value != "ARS" {
		t.Errorf("rows[0].id = %v, want 'ARS'", id)
	}
}

func TestTabularReaderFIDColumns(t *testing.T) {
	schema := makeTeamSchema()

	// Use FID-style column headers: #1 #2 #3
	input := `@tab Team [#1 #2 #3]
^t:ARS Arsenal EPL
^t:LIV Liverpool EPL
@end`

	tr := NewTabularReaderFromString(input, schema)
	rows, err := tr.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}

	// FID 1 = id, FID 2 = name, FID 3 = league
	if rows[0].Get("id").idVal.Value != "ARS" {
		t.Errorf("rows[0].id = %v, want 'ARS'", rows[0].Get("id"))
	}
	if mustAsStr(t, rows[0].Get("name")) != "Arsenal" {
		t.Errorf("rows[0].name = %v, want 'Arsenal'", rows[0].Get("name"))
	}
}

func TestTabularRoundTrip(t *testing.T) {
	schema := makeHikeSchemaForTabularTest()

	// Create hikes
	hikes := List(
		Struct("Hike",
			MapEntry{Key: "id", Value: Int(1)},
			MapEntry{Key: "name", Value: Str("Blue Lake Trail")},
			MapEntry{Key: "distance", Value: Float(7.5)},
			MapEntry{Key: "elevation", Value: Int(320)},
			MapEntry{Key: "creator", Value: ID("p", "ana")},
			MapEntry{Key: "starred", Value: Bool(true)},
		),
		Struct("Hike",
			MapEntry{Key: "id", Value: Int(2)},
			MapEntry{Key: "name", Value: Str("Ridge Overlook")},
			MapEntry{Key: "distance", Value: Float(9.2)},
			MapEntry{Key: "elevation", Value: Int(540)},
			MapEntry{Key: "creator", Value: ID("p", "luis")},
			MapEntry{Key: "starred", Value: Bool(false)},
		),
	)

	// Emit as tabular
	tabular, err := EmitTabular(hikes, schema)
	if err != nil {
		t.Fatalf("EmitTabular error: %v", err)
	}

	t.Logf("Tabular output:\n%s", tabular)

	// Parse back
	tr := NewTabularReaderFromString(tabular, schema)
	parsed, err := tr.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	if len(parsed) != 2 {
		t.Fatalf("got %d rows, want 2", len(parsed))
	}

	// Compare
	if mustAsStr(t, parsed[0].Get("name")) != "Blue Lake Trail" {
		t.Errorf("parsed[0].name = %q, want 'Blue Lake Trail'", mustAsStr(t, parsed[0].Get("name")))
	}
	if mustAsFloat(t, parsed[1].Get("distance")) != 9.2 {
		t.Errorf("parsed[1].distance = %v, want 9.2", mustAsFloat(t, parsed[1].Get("distance")))
	}
}

// makeHikeSchemaForTabularTest creates the Hike schema for tabular reader tests.
func makeHikeSchemaForTabularTest() *Schema {
	return NewSchemaBuilder().
		AddPackedStruct("Hike", "v2",
			Field("id", PrimitiveType("int"), WithFID(1), WithWireKey("i")),
			Field("name", PrimitiveType("str"), WithFID(2), WithWireKey("n")),
			Field("distance", PrimitiveType("float"), WithFID(3), WithWireKey("d")),
			Field("elevation", PrimitiveType("int"), WithFID(4), WithWireKey("e")),
			Field("creator", PrimitiveType("id"), WithFID(5), WithWireKey("c")),
			Field("starred", PrimitiveType("bool"), WithFID(6), WithWireKey("s")),
		).
		Build()
}

func TestTabularReaderWithTime(t *testing.T) {
	schema := makeMatchSchema()

	input := `@tab Match [m k H A O P fh fa]
^m:TEST 2025-12-19T20:00:00Z Team@(^t:ARS Arsenal EPL) Team@(^t:LIV Liverpool EPL) Odds@(2.1 3.4 3.25) Pred@(0.45 0.27 0.28 1.72 1.31) 2 1
@end`

	tr := NewTabularReaderFromString(input, schema)
	rows, err := tr.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}

	kickoff := rows[0].Get("kickoff")
	if kickoff == nil || kickoff.typ != TypeTime {
		t.Fatalf("kickoff not a time: %v", kickoff)
	}

	expected := time.Date(2025, 12, 19, 20, 0, 0, 0, time.UTC)
	if !kickoff.timeVal.Equal(expected) {
		t.Errorf("kickoff = %v, want %v", kickoff.timeVal, expected)
	}
}

func TestTabularReaderDateOnly(t *testing.T) {
	schema := makeMatchSchema()

	input := `@tab Match [m k H A O P fh fa]
^m:TEST 2025-12-19 Team@(^t:ARS Arsenal EPL) Team@(^t:LIV Liverpool EPL) Odds@(2.1 3.4 3.25) Pred@(0.45 0.27 0.28 1.72 1.31) 2 1
@end`

	tr := NewTabularReaderFromString(input, schema)
	rows, err := tr.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}

	kickoff := rows[0].Get("kickoff")
	if kickoff == nil || kickoff.typ != TypeTime {
		t.Fatalf("kickoff not a time: %v", kickoff)
	}

	expected := time.Date(2025, 12, 19, 0, 0, 0, 0, time.UTC)
	if !kickoff.timeVal.Equal(expected) {
		t.Errorf("kickoff = %v, want %v", kickoff.timeVal, expected)
	}
}

func TestTabularReaderNullAliases(t *testing.T) {
	schema := makeHikeSchemaForTabularTest()

	input := `@tab Hike [i n d e c s]
1 "Blue Lake Trail" 7.5 320 ^p:ana _
2 "Ridge Overlook" 9.2 540 ^p:luis null
@end`

	tr := NewTabularReaderFromString(input, schema)
	rows, err := tr.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[0].Get("starred") == nil || !rows[0].Get("starred").IsNull() {
		t.Fatalf("row1.starred = %v, want null", rows[0].Get("starred"))
	}
	if rows[1].Get("starred") == nil || !rows[1].Get("starred").IsNull() {
		t.Fatalf("row2.starred = %v, want null", rows[1].Get("starred"))
	}
}

func TestTabularReaderExtraColumns(t *testing.T) {
	schema := makeTeamSchema()

	input := `@tab Team [t n l]
^t:ARS Arsenal EPL EXTRA
@end`

	tr := NewTabularReaderFromString(input, schema)
	if _, err := tr.ReadAll(); err == nil {
		t.Fatal("expected error for extra columns")
	}
}

func TestInlineTabularExtraColumns(t *testing.T) {
	schema := makeTeamSchema()

	input := "@tab Team [t n l] ^t:ARS Arsenal EPL EXTRA @end"
	if _, err := ParseInlineTabular(input, schema); err == nil {
		t.Fatal("expected error for extra columns in inline tabular")
	}
}

func TestInlineTabularSplitWithPipes(t *testing.T) {
	schema := makeTeamSchema()

	input := `@tab Team [t n l] ^t:ARS "Ars|enal" EPL | ^t:LIV "Liv|erpool" EPL @end`
	rows, err := ParseInlineTabular(input, schema)
	if err != nil {
		t.Fatalf("ParseInlineTabular error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if mustAsStr(t, rows[0].Get("name")) != "Ars|enal" {
		t.Errorf("rows[0].name = %q, want %q", mustAsStr(t, rows[0].Get("name")), "Ars|enal")
	}
	if mustAsStr(t, rows[1].Get("name")) != "Liv|erpool" {
		t.Errorf("rows[1].name = %q, want %q", mustAsStr(t, rows[1].Get("name")), "Liv|erpool")
	}
}

func TestTabularReaderNestedCommas(t *testing.T) {
	schema := makeListMapSchemaForTabularTest()

	input := `@tab Foo [v a]
[1, 2, 3] {x=1, y=2}
@end`

	tr := NewTabularReaderFromString(input, schema)
	rows, err := tr.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}

	vals := mustAsList(t, rows[0].Get("vals"))
	if len(vals) != 3 || mustAsInt(t, vals[0]) != 1 || mustAsInt(t, vals[1]) != 2 || mustAsInt(t, vals[2]) != 3 {
		t.Errorf("vals mismatch: %v", vals)
	}
	attrs := rows[0].Get("attrs")
	if mustAsInt(t, attrs.Get("x")) != 1 || mustAsInt(t, attrs.Get("y")) != 2 {
		t.Errorf("attrs mismatch: %s", CanonicalizeLoose(attrs))
	}
}

func TestTabularReaderNilSchema(t *testing.T) {
	input := `@tab Team [t n l]
^t:ARS Arsenal EPL
@end`

	tr := NewTabularReaderFromString(input, nil)
	if _, _, err := tr.ReadHeader(); err == nil {
		t.Fatal("expected error for nil schema")
	}
}

func TestInlineTabularNilSchema(t *testing.T) {
	input := "@tab Team [t n l] ^t:ARS Arsenal EPL @end"
	if _, err := ParseInlineTabular(input, nil); err == nil {
		t.Fatal("expected error for nil schema")
	}
}

func TestTabularReaderLargeRow(t *testing.T) {
	schema := makeSingleStringSchemaForTabularTest()
	large := strings.Repeat("a", 200000)
	input := fmt.Sprintf("@tab Blob [n]\n\"%s\"\n@end", large)

	tr := NewTabularReaderFromString(input, schema)
	rows, err := tr.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if mustAsStr(t, rows[0].Get("name")) != large {
		t.Errorf("name length = %d, want %d", len(mustAsStr(t, rows[0].Get("name"))), len(large))
	}
}

func makeListMapSchemaForTabularTest() *Schema {
	return NewSchemaBuilder().
		AddStruct("Foo", "v1",
			Field("vals", ListType(PrimitiveType("int")), WithFID(1), WithWireKey("v")),
			Field("attrs", MapType(PrimitiveType("str"), PrimitiveType("int")), WithFID(2), WithWireKey("a")),
		).
		Build()
}

func makeSingleStringSchemaForTabularTest() *Schema {
	return NewSchemaBuilder().
		AddStruct("Blob", "v1",
			Field("name", PrimitiveType("str"), WithFID(1), WithWireKey("n")),
		).
		Build()
}
