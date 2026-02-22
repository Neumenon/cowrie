package glyph

import (
	"strings"
	"testing"
	"time"
)

// ============================================================
// Tabular Encoding Tests
// ============================================================

func makeHikeSchema() *Schema {
	return NewSchemaBuilder().
		AddPackedStruct("Hike", "v2",
			Field("id", PrimitiveType("int"), WithFID(1), WithWireKey("i")),
			Field("name", PrimitiveType("str"), WithFID(2), WithWireKey("n")),
			Field("distanceKm", PrimitiveType("float"), WithFID(3), WithWireKey("d")),
			Field("elevationGain", PrimitiveType("int"), WithFID(4), WithWireKey("e")),
			Field("companion", PrimitiveType("id"), WithFID(5), WithWireKey("c")),
			Field("wasSunny", PrimitiveType("bool"), WithFID(6), WithWireKey("s")),
		).
		WithPack("Hike").
		WithTab("Hike").
		Build()
}

func makeHikeValue(id int, name string, dist float64, elev int, companion string, sunny bool) *GValue {
	return Struct("Hike",
		FieldVal("id", Int(int64(id))),
		FieldVal("name", Str(name)),
		FieldVal("distanceKm", Float(dist)),
		FieldVal("elevationGain", Int(int64(elev))),
		FieldVal("companion", ID("p", companion)),
		FieldVal("wasSunny", Bool(sunny)),
	)
}

func TestTabularBasic(t *testing.T) {
	schema := makeHikeSchema()

	hikes := List(
		makeHikeValue(1, "Blue Lake Trail", 7.5, 320, "ana", true),
		makeHikeValue(2, "Ridge Overlook", 9.2, 540, "luis", false),
		makeHikeValue(3, "Wildflower Loop", 5.1, 180, "sam", true),
	)

	got, err := EmitTabular(hikes, schema)
	if err != nil {
		t.Fatalf("EmitTabular error: %v", err)
	}

	// Verify structure
	if !strings.HasPrefix(got, "@tab Hike [") {
		t.Errorf("Expected @tab header, got: %s", got[:min(50, len(got))])
	}
	if !strings.HasSuffix(got, "@end") {
		t.Errorf("Expected @end footer, got: %s", got[max(0, len(got)-20):])
	}

	// Verify columns are in FID order with wire keys
	if !strings.Contains(got, "[i n d e c s]") {
		t.Errorf("Expected columns [i n d e c s], got: %s", got)
	}

	// Verify data rows
	lines := strings.Split(got, "\n")
	if len(lines) != 5 { // header + 3 rows + footer
		t.Errorf("Expected 5 lines, got %d: %v", len(lines), lines)
	}

	// First row should have: 1 "Blue Lake Trail" 7.5 320 ^p:ana t
	if !strings.Contains(lines[1], "1") {
		t.Errorf("Row 1 missing id, got: %s", lines[1])
	}

	t.Logf("Tabular output:\n%s", got)
}

func TestTabularEmpty(t *testing.T) {
	schema := makeHikeSchema()
	hikes := List()

	got, err := EmitTabular(hikes, schema)
	if err != nil {
		t.Fatalf("EmitTabular error: %v", err)
	}

	if got != "[]" {
		t.Errorf("Expected [], got: %q", got)
	}
}

func TestTabularWithOptionals(t *testing.T) {
	schema := NewSchemaBuilder().
		AddPackedStruct("Entry", "v2",
			Field("id", PrimitiveType("int"), WithFID(1)),
			Field("name", PrimitiveType("str"), WithFID(2)),
			Field("notes", PrimitiveType("str"), WithFID(3), WithOptional()),
		).
		WithPack("Entry").
		WithTab("Entry").
		Build()

	entries := List(
		Struct("Entry",
			FieldVal("id", Int(1)),
			FieldVal("name", Str("First")),
			FieldVal("notes", Str("Has notes")),
		),
		Struct("Entry",
			FieldVal("id", Int(2)),
			FieldVal("name", Str("Second")),
			// notes missing
		),
		Struct("Entry",
			FieldVal("id", Int(3)),
			FieldVal("name", Str("Third")),
			FieldVal("notes", Null()),
		),
	)

	got, err := EmitTabular(entries, schema)
	if err != nil {
		t.Fatalf("EmitTabular error: %v", err)
	}

	// Row 2 and 3 should have ∅ for notes
	lines := strings.Split(got, "\n")
	if len(lines) < 4 {
		t.Fatalf("Expected at least 4 lines, got %d", len(lines))
	}

	// Row 2: should end with ∅
	if !strings.HasSuffix(strings.TrimSpace(lines[2]), "∅") {
		t.Errorf("Row 2 should have null notes, got: %q", lines[2])
	}

	t.Logf("Tabular with optionals:\n%s", got)
}

func TestTabularNestedPacked(t *testing.T) {
	schema := NewSchemaBuilder().
		AddPackedStruct("Tag", "v2",
			Field("id", PrimitiveType("id"), WithFID(1)),
			Field("label", PrimitiveType("str"), WithFID(2)),
		).
		AddPackedStruct("Item", "v2",
			Field("id", PrimitiveType("int"), WithFID(1)),
			Field("tag", RefType("Tag"), WithFID(2)),
		).
		WithPack("Tag").
		WithPack("Item").
		WithTab("Item").
		Build()

	items := List(
		Struct("Item",
			FieldVal("id", Int(1)),
			FieldVal("tag", Struct("Tag",
				FieldVal("id", ID("t", "001")),
				FieldVal("label", Str("urgent")),
			)),
		),
		Struct("Item",
			FieldVal("id", Int(2)),
			FieldVal("tag", Struct("Tag",
				FieldVal("id", ID("t", "002")),
				FieldVal("label", Str("low")),
			)),
		),
		Struct("Item",
			FieldVal("id", Int(3)),
			FieldVal("tag", Struct("Tag",
				FieldVal("id", ID("t", "003")),
				FieldVal("label", Str("normal")),
			)),
		),
	)

	got, err := EmitTabular(items, schema)
	if err != nil {
		t.Fatalf("EmitTabular error: %v", err)
	}

	// Nested Tag should be packed: Tag@(^t:001 urgent)
	if !strings.Contains(got, "Tag@(") {
		t.Errorf("Expected nested packed Tag, got: %s", got)
	}

	t.Logf("Tabular with nested packed:\n%s", got)
}

func TestTabularInline(t *testing.T) {
	schema := makeHikeSchema()

	hikes := List(
		makeHikeValue(1, "Blue Lake", 7.5, 320, "ana", true),
		makeHikeValue(2, "Ridge", 9.2, 540, "luis", false),
	)

	got, err := EmitInlineTabular(hikes, schema)
	if err != nil {
		t.Fatalf("EmitInlineTabular error: %v", err)
	}

	// Should be single line with | separators
	if strings.Contains(got, "\n") {
		t.Errorf("Inline tabular should not contain newlines, got: %s", got)
	}

	if !strings.Contains(got, " | ") {
		t.Errorf("Inline tabular should have | separators, got: %s", got)
	}

	if !strings.HasPrefix(got, "@tab Hike [") || !strings.HasSuffix(got, "@end") {
		t.Errorf("Invalid inline format, got: %s", got)
	}

	t.Logf("Inline tabular: %s", got)
}

func TestTabularWriter(t *testing.T) {
	schema := makeHikeSchema()
	td := schema.GetType("Hike")

	tw := NewTabularWriter(td, DefaultTabularOptions(schema))

	// Write rows incrementally
	for i := 1; i <= 5; i++ {
		row := makeHikeValue(i, "Trail"+string(rune('A'+i-1)), float64(i)*2.5, i*100, "hiker", i%2 == 0)
		if err := tw.WriteRow(row); err != nil {
			t.Fatalf("WriteRow error: %v", err)
		}
	}

	got, err := tw.Finish()
	if err != nil {
		t.Fatalf("Finish error: %v", err)
	}

	if tw.RowCount() != 5 {
		t.Errorf("Expected 5 rows, got %d", tw.RowCount())
	}

	lines := strings.Split(got, "\n")
	if len(lines) != 7 { // header + 5 rows + footer
		t.Errorf("Expected 7 lines, got %d", len(lines))
	}

	t.Logf("Streaming tabular:\n%s", got)
}

func TestTabularWithTime(t *testing.T) {
	schema := NewSchemaBuilder().
		AddPackedStruct("Event", "v2",
			Field("id", PrimitiveType("int"), WithFID(1)),
			Field("name", PrimitiveType("str"), WithFID(2)),
			Field("when", PrimitiveType("time"), WithFID(3)),
		).
		WithPack("Event").
		WithTab("Event").
		Build()

	t1 := time.Date(2025, 12, 19, 10, 30, 0, 0, time.UTC)
	t2 := time.Date(2025, 12, 20, 14, 0, 0, 0, time.UTC)
	t3 := time.Date(2025, 12, 21, 9, 15, 0, 0, time.UTC)

	events := List(
		Struct("Event",
			FieldVal("id", Int(1)),
			FieldVal("name", Str("Meeting")),
			FieldVal("when", Time(t1)),
		),
		Struct("Event",
			FieldVal("id", Int(2)),
			FieldVal("name", Str("Lunch")),
			FieldVal("when", Time(t2)),
		),
		Struct("Event",
			FieldVal("id", Int(3)),
			FieldVal("name", Str("Review")),
			FieldVal("when", Time(t3)),
		),
	)

	got, err := EmitTabular(events, schema)
	if err != nil {
		t.Fatalf("EmitTabular error: %v", err)
	}

	// Verify ISO-8601 time format
	if !strings.Contains(got, "2025-12-19T10:30:00Z") {
		t.Errorf("Expected ISO time format, got: %s", got)
	}

	t.Logf("Tabular with time:\n%s", got)
}

func TestTabularTokenEstimate(t *testing.T) {
	schema := makeHikeSchema()
	td := schema.GetType("Hike")

	rows := []*GValue{
		makeHikeValue(1, "Trail A", 5.0, 100, "a", true),
		makeHikeValue(2, "Trail B", 6.0, 200, "b", false),
		makeHikeValue(3, "Trail C", 7.0, 300, "c", true),
		makeHikeValue(4, "Trail D", 8.0, 400, "d", false),
		makeHikeValue(5, "Trail E", 9.0, 500, "e", true),
	}

	tabTokens, packedTokens := EstimateTabularTokens(rows, td, schema)

	// Tabular should be more efficient for multiple rows
	t.Logf("Estimated tokens - tabular: %d, packed: %d", tabTokens, packedTokens)

	if tabTokens >= packedTokens {
		t.Logf("Note: For %d rows, tabular (%d) >= packed (%d). Tabular is more efficient with more rows.",
			len(rows), tabTokens, packedTokens)
	}
}

func TestTabularKeyModes(t *testing.T) {
	schema := makeHikeSchema()

	hikes := List(
		makeHikeValue(1, "Trail", 5.0, 100, "a", true),
		makeHikeValue(2, "Path", 6.0, 200, "b", false),
		makeHikeValue(3, "Route", 7.0, 300, "c", true),
	)

	tests := []struct {
		mode     KeyMode
		expected string // expected column header pattern
	}{
		{KeyModeWire, "[i n d e c s]"},
		{KeyModeName, "[id name distanceKm elevationGain companion wasSunny]"},
		{KeyModeFID, "[#1 #2 #3 #4 #5 #6]"},
	}

	for _, tc := range tests {
		opts := DefaultTabularOptions(schema)
		opts.KeyMode = tc.mode

		got, err := EmitTabularWithOptions(hikes, opts)
		if err != nil {
			t.Fatalf("EmitTabular error for mode %v: %v", tc.mode, err)
		}

		if !strings.Contains(got, tc.expected) {
			t.Errorf("Mode %v: expected columns %s, got: %s", tc.mode, tc.expected, got[:min(100, len(got))])
		}
	}
}

func TestTabularIndent(t *testing.T) {
	schema := makeHikeSchema()

	hikes := List(
		makeHikeValue(1, "Trail", 5.0, 100, "a", true),
		makeHikeValue(2, "Path", 6.0, 200, "b", false),
	)

	opts := DefaultTabularOptions(schema)
	opts.IndentPrefix = "    " // 4 spaces

	got, err := EmitTabularWithOptions(hikes, opts)
	if err != nil {
		t.Fatalf("EmitTabular error: %v", err)
	}

	lines := strings.Split(got, "\n")
	// Data rows (not header or footer) should be indented
	for i := 1; i < len(lines)-1; i++ {
		if !strings.HasPrefix(lines[i], "    ") {
			t.Errorf("Row %d not indented: %q", i, lines[i])
		}
	}

	t.Logf("Indented tabular:\n%s", got)
}

// ============================================================
// Error Cases
// ============================================================

func TestTabularNotList(t *testing.T) {
	schema := makeHikeSchema()
	notList := Str("not a list")

	_, err := EmitTabular(notList, schema)
	if err == nil {
		t.Error("Expected error for non-list value")
	}
}

func TestTabularMixedTypes(t *testing.T) {
	schema := NewSchemaBuilder().
		AddPackedStruct("TypeA", "v2",
			Field("id", PrimitiveType("int"), WithFID(1)),
		).
		AddPackedStruct("TypeB", "v2",
			Field("id", PrimitiveType("int"), WithFID(1)),
		).
		WithTab("TypeA").
		WithTab("TypeB").
		Build()

	mixed := List(
		Struct("TypeA", FieldVal("id", Int(1))),
		Struct("TypeB", FieldVal("id", Int(2))), // Different type!
	)

	_, err := EmitTabular(mixed, schema)
	if err == nil {
		t.Error("Expected error for mixed type list")
	}
}

func TestTabularUnknownType(t *testing.T) {
	schema := NewSchemaBuilder().Build() // Empty schema

	items := List(
		Struct("Unknown", FieldVal("x", Int(1))),
		Struct("Unknown", FieldVal("x", Int(2))),
		Struct("Unknown", FieldVal("x", Int(3))),
	)

	_, err := EmitTabular(items, schema)
	if err == nil {
		t.Error("Expected error for unknown type")
	}
}
