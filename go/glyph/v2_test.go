package glyph

import (
	"strings"
	"testing"
	"time"
)

// ============================================================
// LYPH v2 Integration Tests
// ============================================================

func makeHikesSchema() *Schema {
	return NewSchemaBuilder().
		AddPackedStruct("Hike", "v2",
			Field("id", PrimitiveType("int"), WithFID(1), WithWireKey("i")),
			Field("name", PrimitiveType("str"), WithFID(2), WithWireKey("n")),
			Field("distanceKm", PrimitiveType("float"), WithFID(3), WithWireKey("d")),
			Field("elevationGain", PrimitiveType("int"), WithFID(4), WithWireKey("e")),
			Field("companion", PrimitiveType("id"), WithFID(5), WithWireKey("c")),
			Field("wasSunny", PrimitiveType("bool"), WithFID(6), WithWireKey("s")),
		).
		AddPackedStruct("Trip", "v2",
			Field("context", RefType("Context"), WithFID(1)),
			Field("participants", ListType(PrimitiveType("id")), WithFID(2)),
			Field("hikes", ListType(RefType("Hike")), WithFID(3)),
		).
		AddPackedStruct("Context", "v2",
			Field("description", PrimitiveType("str"), WithFID(1)),
			Field("location", PrimitiveType("str"), WithFID(2)),
			Field("season", PrimitiveType("str"), WithFID(3)),
		).
		WithPack("Hike").
		WithPack("Trip").
		WithPack("Context").
		WithTab("Hike").
		Build()
}

func makeHikeTripData() *GValue {
	return Struct("Trip",
		FieldVal("context", Struct("Context",
			FieldVal("description", Str("Our favorite hikes together")),
			FieldVal("location", Str("Boulder")),
			FieldVal("season", Str("spring_2025")),
		)),
		FieldVal("participants", List(
			ID("p", "ana"),
			ID("p", "luis"),
			ID("p", "sam"),
		)),
		FieldVal("hikes", List(
			Struct("Hike",
				FieldVal("id", Int(1)),
				FieldVal("name", Str("Blue Lake Trail")),
				FieldVal("distanceKm", Float(7.5)),
				FieldVal("elevationGain", Int(320)),
				FieldVal("companion", ID("p", "ana")),
				FieldVal("wasSunny", Bool(true)),
			),
			Struct("Hike",
				FieldVal("id", Int(2)),
				FieldVal("name", Str("Ridge Overlook")),
				FieldVal("distanceKm", Float(9.2)),
				FieldVal("elevationGain", Int(540)),
				FieldVal("companion", ID("p", "luis")),
				FieldVal("wasSunny", Bool(false)),
			),
			Struct("Hike",
				FieldVal("id", Int(3)),
				FieldVal("name", Str("Wildflower Loop")),
				FieldVal("distanceKm", Float(5.1)),
				FieldVal("elevationGain", Int(180)),
				FieldVal("companion", ID("p", "sam")),
				FieldVal("wasSunny", Bool(true)),
			),
		)),
	)
}

func TestV2HeaderParsing(t *testing.T) {
	tests := []struct {
		input    string
		version  string
		schemaID string
		mode     Mode
		keyMode  KeyMode
	}{
		{
			"@lyph v2 @schema#abc123 @mode=packed @keys=wire",
			"v2", "abc123", ModePacked, KeyModeWire,
		},
		{
			"@lyph v2 @mode=tabular",
			"v2", "", ModeTabular, KeyModeWire,
		},
		{
			"@glyph v2 @keys=fid",
			"v2", "", ModeAuto, KeyModeFID,
		},
		{
			"@lyph v2",
			"v2", "", ModeAuto, KeyModeWire,
		},
	}

	for _, tc := range tests {
		h, err := ParseHeader(tc.input)
		if err != nil {
			t.Fatalf("ParseHeader(%q) error: %v", tc.input, err)
		}
		if h == nil {
			t.Fatalf("ParseHeader(%q) returned nil", tc.input)
		}

		if h.Version != tc.version {
			t.Errorf("Version: expected %q, got %q", tc.version, h.Version)
		}
		if h.SchemaID != tc.schemaID {
			t.Errorf("SchemaID: expected %q, got %q", tc.schemaID, h.SchemaID)
		}
		if h.Mode != tc.mode {
			t.Errorf("Mode: expected %v, got %v", tc.mode, h.Mode)
		}
		if h.KeyMode != tc.keyMode {
			t.Errorf("KeyMode: expected %v, got %v", tc.keyMode, h.KeyMode)
		}
	}
}

func TestV2EmitHeader(t *testing.T) {
	h := &Header{
		Version:  "v2",
		SchemaID: "abc123",
		Mode:     ModePacked,
		KeyMode:  KeyModeName,
	}

	got := EmitHeader(h)
	expected := "@lyph v2 @schema#abc123 @mode=packed @keys=name"

	if got != expected {
		t.Errorf("EmitHeader:\n  got:  %q\n  want: %q", got, expected)
	}
}

func TestV2PackedRoundTrip(t *testing.T) {
	schema := makeTeamSchema()
	team := makeTeamValue("ARS", "Arsenal", "EPL")

	// Emit as packed
	packed, err := EmitPacked(team, schema)
	if err != nil {
		t.Fatalf("EmitPacked error: %v", err)
	}

	t.Logf("Packed: %s", packed)

	// Parse back
	parsed, err := ParsePacked(packed, schema)
	if err != nil {
		t.Fatalf("ParsePacked error: %v", err)
	}

	// Verify fields
	if mustAsID(t, parsed.Get("id")).Value != "ARS" {
		t.Errorf("id mismatch: expected ARS, got %v", parsed.Get("id"))
	}
	if mustAsStr(t, parsed.Get("name")) != "Arsenal" {
		t.Errorf("name mismatch: expected Arsenal, got %v", parsed.Get("name"))
	}
	if mustAsStr(t, parsed.Get("league")) != "EPL" {
		t.Errorf("league mismatch: expected EPL, got %v", parsed.Get("league"))
	}
}

func TestV2BitmapRoundTrip(t *testing.T) {
	schema := makeMatchSchema()
	kickoff := time.Date(2025, 12, 19, 20, 0, 0, 0, time.UTC)

	home := makeTeamValue("ARS", "Arsenal", "EPL")
	away := makeTeamValue("LIV", "Liverpool", "EPL")

	// Match with only some optionals set
	match := Struct("Match",
		FieldVal("id", ID("m", "ARS-LIV")),
		FieldVal("kickoff", Time(kickoff)),
		FieldVal("home", home),
		FieldVal("away", away),
		// odds, pred missing
		FieldVal("ft_h", Int(2)),
		// ft_a missing
	)

	packed, err := EmitPacked(match, schema)
	if err != nil {
		t.Fatalf("EmitPacked error: %v", err)
	}

	t.Logf("Bitmap packed: %s", packed)

	// Should contain bitmap marker
	if !strings.Contains(packed, "@{bm=") {
		t.Errorf("Expected bitmap form, got: %s", packed)
	}

	// Parse back
	parsed, err := ParsePacked(packed, schema)
	if err != nil {
		t.Fatalf("ParsePacked error: %v", err)
	}

	// Verify present fields
	if mustAsID(t, parsed.Get("id")).Value != "ARS-LIV" {
		t.Errorf("id mismatch")
	}
	if mustAsInt(t, parsed.Get("ft_h")) != 2 {
		t.Errorf("ft_h mismatch: expected 2, got %v", parsed.Get("ft_h"))
	}

	// Verify missing fields are null
	if !parsed.Get("odds").IsNull() {
		t.Errorf("odds should be null")
	}
	if !parsed.Get("ft_a").IsNull() {
		t.Errorf("ft_a should be null")
	}
}

func TestV2TabularHikes(t *testing.T) {
	schema := makeHikesSchema()
	trip := makeHikeTripData()

	// Get hikes list
	hikes := trip.Get("hikes")

	// Emit as tabular
	tabular, err := EmitTabular(hikes, schema)
	if err != nil {
		t.Fatalf("EmitTabular error: %v", err)
	}

	t.Logf("Tabular hikes:\n%s", tabular)

	// Verify structure
	if !strings.HasPrefix(tabular, "@tab Hike [") {
		t.Errorf("Missing @tab header")
	}
	if !strings.HasSuffix(tabular, "@end") {
		t.Errorf("Missing @end footer")
	}

	// Count data rows
	lines := strings.Split(tabular, "\n")
	dataRows := 0
	for _, line := range lines {
		if !strings.HasPrefix(line, "@") && len(strings.TrimSpace(line)) > 0 {
			dataRows++
		}
	}
	if dataRows != 3 {
		t.Errorf("Expected 3 data rows, got %d", dataRows)
	}
}

func TestV2FullTrip(t *testing.T) {
	schema := makeHikesSchema()
	trip := makeHikeTripData()

	opts := DefaultV2Options(schema)
	opts.IncludeHeader = true

	output, err := EmitV2(trip, opts)
	if err != nil {
		t.Fatalf("EmitV2 error: %v", err)
	}

	t.Logf("Full trip v2:\n%s", output)

	// Should have header
	if !strings.HasPrefix(output, "@lyph v2") {
		t.Errorf("Missing v2 header")
	}

	// Should be packed (Trip has PackEnabled)
	if !strings.Contains(output, "Trip@(") {
		t.Errorf("Expected packed Trip format")
	}
}

func TestV2TokenComparison(t *testing.T) {
	schema := makeHikesSchema()
	hikes := makeHikeTripData().Get("hikes")

	// Count tokens in different formats
	tabular, _ := EmitTabular(hikes, schema)

	// For comparison, emit each hike as packed
	var packedTotal int
	for _, hike := range mustAsList(t, hikes) {
		packed, _ := EmitPacked(hike, schema)
		packedTotal += len(strings.Fields(packed))
	}

	tabTokens := len(strings.Fields(tabular))

	t.Logf("Token comparison for 3 hikes:")
	t.Logf("  Tabular: ~%d tokens", tabTokens)
	t.Logf("  Packed (3x): ~%d tokens", packedTotal)

	// Tabular should be more efficient for multiple rows
	// (header overhead amortized across rows)
}

func TestV2ModeDetection(t *testing.T) {
	tests := []struct {
		input    string
		expected Mode
	}{
		{"@lyph v2 @mode=packed", ModePacked},
		{"@patch @target=m:123", ModePatch},
		{"@tab Hike [id name]", ModeTabular},
		{"Team@(^t:ARS Arsenal EPL)", ModePacked},
		{"Team@{bm=0b1}(data)", ModePacked},
		{"Team{id=^t:ARS name=Arsenal}", ModeStruct},
	}

	for _, tc := range tests {
		got := DetectMode(tc.input)
		if got != tc.expected {
			t.Errorf("DetectMode(%q): expected %v, got %v", tc.input, tc.expected, got)
		}
	}
}

func TestV2PatchIntegration(t *testing.T) {
	schema := makeMatchSchema()

	// Initial state
	match := Struct("Match",
		FieldVal("id", ID("m", "ARS-LIV")),
		FieldVal("status", Str("scheduled")),
		FieldVal("events", List()),
	)

	// Create patch
	patch := NewPatch(RefID{Prefix: "m", Value: "ARS-LIV"}, schema.Hash).
		Set("status", Str("live")).
		Append("events", Str("Kickoff!")).
		Set("minute", Int(0))

	// Emit patch
	patchStr, err := EmitPatch(patch, schema)
	if err != nil {
		t.Fatalf("EmitPatch error: %v", err)
	}

	t.Logf("Patch:\n%s", patchStr)

	// Apply patch
	updated, err := ApplyPatch(match, patch)
	if err != nil {
		t.Fatalf("ApplyPatch error: %v", err)
	}

	// Verify changes
	if mustAsStr(t, updated.Get("status")) != "live" {
		t.Errorf("status should be 'live'")
	}
	if updated.Get("events").Len() != 1 {
		t.Errorf("events should have 1 element")
	}
	if mustAsInt(t, updated.Get("minute")) != 0 {
		t.Errorf("minute should be 0")
	}

	// Original unchanged
	if mustAsStr(t, match.Get("status")) != "scheduled" {
		t.Errorf("original should be unchanged")
	}
}

func TestV2DiffIntegration(t *testing.T) {
	before := Struct("Match",
		FieldVal("id", ID("m", "123")),
		FieldVal("status", Str("scheduled")),
		FieldVal("minute", Int(0)),
	)

	after := Struct("Match",
		FieldVal("id", ID("m", "123")),
		FieldVal("status", Str("finished")),
		FieldVal("minute", Int(90)),
		FieldVal("result", Str("home")),
	)

	patch := Diff(before, after, "Match")

	// Apply patch to before
	result, err := ApplyPatch(before, patch)
	if err != nil {
		t.Fatalf("ApplyPatch error: %v", err)
	}

	// Verify result matches after
	if mustAsStr(t, result.Get("status")) != "finished" {
		t.Errorf("status mismatch")
	}
	if mustAsInt(t, result.Get("minute")) != 90 {
		t.Errorf("minute mismatch")
	}
	if mustAsStr(t, result.Get("result")) != "home" {
		t.Errorf("result mismatch")
	}
}

func TestV2NestedPackedParsing(t *testing.T) {
	schema := NewSchemaBuilder().
		AddPackedStruct("Inner", "v2",
			Field("x", PrimitiveType("int"), WithFID(1)),
			Field("y", PrimitiveType("int"), WithFID(2)),
		).
		AddPackedStruct("Outer", "v2",
			Field("id", PrimitiveType("int"), WithFID(1)),
			Field("inner", RefType("Inner"), WithFID(2)),
		).
		WithPack("Inner").
		WithPack("Outer").
		Build()

	outer := Struct("Outer",
		FieldVal("id", Int(1)),
		FieldVal("inner", Struct("Inner",
			FieldVal("x", Int(10)),
			FieldVal("y", Int(20)),
		)),
	)

	packed, err := EmitPacked(outer, schema)
	if err != nil {
		t.Fatalf("EmitPacked error: %v", err)
	}

	t.Logf("Nested packed: %s", packed)

	// Should contain nested packed
	if !strings.Contains(packed, "Inner@(") {
		t.Errorf("Expected nested Inner@(...)")
	}

	// Parse back
	parsed, err := ParsePacked(packed, schema)
	if err != nil {
		t.Fatalf("ParsePacked error: %v", err)
	}

	inner := parsed.Get("inner")
	if mustAsInt(t, inner.Get("x")) != 10 {
		t.Errorf("inner.x mismatch")
	}
	if mustAsInt(t, inner.Get("y")) != 20 {
		t.Errorf("inner.y mismatch")
	}
}

func TestV2SchemaHash(t *testing.T) {
	schema1 := NewSchemaBuilder().
		AddPackedStruct("Foo", "v1",
			Field("id", PrimitiveType("int"), WithFID(1)),
		).Build()

	schema2 := NewSchemaBuilder().
		AddPackedStruct("Foo", "v1",
			Field("id", PrimitiveType("int"), WithFID(1)),
		).Build()

	// Same schemas should have same hash
	if schema1.Hash != schema2.Hash {
		t.Errorf("Identical schemas should have same hash: %s vs %s", schema1.Hash, schema2.Hash)
	}

	schema3 := NewSchemaBuilder().
		AddPackedStruct("Foo", "v1",
			Field("id", PrimitiveType("int"), WithFID(1)),
			Field("name", PrimitiveType("str"), WithFID(2)),
		).Build()

	// Different schemas should have different hash
	if schema1.Hash == schema3.Hash {
		t.Errorf("Different schemas should have different hash")
	}
}

// ============================================================
// Benchmark vs JSON
// ============================================================

func BenchmarkV2Packed(b *testing.B) {
	schema := makeTeamSchema()
	team := makeTeamValue("ARS", "Arsenal", "EPL")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EmitPacked(team, schema)
	}
}

func BenchmarkV2Tabular(b *testing.B) {
	schema := makeHikesSchema()
	hikes := makeHikeTripData().Get("hikes")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EmitTabular(hikes, schema)
	}
}

func BenchmarkV2PackedParse(b *testing.B) {
	schema := makeTeamSchema()
	team := makeTeamValue("ARS", "Arsenal", "EPL")
	packed, _ := EmitPacked(team, schema)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParsePacked(packed, schema)
	}
}
