package glyph

import (
	"strings"
	"testing"
)

// ============================================================
// Patch Encoding Tests
// ============================================================

func makeMatchForPatch() *GValue {
	return Struct("Match",
		FieldVal("id", ID("m", "ARS-LIV")),
		FieldVal("home", Struct("Team",
			FieldVal("id", ID("t", "ARS")),
			FieldVal("name", Str("Arsenal")),
			FieldVal("rating", Float(1850.5)),
		)),
		FieldVal("away", Struct("Team",
			FieldVal("id", ID("t", "LIV")),
			FieldVal("name", Str("Liverpool")),
			FieldVal("rating", Float(1890.0)),
		)),
		FieldVal("events", List()),
	)
}

func TestPatchBasic(t *testing.T) {
	schema := NewSchemaBuilder().Build()

	patch := NewPatch(RefID{Prefix: "m", Value: "ARS-LIV"}, "abc123").
		Set("home.ft_h", Int(2)).
		Set("away.ft_a", Int(1))

	got, err := EmitPatch(patch, schema)
	if err != nil {
		t.Fatalf("EmitPatch error: %v", err)
	}

	// Verify header
	if !strings.HasPrefix(got, "@patch") {
		t.Errorf("Expected @patch header, got: %s", got[:min(50, len(got))])
	}
	if !strings.Contains(got, "@schema#abc123") {
		t.Errorf("Expected schema hash, got: %s", got)
	}
	if !strings.Contains(got, "@target=m:ARS-LIV") {
		t.Errorf("Expected target, got: %s", got)
	}

	// Verify operations
	if !strings.Contains(got, "= away.ft_a 1") {
		t.Errorf("Expected set operation for away.ft_a, got: %s", got)
	}
	if !strings.Contains(got, "= home.ft_h 2") {
		t.Errorf("Expected set operation for home.ft_h, got: %s", got)
	}

	// Verify footer
	if !strings.HasSuffix(got, "@end") {
		t.Errorf("Expected @end footer, got: %s", got)
	}

	t.Logf("Patch output:\n%s", got)
}

func TestPatchAllOperations(t *testing.T) {
	schema := NewSchemaBuilder().Build()

	patch := NewPatch(RefID{Prefix: "m", Value: "123"}, "").
		Set("score", Int(5)).
		Append("events", Str("Goal!")).
		Delete("odds").
		Delta("home.rating", 0.15)

	got, err := EmitPatch(patch, schema)
	if err != nil {
		t.Fatalf("EmitPatch error: %v", err)
	}

	// Verify all operation types
	if !strings.Contains(got, "= score 5") {
		t.Errorf("Missing set operation, got: %s", got)
	}
	if !strings.Contains(got, `+ events "Goal!"`) {
		t.Errorf("Missing append operation, got: %s", got)
	}
	if !strings.Contains(got, "- odds") {
		t.Errorf("Missing delete operation, got: %s", got)
	}
	if !strings.Contains(got, "~ home.rating +0.15") {
		t.Errorf("Missing delta operation, got: %s", got)
	}

	t.Logf("All operations:\n%s", got)
}

func TestPatchNegativeDelta(t *testing.T) {
	schema := NewSchemaBuilder().Build()

	patch := NewPatch(RefID{Prefix: "m", Value: "123"}, "").
		Delta("score", -3)

	got, err := EmitPatch(patch, schema)
	if err != nil {
		t.Fatalf("EmitPatch error: %v", err)
	}

	if !strings.Contains(got, "~ score -3") {
		t.Errorf("Expected negative delta, got: %s", got)
	}

	t.Logf("Negative delta:\n%s", got)
}

func TestPatchComplexValue(t *testing.T) {
	schema := NewSchemaBuilder().Build()

	event := Struct("Event",
		FieldVal("minute", Int(90)),
		FieldVal("type", Str("Goal")),
		FieldVal("player", ID("p", "smith")),
	)

	patch := NewPatch(RefID{Prefix: "m", Value: "123"}, "").
		Append("events", event)

	got, err := EmitPatch(patch, schema)
	if err != nil {
		t.Fatalf("EmitPatch error: %v", err)
	}

	// Verify nested struct in patch
	if !strings.Contains(got, "Event{") {
		t.Errorf("Expected struct value in append, got: %s", got)
	}

	t.Logf("Complex value patch:\n%s", got)
}

func TestPatchBuilder(t *testing.T) {
	patch := NewPatchBuilder(RefID{Prefix: "m", Value: "ARS-LIV"}).
		WithSchemaID("schema123").
		Set("home.ft_h", Int(2)).
		Append("events", Str("HT")).
		Delete("odds.pre").
		Delta("home.rating", 15.5).
		Build()

	if patch.SchemaID != "schema123" {
		t.Errorf("Expected schema ID schema123, got: %s", patch.SchemaID)
	}
	if len(patch.Ops) != 4 {
		t.Errorf("Expected 4 operations, got: %d", len(patch.Ops))
	}
}

func TestPatchApply(t *testing.T) {
	match := makeMatchForPatch()

	patch := NewPatch(RefID{}, "").
		Set("home.ft_h", Int(2)).
		Set("away.ft_a", Int(1))

	result, err := ApplyPatch(match, patch)
	if err != nil {
		t.Fatalf("ApplyPatch error: %v", err)
	}

	// Verify the changes
	home := result.Get("home")
	if home == nil {
		t.Fatal("home field missing")
	}
	ftH := home.Get("ft_h")
	if ftH == nil || mustAsInt(t, ftH) != 2 {
		t.Errorf("Expected home.ft_h = 2, got: %v", ftH)
	}

	away := result.Get("away")
	if away == nil {
		t.Fatal("away field missing")
	}
	ftA := away.Get("ft_a")
	if ftA == nil || mustAsInt(t, ftA) != 1 {
		t.Errorf("Expected away.ft_a = 1, got: %v", ftA)
	}

	// Verify original is unchanged (immutable)
	origHome := match.Get("home")
	if origHome.Get("ft_h") != nil {
		t.Error("Original should not be modified")
	}
}

func TestPatchApplyAppend(t *testing.T) {
	match := makeMatchForPatch()

	patch := NewPatch(RefID{}, "").
		Append("events", Str("Kickoff")).
		Append("events", Str("Goal!"))

	result, err := ApplyPatch(match, patch)
	if err != nil {
		t.Fatalf("ApplyPatch error: %v", err)
	}

	events := result.Get("events")
	if events == nil || events.Len() != 2 {
		t.Errorf("Expected 2 events, got: %v", events)
	}
	idx0, err := events.Index(0)
	if err != nil {
		t.Fatalf("Index(0) failed: %v", err)
	}
	if mustAsStr(t, idx0) != "Kickoff" {
		t.Errorf("Expected first event = Kickoff, got: %s", mustAsStr(t, idx0))
	}
}

func TestPatchApplyDelete(t *testing.T) {
	match := makeMatchForPatch()

	patch := NewPatch(RefID{}, "").
		Delete("events")

	result, err := ApplyPatch(match, patch)
	if err != nil {
		t.Fatalf("ApplyPatch error: %v", err)
	}

	events := result.Get("events")
	if events != nil {
		t.Errorf("Expected events to be deleted, got: %v", events)
	}
}

func TestPatchApplyDelta(t *testing.T) {
	match := makeMatchForPatch()

	patch := NewPatch(RefID{}, "").
		Delta("home.rating", 50.5)

	result, err := ApplyPatch(match, patch)
	if err != nil {
		t.Fatalf("ApplyPatch error: %v", err)
	}

	home := result.Get("home")
	rating := home.Get("rating")
	expected := 1850.5 + 50.5
	if mustAsFloat(t, rating) != expected {
		t.Errorf("Expected rating = %f, got: %f", expected, mustAsFloat(t, rating))
	}
}

func TestDiff(t *testing.T) {
	from := Struct("Match",
		FieldVal("id", ID("m", "123")),
		FieldVal("score", Int(0)),
		FieldVal("status", Str("pending")),
	)

	to := Struct("Match",
		FieldVal("id", ID("m", "123")),
		FieldVal("score", Int(3)),
		FieldVal("status", Str("finished")),
		FieldVal("winner", Str("home")),
	)

	patch := Diff(from, to, "Match")

	// Should have: score change, status change, winner added
	if len(patch.Ops) < 3 {
		t.Errorf("Expected at least 3 operations, got: %d", len(patch.Ops))
	}

	// Verify operations exist
	hasScore := false
	hasStatus := false
	hasWinner := false

	for _, op := range patch.Ops {
		pathStr := pathSegsStr(op.Path)
		if pathStr == "score" {
			hasScore = true
		}
		if pathStr == "status" {
			hasStatus = true
		}
		if pathStr == "winner" {
			hasWinner = true
		}
	}

	if !hasScore {
		t.Error("Missing score change")
	}
	if !hasStatus {
		t.Error("Missing status change")
	}
	if !hasWinner {
		t.Error("Missing winner addition")
	}

	t.Logf("Diff generated %d operations", len(patch.Ops))
}

func TestDiffWithDeletion(t *testing.T) {
	from := Struct("Match",
		FieldVal("id", ID("m", "123")),
		FieldVal("odds", Float(1.5)),
		FieldVal("pred", Str("home")),
	)

	to := Struct("Match",
		FieldVal("id", ID("m", "123")),
		// odds and pred removed
	)

	patch := Diff(from, to, "Match")

	// Should have deletions for odds and pred
	deleteCount := 0
	for _, op := range patch.Ops {
		if op.Op == OpDelete {
			deleteCount++
		}
	}

	if deleteCount != 2 {
		t.Errorf("Expected 2 deletions, got: %d", deleteCount)
	}
}

func TestPatchSorting(t *testing.T) {
	patch := NewPatch(RefID{Prefix: "m", Value: "123"}, "").
		Set("z.field", Int(1)).
		Set("a.field", Int(2)).
		Set("m.field", Int(3))

	schema := NewSchemaBuilder().Build()
	opts := DefaultPatchOptions(schema)
	opts.SortOps = true

	got, err := EmitPatchWithOptions(patch, opts)
	if err != nil {
		t.Fatalf("EmitPatch error: %v", err)
	}

	lines := strings.Split(got, "\n")

	// Find operation lines (skip header and footer)
	var opLines []string
	for _, line := range lines {
		if strings.HasPrefix(line, "=") {
			opLines = append(opLines, line)
		}
	}

	// Should be sorted: a.field, m.field, z.field
	if len(opLines) != 3 {
		t.Fatalf("Expected 3 op lines, got: %d", len(opLines))
	}
	if !strings.Contains(opLines[0], "a.field") {
		t.Errorf("First op should be a.field, got: %s", opLines[0])
	}
	if !strings.Contains(opLines[1], "m.field") {
		t.Errorf("Second op should be m.field, got: %s", opLines[1])
	}
	if !strings.Contains(opLines[2], "z.field") {
		t.Errorf("Third op should be z.field, got: %s", opLines[2])
	}
}

func TestPathParsing(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"home.ft_h", []string{"home", "ft_h"}},
		{"a.b.c.d", []string{"a", "b", "c", "d"}},
		{"single", []string{"single"}},
		{"", nil},
	}

	for _, tc := range tests {
		got := parsePathToSegs(tc.input)
		if len(got) != len(tc.expected) {
			t.Errorf("parsePathToSegs(%q): expected %d segs, got %d", tc.input, len(tc.expected), len(got))
			continue
		}
		for i := range got {
			if got[i].Field != tc.expected[i] {
				t.Errorf("parsePathToSegs(%q)[%d]: expected %q, got %q", tc.input, i, tc.expected[i], got[i].Field)
			}
		}
	}
}

func TestPathFIDParsing(t *testing.T) {
	tests := []struct {
		input       string
		expectedFID []int
	}{
		{"#3.#2", []int{3, 2}},
		{"#1", []int{1}},
		{"home.#2", []int{0, 2}}, // home has no FID, #2 has FID=2
		{"#3.name", []int{3, 0}}, // #3 has FID=3, name has no FID
	}

	for _, tc := range tests {
		got := parsePathToSegs(tc.input)
		if len(got) != len(tc.expectedFID) {
			t.Errorf("parsePathToSegs(%q): expected %d segs, got %d", tc.input, len(tc.expectedFID), len(got))
			continue
		}
		for i := range got {
			if got[i].FID != tc.expectedFID[i] {
				t.Errorf("parsePathToSegs(%q)[%d]: expected FID=%d, got FID=%d", tc.input, i, tc.expectedFID[i], got[i].FID)
			}
		}
	}
}

func TestDeepCopy(t *testing.T) {
	original := Struct("Test",
		FieldVal("num", Int(42)),
		FieldVal("str", Str("hello")),
		FieldVal("list", List(Int(1), Int(2), Int(3))),
		FieldVal("nested", Struct("Inner",
			FieldVal("x", Float(3.14)),
		)),
	)

	copied := deepCopy(original)

	// Modify the copy
	copied.Get("num").intVal = 100
	copied.Get("str").strVal = "modified"
	copied.Get("list").listVal[0].intVal = 999
	copied.Get("nested").Get("x").floatVal = 9.99

	// Verify original is unchanged
	if mustAsInt(t, original.Get("num")) != 42 {
		t.Error("Original num was modified")
	}
	if mustAsStr(t, original.Get("str")) != "hello" {
		t.Error("Original str was modified")
	}
	idx0, err := original.Get("list").Index(0)
	if err != nil {
		t.Fatalf("Index(0) failed: %v", err)
	}
	if mustAsInt(t, idx0) != 1 {
		t.Error("Original list was modified")
	}
	if mustAsFloat(t, original.Get("nested").Get("x")) != 3.14 {
		t.Error("Original nested was modified")
	}
}

// ============================================================
// v2.4.0: Base Fingerprint Tests
// ============================================================

func TestPatchWithBaseFingerprint(t *testing.T) {
	schema := NewSchemaBuilder().Build()

	// Create a base state
	baseState := Map(
		MapEntry{Key: "score", Value: Int(0)},
		MapEntry{Key: "status", Value: Str("pending")},
	)

	// Create patch with base fingerprint
	patch := NewPatchBuilder(RefID{Prefix: "m", Value: "123"}).
		WithBaseValue(baseState).
		Set("score", Int(5)).
		Build()

	got, err := EmitPatch(patch, schema)
	if err != nil {
		t.Fatalf("EmitPatch error: %v", err)
	}

	// Verify base fingerprint is in header
	if !strings.Contains(got, "@base=") {
		t.Errorf("Expected @base= in header, got: %s", got)
	}

	// Fingerprint should be 16 hex chars
	if patch.BaseFingerprint == "" || len(patch.BaseFingerprint) != 16 {
		t.Errorf("Expected 16-char fingerprint, got: %q", patch.BaseFingerprint)
	}

	t.Logf("Patch with base fingerprint:\n%s", got)
}

func TestPatchBaseFingerprint_Parse(t *testing.T) {
	input := `@patch @schema#abc123 @keys=wire @target=m:123 @base=1234567890abcdef
= score 5
@end`

	patch, err := ParsePatch(input, nil)
	if err != nil {
		t.Fatalf("ParsePatch error: %v", err)
	}

	if patch.BaseFingerprint != "1234567890abcdef" {
		t.Errorf("Expected base fingerprint '1234567890abcdef', got: %q", patch.BaseFingerprint)
	}
}

func TestPatchBaseFingerprint_Roundtrip(t *testing.T) {
	schema := NewSchemaBuilder().Build()

	// Create a base state
	baseState := Map(
		MapEntry{Key: "x", Value: Int(10)},
		MapEntry{Key: "y", Value: Int(20)},
	)

	// Create patch with base fingerprint
	originalPatch := NewPatchBuilder(RefID{Prefix: "m", Value: "test"}).
		WithBaseValue(baseState).
		Set("x", Int(100)).
		Build()

	// Emit
	patchText, err := EmitPatch(originalPatch, schema)
	if err != nil {
		t.Fatalf("EmitPatch error: %v", err)
	}

	// Parse back
	parsedPatch, err := ParsePatch(patchText, schema)
	if err != nil {
		t.Fatalf("ParsePatch error: %v", err)
	}

	// Verify fingerprints match
	if originalPatch.BaseFingerprint != parsedPatch.BaseFingerprint {
		t.Errorf("Fingerprint mismatch:\nOriginal: %s\nParsed: %s",
			originalPatch.BaseFingerprint, parsedPatch.BaseFingerprint)
	}
}

func TestPatchWithExplicitFingerprint(t *testing.T) {
	schema := NewSchemaBuilder().Build()

	// Create patch with explicit fingerprint
	patch := NewPatchBuilder(RefID{Prefix: "m", Value: "123"}).
		WithBaseFingerprint("abcdef0123456789").
		Set("value", Int(42)).
		Build()

	got, err := EmitPatch(patch, schema)
	if err != nil {
		t.Fatalf("EmitPatch error: %v", err)
	}

	if !strings.Contains(got, "@base=abcdef0123456789") {
		t.Errorf("Expected explicit base fingerprint, got: %s", got)
	}
}
