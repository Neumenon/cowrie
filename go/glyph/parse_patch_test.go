package glyph

import (
	"strings"
	"testing"
)

func TestParsePatchBasic(t *testing.T) {
	input := `@patch @schema#abc123 @keys=wire @target=m:ARS-LIV
= home.score 2
= away.score 1
+ events "Goal!"
- odds
~ rating +0.15
@end`

	patch, err := ParsePatch(input, nil)
	if err != nil {
		t.Fatalf("ParsePatch error: %v", err)
	}

	// Verify header
	if patch.SchemaID != "abc123" {
		t.Errorf("SchemaID = %q, want %q", patch.SchemaID, "abc123")
	}
	if patch.Target.Prefix != "m" || patch.Target.Value != "ARS-LIV" {
		t.Errorf("Target = %v, want m:ARS-LIV", patch.Target)
	}

	// Verify operations
	if len(patch.Ops) != 5 {
		t.Fatalf("len(Ops) = %d, want 5", len(patch.Ops))
	}

	// Op 0: = home.score 2
	if patch.Ops[0].Op != OpSet {
		t.Errorf("Op[0].Op = %v, want OpSet", patch.Ops[0].Op)
	}
	if len(patch.Ops[0].Path) != 2 {
		t.Errorf("Op[0].Path len = %d, want 2", len(patch.Ops[0].Path))
	}
	if patch.Ops[0].Path[0].Field != "home" {
		t.Errorf("Op[0].Path[0] = %q, want home", patch.Ops[0].Path[0].Field)
	}
	if patch.Ops[0].Value.intVal != 2 {
		t.Errorf("Op[0].Value = %v, want 2", patch.Ops[0].Value)
	}

	// Op 2: + events "Goal!"
	if patch.Ops[2].Op != OpAppend {
		t.Errorf("Op[2].Op = %v, want OpAppend", patch.Ops[2].Op)
	}
	if patch.Ops[2].Value.strVal != "Goal!" {
		t.Errorf("Op[2].Value = %q, want Goal!", patch.Ops[2].Value.strVal)
	}

	// Op 3: - odds
	if patch.Ops[3].Op != OpDelete {
		t.Errorf("Op[3].Op = %v, want OpDelete", patch.Ops[3].Op)
	}

	// Op 4: ~ rating +0.15
	if patch.Ops[4].Op != OpDelta {
		t.Errorf("Op[4].Op = %v, want OpDelta", patch.Ops[4].Op)
	}
	if patch.Ops[4].Value.floatVal != 0.15 {
		t.Errorf("Op[4].Value = %v, want 0.15", patch.Ops[4].Value.floatVal)
	}
}

func TestParsePatchFIDMode(t *testing.T) {
	input := `@patch @keys=fid @target=m:123
= #1.#2 42
= #3 "hello"
@end`

	patch, err := ParsePatch(input, nil)
	if err != nil {
		t.Fatalf("ParsePatch error: %v", err)
	}

	if len(patch.Ops) != 2 {
		t.Fatalf("len(Ops) = %d, want 2", len(patch.Ops))
	}

	// First op: = #1.#2 42
	if patch.Ops[0].Path[0].FID != 1 {
		t.Errorf("Op[0].Path[0].FID = %d, want 1", patch.Ops[0].Path[0].FID)
	}
	if patch.Ops[0].Path[1].FID != 2 {
		t.Errorf("Op[0].Path[1].FID = %d, want 2", patch.Ops[0].Path[1].FID)
	}
}

func TestParsePatchWithPackedStruct(t *testing.T) {
	schema := makeTeamSchema()

	input := `@patch @target=m:123
= home Team@(^t:ARS Arsenal EPL)
@end`

	patch, err := ParsePatch(input, schema)
	if err != nil {
		t.Fatalf("ParsePatch error: %v", err)
	}

	if len(patch.Ops) != 1 {
		t.Fatalf("len(Ops) = %d, want 1", len(patch.Ops))
	}

	val := patch.Ops[0].Value
	if val.typ != TypeStruct {
		t.Errorf("Value.typ = %v, want TypeStruct", val.typ)
	}
	if val.structVal.TypeName != "Team" {
		t.Errorf("TypeName = %q, want Team", val.structVal.TypeName)
	}
}

func TestParsePatchRoundTrip(t *testing.T) {
	schema := makeMatchSchema()

	// Emit a patch
	original := NewPatch(RefID{Prefix: "m", Value: "ARS-LIV"}, schema.Hash)
	original.TargetType = "Match"
	original.Set("ft_h", Int(2))
	original.Set("ft_a", Int(1))
	original.Append("events", Str("Goal!"))
	original.Delete("odds")
	original.Delta("rating", 0.15)

	emitted, err := EmitPatch(original, schema)
	if err != nil {
		t.Fatalf("EmitPatch error: %v", err)
	}

	t.Logf("Emitted:\n%s", emitted)

	// Parse it back
	parsed, err := ParsePatch(emitted, schema)
	if err != nil {
		t.Fatalf("ParsePatch error: %v", err)
	}

	// Verify same number of operations
	if len(parsed.Ops) != len(original.Ops) {
		t.Fatalf("Parsed %d ops, original had %d", len(parsed.Ops), len(original.Ops))
	}

	// Re-emit and compare
	reEmitted, err := EmitPatch(parsed, schema)
	if err != nil {
		t.Fatalf("Re-emit error: %v", err)
	}

	t.Logf("Re-emitted:\n%s", reEmitted)

	if emitted != reEmitted {
		t.Errorf("Round-trip mismatch:\nOriginal:\n%s\nRe-emitted:\n%s", emitted, reEmitted)
	}
}

func TestParsePatchApplyRoundTrip(t *testing.T) {
	schema := makeMatchSchema()

	// Create a match
	home := makeTeamValue("ARS", "Arsenal", "EPL")
	away := makeTeamValue("LIV", "Liverpool", "EPL")
	odds := makeOddsValue(2.1, 3.4, 3.25)
	match := makeMatchValue("ARS-LIV", mustParseTime("2025-12-19T20:00:00Z"), home, away, odds, nil, nil, nil)

	// Create and emit patch
	patch := NewPatch(RefID{Prefix: "m", Value: "ARS-LIV"}, schema.Hash)
	patch.TargetType = "Match"
	ftH := 2
	ftA := 1
	patch.Set("ft_h", Int(int64(ftH)))
	patch.Set("ft_a", Int(int64(ftA)))

	emitted, err := EmitPatch(patch, schema)
	if err != nil {
		t.Fatalf("EmitPatch error: %v", err)
	}

	t.Logf("Patch:\n%s", emitted)

	// Parse patch back
	parsedPatch, err := ParsePatch(emitted, schema)
	if err != nil {
		t.Fatalf("ParsePatch error: %v", err)
	}

	// Apply parsed patch
	result, err := ApplyPatch(match, parsedPatch)
	if err != nil {
		t.Fatalf("ApplyPatch error: %v", err)
	}

	// Verify result
	ftHVal := result.Get("ft_h")
	if ftHVal == nil || ftHVal.intVal != 2 {
		t.Errorf("ft_h = %v, want 2", ftHVal)
	}

	ftAVal := result.Get("ft_a")
	if ftAVal == nil || ftAVal.intVal != 1 {
		t.Errorf("ft_a = %v, want 1", ftAVal)
	}
}

func TestParsePatchListIndex(t *testing.T) {
	input := `@patch @target=m:123
= items[0] "first"
= items[2] "third"
@end`

	patch, err := ParsePatch(input, nil)
	if err != nil {
		t.Fatalf("ParsePatch error: %v", err)
	}

	if len(patch.Ops) != 2 {
		t.Fatalf("len(Ops) = %d, want 2", len(patch.Ops))
	}

	// Check path segments
	if patch.Ops[0].Path[0].Field != "items" {
		t.Errorf("Path[0].Field = %q, want items", patch.Ops[0].Path[0].Field)
	}
	if patch.Ops[0].Path[1].Kind != PathSegListIdx {
		t.Errorf("Path[1].Kind = %v, want PathSegListIdx", patch.Ops[0].Path[1].Kind)
	}
	if patch.Ops[0].Path[1].ListIdx != 0 {
		t.Errorf("Path[1].ListIdx = %d, want 0", patch.Ops[0].Path[1].ListIdx)
	}
}

func TestParsePatchMapKey(t *testing.T) {
	input := `@patch @target=m:123
= config["timeout"] 30000
= config["name"] "test"
@end`

	patch, err := ParsePatch(input, nil)
	if err != nil {
		t.Fatalf("ParsePatch error: %v", err)
	}

	if len(patch.Ops) != 2 {
		t.Fatalf("len(Ops) = %d, want 2", len(patch.Ops))
	}

	if patch.Ops[0].Path[1].Kind != PathSegMapKey {
		t.Errorf("Path[1].Kind = %v, want PathSegMapKey", patch.Ops[0].Path[1].Kind)
	}
	if patch.Ops[0].Path[1].MapKey != "timeout" {
		t.Errorf("Path[1].MapKey = %q, want timeout", patch.Ops[0].Path[1].MapKey)
	}
}

func TestParsePatchNegativeDelta(t *testing.T) {
	input := `@patch @target=m:123
~ score -5
~ rating -0.25
@end`

	patch, err := ParsePatch(input, nil)
	if err != nil {
		t.Fatalf("ParsePatch error: %v", err)
	}

	if len(patch.Ops) != 2 {
		t.Fatalf("len(Ops) = %d, want 2", len(patch.Ops))
	}

	// Check delta values - ParseFloat handles -5 as float
	val0, ok0 := patch.Ops[0].Value.Number()
	if !ok0 || val0 != -5 {
		t.Errorf("Op[0].Value = %v, want -5", val0)
	}
	val1, ok1 := patch.Ops[1].Value.Number()
	if !ok1 || val1 != -0.25 {
		t.Errorf("Op[1].Value = %v, want -0.25", val1)
	}
}

func TestParsePatchInsertAtIndex(t *testing.T) {
	input := `@patch @target=m:123
+ events "Inserted" @idx=2
@end`

	patch, err := ParsePatch(input, nil)
	if err != nil {
		t.Fatalf("ParsePatch error: %v", err)
	}

	if len(patch.Ops) != 1 {
		t.Fatalf("len(Ops) = %d, want 1", len(patch.Ops))
	}

	if patch.Ops[0].Op != OpAppend {
		t.Errorf("Op = %v, want OpAppend", patch.Ops[0].Op)
	}
	if patch.Ops[0].Index != 2 {
		t.Errorf("Index = %d, want 2", patch.Ops[0].Index)
	}
}

func TestParsePatchErrors(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"missing header", "= foo 1\n@end"},
		{"unknown op", "@patch @target=m:1\n? foo 1\n@end"},
		{"missing path", "@patch @target=m:1\n= \n@end"},
		{"invalid delta", "@patch @target=m:1\n~ foo abc\n@end"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParsePatch(tc.input, nil)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

// TestParsePatchGolden tests against golden patch examples
func TestParsePatchGolden(t *testing.T) {
	schema := makeMatchSchema()

	golden := `@patch @schema#` + schema.Hash + ` @keys=wire @target=m:ARS-LIV
+ events "Kickoff!"
= minute 0
= status live
@end`

	patch, err := ParsePatch(golden, schema)
	if err != nil {
		t.Fatalf("ParsePatch error: %v", err)
	}

	// Re-emit
	reEmitted, err := EmitPatch(patch, schema)
	if err != nil {
		t.Fatalf("EmitPatch error: %v", err)
	}

	// Normalize both for comparison (sort order may differ)
	if !strings.Contains(reEmitted, "@patch") {
		t.Error("Re-emitted missing @patch header")
	}
	if !strings.Contains(reEmitted, "@end") {
		t.Error("Re-emitted missing @end")
	}
	if !strings.Contains(reEmitted, "+ events") {
		t.Error("Re-emitted missing + events operation")
	}
}
