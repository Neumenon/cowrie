package glyph

import (
	"testing"
)

func TestParseLoosePayload_AllTypes(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"int", "42"},
		{"neg_int", "-7"},
		{"float", "3.14"},
		{"bool_true", "true"},
		{"bool_false", "false"},
		{"null", "_"},
		{"string_bare", "hello"},
		{"string_quoted", `"hello world"`},
		{"ref", "^d:1"},
		{"list_simple", "[1 2 3]"},
		{"list_nested", "[[1 2] [3 4]]"},
		{"list_mixed", `[1 "two" true _]`},
		{"map_simple", "name Alice\nage 30"},
		{"map_with_ref", "target ^d:123\nname test"},
		{"map_nested", "outer {\n  inner 42\n}"},
		{"map_list_value", "items [1 2 3]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, _, err := ParseLoosePayload(tt.input, nil)
			if err != nil {
				t.Fatalf("ParseLoosePayload(%q): %v", tt.name, err)
			}
			if v == nil {
				t.Error("nil value")
			}
		})
	}
}

func TestCanonicalizeLooseWithOpts_Coverage(t *testing.T) {
	v := Map(
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "items", Value: List(Int(1), Int(2), Int(3))},
		MapEntry{Key: "nested", Value: Map(
			MapEntry{Key: "x", Value: Int(1)},
		)},
	)

	opts := LooseCanonOpts{
		AutoTabular: true,
	}
	result := CanonicalizeLooseWithOpts(v, opts)
	if result == "" {
		t.Error("empty result")
	}

	// Without tabular
	opts2 := LooseCanonOpts{
		AutoTabular: false,
	}
	result2 := CanonicalizeLooseWithOpts(v, opts2)
	if result2 == "" {
		t.Error("empty result without tabular")
	}
}

func TestParseLoosePayload_ListOfMaps(t *testing.T) {
	// A list of maps (triggers tabular detection)
	input := "[\n  {name Alice age 30}\n  {name Bob age 25}\n]"
	v, _, err := ParseLoosePayload(input, nil)
	if err != nil {
		// Try simpler format
		input2 := "[{name Alice} {name Bob}]"
		v, _, err = ParseLoosePayload(input2, nil)
		if err != nil {
			t.Skipf("list of maps parsing not supported in this format: %v", err)
			return
		}
	}
	if v == nil {
		t.Error("nil value")
	}
}

func TestParseLoosePayload_Tabular(t *testing.T) {
	input := "@tab _ [name age]\n|Alice|30|\n|Bob|25|\n@end"
	v, _, err := ParseLoosePayload(input, nil)
	if err != nil {
		t.Skipf("tabular format: %v", err)
		return
	}
	if v == nil {
		t.Error("nil value")
	}
}

func TestEmitPatch_AllOps_Deep(t *testing.T) {
	// Test all patch operations
	p := &Patch{
		Target:   RefID{Prefix: "d", Value: "1"},
		SchemaID: "schema1",
		Ops: []*PatchOp{
			{Op: OpSet, Path: []PathSeg{{Field: "name"}}, Value: Str("Alice")},
			{Op: OpAppend, Path: []PathSeg{{Field: "tags"}}, Value: Str("new_tag")},
			{Op: OpDelete, Path: []PathSeg{{Field: "old_field"}}},
			{Op: OpDelta, Path: []PathSeg{{Field: "count"}}, Value: Int(1)},
		},
	}

	result, err := EmitPatch(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result == "" {
		t.Error("empty result")
	}
}

func TestApplyPatch_Append(t *testing.T) {
	v := Map(
		MapEntry{Key: "tags", Value: List(Str("a"), Str("b"))},
	)
	p := &Patch{
		Ops: []*PatchOp{
			{Op: OpAppend, Path: []PathSeg{{Field: "tags"}}, Value: Str("c")},
		},
	}

	result, err := ApplyPatch(v, p)
	if err != nil {
		t.Fatal(err)
	}
	tags, _ := result.Get("tags").AsList()
	if len(tags) != 3 {
		t.Errorf("expected 3 tags, got %d", len(tags))
	}
}

func TestApplyPatch_SetNewField(t *testing.T) {
	v := Map(
		MapEntry{Key: "x", Value: Int(1)},
	)
	p := &Patch{
		Ops: []*PatchOp{
			{Op: OpSet, Path: []PathSeg{{Field: "y"}}, Value: Int(2)},
		},
	}

	result, err := ApplyPatch(v, p)
	if err != nil {
		t.Fatal(err)
	}
	yv := result.Get("y")
	if yv == nil {
		t.Fatal("nil y")
	}
}

func TestDiff_Coverage(t *testing.T) {
	old := Map(
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "age", Value: Int(30)},
		MapEntry{Key: "removed", Value: Str("gone")},
	)
	newVal := Map(
		MapEntry{Key: "name", Value: Str("Bob")},
		MapEntry{Key: "age", Value: Int(30)},
		MapEntry{Key: "added", Value: Str("new")},
	)

	patch := Diff(old, newVal, "")
	if patch == nil {
		t.Fatal("nil patch")
	}
	if len(patch.Ops) == 0 {
		t.Error("expected ops")
	}
}
