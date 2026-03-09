package glyph

import (
	"strings"
	"testing"
)

func TestNewPatch(t *testing.T) {
	p := NewPatch(RefID{Prefix: "d", Value: "1"}, "abc123")
	if p.Target.Prefix != "d" {
		t.Error("target prefix")
	}
	if p.SchemaID != "abc123" {
		t.Error("schema ID")
	}
}

func TestEmitPatch_Basic(t *testing.T) {
	p := &Patch{
		Target:   RefID{Prefix: "d", Value: "1"},
		SchemaID: "abc",
		Ops: []*PatchOp{
			{Op: OpSet, Path: []PathSeg{{Field: "name"}}, Value: Str("Alice")},
			{Op: OpSet, Path: []PathSeg{{Field: "age"}}, Value: Int(30)},
		},
	}

	result, err := EmitPatch(p, nil)
	if err != nil {
		t.Fatalf("EmitPatch error: %v", err)
	}
	if result == "" {
		t.Error("empty result")
	}
	if !strings.Contains(result, "@patch") {
		t.Error("expected @patch header")
	}
}

func TestEmitPatch_Nil(t *testing.T) {
	_, err := EmitPatch(nil, nil)
	if err == nil {
		t.Error("expected error for nil patch")
	}
}

func TestEmitPatch_AllOps(t *testing.T) {
	p := &Patch{
		Target: RefID{Prefix: "d", Value: "1"},
		Ops: []*PatchOp{
			{Op: OpSet, Path: []PathSeg{{Field: "x"}}, Value: Int(42)},
			{Op: OpAppend, Path: []PathSeg{{Field: "items"}}, Value: Str("new")},
			{Op: OpDelete, Path: []PathSeg{{Field: "old"}}},
			{Op: OpDelta, Path: []PathSeg{{Field: "count"}}, Value: Int(1)},
		},
	}

	result, err := EmitPatch(p, nil)
	if err != nil {
		t.Fatalf("EmitPatch error: %v", err)
	}
	if result == "" {
		t.Error("empty result")
	}
}

func TestParsePatch_Basic(t *testing.T) {
	input := "@patch ^d:1\n= name Alice\n= age 30"
	p, err := ParsePatch(input, nil)
	if err != nil {
		t.Fatalf("ParsePatch error: %v", err)
	}
	if p == nil {
		t.Fatal("nil patch")
	}
	if len(p.Ops) != 2 {
		t.Errorf("expected 2 ops, got %d", len(p.Ops))
	}
}

func TestApplyPatch_Set(t *testing.T) {
	v := Map(
		MapEntry{Key: "name", Value: Str("Bob")},
		MapEntry{Key: "age", Value: Int(25)},
	)
	p := &Patch{
		Ops: []*PatchOp{
			{Op: OpSet, Path: []PathSeg{{Field: "name"}}, Value: Str("Alice")},
		},
	}

	result, err := ApplyPatch(v, p)
	if err != nil {
		t.Fatalf("ApplyPatch error: %v", err)
	}
	sv, _ := result.Get("name").AsStr()
	if sv != "Alice" {
		t.Errorf("expected Alice, got %s", sv)
	}
}

func TestApplyPatch_Delete(t *testing.T) {
	v := Map(
		MapEntry{Key: "x", Value: Int(1)},
		MapEntry{Key: "y", Value: Int(2)},
	)
	p := &Patch{
		Ops: []*PatchOp{
			{Op: OpDelete, Path: []PathSeg{{Field: "y"}}},
		},
	}

	result, err := ApplyPatch(v, p)
	if err != nil {
		t.Fatalf("ApplyPatch error: %v", err)
	}
	if result.Get("y") != nil {
		t.Error("y should be deleted")
	}
}

func TestApplyPatch_Nil(t *testing.T) {
	_, err := ApplyPatch(nil, &Patch{})
	if err == nil {
		t.Error("expected error for nil value")
	}
}

func TestParsePatchRoundTrip_Coverage(t *testing.T) {
	input := "@patch ^d:1\n= name Alice\n= age 30"
	output, err := ParsePatchRoundTrip(input, nil)
	if err != nil {
		t.Fatalf("ParsePatchRoundTrip error: %v", err)
	}
	if output == "" {
		t.Error("empty output")
	}
}
