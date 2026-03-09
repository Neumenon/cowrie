package glyph

import (
	"testing"
)

func TestParseDocument_SimpleMap(t *testing.T) {
	input := `{a=1 b=2}`
	gv, err := ParseDocument(input)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}
	if gv.Type() != TypeMap {
		t.Fatalf("expected map, got %v", gv.Type())
	}
}

func TestParseDocument_TopLevelTab(t *testing.T) {
	input := `@tab _ [id name]
|1|alice|
|2|bob|
@end`
	gv, err := ParseDocument(input)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}
	if gv.Type() != TypeList {
		t.Fatalf("expected list, got %v", gv.Type())
	}
	items, _ := gv.AsList()
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
}

func TestParseDocument_PoolWithRefs(t *testing.T) {
	input := `@pool.str id=S1 ["hello" "world"]

{a=^S1:0 b=^S1:1}`
	gv, err := ParseDocument(input)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}
	entries, _ := gv.AsMap()
	aVal, _ := entries[0].Value.AsStr()
	bVal, _ := entries[1].Value.AsStr()
	if aVal != "hello" {
		t.Fatalf("expected 'hello', got %q", aVal)
	}
	if bVal != "world" {
		t.Fatalf("expected 'world', got %q", bVal)
	}
}

func TestParseDocument_Schema(t *testing.T) {
	input := `@schema#test @keys=[name age]
{#0=Alice #1=30}`
	gv, err := ParseDocument(input)
	if err != nil {
		t.Fatalf("ParseDocument: %v", err)
	}
	if gv.Type() != TypeMap {
		t.Fatalf("expected map, got %v", gv.Type())
	}
}

func TestParseDocument_NoValue(t *testing.T) {
	_, err := ParseDocument("")
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestResolvePoolRefs_NestedList(t *testing.T) {
	poolReg := NewPoolRegistry()
	pool := &Pool{
		ID:      "S1",
		Kind:    PoolKindString,
		Entries: []*GValue{Str("resolved")},
	}
	poolReg.Define(pool)

	gv := List(Str("^S1:0"), Int(42))
	resolved, err := ResolvePoolRefs(gv, poolReg)
	if err != nil {
		t.Fatalf("ResolvePoolRefs: %v", err)
	}
	items, _ := resolved.AsList()
	s, _ := items[0].AsStr()
	if s != "resolved" {
		t.Fatalf("expected 'resolved', got %q", s)
	}
}

func TestResolvePoolRefs_NestedStruct(t *testing.T) {
	poolReg := NewPoolRegistry()
	pool := &Pool{
		ID:      "S1",
		Kind:    PoolKindString,
		Entries: []*GValue{Str("pooled_value")},
	}
	poolReg.Define(pool)

	gv := Struct("Test",
		MapEntry{Key: "field", Value: Str("^S1:0")},
	)
	resolved, err := ResolvePoolRefs(gv, poolReg)
	if err != nil {
		t.Fatalf("ResolvePoolRefs: %v", err)
	}
	sv, _ := resolved.AsStruct()
	s, _ := sv.Fields[0].Value.AsStr()
	if s != "pooled_value" {
		t.Fatalf("expected 'pooled_value', got %q", s)
	}
}
