package glyph

import (
	"strings"
	"testing"
)

func TestParseEmit_Map(t *testing.T) {
	input := `{
  name "Alice Smith"
  age 30
  active true
  score 3.14
  tags [a b c]
  nested {
    x 1
    y 2
  }
}`
	pr, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if pr.Value == nil {
		t.Fatal("nil value")
	}

	out := Emit(pr.Value)
	if out == "" {
		t.Error("empty emit")
	}

	// Pretty emit
	outPretty := EmitWithOptions(pr.Value, EmitOptions{Pretty: true})
	if outPretty == "" {
		t.Error("empty pretty emit")
	}
}

func TestParseEmit_Tabular(t *testing.T) {
	input := "@tab _ [name age]\n|Alice|30|\n|Bob|25|\n@end"
	v, err := ParseDocument(input)
	if err != nil {
		t.Fatal(err)
	}
	if v == nil {
		t.Fatal("nil value")
	}
	out := Emit(v)
	if out == "" {
		t.Error("empty emit")
	}
}

func TestParseDocument_Deep(t *testing.T) {
	input := "name Alice\nage 30"
	v, err := ParseDocument(input)
	if err != nil {
		t.Fatal(err)
	}
	if v == nil {
		t.Fatal("nil value")
	}
}

func TestParseEmit_Struct(t *testing.T) {
	input := `Person {
  name Alice
  age 30
}`
	pr, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if pr.Value == nil {
		t.Fatal("nil value")
	}
	out := Emit(pr.Value)
	if out == "" {
		t.Error("empty emit")
	}
}

func TestParseEmit_List(t *testing.T) {
	input := `[1 2 3 4 5]`
	pr, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if pr.Value == nil {
		t.Fatal("nil value")
	}
	out := Emit(pr.Value)
	if out == "" {
		t.Error("empty emit")
	}
}

func TestParseEmit_NestedList(t *testing.T) {
	input := `[[1 2] [3 4] [5 6]]`
	pr, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if pr.Value == nil {
		t.Fatal("nil value")
	}
}

func TestParseEmit_Refs(t *testing.T) {
	input := `{
  target ^d:123
  user ^u:abc
}`
	pr, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if pr.Value == nil {
		t.Fatal("nil value")
	}
	out := Emit(pr.Value)
	if !strings.Contains(out, "^") {
		t.Error("expected refs in output")
	}
}

func TestParseEmit_Strings(t *testing.T) {
	tests := []string{
		`"hello world"`,
		`"line1\nline2"`,
		`simple`,
		`"with \"quotes\""`,
		`""`, // empty string
	}
	for _, input := range tests {
		pr, err := Parse(input)
		if err != nil {
			t.Errorf("Parse(%q): %v", input, err)
			continue
		}
		if pr.Value == nil {
			t.Errorf("nil value for %q", input)
			continue
		}
		_ = Emit(pr.Value)
	}
}

func TestParseEmit_Numbers(t *testing.T) {
	tests := []string{
		"0",
		"42",
		"-7",
		"3.14",
		"-0.5",
		"1e10",
		"1.5e-3",
	}
	for _, input := range tests {
		pr, err := Parse(input)
		if err != nil {
			t.Errorf("Parse(%q): %v", input, err)
			continue
		}
		if pr.Value == nil {
			t.Errorf("nil value for %q", input)
		}
	}
}

func TestParsePatch_Deep(t *testing.T) {
	input := "@patch ^d:1\n= name Alice\n= age 30"
	p, err := ParsePatch(input, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		t.Fatal("nil patch")
	}
	if len(p.Ops) < 2 {
		t.Errorf("expected >= 2 ops, got %d", len(p.Ops))
	}
}

func TestCanonicalizeLoose_Coverage(t *testing.T) {
	tests := []*GValue{
		Map(MapEntry{Key: "name", Value: Str("Alice")}, MapEntry{Key: "age", Value: Int(30)}),
		List(Int(1), Int(2), Int(3)),
		Str("hello"),
		Int(42),
		Bool(true),
		Null(),
		Map(MapEntry{Key: "nested", Value: Map(MapEntry{Key: "x", Value: Int(1)})}),
	}

	for i, v := range tests {
		result := CanonicalizeLoose(v)
		if result == "" {
			t.Errorf("empty result for test %d", i)
		}
	}

	// Also test CanonicalizeLooseNoTabular and CanonicalizeLooseTabular
	v := Map(MapEntry{Key: "x", Value: Int(1)})
	s1 := CanonicalizeLooseNoTabular(v)
	if s1 == "" {
		t.Error("empty no-tabular result")
	}
	s2 := CanonicalizeLooseTabular(v)
	if s2 == "" {
		t.Error("empty tabular result")
	}
}

func TestEmitPretty_Coverage(t *testing.T) {
	v := Map(
		MapEntry{Key: "users", Value: List(
			Map(
				MapEntry{Key: "name", Value: Str("Alice")},
				MapEntry{Key: "age", Value: Int(30)},
			),
			Map(
				MapEntry{Key: "name", Value: Str("Bob")},
				MapEntry{Key: "age", Value: Int(25)},
			),
		)},
		MapEntry{Key: "count", Value: Int(2)},
	)
	s := EmitWithOptions(v, EmitOptions{Pretty: true})
	if s == "" {
		t.Error("empty pretty emit")
	}
	if !strings.Contains(s, "Alice") {
		t.Error("missing Alice")
	}
}

func TestEmitWithOptions_Coverage(t *testing.T) {
	v := Map(
		MapEntry{Key: "x", Value: Int(1)},
		MapEntry{Key: "y", Value: List(Int(1), Int(2))},
	)

	// Pretty mode
	s1 := EmitWithOptions(v, EmitOptions{Pretty: true})
	if s1 == "" {
		t.Error("pretty empty")
	}

	// Compact mode
	s2 := EmitWithOptions(v, EmitOptions{Compact: true})
	if s2 == "" {
		t.Error("compact empty")
	}
}
