package glyph

import (
	"strings"
	"testing"
	"time"
)

func TestEmit_AllTypes(t *testing.T) {
	types := []struct {
		name string
		val  *GValue
	}{
		{"null", Null()},
		{"nil", nil},
		{"bool_true", Bool(true)},
		{"bool_false", Bool(false)},
		{"int_pos", Int(42)},
		{"int_neg", Int(-7)},
		{"int_zero", Int(0)},
		{"float", Float(3.14)},
		{"float_zero", Float(0.0)},
		{"str_bare", Str("hello")},
		{"str_quoted", Str("hello world")},
		{"str_empty", Str("")},
		{"bytes", Bytes([]byte{1, 2, 3})},
		{"bytes_empty", Bytes(nil)},
		{"time", Time(time.Date(2025, 3, 9, 12, 0, 0, 0, time.UTC))},
		{"id_prefix", ID("m", "123")},
		{"id_no_prefix", ID("", "abc")},
		{"list", List(Int(1), Int(2), Int(3))},
		{"list_empty", List()},
		{"map", Map(MapEntry{Key: "a", Value: Int(1)}, MapEntry{Key: "b", Value: Str("hello")})},
		{"map_empty", Map()},
		{"struct", Struct("Person", MapEntry{Key: "name", Value: Str("Alice")}, MapEntry{Key: "age", Value: Int(30)})},
		{"sum", Sum("Ok", Int(42))},
		{"nested", List(
			Map(MapEntry{Key: "x", Value: Int(1)}),
			Map(MapEntry{Key: "x", Value: Int(2)}),
		)},
	}

	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			result := Emit(tt.val)
			if result == "" && tt.val != nil && tt.val.Type() != TypeNull {
				t.Errorf("unexpected empty emit for %s", tt.name)
			}
		})
	}
}

func TestEmitCompact(t *testing.T) {
	v := Map(MapEntry{Key: "a", Value: Int(1)}, MapEntry{Key: "b", Value: Str("hello")})
	result := EmitCompact(v)
	if result == "" {
		t.Error("empty compact emit")
	}
}

func TestEmitWithOptions_Pretty(t *testing.T) {
	v := Map(
		MapEntry{Key: "a", Value: Int(1)},
		MapEntry{Key: "b", Value: List(Int(1), Int(2))},
	)
	opts := EmitOptions{
		Pretty:     true,
		Indent:     "  ",
		SortFields: true,
	}
	result := EmitWithOptions(v, opts)
	if result == "" {
		t.Error("empty pretty emit")
	}
}

func TestEmitWithOptions_Compact(t *testing.T) {
	v := Map(MapEntry{Key: "a", Value: Int(1)})
	opts := CompactEmitOptions()
	result := EmitWithOptions(v, opts)
	if result == "" {
		t.Error("empty compact emit")
	}
}

func TestDefaultEmitOptions(t *testing.T) {
	opts := DefaultEmitOptions()
	if opts.Compact {
		t.Error("default should not be compact")
	}
	if opts.Pretty {
		t.Error("default should not be pretty")
	}
	if !opts.SortFields {
		t.Error("default should sort fields")
	}
}

func TestCompactEmitOptions(t *testing.T) {
	opts := CompactEmitOptions()
	if !opts.Compact {
		t.Error("compact should be compact")
	}
	if !opts.UseWireKeys {
		t.Error("compact should use wire keys")
	}
}

func TestParse_Coverage(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"null", "∅"},
		{"bool_true", "t"},
		{"bool_false", "f"},
		{"int", "42"},
		{"neg_int", "-7"},
		{"float", "3.14"},
		{"string_bare", "hello"},
		{"string_quoted", `"hello world"`},
		{"string_empty", `""`},
		{"list", "[1 2 3]"},
		{"list_empty", "[]"},
		{"map", "{a=1 b=hello}"},
		{"map_empty", "{}"},
		{"id", "^m:123"},
		{"struct", "Person{name=Alice age=30}"},
		{"sum", "Ok(42)"},
		{"nested_list", "[[1 2] [3 4]]"},
		{"nested_map", "{a={x=1} b={y=2}}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if result == nil || result.Value == nil {
				t.Fatal("nil result")
			}
		})
	}
}

func TestParse_EmitRoundTrip(t *testing.T) {
	// Parse -> Emit -> Parse should be stable
	inputs := []string{
		"42",
		`"hello world"`,
		"[1 2 3]",
		"{a=1 b=hello}",
	}

	for _, input := range inputs {
		result1, err := Parse(input)
		if err != nil {
			t.Fatalf("Parse(%q) error: %v", input, err)
		}
		emitted := Emit(result1.Value)
		result2, err := Parse(emitted)
		if err != nil {
			t.Fatalf("Parse(Emit(%q)) error: %v", input, err)
		}
		if Emit(result2.Value) != emitted {
			t.Errorf("round-trip not stable for %q: got %q", input, Emit(result2.Value))
		}
	}
}

func TestParseError_Coverage(t *testing.T) {
	pe := &ParseError{Message: "test error", Pos: Position{Line: 1, Column: 5}}
	s := pe.Error()
	if !strings.Contains(s, "test error") {
		t.Error("expected error message in string")
	}
}

func TestParseResult_HasErrors(t *testing.T) {
	r := &ParseResult{}
	if r.HasErrors() {
		t.Error("expected no errors")
	}
	r.Errors = []ParseError{{Message: "err"}}
	if !r.HasErrors() {
		t.Error("expected errors")
	}
}
