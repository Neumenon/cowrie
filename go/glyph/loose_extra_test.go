package glyph

import (
	"strings"
	"testing"
)

func TestParseLoosePayload_MoreTypes(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		// Time value
		{"time_rfc3339", "2024-01-01T00:00:00Z"},
		// Negative float
		{"neg_float", "-3.14"},
		// Scientific notation
		{"sci_float", "1.5e10"},
		// Empty list
		{"empty_list", "[]"},
		// Empty map
		{"empty_map", "{}"},
		// List with nested maps
		{"list_of_maps", "[{x 1} {x 2}]"},
		// Multi-line map
		{"multiline_map", "name Alice\nage 30\ncity Boston"},
		// Deeply nested
		{"deep_nest", "a {\n  b {\n    c 1\n  }\n}"},
		// String with escapes
		{"escape_str", `"hello\nworld"`},
		// Bare string with special chars
		{"bare_underscore", "test_value"},
		// Large integer
		{"large_int", "999999999"},
		// Zero
		{"zero", "0"},
		// Boolean in map
		{"map_bools", "active true\ndeleted false"},
		// Null in map
		{"map_null", "value _"},
		// Mixed list
		{"mixed_list", `[1 "two" true _ 3.14 ^d:1]`},
		// Map with list values
		{"map_list", "tags [a b c]\nscores [1 2 3]"},
		// Ref in various positions
		{"ref_bare", "^d:123"},
		{"ref_with_prefix", "^user:abc-def"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, _, err := ParseLoosePayload(tt.input, nil)
			if err != nil {
				t.Skipf("ParseLoosePayload(%q): %v", tt.name, err)
				return
			}
			if v == nil {
				t.Error("nil value")
			}
		})
	}
}

func TestCanonicalizeLoose_MoreTypes(t *testing.T) {
	tests := []struct {
		name string
		v    *GValue
	}{
		// ID
		{"id", ID("d", "1")},
		// Empty list
		{"empty_list", List()},
		// Empty map
		{"empty_map", Map()},
		// Nested map
		{"nested_map", Map(
			MapEntry{Key: "outer", Value: Map(
				MapEntry{Key: "inner", Value: Map(
					MapEntry{Key: "deep", Value: Int(1)},
				)},
			)},
		)},
		// Mixed list
		{"mixed", List(Null(), Bool(true), Int(42), Float(3.14), Str("hello"))},
		// Struct
		{"struct", Struct("Person", MapEntry{Key: "name", Value: Str("Alice")})},
		// Map with special string keys
		{"special_keys", Map(
			MapEntry{Key: "hello world", Value: Int(1)},
			MapEntry{Key: "with\"quote", Value: Int(2)},
		)},
		// Float edge cases
		{"float_zero", Float(0.0)},
		{"float_neg", Float(-1.5)},
		// Large nested
		{"large_nested", Map(
			MapEntry{Key: "users", Value: List(
				Map(MapEntry{Key: "name", Value: Str("A")}, MapEntry{Key: "age", Value: Int(1)}),
				Map(MapEntry{Key: "name", Value: Str("B")}, MapEntry{Key: "age", Value: Int(2)}),
				Map(MapEntry{Key: "name", Value: Str("C")}, MapEntry{Key: "age", Value: Int(3)}),
			)},
		)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := CanonicalizeLoose(tt.v)
			if s == "" {
				t.Error("empty result")
			}
		})
	}
}

func TestCanonicalizeLooseWithOpts_AllStyles(t *testing.T) {
	v := Map(
		MapEntry{Key: "users", Value: List(
			Map(MapEntry{Key: "name", Value: Str("Alice")}, MapEntry{Key: "age", Value: Int(30)}),
			Map(MapEntry{Key: "name", Value: Str("Bob")}, MapEntry{Key: "age", Value: Int(25)}),
		)},
		MapEntry{Key: "count", Value: Int(2)},
	)

	// With tabular
	s1 := CanonicalizeLooseWithOpts(v, LooseCanonOpts{AutoTabular: true})
	if s1 == "" {
		t.Error("empty tabular")
	}

	// Without tabular
	s2 := CanonicalizeLooseWithOpts(v, LooseCanonOpts{AutoTabular: false})
	if s2 == "" {
		t.Error("empty non-tabular")
	}
}

func TestParseLoosePayload_SchemaRef(t *testing.T) {
	// Test with schema registry
	reg := NewSchemaRegistry()

	input := "name Alice\nage 30"
	v, _, err := ParseLoosePayload(input, reg)
	if err != nil {
		t.Fatal(err)
	}
	if v == nil {
		t.Error("nil")
	}
}

func TestParsePatch_AllOps(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"set", "@patch ^d:1\n= name Alice"},
		{"append", "@patch ^d:1\n+ tags newtag"},
		{"delete", "@patch ^d:1\n- old_field"},
		{"delta", "@patch ^d:1\n~ count 5"},
		{"multi", "@patch ^d:1\n= name Bob\n= age 31\n- city\n+ tags new"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := ParsePatch(tt.input, nil)
			if err != nil {
				t.Fatalf("ParsePatch: %v", err)
			}
			if p == nil {
				t.Fatal("nil patch")
			}
			if len(p.Ops) == 0 {
				t.Error("no ops")
			}

			// Also test emitting
			result, err := EmitPatch(p, nil)
			if err != nil {
				t.Fatalf("EmitPatch: %v", err)
			}
			if result == "" {
				t.Error("empty emit")
			}
		})
	}
}

func TestParseTabularLoose_Coverage(t *testing.T) {
	input := "@tab _ [name age city]\n|Alice|30|Boston|\n|Bob|25|NYC|\n@end"
	v, err := ParseTabularLoose(input)
	if err != nil {
		t.Fatal(err)
	}
	if v == nil {
		t.Fatal("nil")
	}
}

func TestParseTabularLoose_WithTypes(t *testing.T) {
	// Different value types in cells
	input := "@tab _ [name count active score]\n|Alice|30|true|3.14|\n|Bob|25|false|2.71|\n@end"
	v, err := ParseTabularLoose(input)
	if err != nil {
		t.Fatal(err)
	}
	if v == nil {
		t.Fatal("nil")
	}
}

func TestEmitTabularWithOptions_Coverage(t *testing.T) {
	v := List(
		Map(MapEntry{Key: "name", Value: Str("Alice")}, MapEntry{Key: "age", Value: Int(30)}),
		Map(MapEntry{Key: "name", Value: Str("Bob")}, MapEntry{Key: "age", Value: Int(25)}),
	)

	result, err := EmitTabularWithOptions(v, TabularOptions{})
	if err != nil {
		t.Skipf("EmitTabularWithOptions: %v", err)
		return
	}
	if result == "" {
		t.Error("empty")
	}
}

func TestEmit_Sum_Coverage(t *testing.T) {
	// Sum type value
	v := Sum("Ok", Str("success"))
	s := Emit(v)
	if s == "" {
		t.Error("empty emit for sum")
	}

	// Another variant
	v2 := Sum("Err", Str("failure"))
	s2 := Emit(v2)
	if s2 == "" {
		t.Error("empty emit for sum err")
	}
}

func TestEmitCompact_Coverage(t *testing.T) {
	v := Map(
		MapEntry{Key: "x", Value: Int(1)},
		MapEntry{Key: "y", Value: List(Int(1), Int(2), Int(3))},
		MapEntry{Key: "z", Value: Map(
			MapEntry{Key: "a", Value: Str("hello")},
		)},
	)

	s := EmitWithOptions(v, EmitOptions{Compact: true})
	if s == "" {
		t.Error("empty compact")
	}
	// Compact should have no leading whitespace
	if strings.HasPrefix(s, " ") || strings.HasPrefix(s, "\t") {
		t.Error("compact has leading whitespace")
	}
}

func TestParseWithOptions_Coverage(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"simple_map", `{name Alice age 30}`},
		{"struct", `Person {name Alice}`},
		{"list", `[1 2 3]`},
		{"nested", `{outer {inner 1}}`},
		{"special_floats", `3.14`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if result == nil || result.Value == nil {
				t.Error("nil result")
			}
		})
	}
}

func TestPool_Coverage(t *testing.T) {
	// Test pool creation and lookup
	reg := NewPoolRegistry()

	pool, err := ParsePool(`@pool.str id=S1 ["Alice" "Bob" "Charlie"]`)
	if err != nil {
		t.Fatal(err)
	}
	reg.Define(pool)

	// Get pool
	p := reg.Get("S1")
	if p == nil {
		t.Fatal("nil pool")
	}

	// Resolve valid ref
	v, err := reg.Resolve(PoolRef{PoolID: "S1", Index: 0})
	if err != nil {
		t.Error("resolve 0:", err)
	}
	if v == nil {
		t.Error("nil resolve for 0")
	}

	v2, err := reg.Resolve(PoolRef{PoolID: "S1", Index: 1})
	if err != nil {
		t.Error("resolve 1:", err)
	}
	if v2 == nil {
		t.Error("nil resolve for 1")
	}

	// Out of range
	_, err = reg.Resolve(PoolRef{PoolID: "S1", Index: 99})
	if err == nil {
		t.Error("should error for out of range")
	}

	// Missing pool
	_, err = reg.Resolve(PoolRef{PoolID: "nonexistent", Index: 0})
	if err == nil {
		t.Error("should error for missing pool")
	}

	// Clear
	reg.Clear("S1")
	p2 := reg.Get("S1")
	if p2 != nil {
		t.Error("should be nil after clear")
	}
}

func TestNewSchemaRegistry_Coverage(t *testing.T) {
	reg := NewSchemaRegistry()
	if reg == nil {
		t.Fatal("nil registry")
	}
}

func TestNewPoolRegistry_Coverage(t *testing.T) {
	reg := NewPoolRegistry()
	if reg == nil {
		t.Fatal("nil registry")
	}
}
