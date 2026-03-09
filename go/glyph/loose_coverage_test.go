package glyph

import (
	"strings"
	"testing"
	"time"
)

func TestCanonicalizeLoose_AllTypes(t *testing.T) {
	tests := []struct {
		name string
		val  *GValue
	}{
		{"null", Null()},
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
		{"bytes_data", Bytes([]byte{1, 2, 3})},
		{"bytes_empty", Bytes(nil)},
		{"time", Time(time.Date(2025, 3, 9, 12, 0, 0, 0, time.UTC))},
		{"id_prefix", ID("m", "123")},
		{"id_no_prefix", ID("", "abc")},
		{"list_simple", List(Int(1), Int(2), Int(3))},
		{"list_empty", List()},
		{"map_simple", Map(MapEntry{Key: "a", Value: Int(1)}, MapEntry{Key: "b", Value: Str("hello")})},
		{"map_empty", Map()},
		{"struct", Struct("Person", MapEntry{Key: "name", Value: Str("Alice")})},
		{"sum", Sum("Ok", Int(42))},
		{"nested_list_of_maps", List(
			Map(MapEntry{Key: "x", Value: Int(1)}),
			Map(MapEntry{Key: "x", Value: Int(2)}),
			Map(MapEntry{Key: "x", Value: Int(3)}),
		)},
		{"nil_value", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CanonicalizeLoose(tt.val)
			if result == "" && tt.val != nil && tt.val.Type() != TypeNull {
				t.Errorf("unexpected empty canonical string for %s", tt.name)
			}
		})
	}
}

func TestCanonicalizeLooseNoTabular(t *testing.T) {
	// List of maps that would normally trigger tabular
	v := List(
		Map(MapEntry{Key: "x", Value: Int(1)}, MapEntry{Key: "y", Value: Int(2)}),
		Map(MapEntry{Key: "x", Value: Int(3)}, MapEntry{Key: "y", Value: Int(4)}),
		Map(MapEntry{Key: "x", Value: Int(5)}, MapEntry{Key: "y", Value: Int(6)}),
	)
	result := CanonicalizeLooseNoTabular(v)
	// Should NOT contain @tab
	if strings.Contains(result, "@tab") {
		t.Error("NoTabular should not produce @tab")
	}
}

func TestCanonicalizeLooseTabular(t *testing.T) {
	v := List(
		Map(MapEntry{Key: "x", Value: Int(1)}, MapEntry{Key: "y", Value: Int(2)}),
		Map(MapEntry{Key: "x", Value: Int(3)}, MapEntry{Key: "y", Value: Int(4)}),
		Map(MapEntry{Key: "x", Value: Int(5)}, MapEntry{Key: "y", Value: Int(6)}),
	)
	result := CanonicalizeLooseTabular(v)
	// Should contain @tab for 3+ homogeneous objects
	if !strings.Contains(result, "@tab") {
		t.Error("expected @tab in tabular output")
	}
}

func TestEqualLoose_Coverage(t *testing.T) {
	a := Map(MapEntry{Key: "x", Value: Int(1)}, MapEntry{Key: "y", Value: Int(2)})
	b := Map(MapEntry{Key: "y", Value: Int(2)}, MapEntry{Key: "x", Value: Int(1)})
	if !EqualLoose(a, b) {
		t.Error("equal maps should be EqualLoose")
	}

	c := Map(MapEntry{Key: "x", Value: Int(99)})
	if EqualLoose(a, c) {
		t.Error("different maps should not be EqualLoose")
	}
}

func TestFingerprintLoose(t *testing.T) {
	v := Map(MapEntry{Key: "a", Value: Int(1)})
	f := FingerprintLoose(v)
	if f == "" {
		t.Error("expected non-empty fingerprint")
	}
	// Same value should produce same fingerprint
	v2 := Map(MapEntry{Key: "a", Value: Int(1)})
	f2 := FingerprintLoose(v2)
	if f != f2 {
		t.Error("same value should produce same fingerprint")
	}
}

func TestBuildKeyDictFromValue(t *testing.T) {
	v := Map(
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "age", Value: Int(30)},
		MapEntry{Key: "nested", Value: Map(MapEntry{Key: "inner", Value: Int(1)})},
	)
	keys := BuildKeyDictFromValue(v)
	if len(keys) < 3 {
		t.Errorf("expected at least 3 keys, got %d: %v", len(keys), keys)
	}
}

func TestBuildKeyDictFromValue_List(t *testing.T) {
	v := List(
		Map(MapEntry{Key: "a", Value: Int(1)}),
		Map(MapEntry{Key: "b", Value: Int(2)}),
	)
	keys := BuildKeyDictFromValue(v)
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d: %v", len(keys), keys)
	}
}

func TestBuildKeyDictFromValue_Struct(t *testing.T) {
	v := Struct("T",
		MapEntry{Key: "field1", Value: Int(1)},
		MapEntry{Key: "field2", Value: Str("hello")},
	)
	keys := BuildKeyDictFromValue(v)
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d: %v", len(keys), keys)
	}
}

func TestParseSchemaHeader(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		ref       string
		keyCount  int
		wantError bool
	}{
		{"basic", "@schema#abc123", "abc123", 0, false},
		{"with_keys", "@schema#hash keys=[a b c]", "hash", 3, false},
		{"no_hash", "@schema keys=[x y]", "", 0, false},
		{"hash_only", "@schema#onlyhash", "onlyhash", 0, false},
		{"not_schema", "not a schema", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, keys, err := ParseSchemaHeader(tt.input)
			if tt.wantError {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ref != tt.ref {
				t.Errorf("ref: got %q, want %q", ref, tt.ref)
			}
			if len(keys) != tt.keyCount {
				t.Errorf("keys: got %d, want %d", len(keys), tt.keyCount)
			}
		})
	}
}

func TestLooseCanonOpts_Variants(t *testing.T) {
	d := DefaultLooseCanonOpts()
	if !d.AutoTabular {
		t.Error("default should have AutoTabular")
	}
	if d.NullStyle != NullStyleUnderscore {
		t.Error("default NullStyle should be underscore")
	}

	l := LLMLooseCanonOpts()
	if !l.AutoTabular {
		t.Error("LLM should have AutoTabular")
	}

	p := PrettyLooseCanonOpts()
	if p.NullStyle != NullStyleSymbol {
		t.Error("Pretty NullStyle should be symbol")
	}

	nt := NoTabularLooseCanonOpts()
	if nt.AutoTabular {
		t.Error("NoTabular should have AutoTabular=false")
	}

	tb := TabularLooseCanonOpts()
	if !tb.AutoTabular {
		t.Error("Tabular should have AutoTabular=true")
	}
}

func TestCanonicalizeLooseWithSchema(t *testing.T) {
	v := Map(
		MapEntry{Key: "name", Value: Str("Alice")},
		MapEntry{Key: "age", Value: Int(30)},
	)

	opts := LooseCanonOpts{
		SchemaRef:      "abc123",
		KeyDict:        []string{"name", "age"},
		UseCompactKeys: true,
	}

	result := CanonicalizeLooseWithSchema(v, opts)
	if !strings.Contains(result, "@schema") {
		t.Error("expected @schema header")
	}
}
