package glyph

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var updateGolden = flag.Bool("update-golden", false, "update golden files with current output")

// normalizeNullForComparison replaces Go's _ null representation with ∅ for JS comparison.
// Only replaces standalone _ (null values), not _ as part of key names or @tab _ marker.
func normalizeNullForComparison(s string) string {
	// Special case: standalone _ is null
	if s == "_" {
		return "∅"
	}

	// Don't modify @tab _ marker - replace it with a placeholder first
	result := strings.ReplaceAll(s, "@tab _ ", "@tab \x00 ")

	// Replace _ that appears as a value (after = or at start of list element)
	// Patterns:
	//   =_ (at end or followed by space/})
	//   [_ (at start of list)
	//   _ | (in tabular)
	//   |_| (in tabular)

	// Replace =_ followed by space, }, ], or end
	for _, suffix := range []string{" ", "}", "]", "|", "\n"} {
		result = strings.ReplaceAll(result, "=_"+suffix, "=∅"+suffix)
	}
	// Replace =_ at end of string
	if strings.HasSuffix(result, "=_") {
		result = result[:len(result)-1] + "∅"
	}

	// Replace [_ at start of list
	result = strings.ReplaceAll(result, "[_]", "[∅]")
	result = strings.ReplaceAll(result, "[_ ", "[∅ ")

	// Replace _ in list (space before)
	result = strings.ReplaceAll(result, " _]", " ∅]")
	result = strings.ReplaceAll(result, " _ ", " ∅ ")

	// Replace |_| in tabular (null cell)
	result = strings.ReplaceAll(result, "|_|", "|∅|")
	result = strings.ReplaceAll(result, "|_\n", "|∅\n")

	// Restore @tab _ marker
	result = strings.ReplaceAll(result, "@tab \x00 ", "@tab _ ")

	return result
}

// ============================================================
// GLYPH-Loose Canonicalization Tests
// ============================================================

func TestCanonicalizeLoose_Scalars(t *testing.T) {
	tests := []struct {
		name     string
		value    *GValue
		expected string
	}{
		{"null", Null(), "_"}, // Default is now underscore (ASCII-safe)
		{"true", Bool(true), "t"},
		{"false", Bool(false), "f"},
		{"int_zero", Int(0), "0"},
		{"int_positive", Int(42), "42"},
		{"int_negative", Int(-100), "-100"},
		{"float_zero", Float(0.0), "0"},
		{"float_positive", Float(3.14), "3.14"},
		{"float_negative", Float(-2.5), "-2.5"},
		{"float_exp", Float(1e10), "1e+10"},
		{"string_bare", Str("hello"), "hello"},
		{"string_quoted", Str("hello world"), `"hello world"`},
		{"string_empty", Str(""), `""`},
		{"string_reserved_t", Str("t"), `"t"`},
		{"string_reserved_f", Str("f"), `"f"`},
		{"string_reserved_null", Str("null"), `"null"`},
		{"id_simple", ID("user", "123"), "^user:123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CanonicalizeLoose(tt.value)
			if got != tt.expected {
				t.Errorf("CanonicalizeLoose() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestCanonicalizeLoose_Lists(t *testing.T) {
	tests := []struct {
		name     string
		value    *GValue
		expected string
	}{
		{"empty", List(), "[]"},
		{"single", List(Int(1)), "[1]"},
		{"multiple", List(Int(1), Int(2), Int(3)), "[1 2 3]"},
		{"mixed", List(Null(), Bool(true), Int(42), Str("hi")), "[_ t 42 hi]"},
		{"nested", List(List(Int(1), Int(2)), List(Int(3))), "[[1 2] [3]]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CanonicalizeLoose(tt.value)
			if got != tt.expected {
				t.Errorf("CanonicalizeLoose() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestCanonicalizeLoose_Maps(t *testing.T) {
	tests := []struct {
		name     string
		value    *GValue
		expected string
	}{
		{"empty", Map(), "{}"},
		{"single", Map(MapEntry{Key: "a", Value: Int(1)}), "{a=1}"},
		// Keys should be sorted: A < _ < a < aa < b (bytewise UTF-8 of bare strings)
		// But wait - "A" and "_" and "a" are bare safe, so they're compared as-is
		// UTF-8 bytewise: "A"=0x41, "_"=0x5F, "a"=0x61, "aa", "b"
		// So order should be: A < _ < a < aa < b
		{"sorted_keys", Map(
			MapEntry{Key: "b", Value: Int(1)},
			MapEntry{Key: "a", Value: Int(2)},
			MapEntry{Key: "aa", Value: Int(3)},
			MapEntry{Key: "A", Value: Int(4)},
			MapEntry{Key: "_", Value: Int(5)},
		), "{A=4 _=5 a=2 aa=3 b=1}"},
		{"nested", Map(
			MapEntry{Key: "inner", Value: Map(MapEntry{Key: "x", Value: Int(1)})},
		), "{inner={x=1}}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CanonicalizeLoose(tt.value)
			if got != tt.expected {
				t.Errorf("CanonicalizeLoose() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestEqualLoose(t *testing.T) {
	// Two maps with same content but different order should be equal
	map1 := Map(
		MapEntry{Key: "a", Value: Int(1)},
		MapEntry{Key: "b", Value: Int(2)},
	)
	map2 := Map(
		MapEntry{Key: "b", Value: Int(2)},
		MapEntry{Key: "a", Value: Int(1)},
	)

	if !EqualLoose(map1, map2) {
		t.Errorf("EqualLoose should return true for maps with same content")
	}

	// Different content should not be equal
	map3 := Map(
		MapEntry{Key: "a", Value: Int(1)},
		MapEntry{Key: "b", Value: Int(3)},
	)
	if EqualLoose(map1, map3) {
		t.Errorf("EqualLoose should return false for maps with different content")
	}
}

// ============================================================
// JSON Corpus Tests
// ============================================================

type manifestCase struct {
	Name string `json:"name"`
	File string `json:"file"`
}

type manifest struct {
	Version     string         `json:"version"`
	Description string         `json:"description"`
	Cases       []manifestCase `json:"cases"`
}

func TestJSONCorpus_RoundTrip(t *testing.T) {
	// Load manifest
	manifestPath := filepath.Join("testdata", "loose_json", "manifest.json")
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Skipf("Skipping JSON corpus tests: %v", err)
		return
	}

	var m manifest
	if err := json.Unmarshal(manifestData, &m); err != nil {
		t.Fatalf("Failed to parse manifest: %v", err)
	}

	for _, tc := range m.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			// Load JSON file
			jsonPath := filepath.Join("testdata", "loose_json", tc.File)
			jsonData, err := os.ReadFile(jsonPath)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", tc.File, err)
			}

			// Parse JSON to GValue
			gv, err := FromJSONLoose(jsonData)
			if err != nil {
				t.Fatalf("FromJSONLoose failed: %v", err)
			}

			// Convert back to JSON
			roundTripped, err := ToJSONLoose(gv)
			if err != nil {
				t.Fatalf("ToJSONLoose failed: %v", err)
			}

			// Check JSON equality (structure, not byte-for-byte)
			equal, err := JSONEqual(jsonData, roundTripped)
			if err != nil {
				t.Fatalf("JSONEqual failed: %v", err)
			}
			if !equal {
				t.Errorf("Round-trip produced different JSON\nOriginal: %s\nResult: %s",
					string(jsonData), string(roundTripped))
			}
		})
	}
}

func TestJSONCorpus_Canonicalization(t *testing.T) {
	// Load manifest
	manifestPath := filepath.Join("testdata", "loose_json", "manifest.json")
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Skipf("Skipping JSON corpus tests: %v", err)
		return
	}

	var m manifest
	if err := json.Unmarshal(manifestData, &m); err != nil {
		t.Fatalf("Failed to parse manifest: %v", err)
	}

	for _, tc := range m.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			// Load JSON file
			jsonPath := filepath.Join("testdata", "loose_json", tc.File)
			jsonData, err := os.ReadFile(jsonPath)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", tc.File, err)
			}

			// Parse JSON to GValue
			gv, err := FromJSONLoose(jsonData)
			if err != nil {
				t.Fatalf("FromJSONLoose failed: %v", err)
			}

			// Get canonical form
			canon := CanonicalizeLoose(gv)

			// Canonicalization should be non-empty
			if canon == "" {
				t.Errorf("CanonicalizeLoose returned empty string")
			}

			// Two parses of the same JSON should produce identical canonical form
			gv2, err := FromJSONLoose(jsonData)
			if err != nil {
				t.Fatalf("Second FromJSONLoose failed: %v", err)
			}
			canon2 := CanonicalizeLoose(gv2)

			if canon != canon2 {
				t.Errorf("Canonicalization not deterministic\nFirst: %s\nSecond: %s", canon, canon2)
			}
		})
	}
}

func TestJSONCorpus_DuplicateKeys(t *testing.T) {
	// Test case 018: duplicate keys - last wins
	jsonData := []byte(`{"k":1,"k":2,"k":3}`)

	gv, err := FromJSONLoose(jsonData)
	if err != nil {
		t.Fatalf("FromJSONLoose failed: %v", err)
	}

	// Should have only one key "k" with value 3 (last wins is Go's default behavior)
	if gv.typ != TypeMap {
		t.Fatalf("Expected map, got %v", gv.typ)
	}

	// Note: Go's json.Unmarshal uses last-wins for duplicate keys
	// So we should have k=3
	entries := gv.mapVal
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}
	if entries[0].Key != "k" {
		t.Errorf("Expected key 'k', got %s", entries[0].Key)
	}
	if entries[0].Value.intVal != 3 {
		t.Errorf("Expected value 3, got %d", entries[0].Value.intVal)
	}
}

func TestJSONCorpus_KeyOrdering(t *testing.T) {
	// Test case 013: key ordering - should be sorted bytewise UTF-8
	jsonData := []byte(`{"b":1,"a":2,"aa":3,"A":4,"_":5}`)

	gv, err := FromJSONLoose(jsonData)
	if err != nil {
		t.Fatalf("FromJSONLoose failed: %v", err)
	}

	canon := CanonicalizeLoose(gv)

	// Expected order: A (0x41) < _ (0x5F) < a (0x61) < aa < b
	expected := "{A=4 _=5 a=2 aa=3 b=1}"
	if canon != expected {
		t.Errorf("Key ordering incorrect\nGot: %s\nWant: %s", canon, expected)
	}
}

// ============================================================
// Edge Case Tests
// ============================================================

func TestFromJSONLoose_RejectsNaN(t *testing.T) {
	// NaN cannot be represented in JSON, so this tests
	// that we reject it if somehow passed in
	var zero float64 = 0
	gv := Float(zero / zero) // NaN

	_, err := ToJSONLoose(gv)
	if err == nil {
		t.Error("Expected error for NaN, got nil")
	}
}

func TestFromJSONLoose_RejectsInfinity(t *testing.T) {
	var one float64 = 1
	var zero float64 = 0
	gv := Float(one / zero) // +Inf

	_, err := ToJSONLoose(gv)
	if err == nil {
		t.Error("Expected error for Infinity, got nil")
	}
}

func TestFromJSONLoose_NegativeZero(t *testing.T) {
	// JSON -0 should be canonicalized to 0
	jsonData := []byte(`{"x":-0}`)

	gv, err := FromJSONLoose(jsonData)
	if err != nil {
		t.Fatalf("FromJSONLoose failed: %v", err)
	}

	// The value should be 0 (not -0)
	canon := CanonicalizeLoose(gv)
	// -0.0 as float64 equals 0, and canonFloat should return "0"
	if canon != "{x=0}" {
		t.Errorf("Expected {x=0}, got %s", canon)
	}
}

// ============================================================
// Bytes Canonicalization Tests
// ============================================================

func TestCanonicalizeLoose_Bytes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected string
	}{
		{"empty", []byte{}, `b64""`},
		{"simple", []byte("hello"), `b64"aGVsbG8="`},
		{"binary", []byte{0x00, 0xFF, 0x42}, `b64"AP9C"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gv := Bytes(tt.data)
			got := CanonicalizeLoose(gv)
			if got != tt.expected {
				t.Errorf("CanonicalizeLoose(Bytes(%v)) = %q, want %q", tt.data, got, tt.expected)
			}
		})
	}
}

// ============================================================
// Golden File Tests
// ============================================================

func TestGoldenFiles(t *testing.T) {
	goldenDir := filepath.Join("testdata", "loose_json", "golden")
	casesDir := filepath.Join("testdata", "loose_json", "cases")

	// If updating, generate golden files from all case files
	if *updateGolden {
		t.Log("Updating golden files...")
		entries, err := os.ReadDir(casesDir)
		if err != nil {
			t.Fatalf("Failed to read cases dir: %v", err)
		}

		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}

			name := strings.TrimSuffix(entry.Name(), ".json")
			jsonPath := filepath.Join(casesDir, entry.Name())
			jsonData, err := os.ReadFile(jsonPath)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", entry.Name(), err)
			}

			gv, err := FromJSONLoose(jsonData)
			if err != nil {
				t.Fatalf("FromJSONLoose failed for %s: %v", entry.Name(), err)
			}

			canon := CanonicalizeLoose(gv)
			wantPath := filepath.Join(goldenDir, name+".want")
			if err := os.WriteFile(wantPath, []byte(canon+"\n"), 0644); err != nil {
				t.Fatalf("Failed to write %s: %v", wantPath, err)
			}
			t.Logf("Updated %s", name+".want")
		}
		return
	}

	// Normal mode: verify existing golden files against their cases
	goldenEntries, err := os.ReadDir(goldenDir)
	if err != nil {
		t.Skipf("Skipping golden file tests: %v", err)
		return
	}

	for _, entry := range goldenEntries {
		if !strings.HasSuffix(entry.Name(), ".want") {
			continue
		}

		name := strings.TrimSuffix(entry.Name(), ".want")
		t.Run(name, func(t *testing.T) {
			// Load JSON input
			jsonPath := filepath.Join(casesDir, name+".json")
			jsonData, err := os.ReadFile(jsonPath)
			if err != nil {
				t.Fatalf("Failed to read JSON: %v", err)
			}

			// Load expected output
			wantPath := filepath.Join(goldenDir, entry.Name())
			wantData, err := os.ReadFile(wantPath)
			if err != nil {
				t.Fatalf("Failed to read golden: %v", err)
			}
			want := strings.TrimSpace(string(wantData))

			// Generate canonical form using NoTabular for golden file compat
			gv, err := FromJSONLoose(jsonData)
			if err != nil {
				t.Fatalf("FromJSONLoose failed: %v", err)
			}
			got := CanonicalizeLooseNoTabular(gv)

			if got != want {
				t.Errorf("Golden mismatch\nGot:  %s\nWant: %s", got, want)
			}
		})
	}
}

// ============================================================
// Auto-Tabular Mode Tests (v2.3.0)
// ============================================================

func TestAutoTabular_Basic(t *testing.T) {
	// Simple array of objects with same keys
	items := List(
		Map(MapEntry{Key: "id", Value: Int(1)}, MapEntry{Key: "name", Value: Str("Alice")}),
		Map(MapEntry{Key: "id", Value: Int(2)}, MapEntry{Key: "name", Value: Str("Bob")}),
		Map(MapEntry{Key: "id", Value: Int(3)}, MapEntry{Key: "name", Value: Str("Carol")}),
	)

	got := CanonicalizeLooseTabular(items)
	// v2.4.0: includes rows=N cols=M for streaming resync
	expected := "@tab _ rows=3 cols=2 [id name]\n|1|Alice|\n|2|Bob|\n|3|Carol|\n@end"

	if got != expected {
		t.Errorf("AutoTabular basic:\nGot:\n%s\n\nWant:\n%s", got, expected)
	}
}

func TestAutoTabular_MissingKeys(t *testing.T) {
	// Objects with different keys - should NOT tabularize when keys are too sparse
	items := List(
		Map(MapEntry{Key: "id", Value: Int(1)}, MapEntry{Key: "name", Value: Str("Alice")}),
		Map(MapEntry{Key: "id", Value: Int(2)}), // missing "name"
		Map(MapEntry{Key: "id", Value: Int(3)}, MapEntry{Key: "extra", Value: Str("x")}, MapEntry{Key: "name", Value: Str("Carol")}),
	)

	got := CanonicalizeLooseTabular(items)
	// Should remain in list form
	expected := "[{id=1 name=Alice} {id=2} {extra=x id=3 name=Carol}]"

	if got != expected {
		t.Errorf("AutoTabular missing keys:\nGot:\n%s\n\nWant:\n%s", got, expected)
	}
}

func TestAutoTabular_NestedValues(t *testing.T) {
	// Objects with nested maps/lists in cells
	items := List(
		Map(MapEntry{Key: "id", Value: Int(1)}, MapEntry{Key: "meta", Value: Map(MapEntry{Key: "a", Value: Int(1)})}),
		Map(MapEntry{Key: "id", Value: Int(2)}, MapEntry{Key: "meta", Value: Map(MapEntry{Key: "b", Value: Int(2)})}),
		Map(MapEntry{Key: "id", Value: Int(3)}, MapEntry{Key: "meta", Value: List(Int(1), Int(2))}),
	)

	got := CanonicalizeLooseTabular(items)
	expected := "@tab _ rows=3 cols=2 [id meta]\n|1|{a=1}|\n|2|{b=2}|\n|3|[1 2]|\n@end"

	if got != expected {
		t.Errorf("AutoTabular nested:\nGot:\n%s\n\nWant:\n%s", got, expected)
	}
}

func TestAutoTabular_BelowThreshold(t *testing.T) {
	// Only 2 rows - should NOT tabularize
	items := List(
		Map(MapEntry{Key: "id", Value: Int(1)}, MapEntry{Key: "name", Value: Str("Alice")}),
		Map(MapEntry{Key: "id", Value: Int(2)}, MapEntry{Key: "name", Value: Str("Bob")}),
	)

	got := CanonicalizeLooseTabular(items)
	// Should be regular list format, not tabular
	expected := "[{id=1 name=Alice} {id=2 name=Bob}]"

	if got != expected {
		t.Errorf("AutoTabular below threshold:\nGot:\n%s\n\nWant:\n%s", got, expected)
	}
}

func TestAutoTabular_HeterogeneousList(t *testing.T) {
	// Mixed types - should NOT tabularize
	items := List(
		Map(MapEntry{Key: "id", Value: Int(1)}),
		Str("not an object"),
		Map(MapEntry{Key: "id", Value: Int(3)}),
	)

	got := CanonicalizeLooseTabular(items)
	// Should be regular list format
	expected := `[{id=1} "not an object" {id=3}]`

	if got != expected {
		t.Errorf("AutoTabular heterogeneous:\nGot:\n%s\n\nWant:\n%s", got, expected)
	}
}

func TestAutoTabular_PipeEscaping(t *testing.T) {
	// Values containing | character need escaping
	items := List(
		Map(MapEntry{Key: "cmd", Value: Str("a|b")}, MapEntry{Key: "id", Value: Int(1)}),
		Map(MapEntry{Key: "cmd", Value: Str("c||d")}, MapEntry{Key: "id", Value: Int(2)}),
		Map(MapEntry{Key: "cmd", Value: Str("e")}, MapEntry{Key: "id", Value: Int(3)}),
	)

	got := CanonicalizeLooseTabular(items)
	// Columns sorted: cmd, id
	// Note: "a|b" is quoted because it contains |, and | inside is escaped
	expected := "@tab _ rows=3 cols=2 [cmd id]\n|\"a\\|b\"|1|\n|\"c\\|\\|d\"|2|\n|e|3|\n@end"

	if got != expected {
		t.Errorf("AutoTabular pipe escaping:\nGot:\n%s\n\nWant:\n%s", got, expected)
	}
}

func TestAutoTabular_BackslashInStrings(t *testing.T) {
	// Values containing \ character - backslash is handled by string quoting,
	// NOT by tabular cell escaping
	items := List(
		Map(MapEntry{Key: "id", Value: Int(1)}, MapEntry{Key: "path", Value: Str(`C:\Users`)}),
		Map(MapEntry{Key: "id", Value: Int(2)}, MapEntry{Key: "path", Value: Str(`D:\Data`)}),
		Map(MapEntry{Key: "id", Value: Int(3)}, MapEntry{Key: "path", Value: Str(`home`)}), // bare-safe
	)

	got := CanonicalizeLooseTabular(items)
	// Backslash in string causes it to be quoted, backslash escaped inside quote
	// "home" is bare-safe so not quoted
	expected := "@tab _ rows=3 cols=2 [id path]\n|1|\"C:\\\\Users\"|\n|2|\"D:\\\\Data\"|\n|3|home|\n@end"

	if got != expected {
		t.Errorf("AutoTabular backslash in strings:\nGot:\n%s\n\nWant:\n%s", got, expected)
	}
}

func TestAutoTabular_EmptyList(t *testing.T) {
	items := List()
	got := CanonicalizeLooseTabular(items)
	expected := "[]"

	if got != expected {
		t.Errorf("AutoTabular empty list: got %q, want %q", got, expected)
	}
}

func TestAutoTabular_StructsAsObjects(t *testing.T) {
	// Structs should also be tabularized
	items := List(
		Struct("User", MapEntry{Key: "id", Value: Int(1)}, MapEntry{Key: "name", Value: Str("A")}),
		Struct("User", MapEntry{Key: "id", Value: Int(2)}, MapEntry{Key: "name", Value: Str("B")}),
		Struct("User", MapEntry{Key: "id", Value: Int(3)}, MapEntry{Key: "name", Value: Str("C")}),
	)

	got := CanonicalizeLooseTabular(items)
	expected := "@tab _ rows=3 cols=2 [id name]\n|1|A|\n|2|B|\n|3|C|\n@end"

	if got != expected {
		t.Errorf("AutoTabular structs:\nGot:\n%s\n\nWant:\n%s", got, expected)
	}
}

func TestAutoTabular_PreservesOriginalOrder(t *testing.T) {
	// Keys should be sorted alphabetically, but rows preserve original order
	items := List(
		Map(MapEntry{Key: "z", Value: Int(1)}, MapEntry{Key: "a", Value: Int(10)}),
		Map(MapEntry{Key: "z", Value: Int(2)}, MapEntry{Key: "a", Value: Int(20)}),
		Map(MapEntry{Key: "z", Value: Int(3)}, MapEntry{Key: "a", Value: Int(30)}),
	)

	got := CanonicalizeLooseTabular(items)
	// Columns sorted: a, z (alphabetical)
	// Rows preserve original order: 1,2,3
	expected := "@tab _ rows=3 cols=2 [a z]\n|10|1|\n|20|2|\n|30|3|\n@end"

	if got != expected {
		t.Errorf("AutoTabular order:\nGot:\n%s\n\nWant:\n%s", got, expected)
	}
}

func TestAutoTabular_EnabledByDefault(t *testing.T) {
	// CanonicalizeLoose (without options) SHOULD tabularize eligible lists
	items := List(
		Map(MapEntry{Key: "id", Value: Int(1)}, MapEntry{Key: "name", Value: Str("Alice")}),
		Map(MapEntry{Key: "id", Value: Int(2)}, MapEntry{Key: "name", Value: Str("Bob")}),
		Map(MapEntry{Key: "id", Value: Int(3)}, MapEntry{Key: "name", Value: Str("Carol")}),
	)

	got := CanonicalizeLoose(items)

	if !strings.Contains(got, "@tab _") || !strings.Contains(got, "@end") {
		t.Errorf("CanonicalizeLoose SHOULD auto-tabularize by default:\nGot:\n%s", got)
	}
}

func TestAutoTabular_CanBeDisabled(t *testing.T) {
	// CanonicalizeLooseNoTabular should NOT tabularize
	items := List(
		Map(MapEntry{Key: "id", Value: Int(1)}, MapEntry{Key: "name", Value: Str("Alice")}),
		Map(MapEntry{Key: "id", Value: Int(2)}, MapEntry{Key: "name", Value: Str("Bob")}),
		Map(MapEntry{Key: "id", Value: Int(3)}, MapEntry{Key: "name", Value: Str("Carol")}),
	)

	got := CanonicalizeLooseNoTabular(items)
	expected := "[{id=1 name=Alice} {id=2 name=Bob} {id=3 name=Carol}]"

	if got != expected {
		t.Errorf("CanonicalizeLooseNoTabular should NOT tabularize:\nGot:\n%s\n\nWant:\n%s", got, expected)
	}
}

func TestAutoTabular_CustomOpts(t *testing.T) {
	items := List(
		Map(MapEntry{Key: "id", Value: Int(1)}),
		Map(MapEntry{Key: "id", Value: Int(2)}),
		Map(MapEntry{Key: "id", Value: Int(3)}),
		Map(MapEntry{Key: "id", Value: Int(4)}),
		Map(MapEntry{Key: "id", Value: Int(5)}),
	)

	// With MinRows=10, should NOT tabularize (only 5 rows)
	opts := LooseCanonOpts{
		AutoTabular: true,
		MinRows:     10,
	}
	got := CanonicalizeLooseWithOpts(items, opts)
	expected := "[{id=1} {id=2} {id=3} {id=4} {id=5}]"

	if got != expected {
		t.Errorf("AutoTabular custom MinRows:\nGot:\n%s\n\nWant:\n%s", got, expected)
	}
}

func TestEscapeUnescapeTabularCell(t *testing.T) {
	tests := []struct {
		input   string
		escaped string
	}{
		{"hello", "hello"},
		{"a|b", `a\|b`},
		{`a\b`, `a\b`},           // backslash NOT escaped (it's part of GLYPH string)
		{`a|b\c|d`, `a\|b\c\|d`}, // only pipes escaped
		{"", ""},
	}

	for _, tt := range tests {
		escaped := escapeTabularCell(tt.input)
		if escaped != tt.escaped {
			t.Errorf("escapeTabularCell(%q) = %q, want %q", tt.input, escaped, tt.escaped)
		}

		unescaped := unescapeTabularCell(escaped)
		if unescaped != tt.input {
			t.Errorf("unescapeTabularCell(%q) = %q, want %q", escaped, unescaped, tt.input)
		}
	}
}

// ============================================================
// Tabular Parsing Tests
// ============================================================

func TestParseTabularLoose_Basic(t *testing.T) {
	input := `@tab _ [id name]
|1|Alice|
|2|Bob|
|3|Carol|
@end`

	got, err := ParseTabularLoose(input)
	if err != nil {
		t.Fatalf("ParseTabularLoose failed: %v", err)
	}

	// Verify structure
	if got.typ != TypeList {
		t.Fatalf("Expected list, got %v", got.typ)
	}
	if len(got.listVal) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(got.listVal))
	}

	// Check first row
	row0 := got.listVal[0]
	if row0.typ != TypeMap {
		t.Fatalf("Expected map, got %v", row0.typ)
	}
	if len(row0.mapVal) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(row0.mapVal))
	}
}

func TestParseTabularLoose_Roundtrip(t *testing.T) {
	// Create original data
	items := List(
		Map(MapEntry{Key: "id", Value: Int(1)}, MapEntry{Key: "name", Value: Str("Alice")}),
		Map(MapEntry{Key: "id", Value: Int(2)}, MapEntry{Key: "name", Value: Str("Bob")}),
		Map(MapEntry{Key: "id", Value: Int(3)}, MapEntry{Key: "name", Value: Str("Carol")}),
	)

	// Emit as tabular
	tabular := CanonicalizeLooseTabular(items)

	// Parse back
	parsed, err := ParseTabularLoose(tabular)
	if err != nil {
		t.Fatalf("ParseTabularLoose failed: %v", err)
	}

	// Compare canonical forms (non-tabular for comparison)
	origCanon := CanonicalizeLoose(items)
	parsedCanon := CanonicalizeLoose(parsed)

	if origCanon != parsedCanon {
		t.Errorf("Roundtrip mismatch:\nOriginal: %s\nParsed:   %s", origCanon, parsedCanon)
	}
}

func TestParseTabularLoose_WithNulls(t *testing.T) {
	input := `@tab _ [a b c]
|1|∅|3|
|∅|2|∅|
@end`

	got, err := ParseTabularLoose(input)
	if err != nil {
		t.Fatalf("ParseTabularLoose failed: %v", err)
	}

	if len(got.listVal) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(got.listVal))
	}

	// First row: a=1, b=null, c=3
	row0 := got.listVal[0]
	for _, e := range row0.mapVal {
		if e.Key == "b" && e.Value.typ != TypeNull {
			t.Errorf("Expected null for b in row 0, got %v", e.Value.typ)
		}
	}
}

func TestParseTabularLoose_NestedValues(t *testing.T) {
	input := `@tab _ [id meta]
|1|{a=1}|
|2|[1 2 3]|
@end`

	got, err := ParseTabularLoose(input)
	if err != nil {
		t.Fatalf("ParseTabularLoose failed: %v", err)
	}

	if len(got.listVal) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(got.listVal))
	}

	// First row: meta should be a map
	row0 := got.listVal[0]
	for _, e := range row0.mapVal {
		if e.Key == "meta" && e.Value.typ != TypeMap {
			t.Errorf("Expected map for meta in row 0, got %v", e.Value.typ)
		}
	}

	// Second row: meta should be a list
	row1 := got.listVal[1]
	for _, e := range row1.mapVal {
		if e.Key == "meta" && e.Value.typ != TypeList {
			t.Errorf("Expected list for meta in row 1, got %v", e.Value.typ)
		}
	}
}

func TestParseTabularLoose_EscapedPipe(t *testing.T) {
	// Test roundtrip with pipe in value
	items := List(
		Map(MapEntry{Key: "cmd", Value: Str("a|b")}, MapEntry{Key: "id", Value: Int(1)}),
		Map(MapEntry{Key: "cmd", Value: Str("c")}, MapEntry{Key: "id", Value: Int(2)}),
		Map(MapEntry{Key: "cmd", Value: Str("d|e|f")}, MapEntry{Key: "id", Value: Int(3)}),
	)

	tabular := CanonicalizeLooseTabular(items)
	parsed, err := ParseTabularLoose(tabular)
	if err != nil {
		t.Fatalf("ParseTabularLoose failed: %v", err)
	}

	origCanon := CanonicalizeLoose(items)
	parsedCanon := CanonicalizeLoose(parsed)

	if origCanon != parsedCanon {
		t.Errorf("Roundtrip with pipes mismatch:\nOriginal: %s\nParsed:   %s\nTabular:\n%s", origCanon, parsedCanon, tabular)
	}
}

func TestParseTabularLoose_QuotedStrings(t *testing.T) {
	input := `@tab _ [id name]
|1|"hello world"|
|2|simple|
@end`

	got, err := ParseTabularLoose(input)
	if err != nil {
		t.Fatalf("ParseTabularLoose failed: %v", err)
	}

	row0 := got.listVal[0]
	for _, e := range row0.mapVal {
		if e.Key == "name" {
			if e.Value.strVal != "hello world" {
				t.Errorf("Expected 'hello world', got %q", e.Value.strVal)
			}
		}
	}
}

func TestParseTabularLoose_EmptyColumns(t *testing.T) {
	// With 0 columns, rows should be just ||
	input := `@tab _ []
||
||
@end`

	got, err := ParseTabularLoose(input)
	if err != nil {
		t.Fatalf("ParseTabularLoose failed: %v", err)
	}

	if len(got.listVal) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(got.listVal))
	}

	// Each row should be an empty map
	for i, row := range got.listVal {
		if len(row.mapVal) != 0 {
			t.Errorf("Row %d: expected empty map, got %d entries", i, len(row.mapVal))
		}
	}
}

func TestParseTabularLoose_SingleColumn(t *testing.T) {
	input := `@tab _ [x]
|1|
|2|
|3|
@end`

	got, err := ParseTabularLoose(input)
	if err != nil {
		t.Fatalf("ParseTabularLoose failed: %v", err)
	}

	if len(got.listVal) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(got.listVal))
	}

	// Check values
	expected := []int64{1, 2, 3}
	for i, row := range got.listVal {
		for _, e := range row.mapVal {
			if e.Key == "x" && e.Value.intVal != expected[i] {
				t.Errorf("Row %d: expected x=%d, got %d", i, expected[i], e.Value.intVal)
			}
		}
	}
}

// ============================================================
// Cross-Implementation Parity Tests
// ============================================================

// TestCrossImpl_CanonicalizeLoose verifies that Go and JS produce
// identical canonical strings for the JSON corpus.
func TestCrossImpl_CanonicalizeLoose(t *testing.T) {
	// Load manifest
	manifestPath := filepath.Join("testdata", "loose_json", "manifest.json")
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Skipf("Skipping cross-impl tests: %v", err)
		return
	}

	var m manifest
	if err := json.Unmarshal(manifestData, &m); err != nil {
		t.Fatalf("Failed to parse manifest: %v", err)
	}

	// Check if node is available
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Skipping cross-impl tests: node not found")
		return
	}

	canonScript := filepath.Join("test", "js", "canon.mjs")
	if _, err := os.Stat(canonScript); err != nil {
		t.Skipf("Skipping cross-impl tests: %v", err)
		return
	}

	for _, tc := range m.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			// Load JSON file
			jsonPath := filepath.Join("testdata", "loose_json", tc.File)
			jsonData, err := os.ReadFile(jsonPath)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", tc.File, err)
			}

			// Get Go canonical form
			gv, err := FromJSONLoose(jsonData)
			if err != nil {
				t.Fatalf("Go FromJSONLoose failed: %v", err)
			}
			goCanon := CanonicalizeLoose(gv)

			// Get JS canonical form via canon.mjs
			cmd := exec.Command("node", canonScript, "canonicalize-loose", string(jsonData))
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("JS canonicalize-loose failed: %v\nOutput: %s", err, output)
			}

			var jsResult struct {
				Success bool   `json:"success"`
				Result  string `json:"result"`
				Error   string `json:"error"`
			}
			if err := json.Unmarshal(output, &jsResult); err != nil {
				t.Fatalf("Failed to parse JS output: %v\nOutput: %s", err, output)
			}
			if !jsResult.Success {
				t.Fatalf("JS error: %s", jsResult.Error)
			}

			// Compare canonical forms
			// Note: Go uses _ as default null, JS uses ∅. Both are valid null representations.
			// Normalize by replacing standalone _ (null) with ∅ for comparison until JS is updated.
			// Only replace _ that's a standalone value, not part of a key name.
			goCanonNormalized := normalizeNullForComparison(goCanon)
			jsCanonNormalized := normalizeNullForComparison(jsResult.Result)
			if goCanonNormalized != jsCanonNormalized {
				t.Errorf("Canonical mismatch\nGo: %s\nJS: %s", goCanon, jsResult.Result)
			}
		})
	}
}

// ============================================================
// v2.4.0 Tests: Null Aliases, LLM Mode, Schema Headers
// ============================================================

func TestNullAlias_Parsing(t *testing.T) {
	// All null aliases should parse to null
	testCases := []string{"∅", "_", "null"}
	for _, alias := range testCases {
		got, err := parseLooseValue(alias)
		if err != nil {
			t.Errorf("Failed to parse null alias %q: %v", alias, err)
			continue
		}
		if got == nil || got.typ != TypeNull {
			t.Errorf("Null alias %q: expected Null, got %v", alias, got)
		}
	}
}

func TestLLMMode_NullEmission(t *testing.T) {
	// LLM mode should emit _ for null
	v := Map(
		MapEntry{Key: "a", Value: Null()},
		MapEntry{Key: "b", Value: Int(42)},
	)

	opts := LLMLooseCanonOpts()
	got := CanonicalizeLooseWithOpts(v, opts)
	expected := "{a=_ b=42}"

	if got != expected {
		t.Errorf("LLM mode null:\nGot: %s\nWant: %s", got, expected)
	}
}

func TestLLMMode_TabularNullEmission(t *testing.T) {
	// LLM mode tabular should emit _ for missing values
	items := List(
		Map(MapEntry{Key: "id", Value: Int(1)}, MapEntry{Key: "name", Value: Str("Alice")}),
		Map(MapEntry{Key: "id", Value: Int(2)}), // missing name
		Map(MapEntry{Key: "id", Value: Int(3)}, MapEntry{Key: "name", Value: Str("Carol")}),
	)

	opts := LLMLooseCanonOpts()
	got := CanonicalizeLooseWithOpts(items, opts)

	// Should contain _ for missing values, not ∅
	if !strings.Contains(got, "|_|") {
		t.Errorf("LLM mode tabular should use _ for null:\n%s", got)
	}
	if strings.Contains(got, "|∅|") {
		t.Errorf("LLM mode tabular should NOT use ∅ for null:\n%s", got)
	}
}

func TestSchemaHeader_Emission(t *testing.T) {
	v := Map(
		MapEntry{Key: "action", Value: Str("search")},
		MapEntry{Key: "query", Value: Str("weather NYC")},
	)

	opts := LooseCanonOpts{
		SchemaRef: "abc123",
		KeyDict:   []string{"action", "query"},
	}
	got := CanonicalizeLooseWithSchema(v, opts)

	// Should have schema header (v2.6 uses @keys= prefix)
	if !strings.HasPrefix(got, "@schema#abc123 @keys=[action query]") {
		t.Errorf("Schema header emission:\n%s", got)
	}
}

func TestCompactKeys_Emission(t *testing.T) {
	v := Map(
		MapEntry{Key: "action", Value: Str("search")},
		MapEntry{Key: "query", Value: Str("weather NYC")},
	)

	opts := LooseCanonOpts{
		SchemaRef:      "abc123",
		KeyDict:        []string{"action", "query"},
		UseCompactKeys: true,
	}
	got := CanonicalizeLooseWithSchema(v, opts)

	// Should use compact keys #0 and #1
	if !strings.Contains(got, "#0=search") || !strings.Contains(got, "#1=") {
		t.Errorf("Compact key emission:\n%s", got)
	}
}

func TestSchemaHeader_Parsing(t *testing.T) {
	// Inline schema
	line := "@schema#abc123 keys=[action query confidence]"
	schemaRef, keyDict, err := ParseSchemaHeader(line)
	if err != nil {
		t.Fatalf("ParseSchemaHeader error: %v", err)
	}
	if schemaRef != "abc123" {
		t.Errorf("schemaRef: got %q, want %q", schemaRef, "abc123")
	}
	if len(keyDict) != 3 {
		t.Errorf("keyDict: got %d keys, want 3", len(keyDict))
	}
	if keyDict[0] != "action" || keyDict[1] != "query" || keyDict[2] != "confidence" {
		t.Errorf("keyDict: got %v", keyDict)
	}

	// External ref only
	line2 := "@schema#def456"
	schemaRef2, keyDict2, err2 := ParseSchemaHeader(line2)
	if err2 != nil {
		t.Fatalf("ParseSchemaHeader error: %v", err2)
	}
	if schemaRef2 != "def456" {
		t.Errorf("schemaRef: got %q, want %q", schemaRef2, "def456")
	}
	if len(keyDict2) != 0 {
		t.Errorf("keyDict: got %d keys, want 0", len(keyDict2))
	}
}

func TestCompactKeys_Parsing(t *testing.T) {
	// Parse map with compact keys using dictionary
	keyDict := []string{"action", "query"}
	input := "{#0=search #1=\"weather NYC\"}"
	got, err := parseLooseMapWithDict(input, keyDict)
	if err != nil {
		t.Fatalf("parseLooseMapWithDict error: %v", err)
	}

	// Check the keys were resolved
	action := got.Get("action")
	if action == nil || mustAsStr(t, action) != "search" {
		t.Errorf("action: got %v", action)
	}
	query := got.Get("query")
	if query == nil || mustAsStr(t, query) != "weather NYC" {
		t.Errorf("query: got %v", query)
	}
}

func TestBuildKeyDict(t *testing.T) {
	v := Map(
		MapEntry{Key: "action", Value: Str("search")},
		MapEntry{Key: "meta", Value: Map(
			MapEntry{Key: "source", Value: Str("web")},
			MapEntry{Key: "score", Value: Float(0.95)},
		)},
	)

	keys := BuildKeyDictFromValue(v)

	// Should include all keys from nested structure
	expected := []string{"action", "meta", "score", "source"}
	if len(keys) != len(expected) {
		t.Errorf("BuildKeyDict: got %v, want %v", keys, expected)
	}
	for i, k := range expected {
		if keys[i] != k {
			t.Errorf("BuildKeyDict[%d]: got %q, want %q", i, keys[i], k)
		}
	}
}

func TestTabularHeaderWithMeta(t *testing.T) {
	// Test parsing header with rows/cols metadata
	line := "@tab _ rows=10 cols=3 [id name score]"
	meta, err := parseTabularLooseHeaderWithMeta(line)
	if err != nil {
		t.Fatalf("parseTabularLooseHeaderWithMeta error: %v", err)
	}
	if meta.Rows != 10 {
		t.Errorf("Rows: got %d, want 10", meta.Rows)
	}
	if meta.Cols != 3 {
		t.Errorf("Cols: got %d, want 3", meta.Cols)
	}
	if len(meta.Keys) != 3 {
		t.Errorf("Keys: got %d, want 3", len(meta.Keys))
	}
}

// ============================================================
// v2.4.0 Cross-Implementation Parity Tests
// ============================================================

// TestCrossImpl_LLMMode verifies LLM mode parity between Go and JS
func TestCrossImpl_LLMMode(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Skipping: node not found")
	}

	canonScript := filepath.Join("test", "js", "canon.mjs")
	if _, err := os.Stat(canonScript); err != nil {
		t.Skipf("Skipping: %v", err)
	}

	testCases := []struct {
		name string
		json string
	}{
		{"null_value", `{"a": null, "b": 42}`},
		{"array_with_nulls", `[null, 1, null, 2, null]`},
		{"nested_null", `{"outer": {"inner": null}}`},
		{"array_of_objects", `[{"id":1,"name":"a"},{"id":2},{"id":3,"name":"c"}]`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Get Go canonical form with LLM mode
			gv, err := FromJSONLoose([]byte(tc.json))
			if err != nil {
				t.Fatalf("Go FromJSONLoose failed: %v", err)
			}
			opts := LLMLooseCanonOpts()
			goCanon := CanonicalizeLooseWithOpts(gv, opts)

			// Get JS canonical form via canon.mjs
			cmd := exec.Command("node", canonScript, "canonicalize-loose-llm", tc.json)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("JS canonicalize-loose-llm failed: %v\nOutput: %s", err, output)
			}

			var jsResult struct {
				Success bool   `json:"success"`
				Result  string `json:"result"`
				Error   string `json:"error"`
			}
			if err := json.Unmarshal(output, &jsResult); err != nil {
				t.Fatalf("Failed to parse JS output: %v\nOutput: %s", err, output)
			}
			if !jsResult.Success {
				t.Fatalf("JS error: %s", jsResult.Error)
			}

			// Compare
			if goCanon != jsResult.Result {
				t.Errorf("LLM mode mismatch\nGo: %s\nJS: %s", goCanon, jsResult.Result)
			}
		})
	}
}

// TestCrossImpl_BuildKeyDict verifies key dictionary building parity
func TestCrossImpl_BuildKeyDict(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Skipping: node not found")
	}

	canonScript := filepath.Join("test", "js", "canon.mjs")
	if _, err := os.Stat(canonScript); err != nil {
		t.Skipf("Skipping: %v", err)
	}

	testCases := []struct {
		name string
		json string
	}{
		{"simple_object", `{"action": "search", "query": "test"}`},
		{"nested_object", `{"meta": {"id": 1, "tags": ["a", "b"]}, "data": "value"}`},
		{"array_of_objects", `[{"id":1,"name":"a"},{"id":2,"name":"b"}]`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Get Go key dict
			gv, err := FromJSONLoose([]byte(tc.json))
			if err != nil {
				t.Fatalf("Go FromJSONLoose failed: %v", err)
			}
			goKeyDict := BuildKeyDictFromValue(gv)

			// Get JS key dict via canon.mjs
			cmd := exec.Command("node", canonScript, "build-key-dict", tc.json)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("JS build-key-dict failed: %v\nOutput: %s", err, output)
			}

			var jsResult struct {
				Success bool   `json:"success"`
				Result  string `json:"result"`
				Error   string `json:"error"`
			}
			if err := json.Unmarshal(output, &jsResult); err != nil {
				t.Fatalf("Failed to parse JS output: %v\nOutput: %s", err, output)
			}
			if !jsResult.Success {
				t.Fatalf("JS error: %s", jsResult.Error)
			}

			var jsKeyDict []string
			if err := json.Unmarshal([]byte(jsResult.Result), &jsKeyDict); err != nil {
				t.Fatalf("Failed to parse JS key dict: %v", err)
			}

			// Compare
			if len(goKeyDict) != len(jsKeyDict) {
				t.Errorf("Key dict length mismatch\nGo: %v\nJS: %v", goKeyDict, jsKeyDict)
				return
			}
			for i := range goKeyDict {
				if goKeyDict[i] != jsKeyDict[i] {
					t.Errorf("Key dict[%d] mismatch\nGo: %s\nJS: %s", i, goKeyDict[i], jsKeyDict[i])
				}
			}
		})
	}
}

// TestCrossImpl_SchemaHeader verifies schema header parsing parity
func TestCrossImpl_SchemaHeader(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Skipping: node not found")
	}

	canonScript := filepath.Join("test", "js", "canon.mjs")
	if _, err := os.Stat(canonScript); err != nil {
		t.Skipf("Skipping: %v", err)
	}

	testCases := []struct {
		name   string
		header string
	}{
		{"inline_schema", "@schema#abc123 keys=[action query confidence]"},
		{"external_ref", "@schema#def456"},
		{"long_hash", "@schema#254c4fa437b5cb5fa05c91e486c55409 keys=[a b c d e]"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Get Go parse result
			goRef, goDict, err := ParseSchemaHeader(tc.header)
			if err != nil {
				t.Fatalf("Go ParseSchemaHeader failed: %v", err)
			}

			// Get JS parse result via canon.mjs
			cmd := exec.Command("node", canonScript, "parse-schema-header", tc.header)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("JS parse-schema-header failed: %v\nOutput: %s", err, output)
			}

			var jsResult struct {
				Success bool   `json:"success"`
				Result  string `json:"result"`
				Error   string `json:"error"`
			}
			if err := json.Unmarshal(output, &jsResult); err != nil {
				t.Fatalf("Failed to parse JS output: %v\nOutput: %s", err, output)
			}
			if !jsResult.Success {
				t.Fatalf("JS error: %s", jsResult.Error)
			}

			var jsParsed struct {
				SchemaRef string   `json:"schemaRef"`
				KeyDict   []string `json:"keyDict"`
			}
			if err := json.Unmarshal([]byte(jsResult.Result), &jsParsed); err != nil {
				t.Fatalf("Failed to parse JS schema header: %v", err)
			}

			// Compare
			if goRef != jsParsed.SchemaRef {
				t.Errorf("SchemaRef mismatch\nGo: %s\nJS: %s", goRef, jsParsed.SchemaRef)
			}
			if len(goDict) != len(jsParsed.KeyDict) {
				t.Errorf("KeyDict length mismatch\nGo: %v\nJS: %v", goDict, jsParsed.KeyDict)
				return
			}
			for i := range goDict {
				if goDict[i] != jsParsed.KeyDict[i] {
					t.Errorf("KeyDict[%d] mismatch\nGo: %s\nJS: %s", i, goDict[i], jsParsed.KeyDict[i])
				}
			}
		})
	}
}

// TestCrossImpl_TabularHeaderMeta verifies tabular header metadata parsing parity
func TestCrossImpl_TabularHeaderMeta(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Skipping: node not found")
	}

	canonScript := filepath.Join("test", "js", "canon.mjs")
	if _, err := os.Stat(canonScript); err != nil {
		t.Skipf("Skipping: %v", err)
	}

	testCases := []struct {
		name   string
		header string
	}{
		{"with_meta", "@tab _ rows=10 cols=3 [id name score]"},
		{"no_meta", "@tab _ [a b c]"},
		{"rows_only", "@tab _ rows=5 [x y]"},
		{"cols_only", "@tab _ cols=4 [p q r s]"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Get Go parse result
			goMeta, err := parseTabularLooseHeaderWithMeta(tc.header)
			if err != nil {
				t.Fatalf("Go parseTabularLooseHeaderWithMeta failed: %v", err)
			}

			// Get JS parse result via canon.mjs
			cmd := exec.Command("node", canonScript, "parse-tabular-header-with-meta", tc.header)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("JS parse-tabular-header-with-meta failed: %v\nOutput: %s", err, output)
			}

			var jsResult struct {
				Success bool   `json:"success"`
				Result  string `json:"result"`
				Error   string `json:"error"`
			}
			if err := json.Unmarshal(output, &jsResult); err != nil {
				t.Fatalf("Failed to parse JS output: %v\nOutput: %s", err, output)
			}
			if !jsResult.Success {
				t.Fatalf("JS error: %s", jsResult.Error)
			}

			var jsParsed struct {
				Rows int      `json:"rows"`
				Cols int      `json:"cols"`
				Keys []string `json:"keys"`
			}
			if err := json.Unmarshal([]byte(jsResult.Result), &jsParsed); err != nil {
				t.Fatalf("Failed to parse JS tabular meta: %v", err)
			}

			// Compare
			if goMeta.Rows != jsParsed.Rows {
				t.Errorf("Rows mismatch\nGo: %d\nJS: %d", goMeta.Rows, jsParsed.Rows)
			}
			if goMeta.Cols != jsParsed.Cols {
				t.Errorf("Cols mismatch\nGo: %d\nJS: %d", goMeta.Cols, jsParsed.Cols)
			}
			if len(goMeta.Keys) != len(jsParsed.Keys) {
				t.Errorf("Keys length mismatch\nGo: %v\nJS: %v", goMeta.Keys, jsParsed.Keys)
				return
			}
			for i := range goMeta.Keys {
				if goMeta.Keys[i] != jsParsed.Keys[i] {
					t.Errorf("Keys[%d] mismatch\nGo: %s\nJS: %s", i, goMeta.Keys[i], jsParsed.Keys[i])
				}
			}
		})
	}
}

// TestCrossImpl_CompactKeysRoundtrip verifies schema+compact keys emission parity
func TestCrossImpl_CompactKeysRoundtrip(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Skipping: node not found")
	}

	canonScript := filepath.Join("test", "js", "canon.mjs")
	if _, err := os.Stat(canonScript); err != nil {
		t.Skipf("Skipping: %v", err)
	}

	testCases := []struct {
		name string
		json string
	}{
		{"simple_object", `{"action": "search", "query": "weather NYC"}`},
		{"with_numbers", `{"id": 42, "score": 0.95, "label": "positive"}`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Get Go canonical form with schema + compact keys
			gv, err := FromJSONLoose([]byte(tc.json))
			if err != nil {
				t.Fatalf("Go FromJSONLoose failed: %v", err)
			}
			keyDict := BuildKeyDictFromValue(gv)
			opts := LooseCanonOpts{
				SchemaRef:      "test123",
				KeyDict:        keyDict,
				UseCompactKeys: true,
			}
			goCanon := CanonicalizeLooseWithSchema(gv, opts)

			// Get JS canonical form via canon.mjs
			optsJSON := fmt.Sprintf(`{"schemaRef":"test123","keyDict":%s,"useCompactKeys":true}`, mustJSON(keyDict))
			cmd := exec.Command("node", canonScript, "canonicalize-loose-with-schema", tc.json, optsJSON)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("JS canonicalize-loose-with-schema failed: %v\nOutput: %s", err, output)
			}

			var jsResult struct {
				Success bool   `json:"success"`
				Result  string `json:"result"`
				Error   string `json:"error"`
			}
			if err := json.Unmarshal(output, &jsResult); err != nil {
				t.Fatalf("Failed to parse JS output: %v\nOutput: %s", err, output)
			}
			if !jsResult.Success {
				t.Fatalf("JS error: %s", jsResult.Error)
			}

			// Compare (normalize header format: Go uses @keys=, JS uses keys=)
			goCanonNorm := strings.ReplaceAll(goCanon, "@keys=", "keys=")
			goCanonNorm = normalizeNullForComparison(goCanonNorm) // Also normalize null
			jsCanonNorm := normalizeNullForComparison(jsResult.Result)
			if goCanonNorm != jsCanonNorm {
				t.Errorf("Compact keys mismatch\nGo: %s\nJS: %s", goCanon, jsResult.Result)
			}
		})
	}
}

func mustJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// ============================================================
// v2.4.0 Triple-Implementation Tests: Go, JS, Python
// ============================================================

// runPythonCanon calls the Python canon.py script and returns the result.
func runPythonCanon(t *testing.T, command string, args ...string) (string, bool) {
	pyScript := filepath.Join("test", "py", "canon.py")
	if _, err := os.Stat(pyScript); err != nil {
		t.Skipf("Skipping Python tests: %v", err)
		return "", false
	}

	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("Skipping: python3 not found")
		return "", false
	}

	cmdArgs := append([]string{pyScript, command}, args...)
	cmd := exec.Command("python3", cmdArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("Python %s failed: %v\nOutput: %s", command, err, output)
		return "", false
	}

	var result struct {
		Success bool   `json:"success"`
		Result  string `json:"result"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Logf("Failed to parse Python output: %v\nOutput: %s", err, output)
		return "", false
	}
	if !result.Success {
		t.Logf("Python error: %s", result.Error)
		return "", false
	}

	return result.Result, true
}

// runJSCanon calls the JS canon.mjs script and returns the result.
func runJSCanon(t *testing.T, command string, args ...string) (string, bool) {
	jsScript := filepath.Join("test", "js", "canon.mjs")
	if _, err := os.Stat(jsScript); err != nil {
		t.Skipf("Skipping JS tests: %v", err)
		return "", false
	}

	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Skipping: node not found")
		return "", false
	}

	cmdArgs := append([]string{jsScript, command}, args...)
	cmd := exec.Command("node", cmdArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("JS %s failed: %v\nOutput: %s", command, err, output)
		return "", false
	}

	var result struct {
		Success bool   `json:"success"`
		Result  string `json:"result"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Logf("Failed to parse JS output: %v\nOutput: %s", err, output)
		return "", false
	}
	if !result.Success {
		t.Logf("JS error: %s", result.Error)
		return "", false
	}

	return result.Result, true
}

// TestTripleImpl_CanonicalizeLoose tests Go, JS, and Python produce identical output.
func TestTripleImpl_CanonicalizeLoose(t *testing.T) {
	testCases := []struct {
		name string
		json string
	}{
		{"null", `null`},
		{"bool_true", `true`},
		{"bool_false", `false`},
		{"int_zero", `0`},
		{"int_positive", `42`},
		{"int_negative", `-100`},
		{"float", `3.14`},
		{"string_simple", `"hello"`},
		{"string_with_space", `"hello world"`},
		{"empty_list", `[]`},
		{"int_list", `[1, 2, 3]`},
		{"empty_object", `{}`},
		{"simple_object", `{"a": 1, "b": 2}`},
		{"nested_object", `{"outer": {"inner": 42}}`},
		{"mixed_list", `[null, true, 42, "hi"]`},
		{"complex", `{"name": "Alice", "scores": [95, 87, 92], "active": true}`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Go
			gv, err := FromJSONLoose([]byte(tc.json))
			if err != nil {
				t.Fatalf("Go FromJSONLoose failed: %v", err)
			}
			goCanon := CanonicalizeLoose(gv)

			// JS
			jsCanon, jsOK := runJSCanon(t, "canonicalize-loose", tc.json)

			// Python
			pyCanon, pyOK := runPythonCanon(t, "canonicalize-loose", tc.json)

			// Normalize null representations: Go uses _, JS uses ∅
			goCanonNorm := normalizeNullForComparison(goCanon)
			jsCanonNorm := normalizeNullForComparison(jsCanon)

			// Compare Go vs JS
			if jsOK && goCanonNorm != jsCanonNorm {
				t.Errorf("Go vs JS mismatch\nGo: %s\nJS: %s", goCanon, jsCanon)
			}

			// Compare Go vs Python (Python now also uses _ as default)
			if pyOK && goCanon != pyCanon {
				t.Errorf("Go vs Python mismatch\nGo: %s\nPy: %s", goCanon, pyCanon)
			}

			// Compare JS vs Python (normalize JS's ∅ to Python's _)
			// Use reverse normalization: replace ∅ values with _
			jsCanonForPy := jsCanon
			// Special case: standalone ∅ is null
			if jsCanonForPy == "∅" {
				jsCanonForPy = "_"
			}
			jsCanonForPy = strings.ReplaceAll(jsCanonForPy, "=∅ ", "=_ ")
			jsCanonForPy = strings.ReplaceAll(jsCanonForPy, "=∅}", "=_}")
			jsCanonForPy = strings.ReplaceAll(jsCanonForPy, "=∅]", "=_]")
			jsCanonForPy = strings.ReplaceAll(jsCanonForPy, "=∅\n", "=_\n")
			jsCanonForPy = strings.ReplaceAll(jsCanonForPy, "[∅]", "[_]")
			jsCanonForPy = strings.ReplaceAll(jsCanonForPy, "[∅ ", "[_ ")
			jsCanonForPy = strings.ReplaceAll(jsCanonForPy, " ∅]", " _]")
			jsCanonForPy = strings.ReplaceAll(jsCanonForPy, " ∅ ", " _ ")
			jsCanonForPy = strings.ReplaceAll(jsCanonForPy, "|∅|", "|_|")
			jsCanonForPy = strings.ReplaceAll(jsCanonForPy, "|∅\n", "|_\n")
			if strings.HasSuffix(jsCanonForPy, "=∅") {
				jsCanonForPy = jsCanonForPy[:len(jsCanonForPy)-len("∅")] + "_"
			}
			if jsOK && pyOK && jsCanonForPy != pyCanon {
				t.Errorf("JS vs Python mismatch\nJS: %s\nPy: %s", jsCanon, pyCanon)
			}
		})
	}
}

// TestTripleImpl_LLMMode tests LLM mode (ASCII-safe nulls) across all implementations.
func TestTripleImpl_LLMMode(t *testing.T) {
	testCases := []struct {
		name string
		json string
	}{
		{"null_value", `{"x": null}`},
		{"nested_null", `{"outer": {"inner": null, "value": 42}}`},
		{"list_with_null", `[1, null, 3]`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Go
			gv, err := FromJSONLoose([]byte(tc.json))
			if err != nil {
				t.Fatalf("Go FromJSONLoose failed: %v", err)
			}
			opts := LLMLooseCanonOpts()
			goCanon := CanonicalizeLooseWithOpts(gv, opts)

			// Verify Go uses _ for null
			if strings.Contains(tc.json, "null") && !strings.Contains(goCanon, "_") {
				t.Errorf("Go LLM mode should emit _ for null, got: %s", goCanon)
			}

			// JS
			jsCanon, jsOK := runJSCanon(t, "canonicalize-loose-llm", tc.json)

			// Python
			pyCanon, pyOK := runPythonCanon(t, "canonicalize-loose-llm", tc.json)

			// Compare
			if jsOK && goCanon != jsCanon {
				t.Errorf("Go vs JS LLM mismatch\nGo: %s\nJS: %s", goCanon, jsCanon)
			}
			if pyOK && goCanon != pyCanon {
				t.Errorf("Go vs Python LLM mismatch\nGo: %s\nPy: %s", goCanon, pyCanon)
			}
		})
	}
}

// TestTripleImpl_BuildKeyDict tests key dictionary building across all implementations.
func TestTripleImpl_BuildKeyDict(t *testing.T) {
	testCases := []struct {
		name string
		json string
	}{
		{"simple", `{"a": 1, "b": 2}`},
		{"nested", `{"outer": {"inner": 1}, "x": 2}`},
		{"list_of_objects", `[{"a": 1, "b": 2}, {"b": 3, "c": 4}]`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Go
			gv, err := FromJSONLoose([]byte(tc.json))
			if err != nil {
				t.Fatalf("Go FromJSONLoose failed: %v", err)
			}
			goKeys := BuildKeyDictFromValue(gv)
			goKeysJSON := mustJSON(goKeys)

			// JS
			jsKeysJSON, jsOK := runJSCanon(t, "build-key-dict", tc.json)

			// Python
			pyKeysJSON, pyOK := runPythonCanon(t, "build-key-dict", tc.json)

			// Compare
			if jsOK && goKeysJSON != jsKeysJSON {
				t.Errorf("Go vs JS key dict mismatch\nGo: %s\nJS: %s", goKeysJSON, jsKeysJSON)
			}
			if pyOK && goKeysJSON != pyKeysJSON {
				t.Errorf("Go vs Python key dict mismatch\nGo: %s\nPy: %s", goKeysJSON, pyKeysJSON)
			}
		})
	}
}

// TestTripleImpl_PatchParse tests patch parsing across all implementations.
func TestTripleImpl_PatchParse(t *testing.T) {
	patches := []struct {
		name  string
		patch string
	}{
		{
			name: "basic_set",
			patch: `@patch @keys=wire @target=m:123
= score 42
@end`,
		},
		{
			name: "with_schema_and_base",
			patch: `@patch @schema#abc123 @keys=wire @target=m:ARS-LIV @base=deadbeef12345678
= home.score 2
= away.score 1
~ rating +0.15
@end`,
		},
		{
			name: "mixed_ops",
			patch: `@patch @keys=wire @target=test:1
= name "Updated"
+ items 42
- obsolete
~ count +5
@end`,
		},
	}

	for _, tc := range patches {
		t.Run(tc.name, func(t *testing.T) {
			// Go
			goPatch, err := ParsePatch(tc.patch, nil)
			if err != nil {
				t.Fatalf("Go ParsePatch failed: %v", err)
			}

			// JS
			jsResultJSON, jsOK := runJSCanon(t, "parse-patch", tc.patch)
			if jsOK {
				var jsResult struct {
					Target   interface{} `json:"target"`
					SchemaId string      `json:"schemaId"`
					OpsCount int         `json:"opsCount"`
				}
				if err := json.Unmarshal([]byte(jsResultJSON), &jsResult); err == nil {
					if jsResult.OpsCount != len(goPatch.Ops) {
						t.Errorf("Go vs JS ops count mismatch: Go=%d, JS=%d", len(goPatch.Ops), jsResult.OpsCount)
					}
				}
			}

			// Python
			pyResultJSON, pyOK := runPythonCanon(t, "parse-patch", tc.patch)
			if pyOK {
				var pyResult struct {
					SchemaId        string `json:"schemaId"`
					BaseFingerprint string `json:"baseFingerprint"`
					OpsCount        int    `json:"opsCount"`
				}
				if err := json.Unmarshal([]byte(pyResultJSON), &pyResult); err == nil {
					if pyResult.OpsCount != len(goPatch.Ops) {
						t.Errorf("Go vs Python ops count mismatch: Go=%d, Py=%d", len(goPatch.Ops), pyResult.OpsCount)
					}
					if pyResult.SchemaId != goPatch.SchemaID {
						t.Errorf("Go vs Python schema ID mismatch: Go=%s, Py=%s", goPatch.SchemaID, pyResult.SchemaId)
					}
					if pyResult.BaseFingerprint != goPatch.BaseFingerprint {
						t.Errorf("Go vs Python base fingerprint mismatch: Go=%s, Py=%s", goPatch.BaseFingerprint, pyResult.BaseFingerprint)
					}
				}
			}
		})
	}
}
