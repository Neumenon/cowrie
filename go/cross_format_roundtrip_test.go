//go:build agentgo

package cowrie

// Suite 2: Cross-Format Round-Trip
//
// Tests glyph text → cowrie binary → glyph text and reverse.
// Only available in agentgo build (requires glyph bridge).
//
// Build tag: agentgo (only builds in the Agent-GO monorepo context
// where glyph/go/glyph/bridge.go is available with the cowrie import).

import (
	"testing"

	"github.com/phenomenon0/Agent-GO/glyph/go/glyph"
)

func TestCrossFormat_GlyphToCowrieThenBack(t *testing.T) {
	cases := []struct {
		name      string
		glyphText string
	}{
		{"simple_map", "name Alice\nage 30"},
		{"nested", "user {\n  name Bob\n  score 42\n}"},
		{"array", "items [1 2 3]"},
		{"null_value", "key _"},
		{"bool_values", "a t\nb f"},
		{"empty_map", ""},
		{"float_value", "pi 3.14159"},
		{"string_quoted", "msg \"hello world\""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse glyph text → GValue
			gv, err := glyph.FromJSONLoose(tc.glyphText)
			if err != nil {
				t.Fatalf("FromJSONLoose failed: %v", err)
			}

			// Convert GValue → cowrie.Value
			cv := glyph.ToSJSON(gv)
			if cv == nil {
				t.Fatal("ToSJSON returned nil")
			}

			// Encode cowrie binary
			data, err := Encode(cv)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			// Decode cowrie binary
			decoded, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			// Convert back to GValue
			gv2 := glyph.FromSJSON(decoded)
			if gv2 == nil {
				t.Fatal("FromSJSON returned nil")
			}

			// Canonicalize both and compare
			opts := glyph.LLMLooseCanonOpts()
			canon1 := glyph.CanonicalizeLooseWithOpts(gv, opts)
			canon2 := glyph.CanonicalizeLooseWithOpts(gv2, opts)

			if canon1 != canon2 {
				t.Errorf("cross-format roundtrip mismatch:\n  original: %s\n  roundtripped: %s", canon1, canon2)
			}
		})
	}
}

func TestCrossFormat_CowrieBinaryToGlyphAndBack(t *testing.T) {
	// Start from cowrie values, go through glyph text, and back
	values := []struct {
		name string
		val  *Value
	}{
		{"null", Null()},
		{"bool_true", Bool(true)},
		{"int", Int64(42)},
		{"float", Float64(3.14)},
		{"string", String("hello")},
		{"empty_array", Array()},
		{"empty_object", Object()},
		{"nested_object", Object(
			Member{Key: "a", Value: Int64(1)},
			Member{Key: "b", Value: String("two")},
			Member{Key: "c", Value: Array(Int64(3), Bool(false))},
		)},
	}

	for _, tc := range values {
		t.Run(tc.name, func(t *testing.T) {
			// Encode to binary
			data, err := Encode(tc.val)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			// Decode from binary
			decoded, err := Decode(data)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			// Convert to GValue
			gv := glyph.FromSJSON(decoded)
			if gv == nil {
				t.Fatal("FromSJSON returned nil")
			}

			// Canonicalize to glyph text
			opts := glyph.LLMLooseCanonOpts()
			glyphText := glyph.CanonicalizeLooseWithOpts(gv, opts)

			// Parse glyph text back
			gv2, err := glyph.FromJSONLoose(glyphText)
			if err != nil {
				t.Fatalf("FromJSONLoose(%q) failed: %v", glyphText, err)
			}

			// Convert back to cowrie value
			cv2 := glyph.ToSJSON(gv2)

			// Re-encode
			data2, err := Encode(cv2)
			if err != nil {
				t.Fatalf("Re-encode failed: %v", err)
			}

			// Decode again and compare
			decoded2, err := Decode(data2)
			if err != nil {
				t.Fatalf("Re-decode failed: %v", err)
			}

			// Verify types match
			if decoded.Type() != decoded2.Type() {
				t.Errorf("type mismatch: %v vs %v", decoded.Type(), decoded2.Type())
			}
		})
	}
}
