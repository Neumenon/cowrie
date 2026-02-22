package glyph

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGoldenCrossLanguage ensures Go produces identical output to the golden fixtures.
// These same fixtures are tested by JavaScript and Python implementations.
func TestGoldenCrossLanguage(t *testing.T) {
	goldenDir := "testdata/golden"

	entries, err := os.ReadDir(goldenDir)
	if err != nil {
		t.Fatalf("failed to read golden dir: %v", err)
	}

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		name := strings.TrimSuffix(entry.Name(), ".json")
		t.Run(name, func(t *testing.T) {
			jsonPath := filepath.Join(goldenDir, name+".json")
			glyphPath := filepath.Join(goldenDir, name+".glyph")

			// Read input JSON
			jsonBytes, err := os.ReadFile(jsonPath)
			if err != nil {
				t.Fatalf("failed to read JSON: %v", err)
			}

			// Read expected GLYPH output
			expectedBytes, err := os.ReadFile(glyphPath)
			if err != nil {
				t.Fatalf("failed to read expected GLYPH: %v", err)
			}
			expected := strings.TrimSpace(string(expectedBytes))

			// Convert JSON to GValue
			gval, err := FromJSONLoose(jsonBytes)
			if err != nil {
				t.Fatalf("FromJSONLoose failed: %v", err)
			}

			// Convert to GLYPH canonical form
			got := CanonicalizeLoose(gval)

			if got != expected {
				t.Errorf("output mismatch\n  got:      %s\n  expected: %s", got, expected)
			}

			// Round-trip: parse GLYPH back to value
			parsed, _, err := ParseLoosePayload(got, nil)
			if err != nil {
				t.Fatalf("ParseLoosePayload failed: %v", err)
			}

			// Re-emit and verify determinism
			reemit := CanonicalizeLoose(parsed)

			if reemit != got {
				t.Errorf("non-deterministic output\n  first:  %s\n  second: %s", got, reemit)
			}
		})
	}
}

// TestGoldenRoundTrip verifies JSON → GLYPH → JSON round-trip preserves data.
func TestGoldenRoundTrip(t *testing.T) {
	goldenDir := "testdata/golden"

	entries, err := os.ReadDir(goldenDir)
	if err != nil {
		t.Fatalf("failed to read golden dir: %v", err)
	}

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		name := strings.TrimSuffix(entry.Name(), ".json")
		t.Run(name, func(t *testing.T) {
			jsonPath := filepath.Join(goldenDir, name+".json")

			jsonBytes, err := os.ReadFile(jsonPath)
			if err != nil {
				t.Fatalf("failed to read JSON: %v", err)
			}

			// JSON → GValue
			original, err := FromJSONLoose(jsonBytes)
			if err != nil {
				t.Fatalf("FromJSONLoose failed: %v", err)
			}

			// GValue → GLYPH string
			glyphStr := CanonicalizeLoose(original)

			// GLYPH → parsed GValue
			parsed, _, err := ParseLoosePayload(glyphStr, nil)
			if err != nil {
				t.Fatalf("ParseLoosePayload failed: %v", err)
			}

			// Re-emit and compare
			reemit := CanonicalizeLoose(parsed)

			if reemit != glyphStr {
				t.Errorf("round-trip mismatch\n  original: %s\n  parsed:   %s", glyphStr, reemit)
			}
		})
	}
}
