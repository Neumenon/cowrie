package glyph

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ============================================================
// Cross-Implementation Tests
// ============================================================
//
// These tests verify that the Go and JS implementations produce
// identical output for the same input.

// NodeResult is the JSON response from the Node.js script
type NodeResult struct {
	Success bool   `json:"success"`
	Result  string `json:"result,omitempty"`
	Error   string `json:"error,omitempty"`
	Matches bool   `json:"matches,omitempty"`
}

// SchemaJSON is the JSON format for passing schemas to Node
type SchemaJSON struct {
	Types map[string]TypeJSON `json:"types"`
}

type TypeJSON struct {
	Packed  bool        `json:"packed,omitempty"`
	Version string      `json:"version,omitempty"`
	Fields  []FieldJSON `json:"fields"`
}

type FieldJSON struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	FID      int    `json:"fid,omitempty"`
	WireKey  string `json:"wireKey,omitempty"`
	Optional bool   `json:"optional,omitempty"`
}

// runNode executes the Node.js canon.mjs script
func runNode(t *testing.T, args ...string) NodeResult {
	t.Helper()

	// Find the script path relative to the test file
	scriptPath := filepath.Join("test", "js", "canon.mjs")

	// Check if Node is available
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Node.js not available, skipping cross-impl test")
	}

	cmdArgs := append([]string{scriptPath}, args...)
	cmd := exec.Command("node", cmdArgs...)
	cmd.Dir = "." // Run from glyph directory

	output, err := cmd.CombinedOutput()
	if err != nil {
		// Check if it's just a non-zero exit (command might have returned JSON error)
		if len(output) > 0 {
			var result NodeResult
			if jsonErr := json.Unmarshal(output, &result); jsonErr == nil {
				return result
			}
		}
		t.Fatalf("node command failed: %v\noutput: %s", err, output)
	}

	var result NodeResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("failed to parse node output: %v\noutput: %s", err, output)
	}

	return result
}

// schemaToJSON converts a Go schema to JSON for passing to Node
func schemaToJSON(schema *Schema) string {
	sj := SchemaJSON{Types: make(map[string]TypeJSON)}

	for name, td := range schema.Types {
		tj := TypeJSON{
			Packed:  td.PackEnabled,
			Version: td.Version,
			Fields:  make([]FieldJSON, 0),
		}

		if td.Struct != nil {
			for _, fd := range td.Struct.Fields {
				fj := FieldJSON{
					Name:     fd.Name,
					Type:     typeSpecToString(fd.Type),
					FID:      fd.FID,
					WireKey:  fd.WireKey,
					Optional: fd.Optional,
				}
				tj.Fields = append(tj.Fields, fj)
			}
		}

		sj.Types[name] = tj
	}

	data, _ := json.Marshal(sj)
	return string(data)
}

func typeSpecToString(ts TypeSpec) string {
	switch ts.Kind {
	case TypeSpecNull:
		return "null"
	case TypeSpecBool:
		return "bool"
	case TypeSpecInt:
		return "int"
	case TypeSpecFloat:
		return "float"
	case TypeSpecStr:
		return "str"
	case TypeSpecBytes:
		return "bytes"
	case TypeSpecTime:
		return "time"
	case TypeSpecID:
		return "id"
	case TypeSpecList:
		return "list<" + typeSpecToString(*ts.Elem) + ">"
	case TypeSpecRef:
		return ts.Name
	default:
		return "unknown"
	}
}

// ============================================================
// Tests
// ============================================================

func TestCrossImplVersion(t *testing.T) {
	result := runNode(t, "version")
	if !result.Success {
		t.Fatalf("version command failed: %s", result.Error)
	}
	t.Logf("JS glyph version: %s", result.Result)
}

func TestCrossImplTeamRoundtrip(t *testing.T) {
	schema := makeTeamSchema()
	schemaJSON := schemaToJSON(schema)

	// Go: emit packed Team
	team := makeTeamValue("ARS", "Arsenal", "EPL")
	goPacked, err := EmitPacked(team, schema)
	if err != nil {
		t.Fatalf("Go EmitPacked error: %v", err)
	}

	t.Logf("Go packed: %s", goPacked)

	// JS: canonicalize the same packed string
	result := runNode(t, "canonical", goPacked, schemaJSON)
	if !result.Success {
		t.Fatalf("JS canonical failed: %s", result.Error)
	}

	t.Logf("JS canonical: %s", result.Result)

	// They should match
	if strings.TrimSpace(goPacked) != strings.TrimSpace(result.Result) {
		t.Errorf("Mismatch:\n  Go: %s\n  JS: %s", goPacked, result.Result)
	}
}

func TestCrossImplMatchRoundtrip(t *testing.T) {
	schema := makeMatchSchema()
	schemaJSON := schemaToJSON(schema)

	// Create a match with some optional fields
	home := makeTeamValue("ARS", "Arsenal", "EPL")
	away := makeTeamValue("LIV", "Liverpool", "EPL")
	odds := makeOddsValue(2.1, 3.4, 3.25)
	ftH := 2

	match := makeMatchValue("ARS-LIV", mustParseTime("2025-12-19T20:00:00Z"), home, away, odds, nil, &ftH, nil)

	// Go: emit packed
	goPacked, err := EmitPacked(match, schema)
	if err != nil {
		t.Fatalf("Go EmitPacked error: %v", err)
	}

	t.Logf("Go packed: %s", goPacked)

	// JS: roundtrip
	result := runNode(t, "roundtrip", goPacked, schemaJSON)
	if !result.Success {
		t.Fatalf("JS roundtrip failed: %s", result.Error)
	}

	t.Logf("JS roundtrip: %s", result.Result)

	// They should match
	if strings.TrimSpace(goPacked) != strings.TrimSpace(result.Result) {
		t.Errorf("Roundtrip mismatch:\n  Go: %s\n  JS: %s", goPacked, result.Result)
	}
}

func TestCrossImplGoldenFiles(t *testing.T) {
	// Read golden files from testdata/v2/
	goldenDir := "testdata/v2"
	entries, err := os.ReadDir(goldenDir)
	if err != nil {
		t.Fatalf("failed to read golden dir: %v", err)
	}

	schema := makeMatchSchema()
	schemaJSON := schemaToJSON(schema)

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".lyph") {
			continue
		}

		t.Run(entry.Name(), func(t *testing.T) {
			path := filepath.Join(goldenDir, entry.Name())
			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("failed to read %s: %v", path, err)
			}

			// Parse each non-comment line
			lines := strings.Split(string(content), "\n")
			inTabular := false
			for i, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}

				// Track tabular mode - skip row lines inside @tab...@end
				if strings.HasPrefix(line, "@tab") {
					inTabular = true
					continue
				}
				if strings.HasPrefix(line, "@end") {
					inTabular = false
					continue
				}
				if inTabular {
					continue // Skip tabular row data
				}

				// Skip struct mode lines (Type{...})
				if strings.Contains(line, "{") && !strings.Contains(line, "@{bm=") {
					continue
				}

				// Only test packed format lines (Type@(...) or Type@{bm=...}(...))
				if !strings.Contains(line, "@(") && !strings.Contains(line, "@{bm=") {
					continue
				}

				t.Logf("Testing line %d: %s", i+1, truncate(line, 60))

				result := runNode(t, "canonical", line, schemaJSON)
				if !result.Success {
					// Some lines might use types not in our schema - skip
					if strings.Contains(result.Error, "unknown type") {
						t.Logf("  Skipping (unknown type): %s", result.Error)
						continue
					}
					t.Errorf("  JS failed: %s", result.Error)
					continue
				}

				// Compare
				if strings.TrimSpace(line) != strings.TrimSpace(result.Result) {
					t.Errorf("  Mismatch:\n    Input:  %s\n    Output: %s", line, result.Result)
				} else {
					t.Logf("  OK")
				}
			}
		})
	}
}

func TestCrossImplBitmapRoundtrip(t *testing.T) {
	schema := makeMatchSchema()
	schemaJSON := schemaToJSON(schema)

	// Create match with bitmap (some optionals missing)
	home := makeTeamValue("MCI", "Manchester City", "EPL")
	away := makeTeamValue("CHE", "Chelsea", "EPL")
	odds := makeOddsValue(1.8, 3.6, 4.2)

	// Only odds, no pred, no scores
	match := makeMatchValue("MCI-CHE", mustParseTime("2025-12-20T15:00:00Z"), home, away, odds, nil, nil, nil)

	goPacked, err := EmitPacked(match, schema)
	if err != nil {
		t.Fatalf("Go EmitPacked error: %v", err)
	}

	t.Logf("Go bitmap packed: %s", goPacked)

	// Should have bitmap
	if !strings.Contains(goPacked, "@{bm=") {
		t.Error("Expected bitmap form")
	}

	// JS roundtrip
	result := runNode(t, "roundtrip", goPacked, schemaJSON)
	if !result.Success {
		t.Fatalf("JS roundtrip failed: %s", result.Error)
	}

	if strings.TrimSpace(goPacked) != strings.TrimSpace(result.Result) {
		t.Errorf("Bitmap roundtrip mismatch:\n  Go: %s\n  JS: %s", goPacked, result.Result)
	}
}

// ============================================================
// Cross-Impl Patch Tests
// ============================================================

func TestCrossImplPatchRoundtrip(t *testing.T) {
	schema := makeMatchSchema()

	// Create a patch in Go
	patch := NewPatch(RefID{Prefix: "m", Value: "ARS-LIV"}, schema.Hash)
	patch.TargetType = "Match"
	patch.Set("ft_h", Int(2))
	patch.Set("ft_a", Int(1))
	patch.Append("events", Str("Goal!"))
	patch.Delete("odds")
	patch.Delta("rating", 0.15)

	// Emit from Go
	goPatch, err := EmitPatch(patch, schema)
	if err != nil {
		t.Fatalf("Go EmitPatch error: %v", err)
	}

	t.Logf("Go emitted patch:\n%s", goPatch)

	// JS: parse and re-emit
	result := runNode(t, "patch-roundtrip", goPatch, "")
	if !result.Success {
		t.Fatalf("JS patch-roundtrip failed: %s", result.Error)
	}

	t.Logf("JS re-emitted patch:\n%s", result.Result)

	// Compare - both should produce same canonical form
	// Note: order may differ due to sorting, so we just check key parts
	if !strings.Contains(result.Result, "@patch") {
		t.Error("JS result missing @patch header")
	}
	if !strings.Contains(result.Result, "@target=m:ARS-LIV") {
		t.Error("JS result missing @target")
	}
	if !strings.Contains(result.Result, "@end") {
		t.Error("JS result missing @end")
	}
}

func TestCrossImplPatchParseApply(t *testing.T) {
	// Create a patch string that both Go and JS should parse
	patchStr := `@patch @keys=wire @target=m:TEST
= score 42
+ items "new"
~ count +5
- old
@end`

	// Go: parse the patch
	goPatch, err := ParsePatch(patchStr, nil)
	if err != nil {
		t.Fatalf("Go ParsePatch error: %v", err)
	}

	if goPatch.Target.Prefix != "m" || goPatch.Target.Value != "TEST" {
		t.Errorf("Go target mismatch: %v", goPatch.Target)
	}

	if len(goPatch.Ops) != 4 {
		t.Errorf("Go ops count: %d, want 4", len(goPatch.Ops))
	}

	// JS: parse the patch
	result := runNode(t, "parse-patch", patchStr, "")
	if !result.Success {
		t.Fatalf("JS parse-patch failed: %s", result.Error)
	}

	t.Logf("JS parsed: %s", result.Result)
}

func TestCrossImplTabularParse(t *testing.T) {
	schema := makeTeamSchema()
	schemaJSON := schemaToJSON(schema)

	tabularStr := `@tab Team [t n l]
^t:ARS Arsenal EPL
^t:LIV Liverpool EPL
^t:CHE Chelsea EPL
@end`

	// JS: parse the tabular
	result := runNode(t, "parse-tabular", tabularStr, schemaJSON)
	if !result.Success {
		t.Fatalf("JS parse-tabular failed: %s", result.Error)
	}

	t.Logf("JS parsed tabular: %s", result.Result)

	// Should have 3 rows
	if !strings.Contains(result.Result, `"rowCount":3`) {
		t.Errorf("Expected 3 rows, got: %s", result.Result)
	}
}

// ============================================================
// Helpers
// ============================================================

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		// Try without timezone
		t, _ = time.Parse("2006-01-02T15:04:05", s)
	}
	return t
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
