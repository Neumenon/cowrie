// bench - GLYPH benchmark runner
//
// Compares GLYPH-Loose canonical encoding vs JSON-minified:
//   - Bytes on wire
//   - Approximate token counts (using byte-based heuristics)
//
// Output: CSV and markdown summary
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/Neumenon/cowrie/go/glyph"
)

type CaseResult struct {
	Name        string
	JSONBytes   int
	GLYPHBytes  int
	BytesSaved  int
	BytesPct    float64
	JSONTokens  int
	GLYPHTokens int
	TokensSaved int
	TokensPct   float64
}

type Manifest struct {
	Version     string `json:"version"`
	Description string `json:"description"`
	Cases       []struct {
		Name string `json:"name"`
		File string `json:"file"`
	} `json:"cases"`
}

func main() {
	// Find testdata directory
	testdataDir := findTestdata()
	if testdataDir == "" {
		fmt.Fprintln(os.Stderr, "Cannot find testdata/loose_json directory")
		os.Exit(1)
	}

	// Load manifest
	manifestPath := filepath.Join(testdataDir, "manifest.json")
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot read manifest: %v\n", err)
		os.Exit(1)
	}

	var manifest Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		fmt.Fprintf(os.Stderr, "Cannot parse manifest: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "GLYPH Benchmark Runner\n")
	fmt.Fprintf(os.Stderr, "======================\n")
	fmt.Fprintf(os.Stderr, "Corpus: %s (%d cases)\n\n", manifest.Version, len(manifest.Cases))

	var results []CaseResult
	var totalJSONBytes, totalGLYPHBytes int
	var totalJSONTokens, totalGLYPHTokens int

	for _, c := range manifest.Cases {
		casePath := filepath.Join(testdataDir, c.File)
		jsonData, err := os.ReadFile(casePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Skip %s: %v\n", c.Name, err)
			continue
		}

		// Parse and convert to GLYPH
		gv, err := glyph.FromJSONLoose(jsonData)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Skip %s: parse error: %v\n", c.Name, err)
			continue
		}

		glyphStr := glyph.CanonicalizeLoose(gv)

		// Minify JSON for fair comparison
		var minified interface{}
		if err := json.Unmarshal(jsonData, &minified); err != nil {
			fmt.Fprintf(os.Stderr, "Skip %s: JSON unmarshal error: %v\n", c.Name, err)
			continue
		}
		jsonMin, _ := json.Marshal(minified)

		// Calculate metrics
		jsonBytes := len(jsonMin)
		glyphBytes := len(glyphStr)
		bytesSaved := jsonBytes - glyphBytes
		bytesPct := 0.0
		if jsonBytes > 0 {
			bytesPct = float64(bytesSaved) / float64(jsonBytes) * 100.0
		}

		// Token estimation (using cl100k_base-like heuristics)
		jsonTokens := estimateTokens(string(jsonMin))
		glyphTokens := estimateTokens(glyphStr)
		tokensSaved := jsonTokens - glyphTokens
		tokensPct := 0.0
		if jsonTokens > 0 {
			tokensPct = float64(tokensSaved) / float64(jsonTokens) * 100.0
		}

		results = append(results, CaseResult{
			Name:        c.Name,
			JSONBytes:   jsonBytes,
			GLYPHBytes:  glyphBytes,
			BytesSaved:  bytesSaved,
			BytesPct:    bytesPct,
			JSONTokens:  jsonTokens,
			GLYPHTokens: glyphTokens,
			TokensSaved: tokensSaved,
			TokensPct:   tokensPct,
		})

		totalJSONBytes += jsonBytes
		totalGLYPHBytes += glyphBytes
		totalJSONTokens += jsonTokens
		totalGLYPHTokens += glyphTokens
	}

	// Output CSV
	csvPath := "bench_results.csv"
	csvFile, err := os.Create(csvPath)
	if err == nil {
		writeCSV(csvFile, results)
		csvFile.Close()
		fmt.Fprintf(os.Stderr, "CSV written to: %s\n", csvPath)
	}

	// Output Markdown
	mdPath := "BENCH_2025-12-20.md"
	mdFile, err := os.Create(mdPath)
	if err == nil {
		writeMarkdown(mdFile, results, totalJSONBytes, totalGLYPHBytes, totalJSONTokens, totalGLYPHTokens, manifest.Version)
		mdFile.Close()
		fmt.Fprintf(os.Stderr, "Markdown written to: %s\n", mdPath)
	}

	// Summary to stdout
	fmt.Printf("\n=== SUMMARY ===\n")
	fmt.Printf("Cases:        %d\n", len(results))
	fmt.Printf("JSON total:   %d bytes, ~%d tokens\n", totalJSONBytes, totalJSONTokens)
	fmt.Printf("GLYPH total:  %d bytes, ~%d tokens\n", totalGLYPHBytes, totalGLYPHTokens)
	fmt.Printf("Bytes saved:  %d (%.1f%%)\n", totalJSONBytes-totalGLYPHBytes, float64(totalJSONBytes-totalGLYPHBytes)/float64(totalJSONBytes)*100)
	fmt.Printf("Tokens saved: %d (%.1f%%)\n", totalJSONTokens-totalGLYPHTokens, float64(totalJSONTokens-totalGLYPHTokens)/float64(totalJSONTokens)*100)
}

// estimateTokens provides a rough token count approximation
// Based on cl100k_base behavior: ~4 chars per token for ASCII,
// punctuation and special chars often get their own tokens
func estimateTokens(s string) int {
	if len(s) == 0 {
		return 0
	}

	tokens := 0
	i := 0
	for i < len(s) {
		c := s[i]

		// Punctuation/structural chars often get their own token
		if isPunctuation(c) {
			tokens++
			i++
			continue
		}

		// Whitespace
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue // whitespace often merged with adjacent tokens
		}

		// Numbers: usually tokenized as chunks
		if c >= '0' && c <= '9' {
			numLen := 0
			for i < len(s) && ((s[i] >= '0' && s[i] <= '9') || s[i] == '.' || s[i] == '-' || s[i] == '+' || s[i] == 'e' || s[i] == 'E') {
				numLen++
				i++
			}
			// Numbers roughly 1 token per 3-4 digits
			tokens += (numLen + 3) / 4
			continue
		}

		// ASCII alpha: roughly 4 chars per token
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
			wordLen := 0
			for i < len(s) && (isAlphaNum(s[i]) || s[i] == '_') {
				wordLen++
				i++
			}
			tokens += (wordLen + 3) / 4
			continue
		}

		// Other: count as single token
		tokens++
		i++
	}

	return max(1, tokens)
}

func isPunctuation(c byte) bool {
	return c == '{' || c == '}' || c == '[' || c == ']' ||
		c == '(' || c == ')' || c == ':' || c == ',' ||
		c == '"' || c == '\'' || c == '=' || c == '@' ||
		c == '.' || c == ';' || c == '!' || c == '?'
}

func isAlphaNum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func findTestdata() string {
	// Try relative paths from likely locations
	paths := []string{
		"testdata/loose_json",
		"../testdata/loose_json",
		"../../testdata/loose_json",
		"sjson/glyph/testdata/loose_json",
	}

	for _, p := range paths {
		if _, err := os.Stat(filepath.Join(p, "manifest.json")); err == nil {
			return p
		}
	}

	return ""
}

func writeCSV(w io.Writer, results []CaseResult) {
	fmt.Fprintln(w, "name,json_bytes,glyph_bytes,bytes_saved,bytes_pct,json_tokens,glyph_tokens,tokens_saved,tokens_pct")
	for _, r := range results {
		fmt.Fprintf(w, "%s,%d,%d,%d,%.1f,%d,%d,%d,%.1f\n",
			r.Name, r.JSONBytes, r.GLYPHBytes, r.BytesSaved, r.BytesPct,
			r.JSONTokens, r.GLYPHTokens, r.TokensSaved, r.TokensPct)
	}
}

func writeMarkdown(w io.Writer, results []CaseResult, totalJSON, totalGLYPH, totalJSONTok, totalGLYPHTok int, version string) {
	fmt.Fprintf(w, "# GLYPH Benchmark Results\n\n")
	fmt.Fprintf(w, "**Date:** 2025-12-20  \n")
	fmt.Fprintf(w, "**Corpus:** %s (%d cases)  \n", version, len(results))
	fmt.Fprintf(w, "**GLYPH Version:** 0.2.3 (spec 2.2.2-gs1)  \n\n")

	fmt.Fprintf(w, "## Summary\n\n")
	fmt.Fprintf(w, "| Metric | JSON (minified) | GLYPH-Loose | Savings |\n")
	fmt.Fprintf(w, "|--------|-----------------|-------------|--------|\n")
	bytesSaved := totalJSON - totalGLYPH
	bytesPct := float64(bytesSaved) / float64(totalJSON) * 100
	tokensSaved := totalJSONTok - totalGLYPHTok
	tokensPct := float64(tokensSaved) / float64(totalJSONTok) * 100
	fmt.Fprintf(w, "| **Bytes** | %d | %d | %d (%.1f%%) |\n", totalJSON, totalGLYPH, bytesSaved, bytesPct)
	fmt.Fprintf(w, "| **Tokens** (est.) | ~%d | ~%d | ~%d (%.1f%%) |\n\n", totalJSONTok, totalGLYPHTok, tokensSaved, tokensPct)

	fmt.Fprintf(w, "## Analysis\n\n")

	// Find best/worst cases
	sorted := make([]CaseResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].BytesPct > sorted[j].BytesPct
	})

	fmt.Fprintf(w, "### Top 5 Space Savings (by bytes)\n\n")
	fmt.Fprintf(w, "| Case | JSON | GLYPH | Saved |\n")
	fmt.Fprintf(w, "|------|------|-------|-------|\n")
	for i := 0; i < min(5, len(sorted)); i++ {
		r := sorted[i]
		fmt.Fprintf(w, "| %s | %d | %d | %.1f%% |\n", r.Name, r.JSONBytes, r.GLYPHBytes, r.BytesPct)
	}

	fmt.Fprintf(w, "\n### Cases Where JSON is Smaller\n\n")
	var worse []CaseResult
	for _, r := range results {
		if r.BytesSaved < 0 {
			worse = append(worse, r)
		}
	}
	if len(worse) == 0 {
		fmt.Fprintf(w, "_None - GLYPH is smaller or equal in all cases._\n\n")
	} else {
		fmt.Fprintf(w, "| Case | JSON | GLYPH | Overhead |\n")
		fmt.Fprintf(w, "|------|------|-------|----------|\n")
		for _, r := range worse {
			fmt.Fprintf(w, "| %s | %d | %d | +%d bytes |\n", r.Name, r.JSONBytes, r.GLYPHBytes, -r.BytesSaved)
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintf(w, "## Methodology\n\n")
	fmt.Fprintf(w, "- **JSON:** Minified (no whitespace), using Go's `json.Marshal`\n")
	fmt.Fprintf(w, "- **GLYPH:** Canonical GLYPH-Loose format via `glyph.CanonicalizeLoose`\n")
	fmt.Fprintf(w, "- **Tokens:** Estimated using cl100k_base-like heuristics (~4 chars/token for words, punctuation as separate tokens)\n\n")

	fmt.Fprintf(w, "## Detailed Results\n\n")
	fmt.Fprintf(w, "| Case | JSON Bytes | GLYPH Bytes | Bytes %% | JSON Tok | GLYPH Tok | Tok %% |\n")
	fmt.Fprintf(w, "|------|------------|-------------|---------|----------|-----------|-------|\n")
	for _, r := range results {
		sign := ""
		if r.BytesPct > 0 {
			sign = "+"
		}
		tokSign := ""
		if r.TokensPct > 0 {
			tokSign = "+"
		}
		fmt.Fprintf(w, "| %s | %d | %d | %s%.1f%% | %d | %d | %s%.1f%% |\n",
			truncateName(r.Name, 25), r.JSONBytes, r.GLYPHBytes, sign, r.BytesPct,
			r.JSONTokens, r.GLYPHTokens, tokSign, r.TokensPct)
	}
}

func truncateName(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
