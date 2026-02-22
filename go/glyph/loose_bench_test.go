package glyph

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// ============================================================
// GLYPH-Loose Canonicalization Benchmarks
// ============================================================
//
// Run with:
//   go test -bench=BenchmarkCanonicalizeLoose -benchmem -count=5 ./cowrie/glyph/
//
// For memory profiling:
//   go test -bench=BenchmarkCanonicalizeLoose -benchmem -memprofile=mem.out ./cowrie/glyph/
//   go tool pprof -top mem.out
//
// For CPU profiling:
//   go test -bench=BenchmarkCanonicalizeLoose -cpuprofile=cpu.out ./cowrie/glyph/
//   go tool pprof -top cpu.out

// ============================================================
// Synthetic Benchmarks - Small Objects
// ============================================================

// BenchmarkCanonicalizeLoose_Null benchmarks null value canonicalization.
func BenchmarkCanonicalizeLoose_Null(b *testing.B) {
	v := Null()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Bool benchmarks boolean canonicalization.
func BenchmarkCanonicalizeLoose_Bool(b *testing.B) {
	v := Bool(true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Int benchmarks integer canonicalization.
func BenchmarkCanonicalizeLoose_Int(b *testing.B) {
	v := Int(1234567890)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Float benchmarks float canonicalization.
func BenchmarkCanonicalizeLoose_Float(b *testing.B) {
	v := Float(3.141592653589793)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_String_Bare benchmarks bare string canonicalization.
func BenchmarkCanonicalizeLoose_String_Bare(b *testing.B) {
	v := Str("hello")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_String_Quoted benchmarks quoted string canonicalization.
func BenchmarkCanonicalizeLoose_String_Quoted(b *testing.B) {
	v := Str("hello world with spaces and \"quotes\"")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Bytes_Small benchmarks small bytes canonicalization.
func BenchmarkCanonicalizeLoose_Bytes_Small(b *testing.B) {
	v := Bytes([]byte("hello world"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Bytes_Large benchmarks large bytes canonicalization.
func BenchmarkCanonicalizeLoose_Bytes_Large(b *testing.B) {
	data := make([]byte, 1024) // 1KB
	for i := range data {
		data[i] = byte(i % 256)
	}
	v := Bytes(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// ============================================================
// Synthetic Benchmarks - Simple Containers
// ============================================================

// BenchmarkCanonicalizeLoose_List_Empty benchmarks empty list canonicalization.
func BenchmarkCanonicalizeLoose_List_Empty(b *testing.B) {
	v := List()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_List_Small benchmarks small list canonicalization.
func BenchmarkCanonicalizeLoose_List_Small(b *testing.B) {
	v := List(Int(1), Int(2), Int(3), Int(4), Int(5))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_List_Medium benchmarks medium list canonicalization.
func BenchmarkCanonicalizeLoose_List_Medium(b *testing.B) {
	items := make([]*GValue, 100)
	for i := range items {
		items[i] = Int(int64(i))
	}
	v := List(items...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_List_Large benchmarks large list canonicalization.
func BenchmarkCanonicalizeLoose_List_Large(b *testing.B) {
	items := make([]*GValue, 1000)
	for i := range items {
		items[i] = Int(int64(i))
	}
	v := List(items...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Map_Empty benchmarks empty map canonicalization.
func BenchmarkCanonicalizeLoose_Map_Empty(b *testing.B) {
	v := Map()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Map_Small benchmarks small map canonicalization.
func BenchmarkCanonicalizeLoose_Map_Small(b *testing.B) {
	v := Map(
		MapEntry{Key: "a", Value: Int(1)},
		MapEntry{Key: "b", Value: Int(2)},
		MapEntry{Key: "c", Value: Int(3)},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Map_Medium benchmarks medium map canonicalization.
func BenchmarkCanonicalizeLoose_Map_Medium(b *testing.B) {
	entries := make([]MapEntry, 50)
	for i := range entries {
		entries[i] = MapEntry{Key: string(rune('a'+(i%26))) + string(rune('0'+(i/26))), Value: Int(int64(i))}
	}
	v := Map(entries...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Map_Large benchmarks large map canonicalization.
func BenchmarkCanonicalizeLoose_Map_Large(b *testing.B) {
	entries := make([]MapEntry, 200)
	for i := range entries {
		entries[i] = MapEntry{Key: string(rune('a'+(i%26))) + string(rune('0'+(i/26))) + string(rune('0'+(i%10))), Value: Int(int64(i))}
	}
	v := Map(entries...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// ============================================================
// Synthetic Benchmarks - Nested Structures
// ============================================================

// BenchmarkCanonicalizeLoose_Nested_Shallow benchmarks shallow nesting.
func BenchmarkCanonicalizeLoose_Nested_Shallow(b *testing.B) {
	v := Map(
		MapEntry{Key: "user", Value: Map(
			MapEntry{Key: "id", Value: Int(123)},
			MapEntry{Key: "name", Value: Str("Alice")},
		)},
		MapEntry{Key: "score", Value: Float(95.5)},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Nested_Deep benchmarks deep nesting (10 levels).
func BenchmarkCanonicalizeLoose_Nested_Deep(b *testing.B) {
	v := Int(42)
	for i := 0; i < 10; i++ {
		v = Map(MapEntry{Key: "level", Value: v})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Nested_VeryDeep benchmarks very deep nesting (50 levels).
func BenchmarkCanonicalizeLoose_Nested_VeryDeep(b *testing.B) {
	v := Int(42)
	for i := 0; i < 50; i++ {
		v = Map(MapEntry{Key: "level", Value: v})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Nested_Wide benchmarks wide structure with many keys.
func BenchmarkCanonicalizeLoose_Nested_Wide(b *testing.B) {
	inner := make([]MapEntry, 20)
	for i := range inner {
		inner[i] = MapEntry{Key: string(rune('a' + i)), Value: Int(int64(i))}
	}
	outer := make([]MapEntry, 10)
	for i := range outer {
		outer[i] = MapEntry{Key: string(rune('A' + i)), Value: Map(inner...)}
	}
	v := Map(outer...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// ============================================================
// Synthetic Benchmarks - Tabular Mode
// ============================================================

// BenchmarkCanonicalizeLoose_Tabular_3Rows benchmarks minimal tabular (3 rows).
func BenchmarkCanonicalizeLoose_Tabular_3Rows(b *testing.B) {
	items := make([]*GValue, 3)
	for i := range items {
		items[i] = Map(
			MapEntry{Key: "id", Value: Int(int64(i))},
			MapEntry{Key: "name", Value: Str("User")},
		)
	}
	v := List(items...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Tabular_100Rows benchmarks medium tabular (100 rows).
func BenchmarkCanonicalizeLoose_Tabular_100Rows(b *testing.B) {
	items := make([]*GValue, 100)
	for i := range items {
		items[i] = Map(
			MapEntry{Key: "id", Value: Int(int64(i))},
			MapEntry{Key: "name", Value: Str("User")},
			MapEntry{Key: "score", Value: Float(float64(i) * 0.1)},
		)
	}
	v := List(items...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Tabular_1000Rows benchmarks large tabular (1000 rows).
func BenchmarkCanonicalizeLoose_Tabular_1000Rows(b *testing.B) {
	items := make([]*GValue, 1000)
	for i := range items {
		items[i] = Map(
			MapEntry{Key: "id", Value: Int(int64(i))},
			MapEntry{Key: "name", Value: Str("User")},
			MapEntry{Key: "score", Value: Float(float64(i) * 0.1)},
			MapEntry{Key: "active", Value: Bool(i%2 == 0)},
		)
	}
	v := List(items...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Tabular_ManyColumns benchmarks tabular with many columns.
func BenchmarkCanonicalizeLoose_Tabular_ManyColumns(b *testing.B) {
	items := make([]*GValue, 50)
	for i := range items {
		entries := make([]MapEntry, 20)
		for j := range entries {
			entries[j] = MapEntry{Key: string(rune('a' + j)), Value: Int(int64(i*20 + j))}
		}
		items[i] = Map(entries...)
	}
	v := List(items...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_NoTabular_100Rows benchmarks with tabular disabled.
func BenchmarkCanonicalizeLoose_NoTabular_100Rows(b *testing.B) {
	items := make([]*GValue, 100)
	for i := range items {
		items[i] = Map(
			MapEntry{Key: "id", Value: Int(int64(i))},
			MapEntry{Key: "name", Value: Str("User")},
			MapEntry{Key: "score", Value: Float(float64(i) * 0.1)},
		)
	}
	v := List(items...)
	opts := NoTabularLooseCanonOpts()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLooseWithOpts(v, opts)
	}
}

// ============================================================
// Synthetic Benchmarks - Edge Cases
// ============================================================

// BenchmarkCanonicalizeLoose_MixedTypes benchmarks mixed type list.
func BenchmarkCanonicalizeLoose_MixedTypes(b *testing.B) {
	v := List(
		Null(),
		Bool(true),
		Bool(false),
		Int(42),
		Float(3.14),
		Str("hello"),
		Str("hello world"),
		List(Int(1), Int(2), Int(3)),
		Map(MapEntry{Key: "x", Value: Int(1)}),
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_UnicodeStrings benchmarks unicode string handling.
func BenchmarkCanonicalizeLoose_UnicodeStrings(b *testing.B) {
	v := Map(
		MapEntry{Key: "emoji", Value: Str("Hello 👋 World 🌍")},
		MapEntry{Key: "chinese", Value: Str("你好世界")},
		MapEntry{Key: "arabic", Value: Str("مرحبا بالعالم")},
		MapEntry{Key: "mixed", Value: Str("Héllo Wörld")},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_SpecialChars benchmarks special character escaping.
func BenchmarkCanonicalizeLoose_SpecialChars(b *testing.B) {
	v := Map(
		MapEntry{Key: "newlines", Value: Str("line1\nline2\nline3")},
		MapEntry{Key: "tabs", Value: Str("col1\tcol2\tcol3")},
		MapEntry{Key: "quotes", Value: Str(`say "hello"`)},
		MapEntry{Key: "backslash", Value: Str(`C:\Users\test`)},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// ============================================================
// Realistic Benchmarks - From JSON Corpus
// ============================================================

// loadJSONCorpusFile loads a JSON file from testdata and converts to GValue.
func loadJSONCorpusFile(b *testing.B, filename string) *GValue {
	b.Helper()
	path := filepath.Join("testdata", "loose_json", filename)
	data, err := os.ReadFile(path)
	if err != nil {
		b.Skipf("Skipping: %v", err)
		return nil
	}
	gv, err := FromJSONLoose(data)
	if err != nil {
		b.Fatalf("FromJSONLoose failed: %v", err)
	}
	return gv
}

// BenchmarkCanonicalizeLoose_Corpus_Small benchmarks small corpus files.
func BenchmarkCanonicalizeLoose_Corpus_Small(b *testing.B) {
	// Try to load a small test case
	v := loadJSONCorpusFile(b, "cases/001_empty_object.json")
	if v == nil {
		// Fallback to synthetic
		v = Map()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Corpus_AllCases benchmarks all corpus cases.
func BenchmarkCanonicalizeLoose_Corpus_AllCases(b *testing.B) {
	// Load manifest
	manifestPath := filepath.Join("testdata", "loose_json", "manifest.json")
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		b.Skipf("Skipping corpus benchmark: %v", err)
		return
	}

	var m struct {
		Cases []struct {
			Name string `json:"name"`
			File string `json:"file"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(manifestData, &m); err != nil {
		b.Fatalf("Failed to parse manifest: %v", err)
	}

	// Load all cases
	values := make([]*GValue, 0, len(m.Cases))
	for _, tc := range m.Cases {
		path := filepath.Join("testdata", "loose_json", tc.File)
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		gv, err := FromJSONLoose(data)
		if err != nil {
			continue
		}
		values = append(values, gv)
	}

	if len(values) == 0 {
		b.Skip("No corpus files loaded")
		return
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range values {
			_ = CanonicalizeLoose(v)
		}
	}
}

// ============================================================
// Realistic Benchmarks - Simulated Real-World Data
// ============================================================

// BenchmarkCanonicalizeLoose_APIResponse simulates a typical API response.
func BenchmarkCanonicalizeLoose_APIResponse(b *testing.B) {
	// Simulate a paginated API response with user data
	users := make([]*GValue, 25)
	for i := range users {
		users[i] = Map(
			MapEntry{Key: "id", Value: Int(int64(1000 + i))},
			MapEntry{Key: "username", Value: Str("user_" + string(rune('a'+i)))},
			MapEntry{Key: "email", Value: Str("user" + string(rune('a'+i)) + "@example.com")},
			MapEntry{Key: "created_at", Value: Str("2024-01-15T10:30:00Z")},
			MapEntry{Key: "active", Value: Bool(i%3 != 0)},
			MapEntry{Key: "score", Value: Float(float64(i) * 10.5)},
		)
	}
	v := Map(
		MapEntry{Key: "status", Value: Str("success")},
		MapEntry{Key: "page", Value: Int(1)},
		MapEntry{Key: "per_page", Value: Int(25)},
		MapEntry{Key: "total", Value: Int(1000)},
		MapEntry{Key: "data", Value: List(users...)},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_LLMToolCall simulates an LLM tool call response.
func BenchmarkCanonicalizeLoose_LLMToolCall(b *testing.B) {
	v := Map(
		MapEntry{Key: "action", Value: Str("search")},
		MapEntry{Key: "query", Value: Str("weather in New York City tomorrow")},
		MapEntry{Key: "confidence", Value: Float(0.95)},
		MapEntry{Key: "sources", Value: List(
			Str("web"),
			Str("news"),
			Str("weather_api"),
		)},
		MapEntry{Key: "metadata", Value: Map(
			MapEntry{Key: "model", Value: Str("gpt-4")},
			MapEntry{Key: "tokens", Value: Int(150)},
			MapEntry{Key: "latency_ms", Value: Float(234.5)},
		)},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_VectorDBResult simulates a vector search result.
func BenchmarkCanonicalizeLoose_VectorDBResult(b *testing.B) {
	docs := make([]*GValue, 10)
	for i := range docs {
		docs[i] = Map(
			MapEntry{Key: "id", Value: Str("doc_" + string(rune('a'+i)))},
			MapEntry{Key: "score", Value: Float(0.99 - float64(i)*0.05)},
			MapEntry{Key: "title", Value: Str("Document Title " + string(rune('A'+i)))},
			MapEntry{Key: "snippet", Value: Str("This is a snippet of the document content that would typically be longer...")},
			MapEntry{Key: "metadata", Value: Map(
				MapEntry{Key: "source", Value: Str("corpus_" + string(rune('a'+i)))},
				MapEntry{Key: "date", Value: Str("2024-01-15")},
			)},
		)
	}
	v := Map(
		MapEntry{Key: "query", Value: Str("semantic search query")},
		MapEntry{Key: "results", Value: List(docs...)},
		MapEntry{Key: "total_matches", Value: Int(1500)},
		MapEntry{Key: "search_time_ms", Value: Float(12.5)},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_AgentTrace simulates an agent execution trace.
func BenchmarkCanonicalizeLoose_AgentTrace(b *testing.B) {
	steps := make([]*GValue, 5)
	for i := range steps {
		steps[i] = Map(
			MapEntry{Key: "step", Value: Int(int64(i + 1))},
			MapEntry{Key: "action", Value: Str("tool_call")},
			MapEntry{Key: "tool", Value: Str("search")},
			MapEntry{Key: "input", Value: Map(
				MapEntry{Key: "query", Value: Str("step " + string(rune('0'+i)) + " query")},
			)},
			MapEntry{Key: "output", Value: Map(
				MapEntry{Key: "results", Value: List(Str("result1"), Str("result2"))},
				MapEntry{Key: "success", Value: Bool(true)},
			)},
			MapEntry{Key: "duration_ms", Value: Float(float64(100 + i*50))},
		)
	}
	v := Map(
		MapEntry{Key: "trace_id", Value: Str("trace_abc123")},
		MapEntry{Key: "agent", Value: Str("research_agent")},
		MapEntry{Key: "goal", Value: Str("Research and summarize topic X")},
		MapEntry{Key: "steps", Value: List(steps...)},
		MapEntry{Key: "final_output", Value: Str("The research found that...")},
		MapEntry{Key: "total_duration_ms", Value: Float(1250.5)},
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// ============================================================
// Comparison Benchmarks - Options Variants
// ============================================================

// BenchmarkCanonicalizeLoose_DefaultOpts benchmarks default options.
func BenchmarkCanonicalizeLoose_DefaultOpts(b *testing.B) {
	v := createMediumTestData()
	opts := DefaultLooseCanonOpts()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLooseWithOpts(v, opts)
	}
}

// BenchmarkCanonicalizeLoose_LLMOpts benchmarks LLM options.
func BenchmarkCanonicalizeLoose_LLMOpts(b *testing.B) {
	v := createMediumTestData()
	opts := LLMLooseCanonOpts()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLooseWithOpts(v, opts)
	}
}

// BenchmarkCanonicalizeLoose_PrettyOpts benchmarks pretty options.
func BenchmarkCanonicalizeLoose_PrettyOpts(b *testing.B) {
	v := createMediumTestData()
	opts := PrettyLooseCanonOpts()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLooseWithOpts(v, opts)
	}
}

// BenchmarkCanonicalizeLoose_SchemaOpts benchmarks schema context options.
func BenchmarkCanonicalizeLoose_SchemaOpts(b *testing.B) {
	v := createMediumTestData()
	keyDict := []string{"id", "name", "score", "active", "data"}
	opts := LooseCanonOpts{
		AutoTabular:    true,
		MinRows:        3,
		SchemaRef:      "test123",
		KeyDict:        keyDict,
		UseCompactKeys: true,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLooseWithSchema(v, opts)
	}
}

// createMediumTestData creates a medium-sized test structure.
func createMediumTestData() *GValue {
	items := make([]*GValue, 20)
	for i := range items {
		items[i] = Map(
			MapEntry{Key: "id", Value: Int(int64(i))},
			MapEntry{Key: "name", Value: Str("Item " + string(rune('A'+i)))},
			MapEntry{Key: "score", Value: Float(float64(i) * 1.5)},
			MapEntry{Key: "active", Value: Bool(i%2 == 0)},
		)
	}
	return Map(
		MapEntry{Key: "data", Value: List(items...)},
		MapEntry{Key: "count", Value: Int(20)},
	)
}

// ============================================================
// Allocation-Focused Benchmarks
// ============================================================

// BenchmarkCanonicalizeLoose_Allocs_Small measures allocations for small data.
func BenchmarkCanonicalizeLoose_Allocs_Small(b *testing.B) {
	v := Map(
		MapEntry{Key: "a", Value: Int(1)},
		MapEntry{Key: "b", Value: Str("hello")},
	)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Allocs_Medium measures allocations for medium data.
func BenchmarkCanonicalizeLoose_Allocs_Medium(b *testing.B) {
	v := createMediumTestData()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// BenchmarkCanonicalizeLoose_Allocs_Large measures allocations for large data.
func BenchmarkCanonicalizeLoose_Allocs_Large(b *testing.B) {
	items := make([]*GValue, 500)
	for i := range items {
		items[i] = Map(
			MapEntry{Key: "id", Value: Int(int64(i))},
			MapEntry{Key: "data", Value: Str("Some longer string content here")},
			MapEntry{Key: "nested", Value: Map(
				MapEntry{Key: "x", Value: Int(int64(i * 2))},
				MapEntry{Key: "y", Value: Float(float64(i) * 0.5)},
			)},
		)
	}
	v := List(items...)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CanonicalizeLoose(v)
	}
}

// ============================================================
// Helper Benchmarks - Individual Functions
// ============================================================

// BenchmarkBase64Encode_Small benchmarks the custom base64 encoder (small).
func BenchmarkBase64Encode_Small(b *testing.B) {
	data := []byte("hello world")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = base64Encode(data)
	}
}

// BenchmarkBase64Encode_Large benchmarks the custom base64 encoder (large).
func BenchmarkBase64Encode_Large(b *testing.B) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = base64Encode(data)
	}
}

// BenchmarkDetectTabular benchmarks tabular detection.
func BenchmarkDetectTabular(b *testing.B) {
	items := make([]*GValue, 100)
	for i := range items {
		items[i] = Map(
			MapEntry{Key: "id", Value: Int(int64(i))},
			MapEntry{Key: "name", Value: Str("User")},
			MapEntry{Key: "score", Value: Float(float64(i) * 0.1)},
		)
	}
	opts := DefaultLooseCanonOpts()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = detectTabular(items, opts)
	}
}

// BenchmarkSortMapEntries benchmarks map entry sorting.
func BenchmarkSortMapEntries(b *testing.B) {
	entries := make([]MapEntry, 50)
	for i := range entries {
		entries[i] = MapEntry{Key: string(rune('z' - (i % 26))), Value: Int(int64(i))}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Make a copy since sorting modifies
		copied := make([]MapEntry, len(entries))
		copy(copied, entries)
		_ = sortMapEntries(copied)
	}
}

// BenchmarkEscapeTabularCell benchmarks cell escaping.
func BenchmarkEscapeTabularCell(b *testing.B) {
	s := "value with | pipes | in | it"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = escapeTabularCell(s)
	}
}

// BenchmarkEscapeTabularCell_NoPipes benchmarks cell escaping (no pipes).
func BenchmarkEscapeTabularCell_NoPipes(b *testing.B) {
	s := "value without any pipes at all"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = escapeTabularCell(s)
	}
}
