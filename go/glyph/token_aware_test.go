package glyph

import (
	"strings"
	"testing"
)

func TestKeyDict_Add(t *testing.T) {
	d := NewKeyDict("test")

	// Add first pair
	if !d.Add("content", "c") {
		t.Error("expected Add to succeed for new pair")
	}

	// Try adding duplicate long key
	if d.Add("content", "x") {
		t.Error("expected Add to fail for duplicate long key")
	}

	// Try adding duplicate abbr key
	if d.Add("context", "c") {
		t.Error("expected Add to fail for duplicate abbr key")
	}

	// Add different pair
	if !d.Add("role", "r") {
		t.Error("expected Add to succeed for new pair")
	}

	if d.Len() != 2 {
		t.Errorf("expected len 2, got %d", d.Len())
	}
}

func TestKeyDict_Abbreviate(t *testing.T) {
	d := NewKeyDict("test")
	d.Add("content", "c")
	d.Add("role", "r")
	d.Add("messages", "m")

	tests := []struct {
		input    string
		expected string
	}{
		{"content", "c"},
		{"role", "r"},
		{"messages", "m"},
		{"unknown", "unknown"}, // Not in dict, returns as-is
	}

	for _, tc := range tests {
		result := d.Abbreviate(tc.input)
		if result != tc.expected {
			t.Errorf("Abbreviate(%q) = %q, expected %q", tc.input, result, tc.expected)
		}
	}
}

func TestKeyDict_Expand(t *testing.T) {
	d := NewKeyDict("test")
	d.Add("content", "c")
	d.Add("role", "r")

	tests := []struct {
		input    string
		expected string
	}{
		{"c", "content"},
		{"r", "role"},
		{"x", "x"}, // Not in dict, returns as-is
	}

	for _, tc := range tests {
		result := d.Expand(tc.input)
		if result != tc.expected {
			t.Errorf("Expand(%q) = %q, expected %q", tc.input, result, tc.expected)
		}
	}
}

func TestKeyDict_HasAbbreviation(t *testing.T) {
	d := NewKeyDict("test")
	d.Add("content", "c")

	if !d.HasAbbreviation("content") {
		t.Error("expected HasAbbreviation(content) = true")
	}
	if d.HasAbbreviation("unknown") {
		t.Error("expected HasAbbreviation(unknown) = false")
	}
}

func TestKeyDict_IsAbbreviation(t *testing.T) {
	d := NewKeyDict("test")
	d.Add("content", "c")

	if !d.IsAbbreviation("c") {
		t.Error("expected IsAbbreviation(c) = true")
	}
	if d.IsAbbreviation("x") {
		t.Error("expected IsAbbreviation(x) = false")
	}
}

func TestKeyDict_Merge(t *testing.T) {
	d1 := NewKeyDict("d1")
	d1.Add("content", "c")
	d1.Add("role", "r")

	d2 := NewKeyDict("d2")
	d2.Add("messages", "m")
	d2.Add("content", "ct") // Conflicts with d1

	added := d1.Merge(d2)

	// Only "messages" should be added (no conflict)
	if added != 1 {
		t.Errorf("expected 1 added, got %d", added)
	}

	if d1.Len() != 3 {
		t.Errorf("expected len 3, got %d", d1.Len())
	}

	// Verify "content" still maps to "c", not "ct"
	if d1.Abbreviate("content") != "c" {
		t.Error("content should still map to c")
	}
}

func TestLLMDict_Contents(t *testing.T) {
	// Verify LLMDict has expected entries
	expectedPairs := map[string]string{
		"content":     "c",
		"role":        "r",
		"messages":    "m",
		"assistant":   "a",
		"system":      "s",
		"user":        "u",
		"model":       "md",
		"temperature": "tp",
	}

	for long, abbr := range expectedPairs {
		if LLMDict.Abbreviate(long) != abbr {
			t.Errorf("LLMDict.Abbreviate(%q) expected %q", long, abbr)
		}
		if LLMDict.Expand(abbr) != long {
			t.Errorf("LLMDict.Expand(%q) expected %q", abbr, long)
		}
	}
}

func TestToolDict_Contents(t *testing.T) {
	expectedPairs := map[string]string{
		"type":        "ty",
		"properties":  "p",
		"required":    "rq",
		"description": "d",
	}

	for long, abbr := range expectedPairs {
		if ToolDict.Abbreviate(long) != abbr {
			t.Errorf("ToolDict.Abbreviate(%q) expected %q", long, abbr)
		}
	}
}

func TestMLDict_Contents(t *testing.T) {
	expectedPairs := map[string]string{
		"shape":   "sh",
		"dtype":   "dt",
		"weights": "w",
		"float32": "f32",
	}

	for long, abbr := range expectedPairs {
		if MLDict.Abbreviate(long) != abbr {
			t.Errorf("MLDict.Abbreviate(%q) expected %q", long, abbr)
		}
	}
}

func TestEmitTokenAware_Simple(t *testing.T) {
	v := Map(
		FieldVal("content", Str("hello")),
		FieldVal("role", Str("user")),
	)

	result := EmitTokenAware(v)

	// Should use abbreviated keys
	if !strings.Contains(result, "c:") {
		t.Errorf("expected abbreviated key 'c:', got %s", result)
	}
	if !strings.Contains(result, "r:") {
		t.Errorf("expected abbreviated key 'r:', got %s", result)
	}
}

func TestEmitTokenAware_Struct(t *testing.T) {
	v := Struct("Message",
		FieldVal("content", Str("hello")),
		FieldVal("role", Str("assistant")),
	)

	result := EmitTokenAware(v)

	// Should abbreviate field keys
	if !strings.Contains(result, "c=") {
		t.Errorf("expected abbreviated key 'c=', got %s", result)
	}
}

func TestEmitTokenAware_Nested(t *testing.T) {
	v := Map(
		FieldVal("messages", List(
			Map(FieldVal("role", Str("user")), FieldVal("content", Str("hi"))),
			Map(FieldVal("role", Str("assistant")), FieldVal("content", Str("hello"))),
		)),
		FieldVal("model", Str("gpt-4")),
	)

	result := EmitTokenAware(v)

	// Should abbreviate nested keys too
	if !strings.Contains(result, "m:") {
		t.Errorf("expected abbreviated 'messages' -> 'm:', got %s", result)
	}
	if !strings.Contains(result, "r:") {
		t.Errorf("expected abbreviated 'role' -> 'r:', got %s", result)
	}
}

func TestEmitTokenAware_CompactNumbers(t *testing.T) {
	v := Map(
		FieldVal("temperature", Float(0.7)),
		FieldVal("max_tokens", Int(100)),
		FieldVal("count", Int(5)),
	)

	opts := TokenAwareOptions{
		UseAbbreviations: true,
		CompactNumbers:   true,
	}

	result := EmitTokenAwareWithOptions(v, opts)

	// Small integers should be compact
	if strings.Contains(result, " 5") && !strings.Contains(result, ":5") {
		// OK - number is compact
	}

	t.Logf("Compact result: %s", result)
}

func TestEmitTokenAware_OmitDefaults(t *testing.T) {
	v := Map(
		FieldVal("name", Str("test")),
		FieldVal("enabled", Bool(false)), // default
		FieldVal("count", Int(0)),        // default
		FieldVal("value", Str("")),       // default
	)

	opts := TokenAwareOptions{
		UseAbbreviations: false,
		OmitDefaults:     true,
	}

	result := EmitTokenAwareWithOptions(v, opts)

	// Should only contain "name"
	if strings.Contains(result, "enabled") {
		t.Errorf("expected 'enabled' to be omitted, got %s", result)
	}
	if strings.Contains(result, "count") {
		t.Errorf("expected 'count' to be omitted, got %s", result)
	}
}

func TestEmitTokenAware_NoAbbreviations(t *testing.T) {
	v := Map(
		FieldVal("content", Str("hello")),
		FieldVal("role", Str("user")),
	)

	opts := TokenAwareOptions{
		UseAbbreviations: false,
		CompactNumbers:   false,
	}

	result := EmitTokenAwareWithOptions(v, opts)

	// Should use full keys
	if !strings.Contains(result, "content:") {
		t.Errorf("expected full key 'content:', got %s", result)
	}
	if !strings.Contains(result, "role:") {
		t.Errorf("expected full key 'role:', got %s", result)
	}
}

func TestEmitTokenAware_CustomDict(t *testing.T) {
	custom := NewKeyDict("custom")
	custom.Add("myfield", "mf")
	custom.Add("myvalue", "mv")

	v := Map(
		FieldVal("myfield", Str("test")),
		FieldVal("myvalue", Int(42)),
	)

	opts := TokenAwareOptions{
		UseAbbreviations: true,
		CustomDict:       custom,
	}

	result := EmitTokenAwareWithOptions(v, opts)

	if !strings.Contains(result, "mf:") {
		t.Errorf("expected custom abbreviated key 'mf:', got %s", result)
	}
	if !strings.Contains(result, "mv:") {
		t.Errorf("expected custom abbreviated key 'mv:', got %s", result)
	}
}

func TestExpandAbbreviations(t *testing.T) {
	// Create a value with abbreviated keys
	v := Map(
		FieldVal("c", Str("hello")),
		FieldVal("r", Str("user")),
	)

	// Expand using LLMDict
	ExpandAbbreviations(v, LLMDict)

	// Check that keys are expanded
	entries, _ := v.AsMap()
	found := make(map[string]bool)
	for _, e := range entries {
		found[e.Key] = true
	}

	if !found["content"] {
		t.Error("expected 'content' key after expansion")
	}
	if !found["role"] {
		t.Error("expected 'role' key after expansion")
	}
}

func TestExpandAbbreviations_Nested(t *testing.T) {
	v := Map(
		FieldVal("m", List(
			Map(FieldVal("r", Str("u")), FieldVal("c", Str("hi"))),
		)),
	)

	ExpandAbbreviations(v, LLMDict)

	// Check outer key
	outerEntries, _ := v.AsMap()
	if outerEntries[0].Key != "messages" {
		t.Errorf("expected 'messages', got %s", outerEntries[0].Key)
	}

	// Check nested keys
	list, _ := outerEntries[0].Value.AsList()
	innerEntries, _ := list[0].AsMap()
	foundKeys := make(map[string]bool)
	for _, e := range innerEntries {
		foundKeys[e.Key] = true
	}

	if !foundKeys["role"] {
		t.Error("expected 'role' in nested map")
	}
	if !foundKeys["content"] {
		t.Error("expected 'content' in nested map")
	}
}

func TestEstimateTokens(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"", 0},
		{"a", 1},
		{"hello", 2},
		{"hello world", 3},
		{"this is a longer string with more tokens", 10},
	}

	for _, tc := range tests {
		result := EstimateTokens(tc.input)
		// Allow some variance since it's a heuristic
		if result < tc.expected-2 || result > tc.expected+2 {
			t.Errorf("EstimateTokens(%q) = %d, expected ~%d", tc.input, result, tc.expected)
		}
	}
}

func TestTokenSavings(t *testing.T) {
	// Create a typical LLM message
	v := Map(
		FieldVal("messages", List(
			Map(
				FieldVal("role", Str("user")),
				FieldVal("content", Str("Hello, how are you?")),
			),
			Map(
				FieldVal("role", Str("assistant")),
				FieldVal("content", Str("I'm doing well, thank you!")),
			),
		)),
		FieldVal("model", Str("gpt-4")),
		FieldVal("temperature", Float(0.7)),
		FieldVal("max_tokens", Int(100)),
	)

	orig, abbr, savings := TokenSavings(v, LLMDict)

	t.Logf("Original tokens: %d, Abbreviated tokens: %d, Savings: %.1f%%",
		orig, abbr, savings)

	// Should have some savings
	if savings < 5 {
		t.Errorf("expected >5%% savings, got %.1f%%", savings)
	}
}

func TestIsDefaultValue(t *testing.T) {
	tests := []struct {
		value    *GValue
		expected bool
	}{
		{nil, true},
		{Null(), true},
		{Bool(false), true},
		{Bool(true), false},
		{Int(0), true},
		{Int(1), false},
		{Float(0), true},
		{Float(0.1), false},
		{Str(""), true},
		{Str("x"), false},
		{List(), true},
		{List(Int(1)), false},
		{Map(), true},
	}

	for i, tc := range tests {
		result := isDefaultValue(tc.value)
		if result != tc.expected {
			t.Errorf("test %d: isDefaultValue() = %v, expected %v", i, result, tc.expected)
		}
	}
}

func TestCombinedDict(t *testing.T) {
	// CombinedDict should have entries from all dictionaries
	// Check a sample from each

	// From LLMDict
	if CombinedDict.Abbreviate("content") != "c" {
		t.Error("CombinedDict missing LLM entries")
	}

	// From ToolDict (but "type" might conflict)
	if !CombinedDict.HasAbbreviation("properties") {
		t.Error("CombinedDict missing Tool entries")
	}

	// From MLDict
	if !CombinedDict.HasAbbreviation("shape") {
		t.Error("CombinedDict missing ML entries")
	}
}

// Benchmarks

func BenchmarkEmitTokenAware(b *testing.B) {
	v := Map(
		FieldVal("messages", List(
			Map(FieldVal("role", Str("user")), FieldVal("content", Str("Hello"))),
			Map(FieldVal("role", Str("assistant")), FieldVal("content", Str("Hi there!"))),
		)),
		FieldVal("model", Str("gpt-4")),
		FieldVal("temperature", Float(0.7)),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EmitTokenAware(v)
	}
}

func BenchmarkEmitTokenAware_NoAbbr(b *testing.B) {
	v := Map(
		FieldVal("messages", List(
			Map(FieldVal("role", Str("user")), FieldVal("content", Str("Hello"))),
			Map(FieldVal("role", Str("assistant")), FieldVal("content", Str("Hi there!"))),
		)),
		FieldVal("model", Str("gpt-4")),
		FieldVal("temperature", Float(0.7)),
	)

	opts := TokenAwareOptions{UseAbbreviations: false}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EmitTokenAwareWithOptions(v, opts)
	}
}

func BenchmarkKeyDict_Abbreviate(b *testing.B) {
	keys := []string{"content", "role", "messages", "model", "temperature", "unknown"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range keys {
			LLMDict.Abbreviate(k)
		}
	}
}

func BenchmarkExpandAbbreviations(b *testing.B) {
	// Create a value to expand
	original := Map(
		FieldVal("m", List(
			Map(FieldVal("r", Str("u")), FieldVal("c", Str("hello"))),
			Map(FieldVal("r", Str("a")), FieldVal("c", Str("hi"))),
		)),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Need to clone since ExpandAbbreviations modifies in place
		v := Map(
			FieldVal("m", List(
				Map(FieldVal("r", Str("u")), FieldVal("c", Str("hello"))),
				Map(FieldVal("r", Str("a")), FieldVal("c", Str("hi"))),
			)),
		)
		ExpandAbbreviations(v, LLMDict)
	}
	_ = original
}
