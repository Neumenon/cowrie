package dicts

import (
	"testing"
)

func TestNewDict(t *testing.T) {
	d := NewDict("test")

	if d.Name() != "test" {
		t.Errorf("expected name 'test', got %q", d.Name())
	}
	if d.Version() != 1 {
		t.Errorf("expected version 1, got %d", d.Version())
	}
	if d.Len() != 0 {
		t.Errorf("expected len 0, got %d", d.Len())
	}
}

func TestDict_Add(t *testing.T) {
	d := NewDict("test")

	idx1 := d.Add("hello")
	if idx1 != 0 {
		t.Errorf("first Add should return 0, got %d", idx1)
	}

	idx2 := d.Add("world")
	if idx2 != 1 {
		t.Errorf("second Add should return 1, got %d", idx2)
	}

	// Adding same string should return existing index
	idx3 := d.Add("hello")
	if idx3 != 0 {
		t.Errorf("duplicate Add should return 0, got %d", idx3)
	}

	if d.Len() != 2 {
		t.Errorf("expected len 2, got %d", d.Len())
	}
}

func TestDict_AddBatch(t *testing.T) {
	d := NewDict("test")

	d.AddBatch([]string{"a", "b", "c", "d", "e"})

	if d.Len() != 5 {
		t.Errorf("expected len 5, got %d", d.Len())
	}

	// Adding duplicates should not increase length
	d.AddBatch([]string{"a", "b", "f"})
	if d.Len() != 6 {
		t.Errorf("expected len 6, got %d", d.Len())
	}
}

func TestDict_Lookup(t *testing.T) {
	d := NewDict("test")
	d.Add("hello")
	d.Add("world")

	idx := d.Lookup("hello")
	if idx != 0 {
		t.Errorf("Lookup(hello) = %d, expected 0", idx)
	}

	idx = d.Lookup("world")
	if idx != 1 {
		t.Errorf("Lookup(world) = %d, expected 1", idx)
	}

	idx = d.Lookup("unknown")
	if idx != -1 {
		t.Errorf("Lookup(unknown) = %d, expected -1", idx)
	}
}

func TestDict_Get(t *testing.T) {
	d := NewDict("test")
	d.Add("hello")
	d.Add("world")

	s := d.Get(0)
	if s != "hello" {
		t.Errorf("Get(0) = %q, expected 'hello'", s)
	}

	s = d.Get(1)
	if s != "world" {
		t.Errorf("Get(1) = %q, expected 'world'", s)
	}

	s = d.Get(100) // Out of bounds
	if s != "" {
		t.Errorf("Get(100) = %q, expected ''", s)
	}
}

func TestDict_Contains(t *testing.T) {
	d := NewDict("test")
	d.Add("hello")

	if !d.Contains("hello") {
		t.Error("expected Contains(hello) = true")
	}
	if d.Contains("world") {
		t.Error("expected Contains(world) = false")
	}
}

func TestDict_Encode(t *testing.T) {
	d := NewDict("test")
	d.Add("hello")
	d.Add("world")

	idx, ok := d.Encode("hello")
	if !ok || idx != 0 {
		t.Errorf("Encode(hello) = (%d, %v), expected (0, true)", idx, ok)
	}

	idx, ok = d.Encode("unknown")
	if ok {
		t.Errorf("Encode(unknown) = (%d, %v), expected (_, false)", idx, ok)
	}
}

func TestDict_Decode(t *testing.T) {
	d := NewDict("test")
	d.Add("hello")

	s := d.Decode(0)
	if s != "hello" {
		t.Errorf("Decode(0) = %q, expected 'hello'", s)
	}

	s = d.Decode(100)
	if s != "" {
		t.Errorf("Decode(100) = %q, expected ''", s)
	}
}

func TestDict_Entries(t *testing.T) {
	d := NewDict("test")
	d.Add("a")
	d.Add("b")
	d.Add("c")

	entries := d.Entries()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	expected := []string{"a", "b", "c"}
	for i, e := range expected {
		if entries[i] != e {
			t.Errorf("entries[%d] = %q, expected %q", i, entries[i], e)
		}
	}
}

func TestDict_Clone(t *testing.T) {
	d := NewDict("test")
	d.Add("hello")
	d.Add("world")

	clone := d.Clone()

	// Clone should have same contents
	if clone.Len() != d.Len() {
		t.Errorf("clone len %d != original len %d", clone.Len(), d.Len())
	}

	if clone.Get(0) != "hello" {
		t.Error("clone missing 'hello'")
	}

	// Modifying clone should not affect original
	clone.Add("new")
	if d.Contains("new") {
		t.Error("modifying clone affected original")
	}
}

func TestDict_MaxEntries(t *testing.T) {
	d := NewDict("test")
	d.maxEntries = 3

	d.Add("a")
	d.Add("b")
	d.Add("c")

	// Should be full now
	idx := d.Add("d")
	if idx != 0xFFFF {
		t.Errorf("expected 0xFFFF when full, got %d", idx)
	}

	if d.Len() != 3 {
		t.Errorf("expected len 3, got %d", d.Len())
	}
}

func TestMLDict_Contents(t *testing.T) {
	// Verify MLDict has expected entries
	expectedKeys := []string{
		"shape", "dtype", "data", "weights", "bias",
		"float32", "float64", "int8", "learning_rate",
	}

	for _, key := range expectedKeys {
		if !MLDict.Contains(key) {
			t.Errorf("MLDict missing expected key: %s", key)
		}
	}

	t.Logf("MLDict has %d entries", MLDict.Len())
}

func TestLLMDict_Contents(t *testing.T) {
	expectedKeys := []string{
		"role", "content", "messages", "model", "temperature",
		"max_tokens", "tool_calls", "function", "choices",
	}

	for _, key := range expectedKeys {
		if !LLMDict.Contains(key) {
			t.Errorf("LLMDict missing expected key: %s", key)
		}
	}

	t.Logf("LLMDict has %d entries", LLMDict.Len())
}

func TestJSONSchemaDict_Contents(t *testing.T) {
	expectedKeys := []string{
		"type", "properties", "required", "items", "enum",
		"minimum", "maximum", "pattern", "format",
	}

	for _, key := range expectedKeys {
		if !JSONSchemaDict.Contains(key) {
			t.Errorf("JSONSchemaDict missing expected key: %s", key)
		}
	}

	t.Logf("JSONSchemaDict has %d entries", JSONSchemaDict.Len())
}

func TestGGUFDict_Contents(t *testing.T) {
	expectedKeys := []string{
		"general.architecture", "general.name",
		"tokenizer.ggml.model", "q4_0", "q8_0",
	}

	for _, key := range expectedKeys {
		if !GGUFDict.Contains(key) {
			t.Errorf("GGUFDict missing expected key: %s", key)
		}
	}

	t.Logf("GGUFDict has %d entries", GGUFDict.Len())
}

func TestGetDict(t *testing.T) {
	tests := []struct {
		id   DictID
		name string
	}{
		{DictIDML, "ml"},
		{DictIDLLM, "llm"},
		{DictIDJSONSchema, "jsonschema"},
		{DictIDGGUF, "gguf"},
	}

	for _, tc := range tests {
		d := GetDict(tc.id)
		if d == nil {
			t.Errorf("GetDict(%d) returned nil", tc.id)
			continue
		}
		if d.Name() != tc.name {
			t.Errorf("GetDict(%d).Name() = %q, expected %q", tc.id, d.Name(), tc.name)
		}
	}

	// Non-existent dict
	d := GetDict(DictID(99))
	if d != nil {
		t.Error("GetDict(99) should return nil")
	}
}

func TestRegisterDict(t *testing.T) {
	custom := NewDict("custom")
	custom.Add("foo")
	custom.Add("bar")

	RegisterDict(DictIDCustom, custom)

	d := GetDict(DictIDCustom)
	if d == nil {
		t.Fatal("registered dict not found")
	}
	if d.Name() != "custom" {
		t.Errorf("expected name 'custom', got %q", d.Name())
	}
}

func TestDict_ConcurrentAccess(t *testing.T) {
	d := NewDict("test")

	// Add initial entries
	for i := 0; i < 100; i++ {
		d.Add(string(rune('a' + i%26)))
	}

	// Concurrent reads and writes
	done := make(chan bool)

	// Readers
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				d.Lookup("a")
				d.Get(0)
				d.Contains("b")
				d.Len()
			}
			done <- true
		}()
	}

	// Writers
	for i := 0; i < 5; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				d.Add(string(rune('A' + id*10 + j%10)))
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 15; i++ {
		<-done
	}
}

func TestDict_LookupMust_Panic(t *testing.T) {
	d := NewDict("test")
	d.Add("hello")

	// Should not panic
	idx := d.LookupMust("hello")
	if idx != 0 {
		t.Errorf("LookupMust(hello) = %d, expected 0", idx)
	}

	// Should panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("LookupMust(unknown) should panic")
		}
	}()
	d.LookupMust("unknown")
}

// Benchmarks

func BenchmarkDict_Add(b *testing.B) {
	d := NewDict("test")
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = string(rune(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Add(keys[i%len(keys)])
	}
}

func BenchmarkDict_Lookup(b *testing.B) {
	d := NewDict("test")
	for i := 0; i < 1000; i++ {
		d.Add(string(rune(i)))
	}

	keys := []string{"shape", "dtype", "weights", "unknown"}

	// Add the test keys
	for _, k := range keys[:3] {
		d.Add(k)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Lookup(keys[i%len(keys)])
	}
}

func BenchmarkDict_Encode(b *testing.B) {
	d := MLDict
	keys := []string{"shape", "dtype", "weights", "float32", "hidden"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Encode(keys[i%len(keys)])
	}
}

func BenchmarkMLDict_Lookup(b *testing.B) {
	keys := []string{"shape", "dtype", "weights", "learning_rate", "batch_size"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MLDict.Lookup(keys[i%len(keys)])
	}
}

func BenchmarkLLMDict_Lookup(b *testing.B) {
	keys := []string{"role", "content", "messages", "model", "temperature"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LLMDict.Lookup(keys[i%len(keys)])
	}
}
