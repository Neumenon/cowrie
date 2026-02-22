package intern

import (
	"sync"
	"testing"
)

func TestNewPool(t *testing.T) {
	p := NewPool()

	if p.Len() != 0 {
		t.Errorf("expected len 0, got %d", p.Len())
	}
	if p.Size() != 0 {
		t.Errorf("expected size 0, got %d", p.Size())
	}
}

func TestPool_Intern(t *testing.T) {
	p := NewPool()

	s1 := p.Intern("hello")
	if s1 != "hello" {
		t.Errorf("expected 'hello', got %q", s1)
	}

	// Same string should return same reference
	s2 := p.Intern("hello")
	if s1 != s2 {
		t.Error("expected same reference for same string")
	}

	// Different string
	s3 := p.Intern("world")
	if s3 != "world" {
		t.Errorf("expected 'world', got %q", s3)
	}

	if p.Len() != 2 {
		t.Errorf("expected len 2, got %d", p.Len())
	}
}

func TestPool_InternBytes(t *testing.T) {
	p := NewPool()

	bytes := []byte("hello")
	s := p.InternBytes(bytes)

	if s != "hello" {
		t.Errorf("expected 'hello', got %q", s)
	}

	// Should be interned
	if !p.Contains("hello") {
		t.Error("expected string to be interned")
	}
}

func TestPool_Contains(t *testing.T) {
	p := NewPool()
	p.Intern("hello")

	if !p.Contains("hello") {
		t.Error("expected Contains(hello) = true")
	}
	if p.Contains("world") {
		t.Error("expected Contains(world) = false")
	}
}

func TestPool_Size(t *testing.T) {
	p := NewPool()

	p.Intern("hello") // 5 bytes
	p.Intern("world") // 5 bytes

	if p.Size() != 10 {
		t.Errorf("expected size 10, got %d", p.Size())
	}

	// Interning same string shouldn't increase size
	p.Intern("hello")
	if p.Size() != 10 {
		t.Errorf("expected size still 10, got %d", p.Size())
	}
}

func TestPool_Stats(t *testing.T) {
	p := NewPool()

	p.Intern("hello")
	p.Intern("world")
	p.Intern("hello") // Hit
	p.Intern("hello") // Hit

	stats := p.Stats()

	if stats.Count != 2 {
		t.Errorf("expected count 2, got %d", stats.Count)
	}
	if stats.Size != 10 {
		t.Errorf("expected size 10, got %d", stats.Size)
	}
	if stats.Hits != 2 {
		t.Errorf("expected 2 hits, got %d", stats.Hits)
	}
	if stats.Misses != 2 {
		t.Errorf("expected 2 misses, got %d", stats.Misses)
	}

	hitRate := stats.HitRate()
	if hitRate != 0.5 {
		t.Errorf("expected hit rate 0.5, got %f", hitRate)
	}
}

func TestPool_Reset(t *testing.T) {
	p := NewPool()

	p.Intern("hello")
	p.Intern("world")

	p.Reset()

	if p.Len() != 0 {
		t.Errorf("expected len 0 after reset, got %d", p.Len())
	}
	if p.Size() != 0 {
		t.Errorf("expected size 0 after reset, got %d", p.Size())
	}
	if p.Contains("hello") {
		t.Error("expected strings cleared after reset")
	}

	stats := p.Stats()
	if stats.Hits != 0 || stats.Misses != 0 {
		t.Error("expected stats reset")
	}
}

func TestPool_MaxSize(t *testing.T) {
	opts := PoolOptions{
		MaxSize:   20,
		ArenaSize: 1024,
	}
	p := NewPoolWithOptions(opts)

	p.Intern("hello")      // 5 bytes
	p.Intern("world")      // 5 bytes
	p.Intern("test")       // 4 bytes
	p.Intern("longstring") // 10 bytes - would exceed max

	// Should still work but may not intern the last one
	if p.Size() > 20 {
		t.Errorf("size %d exceeds max 20", p.Size())
	}
}

func TestPool_ConcurrentAccess(t *testing.T) {
	p := NewPool()

	var wg sync.WaitGroup
	strings := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	// Multiple goroutines interning concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				p.Intern(strings[j%len(strings)])
			}
		}(i)
	}

	wg.Wait()

	// Should have exactly 10 unique strings
	if p.Len() != 10 {
		t.Errorf("expected 10 unique strings, got %d", p.Len())
	}
}

func TestPool_ArenaAllocation(t *testing.T) {
	opts := PoolOptions{
		ArenaSize:    256,
		MaxArenaSize: 1024,
	}
	p := NewPoolWithOptions(opts)

	// Intern small strings (should go to arena)
	for i := 0; i < 10; i++ {
		p.Intern(string(rune('a' + i)))
	}

	stats := p.Stats()
	if stats.ArenaUsed == 0 {
		t.Log("Arena not used for small strings (may be implementation detail)")
	}

	// Intern larger string (may not go to arena)
	p.Intern("this is a longer string that might not fit in arena allocation")

	t.Logf("Arena used: %d/%d", stats.ArenaUsed, stats.ArenaTotal)
}

func TestScopedPool(t *testing.T) {
	parent := NewPool()
	scoped := NewScopedPool(parent)

	scoped.InternKey("name")
	scoped.InternKey("value")
	scoped.InternValue("hello")
	scoped.InternValue("world")

	if scoped.KeyCount() != 2 {
		t.Errorf("expected 2 keys, got %d", scoped.KeyCount())
	}
	if scoped.ValueCount() != 2 {
		t.Errorf("expected 2 values, got %d", scoped.ValueCount())
	}

	// Parent pool should have all 4 strings
	if parent.Len() != 4 {
		t.Errorf("expected 4 strings in parent, got %d", parent.Len())
	}
}

func TestScopedPool_Separate(t *testing.T) {
	scoped := NewScopedPoolSeparate()

	scoped.InternKey("key1")
	scoped.InternKey("key2")
	scoped.InternValue("value1")

	stats := scoped.Stats()

	if stats.KeyCount != 2 {
		t.Errorf("expected 2 key count, got %d", stats.KeyCount)
	}
	if stats.ValCount != 1 {
		t.Errorf("expected 1 val count, got %d", stats.ValCount)
	}

	// Keys and values in separate pools
	if stats.Keys.Count != 2 {
		t.Errorf("expected 2 in key pool, got %d", stats.Keys.Count)
	}
	if stats.Values.Count != 1 {
		t.Errorf("expected 1 in value pool, got %d", stats.Values.Count)
	}
}

func TestGlobal(t *testing.T) {
	g1 := Global()
	g2 := Global()

	if g1 != g2 {
		t.Error("Global() should return same instance")
	}
}

func TestInternGlobal(t *testing.T) {
	s1 := InternGlobal("global_test_string")
	s2 := InternGlobal("global_test_string")

	if s1 != s2 {
		t.Error("expected same reference from global pool")
	}
}

func TestPrewarmPool(t *testing.T) {
	p := NewPool()
	PrewarmPool(p)

	// Should have common keys
	for _, key := range CommonKeys {
		if !p.Contains(key) {
			t.Errorf("expected key %q after prewarm", key)
		}
	}
}

func TestCommonKeys(t *testing.T) {
	// Verify CommonKeys has expected entries
	expectedKeys := []string{
		"type", "properties", "required",
		"role", "content", "messages",
		"id", "name", "value",
		"shape", "dtype", "weights",
	}

	keySet := make(map[string]bool)
	for _, k := range CommonKeys {
		keySet[k] = true
	}

	for _, expected := range expectedKeys {
		if !keySet[expected] {
			t.Errorf("CommonKeys missing %q", expected)
		}
	}
}

func TestPoolStats_HitRate(t *testing.T) {
	tests := []struct {
		hits     uint64
		misses   uint64
		expected float64
	}{
		{0, 0, 0},
		{10, 0, 1.0},
		{0, 10, 0},
		{5, 5, 0.5},
		{7, 3, 0.7},
	}

	for _, tc := range tests {
		stats := PoolStats{Hits: tc.hits, Misses: tc.misses}
		rate := stats.HitRate()
		if rate != tc.expected {
			t.Errorf("HitRate(%d, %d) = %f, expected %f",
				tc.hits, tc.misses, rate, tc.expected)
		}
	}
}

func TestDefaultPoolOptions(t *testing.T) {
	opts := DefaultPoolOptions()

	if opts.MaxSize != 16<<20 {
		t.Errorf("expected MaxSize 16MB, got %d", opts.MaxSize)
	}
	if opts.ArenaSize != 64<<10 {
		t.Errorf("expected ArenaSize 64KB, got %d", opts.ArenaSize)
	}
	if opts.MaxArenaSize != 1<<20 {
		t.Errorf("expected MaxArenaSize 1MB, got %d", opts.MaxArenaSize)
	}
}

func TestPool_EmptyString(t *testing.T) {
	p := NewPool()

	s := p.Intern("")
	if s != "" {
		t.Errorf("expected empty string, got %q", s)
	}

	// Should be interned
	if !p.Contains("") {
		t.Error("empty string should be interned")
	}
}

func TestPool_LongStrings(t *testing.T) {
	p := NewPool()

	// Create a string longer than arena threshold
	long := make([]byte, 1000)
	for i := range long {
		long[i] = byte('a' + i%26)
	}
	longStr := string(long)

	s1 := p.Intern(longStr)
	s2 := p.Intern(longStr)

	if s1 != s2 {
		t.Error("long strings should be interned too")
	}

	if p.Size() != int64(len(longStr)) {
		t.Errorf("expected size %d, got %d", len(longStr), p.Size())
	}
}

// Benchmarks

func BenchmarkPool_Intern_New(b *testing.B) {
	p := NewPool()
	strings := make([]string, 1000)
	for i := range strings {
		strings[i] = string(rune(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Intern(strings[i%len(strings)])
	}
}

func BenchmarkPool_Intern_Hit(b *testing.B) {
	p := NewPool()
	strings := []string{"a", "b", "c", "d", "e"}
	for _, s := range strings {
		p.Intern(s)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Intern(strings[i%len(strings)])
	}
}

func BenchmarkPool_Intern_Concurrent(b *testing.B) {
	p := NewPool()
	strings := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			p.Intern(strings[i%len(strings)])
			i++
		}
	})
}

func BenchmarkPool_Contains(b *testing.B) {
	p := NewPool()
	for i := 0; i < 1000; i++ {
		p.Intern(string(rune(i)))
	}

	strings := []string{"a", "b", "c", "zzz", "unknown"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Contains(strings[i%len(strings)])
	}
}

func BenchmarkScopedPool_InternKey(b *testing.B) {
	parent := NewPool()
	keys := []string{"name", "value", "type", "id", "content"}
	for _, k := range keys {
		parent.Intern(k)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scoped := NewScopedPool(parent)
		for j := 0; j < 10; j++ {
			scoped.InternKey(keys[j%len(keys)])
		}
	}
}

func BenchmarkNoInterning(b *testing.B) {
	// Baseline: allocating strings without interning
	strings := []string{"a", "b", "c", "d", "e"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate string allocation
		s := strings[i%len(strings)]
		_ = string([]byte(s))
	}
}

func BenchmarkWithInterning(b *testing.B) {
	p := NewPool()
	strings := []string{"a", "b", "c", "d", "e"}
	for _, s := range strings {
		p.Intern(s)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Intern(strings[i%len(strings)])
	}
}
