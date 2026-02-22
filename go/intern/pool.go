// Package intern provides string interning for memory-efficient decoding.
//
// When decoding JSON/SJSON data with repetitive keys (common in ML configs,
// API responses), string interning can significantly reduce memory allocation
// by deduplicating identical strings.
//
// Typical memory savings: 20-40% for schemas with repetitive structure.
//
// Usage:
//
//	pool := intern.NewPool()
//	key := pool.Intern("content")  // Returns interned string
//	pool.Reset()                   // Clear for reuse
package intern

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// Pool is a string interning pool.
// Safe for concurrent use.
type Pool struct {
	mu       sync.RWMutex
	strings  map[string]string
	bytes    []byte // Arena for small strings
	arena    int    // Current position in arena
	size     int64  // Total interned size
	hits     uint64 // Cache hits
	misses   uint64 // Cache misses
	maxSize  int    // Maximum pool size (0 = unlimited)
	arenaMax int    // Maximum arena size
}

// PoolOptions configures pool behavior.
type PoolOptions struct {
	// MaxSize limits total interned string bytes (default: 16MB)
	MaxSize int

	// ArenaSize is the initial arena size for small strings (default: 64KB)
	ArenaSize int

	// MaxArenaSize limits arena growth (default: 1MB)
	MaxArenaSize int
}

// DefaultPoolOptions returns sensible defaults.
func DefaultPoolOptions() PoolOptions {
	return PoolOptions{
		MaxSize:      16 << 20, // 16MB
		ArenaSize:    64 << 10, // 64KB
		MaxArenaSize: 1 << 20,  // 1MB
	}
}

// NewPool creates a new string interning pool with default options.
func NewPool() *Pool {
	return NewPoolWithOptions(DefaultPoolOptions())
}

// NewPoolWithOptions creates a pool with custom options.
func NewPoolWithOptions(opts PoolOptions) *Pool {
	if opts.ArenaSize == 0 {
		opts.ArenaSize = 64 << 10
	}
	if opts.MaxArenaSize == 0 {
		opts.MaxArenaSize = 1 << 20
	}
	if opts.MaxSize == 0 {
		opts.MaxSize = 16 << 20
	}

	return &Pool{
		strings:  make(map[string]string, 256),
		bytes:    make([]byte, opts.ArenaSize),
		maxSize:  opts.MaxSize,
		arenaMax: opts.MaxArenaSize,
	}
}

// Intern returns an interned copy of the string.
// If the string was previously interned, returns the same reference.
func (p *Pool) Intern(s string) string {
	// Fast path: read-only check
	p.mu.RLock()
	if interned, ok := p.strings[s]; ok {
		p.mu.RUnlock()
		atomic.AddUint64(&p.hits, 1)
		return interned
	}
	p.mu.RUnlock()

	// Slow path: write
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if interned, ok := p.strings[s]; ok {
		atomic.AddUint64(&p.hits, 1)
		return interned
	}

	atomic.AddUint64(&p.misses, 1)

	// Check size limit
	if p.maxSize > 0 && p.size+int64(len(s)) > int64(p.maxSize) {
		// Pool full - return copy without interning
		return string([]byte(s))
	}

	// Try to allocate from arena for small strings
	var interned string
	if len(s) <= 64 && p.arena+len(s) <= len(p.bytes) {
		// Copy to arena
		copy(p.bytes[p.arena:], s)
		interned = unsafe.String(&p.bytes[p.arena], len(s))
		p.arena += len(s)
	} else {
		// Allocate separately
		interned = string([]byte(s))
	}

	p.strings[s] = interned
	p.size += int64(len(s))

	return interned
}

// InternBytes interns a byte slice as a string.
func (p *Pool) InternBytes(b []byte) string {
	return p.Intern(string(b))
}

// Contains checks if a string is already interned.
func (p *Pool) Contains(s string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.strings[s]
	return ok
}

// Len returns the number of interned strings.
func (p *Pool) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.strings)
}

// Size returns the total size of interned strings in bytes.
func (p *Pool) Size() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.size
}

// Stats returns pool statistics.
func (p *Pool) Stats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return PoolStats{
		Count:      len(p.strings),
		Size:       p.size,
		ArenaUsed:  p.arena,
		ArenaTotal: len(p.bytes),
		Hits:       atomic.LoadUint64(&p.hits),
		Misses:     atomic.LoadUint64(&p.misses),
	}
}

// PoolStats contains pool statistics.
type PoolStats struct {
	Count      int   // Number of interned strings
	Size       int64 // Total interned bytes
	ArenaUsed  int   // Arena bytes used
	ArenaTotal int   // Total arena size
	Hits       uint64
	Misses     uint64
}

// HitRate returns the cache hit rate (0.0-1.0).
func (s PoolStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// Reset clears the pool for reuse.
// Keeps allocated memory for future use.
func (p *Pool) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Clear map but keep capacity
	for k := range p.strings {
		delete(p.strings, k)
	}

	p.arena = 0
	p.size = 0
	atomic.StoreUint64(&p.hits, 0)
	atomic.StoreUint64(&p.misses, 0)
}

// ============================================================
// Scoped Pool for Decoder Integration
// ============================================================

// ScopedPool is a pool tied to a single decode operation.
// Automatically tracks keys vs values for statistics.
type ScopedPool struct {
	pool     *Pool
	keyPool  *Pool // Separate pool for keys (usually more repetitive)
	parent   *ScopedPool
	keyCount int
	valCount int
}

// NewScopedPool creates a scoped pool for a decode operation.
func NewScopedPool(parent *Pool) *ScopedPool {
	return &ScopedPool{
		pool:    parent,
		keyPool: parent, // Share with parent by default
	}
}

// NewScopedPoolSeparate creates a scoped pool with separate key tracking.
func NewScopedPoolSeparate() *ScopedPool {
	return &ScopedPool{
		pool:    NewPool(),
		keyPool: NewPool(),
	}
}

// InternKey interns a map/struct key.
func (p *ScopedPool) InternKey(key string) string {
	p.keyCount++
	return p.keyPool.Intern(key)
}

// InternValue interns a string value.
func (p *ScopedPool) InternValue(val string) string {
	p.valCount++
	return p.pool.Intern(val)
}

// KeyCount returns the number of keys interned.
func (p *ScopedPool) KeyCount() int {
	return p.keyCount
}

// ValueCount returns the number of values interned.
func (p *ScopedPool) ValueCount() int {
	return p.valCount
}

// Stats returns combined statistics.
func (p *ScopedPool) Stats() ScopedPoolStats {
	return ScopedPoolStats{
		Keys:      p.keyPool.Stats(),
		Values:    p.pool.Stats(),
		KeyCount:  p.keyCount,
		ValCount:  p.valCount,
	}
}

// ScopedPoolStats contains scoped statistics.
type ScopedPoolStats struct {
	Keys     PoolStats
	Values   PoolStats
	KeyCount int
	ValCount int
}

// ============================================================
// Global Pool (for convenience)
// ============================================================

var (
	globalPool     *Pool
	globalPoolOnce sync.Once
)

// Global returns the global interning pool.
// Useful for application-wide string deduplication.
func Global() *Pool {
	globalPoolOnce.Do(func() {
		globalPool = NewPool()
	})
	return globalPool
}

// InternGlobal interns a string in the global pool.
func InternGlobal(s string) string {
	return Global().Intern(s)
}

// ============================================================
// Common String Sets (Pre-intern for performance)
// ============================================================

// CommonKeys contains frequently used JSON/ML keys.
var CommonKeys = []string{
	// JSON Schema
	"type", "properties", "required", "items", "enum",
	"minimum", "maximum", "default", "description",

	// LLM API
	"role", "content", "messages", "model", "temperature",
	"max_tokens", "stop", "stream", "choices", "message",
	"tool_calls", "function", "name", "arguments",

	// Common data
	"id", "name", "value", "data", "result", "error",
	"status", "code", "message", "timestamp", "created",
	"updated", "deleted", "version", "count", "total",

	// ML/Tensor
	"shape", "dtype", "weights", "bias", "input", "output",
	"hidden", "layer", "attention", "embedding",
}

// PrewarmPool adds common keys to a pool.
func PrewarmPool(p *Pool) {
	for _, key := range CommonKeys {
		p.Intern(key)
	}
}

// PrewarmGlobal adds common keys to the global pool.
func PrewarmGlobal() {
	PrewarmPool(Global())
}
