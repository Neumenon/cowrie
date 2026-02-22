package glyph

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Pool resolution errors
var (
	ErrPoolNotFound = errors.New("pool not found")
	ErrPoolIndex    = errors.New("pool index out of bounds")
)

// ============================================================
// Pool Reference Types
// ============================================================

// PoolRef represents a reference to a pooled value.
type PoolRef struct {
	PoolID string // Pool identifier (e.g., "S1", "O1")
	Index  int    // Index within the pool
}

// String returns the pool reference format: ^S1:0
func (p PoolRef) String() string {
	return fmt.Sprintf("^%s:%d", p.PoolID, p.Index)
}

// IsPoolRef checks if a reference string is a pool reference.
// Pool IDs start with uppercase letter followed by digit(s): S1, O1, P42
func IsPoolRef(ref string) bool {
	if len(ref) < 2 {
		return false
	}
	// Must start with uppercase letter
	if ref[0] < 'A' || ref[0] > 'Z' {
		return false
	}
	// Rest must contain at least one digit
	for i := 1; i < len(ref); i++ {
		if ref[i] >= '0' && ref[i] <= '9' {
			return true
		}
		if ref[i] < 'A' || ref[i] > 'Z' {
			return false
		}
	}
	return false
}

// ParsePoolRef parses a pool reference from "^S1:0" format.
func ParsePoolRef(input string) (*PoolRef, error) {
	if !strings.HasPrefix(input, "^") {
		return nil, fmt.Errorf("pool ref must start with ^")
	}
	input = input[1:]

	colonIdx := strings.Index(input, ":")
	if colonIdx < 0 {
		return nil, fmt.Errorf("pool ref must contain colon")
	}

	poolID := input[:colonIdx]
	if !IsPoolRef(poolID) {
		return nil, fmt.Errorf("invalid pool ID: %s", poolID)
	}

	var index int
	_, err := fmt.Sscanf(input[colonIdx+1:], "%d", &index)
	if err != nil {
		return nil, fmt.Errorf("invalid pool index: %v", err)
	}

	return &PoolRef{PoolID: poolID, Index: index}, nil
}

// ============================================================
// Pool Types
// ============================================================

// PoolKind identifies the type of pool.
type PoolKind uint8

const (
	PoolKindString PoolKind = iota // @pool.str
	PoolKindObject                 // @pool.obj
)

// Pool represents a value pool for deduplication.
type Pool struct {
	ID      string    // Pool identifier (S1, O1, etc.)
	Kind    PoolKind  // String or Object pool
	Entries []*GValue // Pool entries
	mu      sync.RWMutex
}

// Get returns the value at the given index.
func (p *Pool) Get(index int) (*GValue, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if index < 0 || index >= len(p.Entries) {
		return nil, fmt.Errorf("%w: %s[%d] (len=%d)", ErrPoolIndex, p.ID, index, len(p.Entries))
	}
	return p.Entries[index], nil
}

// Add appends a value to the pool and returns its index.
func (p *Pool) Add(value *GValue) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	index := len(p.Entries)
	p.Entries = append(p.Entries, value)
	return index
}

// String returns the pool definition format.
func (p *Pool) String() string {
	var sb strings.Builder

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.Kind == PoolKindString {
		sb.WriteString("@pool.str id=")
	} else {
		sb.WriteString("@pool.obj id=")
	}
	sb.WriteString(p.ID)
	sb.WriteString(" [")

	for i, entry := range p.Entries {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(CanonicalizeLoose(entry))
	}

	sb.WriteString("]")
	return sb.String()
}

// ============================================================
// Pool Registry
// ============================================================

// PoolRegistry manages value pools for a session.
type PoolRegistry struct {
	mu    sync.RWMutex
	pools map[string]*Pool
}

// NewPoolRegistry creates a new pool registry.
func NewPoolRegistry() *PoolRegistry {
	return &PoolRegistry{
		pools: make(map[string]*Pool),
	}
}

// Define creates or replaces a pool.
func (r *PoolRegistry) Define(pool *Pool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pools[pool.ID] = pool
}

// Get returns a pool by ID.
func (r *PoolRegistry) Get(id string) *Pool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pools[id]
}

// Resolve resolves a pool reference to its value.
func (r *PoolRegistry) Resolve(ref PoolRef) (*GValue, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	pool, ok := r.pools[ref.PoolID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPoolNotFound, ref.PoolID)
	}
	return pool.Get(ref.Index)
}

// Clear removes a pool by ID.
func (r *PoolRegistry) Clear(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.pools, id)
}

// ClearAll removes all pools.
func (r *PoolRegistry) ClearAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pools = make(map[string]*Pool)
}

// ============================================================
// Pool Value Type
// ============================================================

// TypePoolRef is the GType for pool references
const TypePoolRef GType = 21

// PoolRefValue creates a pool reference value.
func PoolRefValue(poolID string, index int) *GValue {
	return &GValue{
		typ:     TypePoolRef,
		poolRef: &PoolRef{PoolID: poolID, Index: index},
	}
}

// AsPoolRef returns the pool reference.
func (v *GValue) AsPoolRef() (PoolRef, error) {
	if v == nil {
		return PoolRef{}, fmt.Errorf("glyph: nil value")
	}
	if v.typ != TypePoolRef {
		return PoolRef{}, fmt.Errorf("glyph: expected poolref, got %s", v.typ)
	}
	if v.poolRef == nil {
		return PoolRef{}, nil
	}
	return *v.poolRef, nil
}

// IsPoolRef returns true if this is a pool reference.
func (v *GValue) IsPoolRef() bool {
	return v != nil && v.typ == TypePoolRef
}

// ============================================================
// Pool Emit/Parse
// ============================================================

// EmitPool emits a pool definition.
func EmitPool(pool *Pool) string {
	return pool.String()
}

// ParsePool parses a pool definition from "@pool.str id=S1 [...]".
func ParsePool(input string) (*Pool, error) {
	var kind PoolKind
	var rest string

	if strings.HasPrefix(input, "@pool.str ") {
		kind = PoolKindString
		rest = strings.TrimPrefix(input, "@pool.str ")
	} else if strings.HasPrefix(input, "@pool.obj ") {
		kind = PoolKindObject
		rest = strings.TrimPrefix(input, "@pool.obj ")
	} else {
		return nil, fmt.Errorf("expected @pool.str or @pool.obj prefix")
	}

	pool := &Pool{Kind: kind}

	// Parse id=...
	if !strings.HasPrefix(rest, "id=") {
		return nil, fmt.Errorf("expected id= after @pool")
	}
	rest = rest[3:]

	// Extract pool ID
	spaceIdx := strings.IndexAny(rest, " \t[")
	if spaceIdx < 0 {
		return nil, fmt.Errorf("missing pool entries")
	}
	pool.ID = rest[:spaceIdx]
	rest = strings.TrimLeft(rest[spaceIdx:], " \t")

	// Parse entries: [...]
	if !strings.HasPrefix(rest, "[") {
		return nil, fmt.Errorf("expected [ for pool entries")
	}

	endIdx, err := findMatchingBracket(rest)
	if err != nil {
		return nil, err
	}

	entriesStr := rest[1:endIdx]

	if trailing := strings.TrimSpace(rest[endIdx+1:]); trailing != "" {
		return nil, fmt.Errorf("unexpected trailing content after pool entries: %s", trailing)
	}

	listText := "[" + entriesStr + "]"
	res, err := ParseWithOptions(listText, ParseOptions{Tolerant: false})
	if err != nil {
		return nil, err
	}
	if res == nil || res.Value == nil {
		return nil, fmt.Errorf("failed to parse pool entries")
	}
	if len(res.Errors) > 0 {
		return nil, fmt.Errorf("parse pool entries: %s", res.Errors[0].Error())
	}
	if res.Value.typ != TypeList {
		return nil, fmt.Errorf("pool entries must be a list")
	}
	pool.Entries = res.Value.listVal
	if pool.Kind == PoolKindString {
		for i, entry := range pool.Entries {
			if entry == nil || entry.typ != TypeStr {
				entryType := "nil"
				if entry != nil {
					entryType = entry.typ.String()
				}
				return nil, fmt.Errorf("pool.str entries must be strings (entry %d is %s)", i, entryType)
			}
		}
	}

	return pool, nil
}

func findMatchingBracket(input string) (int, error) {
	if input == "" || input[0] != '[' {
		return -1, fmt.Errorf("expected [ for pool entries")
	}

	depth := 0
	inString := false
	escaped := false

	for i := 0; i < len(input); i++ {
		c := input[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == '"' {
				inString = false
			}
			continue
		}

		switch c {
		case '"':
			inString = true
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return i, nil
			}
			if depth < 0 {
				return -1, fmt.Errorf("unbalanced pool entries")
			}
		}
	}

	return -1, fmt.Errorf("unclosed pool entries")
}

// parsePoolEntries parses pool entry values.
func parsePoolEntries(input string, kind PoolKind) ([]*GValue, error) {
	var entries []*GValue
	input = strings.TrimSpace(input)

	for len(input) > 0 {
		input = strings.TrimLeft(input, " \t\n")
		if input == "" {
			break
		}

		// Parse next value
		value, remaining, err := parseNextPoolValue(input)
		if err != nil {
			return nil, err
		}
		entries = append(entries, value)
		input = remaining
	}

	return entries, nil
}

// parseNextPoolValue parses the next value from pool entry string.
func parseNextPoolValue(input string) (*GValue, string, error) {
	input = strings.TrimLeft(input, " \t\n")
	if len(input) == 0 {
		return nil, "", fmt.Errorf("unexpected end of input")
	}

	// Quoted string
	if input[0] == '"' {
		endIdx := 1
		escaped := false
		for endIdx < len(input) {
			c := input[endIdx]
			if escaped {
				// Previous char was backslash, skip this char
				escaped = false
				endIdx++
				continue
			}
			if c == '\\' {
				// Next char is escaped
				escaped = true
				endIdx++
				continue
			}
			if c == '"' {
				// Found unescaped closing quote
				break
			}
			endIdx++
		}
		if endIdx >= len(input) {
			return nil, "", fmt.Errorf("unterminated quoted string")
		}
		value := unquoteBlobString(input[1:endIdx])
		return Str(value), input[endIdx+1:], nil
	}

	// Bare string or other value - find end
	endIdx := 0
	for endIdx < len(input) && input[endIdx] != ' ' && input[endIdx] != '\t' && input[endIdx] != '\n' && input[endIdx] != ']' {
		endIdx++
	}

	token := input[:endIdx]

	// Check for special values
	switch token {
	case "t", "true":
		return Bool(true), input[endIdx:], nil
	case "f", "false":
		return Bool(false), input[endIdx:], nil
	case "∅", "_", "null":
		return Null(), input[endIdx:], nil
	}

	// Check for number
	if (token[0] >= '0' && token[0] <= '9') || token[0] == '-' {
		if strings.Contains(token, ".") || strings.Contains(token, "e") || strings.Contains(token, "E") {
			var f float64
			fmt.Sscanf(token, "%f", &f)
			return Float(f), input[endIdx:], nil
		}
		var i int64
		fmt.Sscanf(token, "%d", &i)
		return Int(i), input[endIdx:], nil
	}

	// Bare string
	return Str(token), input[endIdx:], nil
}

// ============================================================
// Auto-Interning Support
// ============================================================

// AutoInternOpts configures automatic value interning.
type AutoInternOpts struct {
	MinLength   int // Minimum string length to consider (default: 50)
	MinOccurs   int // Minimum occurrences to intern (default: 2)
	MaxPoolSize int // Maximum entries per pool (default: 256)
}

// DefaultAutoInternOpts returns default auto-intern options.
func DefaultAutoInternOpts() AutoInternOpts {
	return AutoInternOpts{
		MinLength:   50,
		MinOccurs:   2,
		MaxPoolSize: 256,
	}
}

// AutoInterner tracks value occurrences and automatically interns repeated values.
// Thread-safe for concurrent use.
type AutoInterner struct {
	opts     AutoInternOpts
	registry *PoolRegistry
	mu       sync.RWMutex
	counts   map[string]int     // value -> occurrence count
	interned map[string]PoolRef // value -> pool reference
	nextPool int                // Next pool ID suffix
}

// NewAutoInterner creates a new auto-interner.
func NewAutoInterner(registry *PoolRegistry, opts AutoInternOpts) *AutoInterner {
	return &AutoInterner{
		opts:     opts,
		registry: registry,
		counts:   make(map[string]int),
		interned: make(map[string]PoolRef),
		nextPool: 1,
	}
}

// Process checks if a string should be interned and returns the appropriate value.
// Thread-safe for concurrent calls.
func (a *AutoInterner) Process(s string) *GValue {
	// Too short to consider - no locking needed
	if len(s) < a.opts.MinLength {
		return Str(s)
	}

	// Fast path: check if already interned (read lock)
	a.mu.RLock()
	if ref, ok := a.interned[s]; ok {
		a.mu.RUnlock()
		return PoolRefValue(ref.PoolID, ref.Index)
	}
	a.mu.RUnlock()

	// Slow path: need write lock
	a.mu.Lock()
	defer a.mu.Unlock()

	// Double-check after acquiring write lock
	if ref, ok := a.interned[s]; ok {
		return PoolRefValue(ref.PoolID, ref.Index)
	}

	// Count occurrence
	a.counts[s]++
	if a.counts[s] < a.opts.MinOccurs {
		return Str(s)
	}

	// Time to intern
	poolID := fmt.Sprintf("S%d", a.nextPool)
	pool := a.registry.Get(poolID)
	if pool == nil {
		pool = &Pool{ID: poolID, Kind: PoolKindString}
		a.registry.Define(pool)
	}

	if len(pool.Entries) >= a.opts.MaxPoolSize {
		// Pool full, start new one
		a.nextPool++
		poolID = fmt.Sprintf("S%d", a.nextPool)
		pool = &Pool{ID: poolID, Kind: PoolKindString}
		a.registry.Define(pool)
	}

	index := pool.Add(Str(s))
	ref := PoolRef{PoolID: poolID, Index: index}
	a.interned[s] = ref

	return PoolRefValue(ref.PoolID, ref.Index)
}
