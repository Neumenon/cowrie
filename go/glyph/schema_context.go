package glyph

import (
	"container/list"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// SchemaContext holds a key dictionary for compact object encoding.
// Keys are mapped to numeric indices to reduce token count.
//
// SchemaContext is IMMUTABLE after creation. Do not modify fields after construction.
//
// Example:
//
//	ctx := NewSchemaContext([]string{"role", "content", "tool_calls"})
//	// Objects can now use: {0="user" 1="Hello"}
//	// Instead of: {role=user content=Hello}
type SchemaContext struct {
	ID      string            // Schema ID (hash or session ID like "S1")
	Keys    []string          // Ordered list of keys (immutable after creation)
	KeyToID map[string]uint32 // Key name -> numeric ID (immutable after creation)
	IDToKey []string          // Numeric ID -> key name (immutable after creation)
	Version int64             // Incremented on eviction to detect stale refs
}

// NewSchemaContext creates a new schema context from a list of keys.
// The schema ID is computed as a SHA-256 hash (first 5 bytes, base32 encoded).
func NewSchemaContext(keys []string) *SchemaContext {
	ctx := &SchemaContext{
		Keys:    make([]string, len(keys)),
		KeyToID: make(map[string]uint32, len(keys)),
		IDToKey: make([]string, len(keys)),
	}
	copy(ctx.Keys, keys)
	copy(ctx.IDToKey, keys)

	for i, k := range keys {
		ctx.KeyToID[k] = uint32(i)
	}

	ctx.ID = ctx.ComputeID()
	return ctx
}

// NewSchemaContextWithID creates a schema context with a specific ID.
// Use this for session-based short IDs like "1", "2", etc.
func NewSchemaContextWithID(id string, keys []string) *SchemaContext {
	ctx := NewSchemaContext(keys)
	ctx.ID = id
	return ctx
}

// ComputeID generates a stable ID from the keys list.
// Uses SHA-256, takes first 5 bytes, encodes as base32 (8 chars).
func (sc *SchemaContext) ComputeID() string {
	h := sha256.New()
	for i, k := range sc.Keys {
		if i > 0 {
			h.Write([]byte{0}) // separator
		}
		h.Write([]byte(k))
	}
	hash := h.Sum(nil)[:5]
	return strings.ToLower(base32.StdEncoding.EncodeToString(hash))[:8]
}

// LookupKey returns the numeric ID for a key, or -1 if not found.
// Thread-safe: SchemaContext is immutable after creation.
func (sc *SchemaContext) LookupKey(key string) int {
	if id, ok := sc.KeyToID[key]; ok {
		return int(id)
	}
	return -1
}

// LookupID returns the key name for a numeric ID, or "" if out of range.
// Thread-safe: SchemaContext is immutable after creation.
func (sc *SchemaContext) LookupID(id int) string {
	if id < 0 || id >= len(sc.IDToKey) {
		return ""
	}
	return sc.IDToKey[id]
}

// HasKey returns true if the key exists in the schema.
// Thread-safe: SchemaContext is immutable after creation.
func (sc *SchemaContext) HasKey(key string) bool {
	_, ok := sc.KeyToID[key]
	return ok
}

// Len returns the number of keys in the schema.
func (sc *SchemaContext) Len() int {
	return len(sc.Keys)
}

// EmitHeader returns the schema header directive.
// If inline is true, includes the keys list: @schema#id @keys=[k1 k2 k3]
// If inline is false, just returns: @schema#id
func (sc *SchemaContext) EmitHeader(inline bool) string {
	if inline {
		var b strings.Builder
		b.WriteString("@schema#")
		b.WriteString(sc.ID)
		b.WriteString(" @keys=[")
		for i, k := range sc.Keys {
			if i > 0 {
				b.WriteByte(' ')
			}
			// Quote keys that need it
			if keyNeedsQuoting(k) {
				b.WriteString(canonString(k))
			} else {
				b.WriteString(k)
			}
		}
		b.WriteByte(']')
		return b.String()
	}
	return "@schema#" + sc.ID
}

// schemaEntry holds a schema context and its position in the LRU list.
type schemaEntry struct {
	ctx     *SchemaContext
	element *list.Element // Pointer to LRU list element (stores schema ID)
}

// SchemaRegistry manages schema contexts for a session with LRU eviction.
type SchemaRegistry struct {
	schemas map[string]*schemaEntry
	lruList *list.List // Front = most recent, Back = least recent; stores schema IDs
	active  *SchemaContext
	mu      sync.RWMutex
	maxSize int // LRU cap, default 64
}

// NewSchemaRegistry creates a new schema registry.
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		schemas: make(map[string]*schemaEntry),
		lruList: list.New(),
		maxSize: 64,
	}
}

// NewSchemaRegistryWithSize creates a new schema registry with custom capacity.
func NewSchemaRegistryWithSize(maxSize int) *SchemaRegistry {
	if maxSize < 1 {
		maxSize = 1
	}
	return &SchemaRegistry{
		schemas: make(map[string]*schemaEntry),
		lruList: list.New(),
		maxSize: maxSize,
	}
}

// Define adds or replaces a schema context.
// If the registry is at capacity, the least recently used schema is evicted.
func (sr *SchemaRegistry) Define(ctx *SchemaContext) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	// Check if already exists
	if entry, ok := sr.schemas[ctx.ID]; ok {
		// Update existing entry and move to front (most recently used)
		entry.ctx = ctx
		sr.lruList.MoveToFront(entry.element)
		sr.active = ctx
		return
	}

	// Evict LRU if at capacity
	for sr.lruList.Len() >= sr.maxSize {
		oldest := sr.lruList.Back()
		if oldest == nil {
			break
		}
		oldID := oldest.Value.(string)

		// Invalidate stale references by incrementing version
		if entry, ok := sr.schemas[oldID]; ok {
			entry.ctx.Version++
		}

		sr.lruList.Remove(oldest)
		delete(sr.schemas, oldID)
	}

	// Add new entry at front (most recently used)
	elem := sr.lruList.PushFront(ctx.ID)
	sr.schemas[ctx.ID] = &schemaEntry{ctx: ctx, element: elem}
	sr.active = ctx
}

// Get returns a schema by ID, or nil if not found.
// Accessing a schema marks it as recently used.
func (sr *SchemaRegistry) Get(id string) *SchemaContext {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	entry, ok := sr.schemas[id]
	if !ok {
		return nil
	}

	// Move to front (most recently used)
	sr.lruList.MoveToFront(entry.element)
	return entry.ctx
}

// SetActive sets the active schema by ID.
// Returns error if schema not found.
// Accessing a schema marks it as recently used.
func (sr *SchemaRegistry) SetActive(id string) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	entry, ok := sr.schemas[id]
	if !ok {
		return fmt.Errorf("schema not found: %s", id)
	}

	// Move to front (most recently used)
	sr.lruList.MoveToFront(entry.element)
	sr.active = entry.ctx
	return nil
}

// Active returns the currently active schema, or nil if none.
func (sr *SchemaRegistry) Active() *SchemaContext {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	return sr.active
}

// Clear removes a schema by ID.
func (sr *SchemaRegistry) Clear(id string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	entry, ok := sr.schemas[id]
	if !ok {
		return
	}

	if sr.active != nil && sr.active.ID == id {
		sr.active = nil
	}
	sr.lruList.Remove(entry.element)
	delete(sr.schemas, id)
}

// ClearActive clears the active schema (returns to normal string keys).
func (sr *SchemaRegistry) ClearActive() {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.active = nil
}

// ClearAll removes all schemas.
func (sr *SchemaRegistry) ClearAll() {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.schemas = make(map[string]*schemaEntry)
	sr.lruList = list.New()
	sr.active = nil
}

// Len returns the number of schemas in the registry.
func (sr *SchemaRegistry) Len() int {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	return len(sr.schemas)
}

// Errors for schema operations
var (
	ErrSchemaMissing     = errors.New("schema context missing")
	ErrSchemaKeyNotFound = errors.New("key not found in schema")
	ErrSchemaIDNotFound  = errors.New("key ID out of range")
	ErrSchemaMismatch    = errors.New("schema mismatch")
)

// ParseSchemaDirective parses a schema directive line.
// Formats:
//   - @schema#id @keys=[k1 k2 k3] - define new schema
//   - @schema#id - switch to existing schema
//   - @schema.clear - clear active schema
//
// Returns (schemaContext, isDefine, error)
func ParseSchemaDirective(line string) (*SchemaContext, bool, error) {
	line = strings.TrimSpace(line)

	// Handle clear directive
	if line == "@schema.clear" {
		return nil, false, nil
	}

	// Must start with @schema#
	if !strings.HasPrefix(line, "@schema#") {
		return nil, false, fmt.Errorf("expected @schema# prefix")
	}

	rest := line[8:] // skip "@schema#"

	// Find end of ID (space or end of string)
	idEnd := strings.IndexByte(rest, ' ')
	var id string
	if idEnd < 0 {
		// Just @schema#id (reference only)
		id = rest
		return &SchemaContext{ID: id}, false, nil
	}

	id = rest[:idEnd]
	rest = strings.TrimSpace(rest[idEnd+1:])

	// Accept both @keys=[...] and keys=[...] for compatibility
	if strings.HasPrefix(rest, "@keys=[") {
		rest = rest[7:] // skip "@keys=["
	} else if strings.HasPrefix(rest, "keys=[") {
		rest = rest[6:] // skip "keys=["
	} else {
		return nil, false, fmt.Errorf("expected @keys=[ or keys=[ after schema ID")
	}

	// Find closing bracket
	bracketCount := 1
	endIdx := -1
	for i, c := range rest {
		if c == '[' {
			bracketCount++
		} else if c == ']' {
			bracketCount--
			if bracketCount == 0 {
				endIdx = i
				break
			}
		}
	}

	if endIdx < 0 {
		return nil, false, fmt.Errorf("unclosed @keys=[]")
	}

	keysStr := rest[:endIdx]
	keys, err := parseKeysList(keysStr)
	if err != nil {
		return nil, false, err
	}

	ctx := NewSchemaContextWithID(id, keys)
	return ctx, true, nil
}

// parseKeysList parses a space-separated list of keys.
// Handles quoted keys for keys with spaces.
func parseKeysList(s string) ([]string, error) {
	var keys []string
	s = strings.TrimSpace(s)

	for len(s) > 0 {
		s = strings.TrimLeft(s, " \t")
		if len(s) == 0 {
			break
		}

		if s[0] == '"' {
			// Quoted key
			endIdx := 1
			for endIdx < len(s) {
				if s[endIdx] == '"' && (endIdx == 1 || s[endIdx-1] != '\\') {
					break
				}
				endIdx++
			}
			if endIdx >= len(s) {
				return nil, fmt.Errorf("unterminated quoted key")
			}
			key := unquoteKeyString(s[1:endIdx])
			keys = append(keys, key)
			s = s[endIdx+1:]
		} else {
			// Bare key
			endIdx := strings.IndexAny(s, " \t")
			if endIdx < 0 {
				keys = append(keys, s)
				break
			}
			keys = append(keys, s[:endIdx])
			s = s[endIdx:]
		}
	}

	return keys, nil
}

// unquoteKeyString removes escape sequences from a quoted key string.
func unquoteKeyString(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case '\\':
				b.WriteByte('\\')
			case '"':
				b.WriteByte('"')
			default:
				b.WriteByte(s[i+1])
			}
			i += 2
		} else {
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String()
}

// IsNumericKey checks if a string represents a numeric key reference.
func IsNumericKey(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// ParseNumericKey parses a numeric key string to an integer.
func ParseNumericKey(s string) (int, error) {
	return strconv.Atoi(s)
}

// keyNeedsQuoting checks if a key needs quoting.
func keyNeedsQuoting(s string) bool {
	if len(s) == 0 {
		return true
	}
	for i, c := range s {
		if i == 0 {
			if !schemaIsLetter(c) && c != '_' {
				return true
			}
		} else {
			if !schemaIsLetter(c) && !schemaIsDigit(c) && c != '_' && c != '-' {
				return true
			}
		}
	}
	// Check reserved words
	switch s {
	case "t", "f", "true", "false", "null", "none", "nil":
		return true
	}
	return false
}

func schemaIsLetter(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func schemaIsDigit(c rune) bool {
	return c >= '0' && c <= '9'
}
