package delta

import (
	"sync"
	"time"

	"github.com/Neumenon/cowrie"
)

// BaseEntry represents a stored base object for delta computation.
type BaseEntry struct {
	ID        uint64       // Unique identifier
	Value     *cowrie.Value // The base Cowrie value
	FieldDict []string     // Field dictionary for this object
	StoredAt  time.Time    // When this entry was stored
	Checksum  uint64       // Fast hash for integrity verification
}

// BaseStore provides an LRU cache for base objects used in delta computation.
// It tracks recently seen objects so deltas can reference them by ID.
type BaseStore struct {
	mu      sync.RWMutex
	entries map[uint64]*BaseEntry
	order   []uint64 // LRU order (newest at end)
	maxSize int
	nextID  uint64
}

// StoreConfig configures the base store.
type StoreConfig struct {
	// MaxEntries is the maximum number of base objects to retain.
	// Default: 1000
	MaxEntries int

	// StartID is the initial ID for new entries.
	// Default: 1
	StartID uint64
}

// DefaultStoreConfig returns default store configuration.
func DefaultStoreConfig() StoreConfig {
	return StoreConfig{
		MaxEntries: 1000,
		StartID:    1,
	}
}

// NewBaseStore creates a new base object store.
func NewBaseStore(cfg StoreConfig) *BaseStore {
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 1000
	}
	if cfg.StartID == 0 {
		cfg.StartID = 1
	}

	return &BaseStore{
		entries: make(map[uint64]*BaseEntry),
		order:   make([]uint64, 0, cfg.MaxEntries),
		maxSize: cfg.MaxEntries,
		nextID:  cfg.StartID,
	}
}

// Store adds a value to the store and returns its ID.
// If the store is full, the oldest entry is evicted.
func (s *BaseStore) Store(value *cowrie.Value) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.nextID
	s.nextID++

	// Extract field dictionary from object
	var fieldDict []string
	if value != nil && value.Type() == cowrie.TypeObject {
		members := value.Members()
		fieldDict = make([]string, len(members))
		for i, m := range members {
			fieldDict[i] = m.Key
		}
	}

	entry := &BaseEntry{
		ID:        id,
		Value:     value,
		FieldDict: fieldDict,
		StoredAt:  time.Now(),
		Checksum:  quickHash(value),
	}

	// Evict oldest if at capacity
	if len(s.entries) >= s.maxSize && len(s.order) > 0 {
		oldest := s.order[0]
		delete(s.entries, oldest)
		s.order = s.order[1:]
	}

	s.entries[id] = entry
	s.order = append(s.order, id)

	return id
}

// Get retrieves a base entry by ID.
func (s *BaseStore) Get(id uint64) (*BaseEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.entries[id]
	return entry, ok
}

// Touch marks an entry as recently used, moving it to the end of LRU.
func (s *BaseStore) Touch(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.entries[id]; !ok {
		return
	}

	// Remove from current position
	for i, oid := range s.order {
		if oid == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}

	// Add to end
	s.order = append(s.order, id)
}

// Evict removes an entry from the store.
func (s *BaseStore) Evict(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.entries, id)

	for i, oid := range s.order {
		if oid == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

// Size returns the number of entries in the store.
func (s *BaseStore) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// Clear removes all entries from the store.
func (s *BaseStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = make(map[uint64]*BaseEntry)
	s.order = s.order[:0]
}

// quickHash computes a fast hash of an Cowrie value for integrity checking.
// This is not cryptographic, just for quick change detection.
func quickHash(v *cowrie.Value) uint64 {
	if v == nil {
		return 0
	}

	// FNV-1a-like hash
	var h uint64 = 14695981039346656037

	hashByte := func(b byte) {
		h ^= uint64(b)
		h *= 1099511628211
	}

	hashUint64 := func(u uint64) {
		for i := 0; i < 8; i++ {
			hashByte(byte(u >> (i * 8)))
		}
	}

	hashString := func(s string) {
		hashUint64(uint64(len(s)))
		for i := 0; i < len(s); i++ {
			hashByte(s[i])
		}
	}

	// Hash type
	hashByte(byte(v.Type()))

	switch v.Type() {
	case cowrie.TypeNull:
		// Nothing more to hash
	case cowrie.TypeBool:
		if v.Bool() {
			hashByte(1)
		} else {
			hashByte(0)
		}
	case cowrie.TypeInt64:
		hashUint64(uint64(v.Int64()))
	case cowrie.TypeUint64:
		hashUint64(v.Uint64())
	case cowrie.TypeFloat64:
		// Use bit pattern
		hashUint64(uint64(v.Float64()))
	case cowrie.TypeString:
		hashString(v.String())
	case cowrie.TypeBytes:
		b := v.Bytes()
		hashUint64(uint64(len(b)))
		for _, bb := range b {
			hashByte(bb)
		}
	case cowrie.TypeArray:
		hashUint64(uint64(v.Len()))
		for i := 0; i < v.Len(); i++ {
			hashUint64(quickHash(v.Index(i)))
		}
	case cowrie.TypeObject:
		members := v.Members()
		hashUint64(uint64(len(members)))
		for _, m := range members {
			hashString(m.Key)
			hashUint64(quickHash(m.Value))
		}
	}

	return h
}

// ComputeAndStore computes a delta and stores the target as a new base.
// This is a convenience function for the common store-then-delta pattern.
func (s *BaseStore) ComputeAndStore(baseID uint64, target *cowrie.Value, cfg DiffConfig) (*Result, uint64) {
	s.mu.Lock()
	base, hasBase := s.entries[baseID]
	s.mu.Unlock()

	var result *Result
	if hasBase {
		result = Compute(base.Value, target, cfg)
		if result.Delta != nil {
			result.Delta.BaseID = baseID
		}
	} else {
		// No base: full replacement
		result = &Result{
			FullValue: target,
			Stats:     DiffStats{UsedDelta: false},
		}
	}

	// Store the new target as a base for future deltas
	newID := s.Store(target)

	return result, newID
}
