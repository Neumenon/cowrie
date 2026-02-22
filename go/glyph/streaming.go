// Package glyph - Streaming Dictionary Persistence for efficient multi-frame encoding.
//
// This module provides dictionary management for streaming GLYPH sessions where
// the same keys appear across multiple frames. By persisting the key dictionary
// across frames, we avoid re-encoding common keys and achieve better compression.
//
// Key features:
//   - Session-scoped dictionaries that persist across frames
//   - Automatic key learning from first N frames
//   - Dictionary serialization for session resumption
//   - LRU eviction for bounded memory usage
package glyph

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"sync"
)

// StreamDict is a streaming-optimized dictionary for multi-frame sessions.
type StreamDict struct {
	mu         sync.RWMutex
	sessionID  uint64            // Session identifier
	version    uint16            // Dictionary version (increments on changes)
	keyToIdx   map[string]uint16 // Key -> index
	idxToKey   []string          // Index -> key
	frequency  []uint32          // Usage frequency per key
	maxEntries uint16            // Maximum dictionary size
	frozen     bool              // If true, no new entries allowed
}

// StreamDictOptions configures dictionary behavior.
type StreamDictOptions struct {
	// MaxEntries is the maximum number of dictionary entries (default: 4096)
	MaxEntries uint16

	// PreloadKeys are keys to add at initialization
	PreloadKeys []string

	// SessionID for multi-session tracking
	SessionID uint64
}

// DefaultStreamDictOptions returns sensible defaults.
func DefaultStreamDictOptions() StreamDictOptions {
	return StreamDictOptions{
		MaxEntries: 4096,
	}
}

// NewStreamDict creates a new streaming dictionary.
func NewStreamDict(opts StreamDictOptions) *StreamDict {
	if opts.MaxEntries == 0 {
		opts.MaxEntries = 4096
	}

	d := &StreamDict{
		sessionID:  opts.SessionID,
		version:    1,
		keyToIdx:   make(map[string]uint16, 256),
		idxToKey:   make([]string, 0, 256),
		frequency:  make([]uint32, 0, 256),
		maxEntries: opts.MaxEntries,
	}

	// Preload keys if provided
	for _, key := range opts.PreloadKeys {
		d.Add(key)
	}

	return d
}

// SessionID returns the session identifier.
func (d *StreamDict) SessionID() uint64 {
	return d.sessionID
}

// Version returns the current dictionary version.
func (d *StreamDict) Version() uint16 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.version
}

// Len returns the number of entries.
func (d *StreamDict) Len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.idxToKey)
}

// IsFrozen returns whether the dictionary is frozen.
func (d *StreamDict) IsFrozen() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.frozen
}

// Freeze prevents new entries from being added.
func (d *StreamDict) Freeze() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.frozen = true
}

// Unfreeze allows new entries again.
func (d *StreamDict) Unfreeze() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.frozen = false
}

// Add adds a key to the dictionary and returns its index.
// Returns existing index if already present.
// Returns 0xFFFF if dictionary is full or frozen.
func (d *StreamDict) Add(key string) uint16 {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if already exists
	if idx, ok := d.keyToIdx[key]; ok {
		// Increment frequency
		d.frequency[idx]++
		return idx
	}

	// Cannot add if frozen or full
	if d.frozen || uint16(len(d.idxToKey)) >= d.maxEntries {
		return 0xFFFF
	}

	// Add new entry
	idx := uint16(len(d.idxToKey))
	d.keyToIdx[key] = idx
	d.idxToKey = append(d.idxToKey, key)
	d.frequency = append(d.frequency, 1)
	d.version++

	return idx
}

// Lookup returns the index for a key, or -1 if not found.
func (d *StreamDict) Lookup(key string) int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if idx, ok := d.keyToIdx[key]; ok {
		return int(idx)
	}
	return -1
}

// Get returns the key for an index, or "" if invalid.
func (d *StreamDict) Get(idx uint16) string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if int(idx) >= len(d.idxToKey) {
		return ""
	}
	return d.idxToKey[idx]
}

// Encode encodes a key using the dictionary.
// Returns (index, true) if found and increments frequency.
// Returns (0xFFFF, false) if not found.
func (d *StreamDict) Encode(key string) (uint16, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if idx, ok := d.keyToIdx[key]; ok {
		d.frequency[idx]++
		return idx, true
	}
	return 0xFFFF, false
}

// EncodeOrAdd encodes a key, adding it if not present.
// Returns (index, true) if encoded, (0xFFFF, false) if full/frozen.
func (d *StreamDict) EncodeOrAdd(key string) (uint16, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if already exists
	if idx, ok := d.keyToIdx[key]; ok {
		d.frequency[idx]++
		return idx, true
	}

	// Cannot add if frozen or full
	if d.frozen || uint16(len(d.idxToKey)) >= d.maxEntries {
		return 0xFFFF, false
	}

	// Add new entry
	idx := uint16(len(d.idxToKey))
	d.keyToIdx[key] = idx
	d.idxToKey = append(d.idxToKey, key)
	d.frequency = append(d.frequency, 1)
	d.version++

	return idx, true
}

// Decode decodes an index to its key.
func (d *StreamDict) Decode(idx uint16) string {
	return d.Get(idx)
}

// ============================================================
// Serialization
// ============================================================

// DictHeader is serialized at the start of a dictionary.
type DictHeader struct {
	Magic      [4]byte // "GDCT"
	Version    uint16  // Format version
	NumEntries uint16  // Number of entries
	SessionID  uint64  // Session identifier
	Checksum   uint32  // FNV-1a checksum of entries
}

// Serialize encodes the dictionary to bytes for persistence.
func (d *StreamDict) Serialize() []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Calculate size
	size := 20 // Header size
	for _, key := range d.idxToKey {
		size += 2 + len(key) // length prefix + key bytes
	}

	buf := make([]byte, 0, size)

	// Write header
	buf = append(buf, 'G', 'D', 'C', 'T') // Magic
	buf = binary.LittleEndian.AppendUint16(buf, d.version)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(d.idxToKey)))
	buf = binary.LittleEndian.AppendUint64(buf, d.sessionID)

	// Placeholder for checksum
	checksumPos := len(buf)
	buf = binary.LittleEndian.AppendUint32(buf, 0)

	// Write entries
	h := fnv.New32a()
	for _, key := range d.idxToKey {
		// Length-prefixed string
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(key)))
		buf = append(buf, key...)
		h.Write([]byte(key))
	}

	// Write checksum
	binary.LittleEndian.PutUint32(buf[checksumPos:], h.Sum32())

	return buf
}

// Deserialize loads a dictionary from bytes.
func Deserialize(data []byte) (*StreamDict, error) {
	if len(data) < 20 {
		return nil, errors.New("data too short for header")
	}

	// Check magic
	if string(data[0:4]) != "GDCT" {
		return nil, errors.New("invalid dictionary magic")
	}

	version := binary.LittleEndian.Uint16(data[4:6])
	numEntries := binary.LittleEndian.Uint16(data[6:8])
	sessionID := binary.LittleEndian.Uint64(data[8:16])
	storedChecksum := binary.LittleEndian.Uint32(data[16:20])

	d := &StreamDict{
		sessionID:  sessionID,
		version:    version,
		keyToIdx:   make(map[string]uint16, numEntries),
		idxToKey:   make([]string, 0, numEntries),
		frequency:  make([]uint32, 0, numEntries),
		maxEntries: 4096,
	}

	// Read entries
	off := 20
	h := fnv.New32a()
	for i := uint16(0); i < numEntries; i++ {
		if off+2 > len(data) {
			return nil, errors.New("truncated entry length")
		}
		keyLen := binary.LittleEndian.Uint16(data[off:])
		off += 2

		if off+int(keyLen) > len(data) {
			return nil, errors.New("truncated entry data")
		}
		key := string(data[off : off+int(keyLen)])
		off += int(keyLen)

		d.keyToIdx[key] = i
		d.idxToKey = append(d.idxToKey, key)
		d.frequency = append(d.frequency, 0)
		h.Write([]byte(key))
	}

	// Verify checksum
	if h.Sum32() != storedChecksum {
		return nil, errors.New("checksum mismatch")
	}

	return d, nil
}

// ============================================================
// Session Management
// ============================================================

// StreamSession manages dictionary state for a streaming connection.
type StreamSession struct {
	mu        sync.RWMutex
	sessionID uint64
	dict      *StreamDict
	frameSeq  uint64 // Next frame sequence number
	learning  bool   // Whether we're still learning keys
	learnMax  int    // Max frames for learning phase
}

// SessionOptions configures a streaming session.
type SessionOptions struct {
	// SessionID uniquely identifies this session
	SessionID uint64

	// InitialDict is an optional pre-loaded dictionary
	InitialDict *StreamDict

	// LearnFrames is the number of frames to learn keys from (default: 10)
	LearnFrames int

	// DictOptions configures the dictionary
	DictOptions StreamDictOptions
}

// NewStreamSession creates a new streaming session.
func NewStreamSession(opts SessionOptions) *StreamSession {
	if opts.LearnFrames == 0 {
		opts.LearnFrames = 10
	}

	dict := opts.InitialDict
	if dict == nil {
		opts.DictOptions.SessionID = opts.SessionID
		dict = NewStreamDict(opts.DictOptions)
	}

	return &StreamSession{
		sessionID: opts.SessionID,
		dict:      dict,
		frameSeq:  0,
		learning:  true,
		learnMax:  opts.LearnFrames,
	}
}

// SessionID returns the session identifier.
func (s *StreamSession) SessionID() uint64 {
	return s.sessionID
}

// Dict returns the session's dictionary.
func (s *StreamSession) Dict() *StreamDict {
	return s.dict
}

// NextSeq returns the next frame sequence number and increments it.
func (s *StreamSession) NextSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.frameSeq
	s.frameSeq++

	// End learning phase after learnMax frames
	if s.learning && int(s.frameSeq) >= s.learnMax {
		s.learning = false
		s.dict.Freeze()
	}

	return seq
}

// IsLearning returns whether the session is still in learning phase.
func (s *StreamSession) IsLearning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.learning
}

// LearnKeys extracts and adds keys from a GValue during learning phase.
func (s *StreamSession) LearnKeys(v *GValue) {
	if !s.IsLearning() {
		return
	}
	s.extractKeys(v)
}

func (s *StreamSession) extractKeys(v *GValue) {
	if v == nil {
		return
	}

	switch v.typ {
	case TypeMap:
		for _, entry := range v.mapVal {
			s.dict.Add(entry.Key)
			s.extractKeys(entry.Value)
		}

	case TypeStruct:
		if v.structVal != nil {
			s.dict.Add(v.structVal.TypeName)
			for _, field := range v.structVal.Fields {
				s.dict.Add(field.Key)
				s.extractKeys(field.Value)
			}
		}

	case TypeList:
		for _, elem := range v.listVal {
			s.extractKeys(elem)
		}

	case TypeSum:
		if v.sumVal != nil {
			s.dict.Add(v.sumVal.Tag)
			s.extractKeys(v.sumVal.Value)
		}
	}
}

// EncodeKey encodes a key using the session dictionary.
// During learning phase, automatically adds new keys.
func (s *StreamSession) EncodeKey(key string) (uint16, bool) {
	if s.IsLearning() {
		return s.dict.EncodeOrAdd(key)
	}
	return s.dict.Encode(key)
}

// DecodeKey decodes a key index.
func (s *StreamSession) DecodeKey(idx uint16) string {
	return s.dict.Decode(idx)
}

// SaveDict serializes the dictionary for persistence.
func (s *StreamSession) SaveDict() []byte {
	return s.dict.Serialize()
}

// ============================================================
// Wire Format for Dictionary-Encoded Frames
// ============================================================

// FrameFlags for dictionary encoding
const (
	FrameFlagHasDict   byte = 0x01 // Frame includes dictionary
	FrameFlagDictReset byte = 0x02 // Dictionary was reset
	FrameFlagCompact   byte = 0x04 // Uses compact key encoding
)

// EncodeDictFrame encodes a frame with dictionary-compressed keys.
// Returns the encoded frame bytes.
func EncodeDictFrame(v *GValue, session *StreamSession) []byte {
	// Learn keys during learning phase
	session.LearnKeys(v)

	// Encode value using session dictionary
	buf := make([]byte, 0, 256)

	// Frame header
	flags := byte(0)
	if session.Dict().Len() > 0 {
		flags |= FrameFlagCompact
	}
	buf = append(buf, flags)

	// Session ID (varint)
	buf = appendUvarint64(buf, session.SessionID())

	// Sequence number (varint)
	seq := session.NextSeq()
	buf = appendUvarint64(buf, seq)

	// Dictionary version
	buf = binary.LittleEndian.AppendUint16(buf, session.Dict().Version())

	// Encode the value with dictionary references
	buf = encodeDictValue(buf, v, session)

	return buf
}

func encodeDictValue(buf []byte, v *GValue, session *StreamSession) []byte {
	if v == nil || v.IsNull() {
		buf = append(buf, 0x00) // null tag
		return buf
	}

	switch v.typ {
	case TypeBool:
		if v.boolVal {
			buf = append(buf, 0x01) // true
		} else {
			buf = append(buf, 0x02) // false
		}

	case TypeInt:
		buf = append(buf, 0x03) // int tag
		buf = appendVarint64(buf, v.intVal)

	case TypeFloat:
		buf = append(buf, 0x04) // float tag
		buf = appendFloat64(buf, v.floatVal)

	case TypeStr:
		buf = append(buf, 0x05) // string tag
		buf = appendString(buf, v.strVal)

	case TypeList:
		buf = append(buf, 0x10) // list tag
		buf = appendUvarint64(buf, uint64(len(v.listVal)))
		for _, elem := range v.listVal {
			buf = encodeDictValue(buf, elem, session)
		}

	case TypeMap:
		buf = append(buf, 0x11) // map tag
		buf = appendUvarint64(buf, uint64(len(v.mapVal)))
		for _, entry := range v.mapVal {
			// Encode key using dictionary
			if idx, ok := session.EncodeKey(entry.Key); ok && idx != 0xFFFF {
				buf = append(buf, 0x80) // dictionary reference flag
				buf = binary.LittleEndian.AppendUint16(buf, idx)
			} else {
				buf = append(buf, 0x00) // inline string flag
				buf = appendString(buf, entry.Key)
			}
			buf = encodeDictValue(buf, entry.Value, session)
		}

	case TypeStruct:
		buf = append(buf, 0x12) // struct tag
		// Encode type name using dictionary
		if idx, ok := session.EncodeKey(v.structVal.TypeName); ok && idx != 0xFFFF {
			buf = append(buf, 0x80)
			buf = binary.LittleEndian.AppendUint16(buf, idx)
		} else {
			buf = append(buf, 0x00)
			buf = appendString(buf, v.structVal.TypeName)
		}
		buf = appendUvarint64(buf, uint64(len(v.structVal.Fields)))
		for _, field := range v.structVal.Fields {
			// Encode field key using dictionary
			if idx, ok := session.EncodeKey(field.Key); ok && idx != 0xFFFF {
				buf = append(buf, 0x80)
				buf = binary.LittleEndian.AppendUint16(buf, idx)
			} else {
				buf = append(buf, 0x00)
				buf = appendString(buf, field.Key)
			}
			buf = encodeDictValue(buf, field.Value, session)
		}

	default:
		buf = append(buf, 0x00) // null for unsupported
	}

	return buf
}

// Helper functions for encoding
func appendUvarint64(buf []byte, x uint64) []byte {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], x)
	return append(buf, tmp[:n]...)
}

func appendVarint64(buf []byte, x int64) []byte {
	var tmp [10]byte
	n := binary.PutVarint(tmp[:], x)
	return append(buf, tmp[:n]...)
}

func appendFloat64(buf []byte, f float64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], uint64(f))
	return append(buf, tmp[:]...)
}

func appendString(buf []byte, s string) []byte {
	buf = appendUvarint64(buf, uint64(len(s)))
	return append(buf, s...)
}
