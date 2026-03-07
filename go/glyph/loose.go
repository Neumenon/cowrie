package glyph

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ============================================================
// GLYPH-Loose Canonicalization
// ============================================================
//
// Provides deterministic canonical string representation for GValues
// in schema-optional mode. Used for hashing, comparison, and deduplication.
//
// Canonical rules:
// - null → "_" (default) or "∅"
// - bool → "t" / "f"
// - int → decimal, no leading zeros, -0 → 0
// - float → shortest roundtrip, E→e, -0→0
// - string → bare if safe, otherwise quoted
// - bytes → "b64" + quoted base64
// - time → ISO-8601 UTC
// - id → ^prefix:value or ^"quoted"
// - list → "[" + space-separated elements + "]"
// - map → "{" + sorted key=value pairs + "}"
//   Keys sorted by bytewise UTF-8 of canonString(key)

// ============================================================
// Object Pools for Allocation Reduction
// ============================================================

// sortableMapEntryPool provides reusable slices for map sorting.
var sortableMapEntryPool = sync.Pool{
	New: func() interface{} {
		slice := make([]sortableMapEntry, 0, 32)
		return &slice
	},
}

// sortableColPool provides reusable slices for column sorting.
var sortableColPool = sync.Pool{
	New: func() interface{} {
		slice := make([]sortableCol, 0, 32)
		return &slice
	},
}

// stringBuilderPool provides reusable string builders.
var stringBuilderPool = sync.Pool{
	New: func() interface{} {
		return &strings.Builder{}
	},
}

// getPooledBuilder gets a builder from pool and resets it.
func getPooledBuilder() *strings.Builder {
	b := stringBuilderPool.Get().(*strings.Builder)
	b.Reset()
	return b
}

// putPooledBuilder returns a builder to the pool.
func putPooledBuilder(b *strings.Builder) {
	// Only return reasonably sized builders to the pool
	if b.Cap() < 64*1024 { // 64KB max
		stringBuilderPool.Put(b)
	}
}

// CanonicalizeLoose returns a deterministic canonical string for any GValue.
// This function produces identical output for semantically equal values,
// making it suitable for hashing, comparison, and deduplication.
//
// Smart auto-tabular is ENABLED by default (v2.3.0+):
// - Lists of 3+ homogeneous objects → @tab blocks (35-65% token savings)
// - All other data → standard format (unchanged)
//
// Use CanonicalizeLooseNoTabular for backward-compatible output.
func CanonicalizeLoose(v *GValue) string {
	return CanonicalizeLooseWithOpts(v, DefaultLooseCanonOpts())
}

// CanonicalizeLooseNoTabular returns canonical form WITHOUT auto-tabular.
// Use for v2.2.x backward compatibility or when tabular format is not desired.
func CanonicalizeLooseNoTabular(v *GValue) string {
	return CanonicalizeLooseWithOpts(v, NoTabularLooseCanonOpts())
}

// CanonicalizeLooseTabular is an alias for CanonicalizeLoose.
// Deprecated: auto-tabular is now the default.
func CanonicalizeLooseTabular(v *GValue) string {
	return CanonicalizeLoose(v)
}

// canonBytes returns canonical bytes representation: b64"..."
func canonBytes(data []byte) string {
	if len(data) == 0 {
		return `b64""`
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	return "b64" + quoteString(encoded)
}

// writeCanonBytes writes canonical bytes representation to builder.
func writeCanonBytes(b *strings.Builder, data []byte) {
	if len(data) == 0 {
		b.WriteString(`b64""`)
		return
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	b.WriteString(`b64"`)
	b.WriteString(encoded)
	b.WriteByte('"')
}

// base64Encode encodes bytes to standard base64.
// Deprecated: Use base64.StdEncoding.EncodeToString directly.
func base64Encode(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// ============================================================
// Loose Mode Hash/Fingerprint
// ============================================================

// FingerprintLoose returns a deterministic fingerprint string for a GValue.
// Useful for caching, deduplication, and equality checks.
func FingerprintLoose(v *GValue) string {
	return CanonicalizeLoose(v)
}

// EqualLoose checks if two GValues are semantically equal using loose canonicalization.
func EqualLoose(a, b *GValue) bool {
	return CanonicalizeLoose(a) == CanonicalizeLoose(b)
}

// ============================================================
// GLYPH v2.4.0: Schema Header + Compact Keys
// ============================================================
//
// Schema headers enable compact key encoding for repeated objects:
//
// Inline schema (self-contained):
//   @schema#abc123 keys=[action query confidence sources]
//   {#0=search #1="weather NYC" #2=0.95 #3=[web news]}
//
// External schema ref (receiver must have schema):
//   @schema#abc123
//   {#0=search #1="weather NYC" #2=0.95 #3=[web news]}
//
// This provides significant token savings for tool calls, agent traces,
// and other repeated structured outputs.

// CanonicalizeLooseWithSchema returns canonical form with schema header.
// If opts.Schema is set, uses the SchemaContext for header and compact keys.
// If opts.KeyDict is set and opts.UseCompactKeys is true, keys are emitted as #N.
// If opts.SchemaRef is set, a @schema header is prepended.
func CanonicalizeLooseWithSchema(v *GValue, opts LooseCanonOpts) string {
	var b strings.Builder

	// Emit schema header if configured
	if opts.Schema != nil || opts.SchemaRef != "" || len(opts.KeyDict) > 0 {
		b.WriteString(emitSchemaHeader(opts))
		b.WriteByte('\n')
	}

	// Emit the value
	b.WriteString(canonLooseWithOpts(v, opts))

	return b.String()
}

// emitSchemaHeader creates the @schema header line.
// Format: @schema#<id> @keys=[key1 key2 ...]
func emitSchemaHeader(opts LooseCanonOpts) string {
	// If Schema is set, use its header emission
	if opts.Schema != nil {
		return opts.Schema.EmitHeader(true)
	}

	// Legacy: use SchemaRef and KeyDict
	var b strings.Builder
	b.WriteString("@schema")

	if opts.SchemaRef != "" {
		b.WriteByte('#')
		b.WriteString(opts.SchemaRef)
	}

	if len(opts.KeyDict) > 0 {
		b.WriteString(" @keys=[")
		for i, key := range opts.KeyDict {
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(canonString(key))
		}
		b.WriteByte(']')
	}

	return b.String()
}

// BuildKeyDictFromValue extracts all unique keys from a value.
// Useful for auto-generating a key dictionary for repeated objects.
func BuildKeyDictFromValue(v *GValue) []string {
	keySet := make(map[string]struct{})
	collectKeys(v, keySet)

	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}

	// Sort for determinism
	sort.Strings(keys)
	return keys
}

// collectKeys recursively collects all map/struct keys.
func collectKeys(v *GValue, keySet map[string]struct{}) {
	if v == nil {
		return
	}

	switch v.typ {
	case TypeMap:
		for _, e := range v.mapVal {
			keySet[e.Key] = struct{}{}
			collectKeys(e.Value, keySet)
		}
	case TypeStruct:
		if v.structVal != nil {
			for _, f := range v.structVal.Fields {
				keySet[f.Key] = struct{}{}
				collectKeys(f.Value, keySet)
			}
		}
	case TypeList:
		for _, item := range v.listVal {
			collectKeys(item, keySet)
		}
	}
}

// ParseSchemaHeader parses a @schema header line.
// Returns schemaRef, keyDict, and any error.
func ParseSchemaHeader(line string) (schemaRef string, keyDict []string, err error) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "@schema") {
		return "", nil, fmt.Errorf("not a schema header: %s", line)
	}

	rest := strings.TrimPrefix(line, "@schema")

	// Parse schema hash if present
	if strings.HasPrefix(rest, "#") {
		rest = rest[1:]
		// Find end of hash (space or end of string)
		end := strings.IndexByte(rest, ' ')
		if end == -1 {
			schemaRef = rest
			return schemaRef, nil, nil
		}
		schemaRef = rest[:end]
		rest = strings.TrimSpace(rest[end:])
	}

	// Parse keys= if present
	if strings.HasPrefix(rest, "keys=") {
		rest = strings.TrimPrefix(rest, "keys=")
		if !strings.HasPrefix(rest, "[") {
			return "", nil, fmt.Errorf("keys= must be followed by []: %s", rest)
		}
		closeIdx := strings.Index(rest, "]")
		if closeIdx == -1 {
			return "", nil, fmt.Errorf("missing ] in keys: %s", rest)
		}
		keysStr := rest[1:closeIdx]
		if keysStr != "" {
			keyDict = strings.Fields(keysStr)
		}
	}

	return schemaRef, keyDict, nil
}

// ============================================================
// GLYPH-Loose Auto-Tabular Mode (v2.3.0)
// ============================================================
//
// Auto-tabular mode detects homogeneous arrays of objects and
// emits them in a compact tabular format:
//
//   @tab _ [col1 col2 col3]
//   |val1|val2|val3|
//   |val4|val5|val6|
//   @end
//
// This provides ZON-like byte efficiency while preserving
// GLYPH's token efficiency.

// NullStyle controls how null values are emitted.
type NullStyle uint8

const (
	// NullStyleSymbol emits null as ∅ (default for human-readable output)
	NullStyleSymbol NullStyle = iota
	// NullStyleUnderscore emits null as _ (LLM-friendly, ASCII-safe)
	NullStyleUnderscore
)

// LooseCanonOpts configures loose canonicalization behavior.
type LooseCanonOpts struct {
	AutoTabular  bool // Enable tabular detection for homogeneous arrays (default: true)
	MinRows      int  // Minimum rows for tabular mode (default: 3)
	MaxCols      int  // Maximum columns allowed (default: 64)
	AllowMissing bool // Fill missing keys with null (default: true)

	// v2.4.0: Null style and schema support
	NullStyle      NullStyle // How to emit null values (default: _)
	SchemaRef      string    // Optional schema hash/id for @schema header
	KeyDict        []string  // Optional key dictionary for compact keys
	UseCompactKeys bool      // Emit #N instead of field names when KeyDict is set

	// v2.6.0: Schema context (alternative to KeyDict)
	// If set, takes precedence over KeyDict
	Schema *SchemaContext
}

// DefaultLooseCanonOpts returns default options with smart auto-tabular ENABLED.
// Lists of 3+ homogeneous objects are automatically emitted as @tab blocks.
// Non-eligible data gracefully falls back to standard format.
// Uses _ for null (ASCII-safe, single token - LLM friendly).
func DefaultLooseCanonOpts() LooseCanonOpts {
	return LooseCanonOpts{
		AutoTabular:  true,
		MinRows:      3,
		MaxCols:      64,
		AllowMissing: true,
		NullStyle:    NullStyleUnderscore,
	}
}

// LLMLooseCanonOpts returns options optimized for LLM output.
// Uses _ for null (ASCII-safe, single token), auto-tabular enabled.
// Note: This is now the same as DefaultLooseCanonOpts since _ is the new default.
func LLMLooseCanonOpts() LooseCanonOpts {
	return LooseCanonOpts{
		AutoTabular:  true,
		MinRows:      3,
		MaxCols:      64,
		AllowMissing: true,
		NullStyle:    NullStyleUnderscore,
	}
}

// PrettyLooseCanonOpts returns options for human-readable "pretty" output.
// Uses ∅ for null (unicode symbol) for nicer visual appearance.
func PrettyLooseCanonOpts() LooseCanonOpts {
	return LooseCanonOpts{
		AutoTabular:  true,
		MinRows:      3,
		MaxCols:      64,
		AllowMissing: true,
		NullStyle:    NullStyleSymbol,
	}
}

// NoTabularLooseCanonOpts returns options with auto-tabular DISABLED.
// Use for backward compatibility or when tabular format is not desired.
func NoTabularLooseCanonOpts() LooseCanonOpts {
	return LooseCanonOpts{
		AutoTabular:  false,
		MinRows:      3,
		MaxCols:      64,
		AllowMissing: true,
	}
}

// TabularLooseCanonOpts is an alias for DefaultLooseCanonOpts.
// Deprecated: auto-tabular is now the default.
func TabularLooseCanonOpts() LooseCanonOpts {
	return DefaultLooseCanonOpts()
}

// SchemaLooseCanonOpts returns options with schema context enabled.
// This enables compact key encoding using numeric indices.
func SchemaLooseCanonOpts(schema *SchemaContext) LooseCanonOpts {
	return LooseCanonOpts{
		AutoTabular:    true,
		MinRows:        3,
		MaxCols:        64,
		AllowMissing:   true,
		NullStyle:      NullStyleUnderscore,
		Schema:         schema,
		UseCompactKeys: true,
	}
}

// CanonicalizeLooseWithOpts returns canonical string with configurable options.
func CanonicalizeLooseWithOpts(v *GValue, opts LooseCanonOpts) string {
	if v == nil {
		return canonNull()
	}

	// Apply defaults for zero values
	if opts.MinRows == 0 {
		opts.MinRows = 3
	}
	if opts.MaxCols == 0 {
		opts.MaxCols = 64
	}

	return canonLooseWithOpts(v, opts)
}

// canonNullWithStyle returns the null representation based on style.
func canonNullWithStyle(style NullStyle) string {
	if style == NullStyleUnderscore {
		return "_"
	}
	return "∅"
}

// writeNullWithStyle writes null representation to builder.
func writeNullWithStyle(b *strings.Builder, style NullStyle) {
	if style == NullStyleUnderscore {
		b.WriteByte('_')
	} else {
		b.WriteString("∅")
	}
}

// canonLooseWithOpts is the internal implementation with options.
// This version builds a string and returns it.
func canonLooseWithOpts(v *GValue, opts LooseCanonOpts) string {
	b := getPooledBuilder()
	writeCanonLoose(b, v, opts)
	result := b.String()
	putPooledBuilder(b)
	return result
}

// writeCanonLoose writes the canonical representation to the builder.
// This is the core buffer-based implementation that avoids intermediate allocations.
func writeCanonLoose(b *strings.Builder, v *GValue, opts LooseCanonOpts) {
	if v == nil {
		writeNullWithStyle(b, opts.NullStyle)
		return
	}

	switch v.typ {
	case TypeNull:
		writeNullWithStyle(b, opts.NullStyle)
	case TypeBool:
		if v.boolVal {
			b.WriteByte('t')
		} else {
			b.WriteByte('f')
		}
	case TypeInt:
		if v.intVal == 0 {
			b.WriteByte('0')
		} else {
			b.WriteString(strconv.FormatInt(v.intVal, 10))
		}
	case TypeFloat:
		if v.floatVal == 0 {
			b.WriteByte('0')
		} else {
			s := strconv.FormatFloat(v.floatVal, 'g', -1, 64)
			s = strings.ReplaceAll(s, "E", "e")
			if s == "-0" {
				b.WriteByte('0')
			} else {
				b.WriteString(s)
			}
		}
	case TypeStr:
		writeCanonString(b, v.strVal)
	case TypeBytes:
		writeCanonBytes(b, v.bytesVal)
	case TypeTime:
		b.WriteString(v.timeVal.UTC().Format("2006-01-02T15:04:05Z"))
	case TypeID:
		writeCanonRef(b, v.idVal)
	case TypeList:
		writeListLoose(b, v.listVal, opts)
	case TypeMap:
		writeMapLoose(b, v.mapVal, opts)
	case TypeStruct:
		writeStructLoose(b, v.structVal, opts)
	case TypeSum:
		writeSumLoose(b, v.sumVal, opts)
	default:
		b.WriteString("∅")
	}
}

// writeCanonString writes the canonical string representation to builder.
func writeCanonString(b *strings.Builder, s string) {
	if isBareSafeV2(s) {
		b.WriteString(s)
	} else {
		writeQuotedString(b, s)
	}
}

// writeQuotedString writes a quoted string with minimal escapes.
func writeQuotedString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 {
				b.WriteString(`\u00`)
				hex := strconv.FormatInt(int64(r), 16)
				if len(hex) == 1 {
					b.WriteByte('0')
				}
				b.WriteString(strings.ToUpper(hex))
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
}

// writeCanonRef writes the canonical reference representation to builder.
func writeCanonRef(b *strings.Builder, r RefID) {
	full := r.String()[1:] // Remove leading ^ from String()
	if isRefSafe(full) {
		b.WriteByte('^')
		b.WriteString(full)
	} else {
		b.WriteByte('^')
		writeQuotedString(b, full)
	}
}

// writeListLoose writes a list to the builder.
func writeListLoose(b *strings.Builder, items []*GValue, opts LooseCanonOpts) {
	if len(items) == 0 {
		b.WriteString("[]")
		return
	}

	// Try tabular detection if enabled
	if opts.AutoTabular {
		if cols, ok := detectTabular(items, opts); ok {
			writeTabularLoose(b, items, cols, opts)
			return
		}
	}

	// Fall back to standard list format
	b.WriteByte('[')
	for i, item := range items {
		if i > 0 {
			b.WriteByte(' ')
		}
		writeCanonLoose(b, item, opts)
	}
	b.WriteByte(']')
}

// writeMapLoose writes a map to the builder using pooled sortable slice.
func writeMapLoose(b *strings.Builder, entries []MapEntry, opts LooseCanonOpts) {
	if len(entries) == 0 {
		b.WriteString("{}")
		return
	}

	// Get sortable slice from pool
	sortablePtr := sortableMapEntryPool.Get().(*[]sortableMapEntry)
	sortable := *sortablePtr

	// Ensure capacity and reset length
	if cap(sortable) < len(entries) {
		sortable = make([]sortableMapEntry, len(entries))
	} else {
		sortable = sortable[:len(entries)]
	}

	// Pre-compute canonical keys for sorting
	for i, e := range entries {
		sortable[i] = sortableMapEntry{
			canonKey: canonString(e.Key),
			entry:    e,
		}
	}

	// Sort by pre-computed canonical key
	sort.Slice(sortable, func(i, j int) bool {
		return sortable[i].canonKey < sortable[j].canonKey
	})

	// Build key index map for O(1) lookup (if using compact keys)
	var keyIndex map[string]int
	if opts.UseCompactKeys {
		if opts.Schema != nil {
			keyIndex = nil // Will use Schema.LookupKey instead
		} else if len(opts.KeyDict) > 0 {
			keyIndex = make(map[string]int, len(opts.KeyDict))
			for i, k := range opts.KeyDict {
				keyIndex[k] = i
			}
		}
	}

	b.WriteByte('{')
	for i, se := range sortable {
		if i > 0 {
			b.WriteByte(' ')
		}
		// Use compact key if enabled and key is in schema or dictionary
		if opts.UseCompactKeys {
			idx := -1
			if opts.Schema != nil {
				idx = opts.Schema.LookupKey(se.entry.Key)
			} else if keyIndex != nil {
				if id, ok := keyIndex[se.entry.Key]; ok {
					idx = id
				}
			}
			if idx >= 0 {
				b.WriteByte('#')
				b.WriteString(strconv.Itoa(idx))
			} else {
				b.WriteString(se.canonKey)
			}
		} else {
			b.WriteString(se.canonKey)
		}
		b.WriteByte('=')
		writeCanonLoose(b, se.entry.Value, opts)
	}
	b.WriteByte('}')

	// Return slice to pool
	*sortablePtr = sortable[:0]
	sortableMapEntryPool.Put(sortablePtr)
}

// writeStructLoose writes a struct to the builder.
func writeStructLoose(b *strings.Builder, s *StructValue, opts LooseCanonOpts) {
	if s == nil || len(s.Fields) == 0 {
		b.WriteString("{}")
		return
	}
	writeMapLoose(b, s.Fields, opts)
}

// writeSumLoose writes a sum/union to the builder.
func writeSumLoose(b *strings.Builder, s *SumValue, opts LooseCanonOpts) {
	if s == nil {
		b.WriteString("{}")
		return
	}
	b.WriteByte('{')
	writeCanonString(b, s.Tag)
	b.WriteByte('=')
	writeCanonLoose(b, s.Value, opts)
	b.WriteByte('}')
}

// ============================================================
// Tabular Detection and Emission
// ============================================================

// sortableMapEntry holds a map entry with its pre-computed canonical key for sorting.
type sortableMapEntry struct {
	canonKey string
	entry    MapEntry
}

// sortableCol holds a column name with its pre-computed canonical form for sorting.
type sortableCol struct {
	canonKey string
	key      string
}

// detectTabular checks if a list qualifies for tabular format.
// Returns the sorted column names and true if tabular is applicable.
func detectTabular(items []*GValue, opts LooseCanonOpts) ([]string, bool) {
	if len(items) < opts.MinRows {
		return nil, false
	}

	// Collect all keys from all elements
	keySet := make(map[string]struct{})
	var commonKeys map[string]struct{}
	var firstKeys map[string]struct{}

	for _, item := range items {
		keys := getObjectKeys(item)
		if keys == nil {
			// Not an object/map/struct - can't tabularize
			return nil, false
		}
		if len(keys) == 0 {
			return nil, false
		}
		itemSet := make(map[string]struct{}, len(keys))
		for _, k := range keys {
			keySet[k] = struct{}{}
			itemSet[k] = struct{}{}
		}
		if firstKeys == nil {
			firstKeys = make(map[string]struct{}, len(itemSet))
			for k := range itemSet {
				firstKeys[k] = struct{}{}
			}
		} else if !opts.AllowMissing {
			if len(itemSet) != len(firstKeys) {
				return nil, false
			}
			for k := range firstKeys {
				if _, ok := itemSet[k]; !ok {
					return nil, false
				}
			}
		}
		if commonKeys == nil {
			commonKeys = make(map[string]struct{}, len(itemSet))
			for k := range itemSet {
				commonKeys[k] = struct{}{}
			}
		} else {
			for k := range commonKeys {
				if _, ok := itemSet[k]; !ok {
					delete(commonKeys, k)
				}
			}
		}
	}

	// Check column count
	if len(keySet) == 0 || len(keySet) > opts.MaxCols {
		return nil, false
	}

	// With AllowMissing, require at least 50% shared keys across rows
	if opts.AllowMissing {
		if commonKeys == nil || len(commonKeys)*2 < len(keySet) {
			return nil, false
		}
	}

	// Pre-compute canonical keys for sorting (avoids O(n log n) canonString calls)
	sortable := make([]sortableCol, 0, len(keySet))
	for k := range keySet {
		sortable = append(sortable, sortableCol{
			canonKey: canonString(k),
			key:      k,
		})
	}
	sort.Slice(sortable, func(i, j int) bool {
		return sortable[i].canonKey < sortable[j].canonKey
	})

	// Extract sorted column names
	cols := make([]string, len(sortable))
	for i, sc := range sortable {
		cols[i] = sc.key
	}

	return cols, true
}

// getObjectKeys returns the keys of a map/struct/object, or nil if not an object type.
func getObjectKeys(v *GValue) []string {
	if v == nil {
		return nil
	}

	switch v.typ {
	case TypeMap:
		keys := make([]string, len(v.mapVal))
		for i, e := range v.mapVal {
			keys[i] = e.Key
		}
		return keys
	case TypeStruct:
		if v.structVal == nil {
			return []string{}
		}
		keys := make([]string, len(v.structVal.Fields))
		for i, f := range v.structVal.Fields {
			keys[i] = f.Key
		}
		return keys
	default:
		return nil
	}
}

// getObjectValue returns the value for a key in a map/struct, or nil if not found.
func getObjectValue(v *GValue, key string) *GValue {
	if v == nil {
		return nil
	}

	switch v.typ {
	case TypeMap:
		for _, e := range v.mapVal {
			if e.Key == key {
				return e.Value
			}
		}
		return nil
	case TypeStruct:
		if v.structVal == nil {
			return nil
		}
		for _, f := range v.structVal.Fields {
			if f.Key == key {
				return f.Value
			}
		}
		return nil
	default:
		return nil
	}
}

// writeTabularLoose writes tabular format to the builder.
// v2.4.0: Includes rows/cols metadata for streaming resync.
func writeTabularLoose(b *strings.Builder, items []*GValue, cols []string, opts LooseCanonOpts) {
	// Build key index map for O(1) lookup (if using compact keys)
	var keyIndex map[string]int
	if opts.UseCompactKeys && len(opts.KeyDict) > 0 {
		keyIndex = make(map[string]int, len(opts.KeyDict))
		for i, k := range opts.KeyDict {
			keyIndex[k] = i
		}
	}

	// Header: @tab _ rows=N cols=M [col1 col2 ...]
	b.WriteString("@tab _ rows=")
	b.WriteString(strconv.Itoa(len(items)))
	b.WriteString(" cols=")
	b.WriteString(strconv.Itoa(len(cols)))
	b.WriteString(" [")
	for i, col := range cols {
		if i > 0 {
			b.WriteByte(' ')
		}
		if keyIndex != nil {
			if idx, ok := keyIndex[col]; ok {
				b.WriteByte('#')
				b.WriteString(strconv.Itoa(idx))
			} else {
				writeCanonString(b, col)
			}
		} else {
			writeCanonString(b, col)
		}
	}
	b.WriteString("]\n")

	// Rows: |val1|val2|...|
	// Use a temporary builder for cell values to enable escaping
	cellBuilder := getPooledBuilder()
	for _, item := range items {
		b.WriteByte('|')
		for i, col := range cols {
			if i > 0 {
				b.WriteByte('|')
			}
			val := getObjectValue(item, col)
			if val == nil {
				writeNullWithStyle(b, opts.NullStyle)
			} else {
				// Write to temp builder, then escape and write to main builder
				cellBuilder.Reset()
				writeCanonLoose(cellBuilder, val, opts)
				cellStr := cellBuilder.String()
				writeEscapedTabularCell(b, cellStr)
			}
		}
		b.WriteString("|\n")
	}
	putPooledBuilder(cellBuilder)

	// Footer
	b.WriteString("@end")
}

// writeEscapedTabularCell writes an escaped cell value to the builder.
func writeEscapedTabularCell(b *strings.Builder, s string) {
	// Fast path: no escaping needed
	if !strings.Contains(s, "|") {
		b.WriteString(s)
		return
	}

	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '|' {
			b.WriteString(`\|`)
		} else {
			b.WriteByte(c)
		}
	}
}

// emitTabularLoose emits a list of objects in tabular format.
// v2.4.0: Includes rows/cols metadata for streaming resync.
func emitTabularLoose(items []*GValue, cols []string, opts LooseCanonOpts) string {
	b := getPooledBuilder()
	writeTabularLoose(b, items, cols, opts)
	result := b.String()
	putPooledBuilder(b)
	return result
}

// escapeTabularCell escapes | characters in a cell value.
// Note: backslashes are NOT escaped here because the cell value is already
// a valid canonical GLYPH string with its own escape sequences.
// We only escape | to prevent it from being interpreted as a cell delimiter.
func escapeTabularCell(s string) string {
	// Fast path: no escaping needed
	if !strings.Contains(s, "|") {
		return s
	}

	var b strings.Builder
	b.Grow(len(s) + 4) // Small buffer for escapes

	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '|' {
			b.WriteString(`\|`)
		} else {
			b.WriteByte(c)
		}
	}

	return b.String()
}

// unescapeTabularCell unescapes \| in a cell value.
// Note: only \| is unescaped; backslashes are part of the inner GLYPH string.
func unescapeTabularCell(s string) string {
	// Fast path: no escaping present
	if !strings.Contains(s, `\|`) {
		return s
	}

	return strings.ReplaceAll(s, `\|`, "|")
}

// ============================================================
// Tabular Loose Parsing
// ============================================================

// ParseTabularLoose parses a @tab _ block into a list of maps.
// Input format:
//
//	@tab _ [col1 col2 col3]
//	|val1|val2|val3|
//	|val4|val5|val6|
//	@end
//
// Returns a list of maps, where each map has the column names as keys.
func ParseTabularLoose(input string) (*GValue, error) {
	lines := strings.Split(input, "\n")
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty tabular input")
	}

	// Find and parse header
	headerIdx := -1
	var cols []string
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "@tab _ ") {
			var err error
			cols, err = parseTabularLooseHeader(line)
			if err != nil {
				return nil, err
			}
			headerIdx = i
			break
		}
		return nil, fmt.Errorf("expected @tab _ header, got: %s", line)
	}

	if headerIdx == -1 {
		return nil, fmt.Errorf("missing @tab _ header")
	}

	// Parse rows
	var rows []*GValue
	for i := headerIdx + 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])

		// Skip empty lines
		if line == "" {
			continue
		}

		// Check for @end
		if line == "@end" {
			break
		}

		// Parse row
		row, err := parseTabularLooseRow(line, cols)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i-headerIdx, err)
		}
		rows = append(rows, row)
	}

	return List(rows...), nil
}

// TabularMetadata contains metadata from a tabular header.
type TabularMetadata struct {
	Rows int      // Expected row count (-1 if not specified)
	Cols int      // Expected column count (-1 if not specified)
	Keys []string // Column names
}

// parseTabularLooseHeader parses: @tab _ [col1 col2 col3]
// Also accepts v2.4.0 format: @tab _ rows=N cols=M [col1 col2 col3]
func parseTabularLooseHeader(line string) ([]string, error) {
	meta, err := parseTabularLooseHeaderWithMeta(line)
	if err != nil {
		return nil, err
	}
	return meta.Keys, nil
}

// parseTabularLooseHeaderWithMeta parses header with full metadata.
func parseTabularLooseHeaderWithMeta(line string) (*TabularMetadata, error) {
	// Remove @tab _ prefix
	rest := strings.TrimPrefix(line, "@tab _ ")
	rest = strings.TrimSpace(rest)

	meta := &TabularMetadata{Rows: -1, Cols: -1}

	// Parse optional rows=N and cols=M before [
	for !strings.HasPrefix(rest, "[") && len(rest) > 0 {
		if strings.HasPrefix(rest, "rows=") {
			rest = rest[5:]
			end := strings.IndexAny(rest, " [")
			if end == -1 {
				return nil, fmt.Errorf("invalid rows= value")
			}
			var n int
			if _, err := fmt.Sscanf(rest[:end], "%d", &n); err == nil {
				meta.Rows = n
			}
			rest = strings.TrimSpace(rest[end:])
		} else if strings.HasPrefix(rest, "cols=") {
			rest = rest[5:]
			end := strings.IndexAny(rest, " [")
			if end == -1 {
				return nil, fmt.Errorf("invalid cols= value")
			}
			var n int
			if _, err := fmt.Sscanf(rest[:end], "%d", &n); err == nil {
				meta.Cols = n
			}
			rest = strings.TrimSpace(rest[end:])
		} else {
			// Skip unknown attributes
			spaceIdx := strings.IndexByte(rest, ' ')
			bracketIdx := strings.IndexByte(rest, '[')
			if spaceIdx == -1 && bracketIdx == -1 {
				return nil, fmt.Errorf("expected '[' in header, got: %s", rest)
			}
			if spaceIdx >= 0 && (bracketIdx == -1 || spaceIdx < bracketIdx) {
				rest = strings.TrimSpace(rest[spaceIdx:])
			} else {
				break
			}
		}
	}

	// Must start with [
	if !strings.HasPrefix(rest, "[") {
		return nil, fmt.Errorf("expected '[' in header, got: %s", rest)
	}

	// Find closing ]
	closeIdx := strings.Index(rest, "]")
	if closeIdx == -1 {
		return nil, fmt.Errorf("missing ']' in header")
	}

	// Parse column names
	colStr := rest[1:closeIdx]
	if colStr == "" {
		meta.Keys = []string{}
	} else {
		// Split by whitespace
		meta.Keys = strings.Fields(colStr)
	}

	return meta, nil
}

// parseTabularLooseRow parses: |val1|val2|val3|
func parseTabularLooseRow(line string, cols []string) (*GValue, error) {
	// Must start and end with |
	if !strings.HasPrefix(line, "|") {
		return nil, fmt.Errorf("row must start with '|'")
	}
	if !strings.HasSuffix(line, "|") {
		return nil, fmt.Errorf("row must end with '|'")
	}

	// Special case: empty columns
	if len(cols) == 0 {
		if line != "||" {
			return nil, fmt.Errorf("expected '||' for empty column row, got %q", line)
		}
		return Map(), nil
	}

	// Remove leading and trailing |
	inner := line[1 : len(line)-1]

	// Split by | (respecting escapes)
	cells := splitTabularCells(inner)

	if len(cells) != len(cols) {
		return nil, fmt.Errorf("expected %d cells, got %d", len(cols), len(cells))
	}

	// Build map
	entries := make([]MapEntry, len(cols))
	for i, col := range cols {
		cellStr := unescapeTabularCell(cells[i])

		// Parse the cell value as GLYPH loose
		val, err := parseLooseValue(cellStr)
		if err != nil {
			return nil, fmt.Errorf("cell %d (%s): %w", i, col, err)
		}

		entries[i] = MapEntry{Key: col, Value: val}
	}

	return Map(entries...), nil
}

// splitTabularCells splits a row by | respecting \| escapes.
func splitTabularCells(s string) []string {
	var cells []string
	var current strings.Builder

	i := 0
	for i < len(s) {
		if s[i] == '\\' && i+1 < len(s) && s[i+1] == '|' {
			// Escaped pipe - keep as \|
			current.WriteString(`\|`)
			i += 2
		} else if s[i] == '|' {
			// Cell delimiter
			cells = append(cells, current.String())
			current.Reset()
			i++
		} else {
			current.WriteByte(s[i])
			i++
		}
	}

	// Add last cell
	cells = append(cells, current.String())

	return cells
}

// parseLooseValue parses a single GLYPH loose value.
// This is a simplified parser for cell values.
func parseLooseValue(s string) (*GValue, error) {
	s = strings.TrimSpace(s)

	if s == "" {
		return Null(), nil
	}

	// Null - accept all aliases: ∅, _, null
	if s == "∅" || s == "_" || s == "null" {
		return Null(), nil
	}

	// Bool
	if s == "t" {
		return Bool(true), nil
	}
	if s == "f" {
		return Bool(false), nil
	}

	// Quoted string
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		unquoted, err := unquoteString(s)
		if err != nil {
			return nil, err
		}
		return Str(unquoted), nil
	}

	// Nested map
	if strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
		return parseLooseMap(s)
	}

	// Nested list
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return parseLooseList(s)
	}

	// Try parsing as number
	if val, ok := tryParseNumber(s); ok {
		return val, nil
	}

	// Bare string
	return Str(s), nil
}

// unquoteString removes quotes and unescapes a string.
func unquoteString(s string) (string, error) {
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return "", fmt.Errorf("invalid quoted string: %s", s)
	}

	inner := s[1 : len(s)-1]
	var b strings.Builder
	b.Grow(len(inner))

	for i := 0; i < len(inner); i++ {
		if inner[i] == '\\' && i+1 < len(inner) {
			next := inner[i+1]
			switch next {
			case '\\':
				b.WriteByte('\\')
			case '"':
				b.WriteByte('"')
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			default:
				// Unknown escape - keep as is
				b.WriteByte('\\')
				b.WriteByte(next)
			}
			i++
		} else {
			b.WriteByte(inner[i])
		}
	}

	return b.String(), nil
}

// tryParseNumber attempts to parse a string as int or float.
func tryParseNumber(s string) (*GValue, bool) {
	// Check for integer
	if isIntString(s) {
		n, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return Int(n), true
		}
	}

	// Check for float
	f, err := strconv.ParseFloat(s, 64)
	if err == nil {
		// Only treat as float if it has decimal point or exponent
		if strings.Contains(s, ".") || strings.ContainsAny(s, "eE") {
			return Float(f), true
		}
		// Otherwise try as int first
		n, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return Int(n), true
		}
		return Float(f), true
	}

	return nil, false
}

// isIntString checks if a string looks like an integer.
func isIntString(s string) bool {
	if len(s) == 0 {
		return false
	}
	start := 0
	if s[0] == '-' || s[0] == '+' {
		start = 1
	}
	if start >= len(s) {
		return false
	}
	for i := start; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// parseLooseMap parses a nested map: {key=val key2=val2}
// Also supports compact keys: {#0=val #1=val2} when keyDict is provided.
func parseLooseMap(s string) (*GValue, error) {
	return parseLooseMapWithDict(s, nil)
}

// parseLooseMapWithDict parses a map with optional key dictionary for compact keys.
func parseLooseMapWithDict(s string, keyDict []string) (*GValue, error) {
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return nil, fmt.Errorf("invalid map: %s", s)
	}

	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return Map(), nil
	}

	// Parse key=value pairs
	var entries []MapEntry
	for len(inner) > 0 {
		// Find key
		eqIdx := findUnnestedChar(inner, '=')
		if eqIdx == -1 {
			return nil, fmt.Errorf("missing '=' in map entry: %s", inner)
		}

		key := strings.TrimSpace(inner[:eqIdx])

		// Check for compact key (#N)
		if strings.HasPrefix(key, "#") && len(key) > 1 {
			idxStr := key[1:]
			if idx, err := parseCompactKeyIndex(idxStr); err == nil {
				if keyDict != nil && idx >= 0 && idx < len(keyDict) {
					key = keyDict[idx]
				} else {
					// Keep as #N if no dictionary or out of range
					// This allows round-tripping when receiver doesn't have schema
				}
			}
		} else if strings.HasPrefix(key, `"`) && strings.HasSuffix(key, `"`) {
			// Remove quotes from key if present
			var err error
			key, err = unquoteString(key)
			if err != nil {
				return nil, err
			}
		}

		rest := inner[eqIdx+1:]

		// Find value (ends at space or end of string, respecting nesting)
		valEnd := findValueEnd(rest)
		valStr := strings.TrimSpace(rest[:valEnd])

		// Parse value with key dictionary for nested structures
		val, err := parseLooseValueWithDict(valStr, keyDict)
		if err != nil {
			return nil, fmt.Errorf("map value for %s: %w", key, err)
		}

		entries = append(entries, MapEntry{Key: key, Value: val})

		// Move to next entry
		inner = strings.TrimSpace(rest[valEnd:])
	}

	return Map(entries...), nil
}

// parseCompactKeyIndex parses a compact key index from "#N" format.
func parseCompactKeyIndex(s string) (int, error) {
	if len(s) == 0 {
		return -1, fmt.Errorf("empty compact key")
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return -1, fmt.Errorf("invalid compact key: %s", s)
		}
	}
	var idx int
	_, err := fmt.Sscanf(s, "%d", &idx)
	return idx, err
}

// parseLooseList parses a nested list: [val1 val2 val3]
func parseLooseList(s string) (*GValue, error) {
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return nil, fmt.Errorf("invalid list: %s", s)
	}

	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return List(), nil
	}

	// Parse values
	var items []*GValue
	for len(inner) > 0 {
		// Find value end
		valEnd := findValueEnd(inner)
		valStr := strings.TrimSpace(inner[:valEnd])

		if valStr != "" {
			val, err := parseLooseValue(valStr)
			if err != nil {
				return nil, fmt.Errorf("list element: %w", err)
			}
			items = append(items, val)
		}

		// Move to next element
		inner = strings.TrimSpace(inner[valEnd:])
	}

	return List(items...), nil
}

// findUnnestedChar finds the first occurrence of char not inside {} [] or "".
func findUnnestedChar(s string, char byte) int {
	depth := 0
	inQuote := false

	for i := 0; i < len(s); i++ {
		c := s[i]

		if c == '\\' && i+1 < len(s) {
			i++ // Skip escaped char
			continue
		}

		if c == '"' {
			inQuote = !inQuote
			continue
		}

		if inQuote {
			continue
		}

		if c == '{' || c == '[' {
			depth++
		} else if c == '}' || c == ']' {
			depth--
		} else if c == char && depth == 0 {
			return i
		}
	}

	return -1
}

// findValueEnd finds where a value ends (space not inside {} [] or "").
func findValueEnd(s string) int {
	depth := 0
	inQuote := false

	for i := 0; i < len(s); i++ {
		c := s[i]

		if c == '\\' && i+1 < len(s) {
			i++ // Skip escaped char
			continue
		}

		if c == '"' {
			inQuote = !inQuote
			continue
		}

		if inQuote {
			continue
		}

		if c == '{' || c == '[' {
			depth++
		} else if c == '}' || c == ']' {
			depth--
		} else if c == ' ' && depth == 0 {
			return i
		}
	}

	return len(s)
}

// ParseLoosePayload parses a GLYPH payload that may include a @schema directive.
// Returns the parsed value, the schema context (if any), and any error.
//
// Supported formats:
//   - @schema#id @keys=[k1 k2 k3]\n{...} - inline schema definition
//   - @schema#id\n{...} - schema reference (registry lookup)
//   - @schema.clear\n{...} - clear active schema
//   - {...} - regular value (no schema)
func ParseLoosePayload(input string, registry *SchemaRegistry) (*GValue, *SchemaContext, error) {
	input = strings.TrimSpace(input)

	// Check for @schema directive
	if strings.HasPrefix(input, "@schema") {
		// Find newline
		nlIdx := strings.IndexByte(input, '\n')
		if nlIdx < 0 {
			// Just a directive with no value
			ctx, _, err := ParseSchemaDirective(input)
			if err != nil {
				return nil, nil, fmt.Errorf("parse schema directive: %w", err)
			}
			return nil, ctx, nil
		}

		directive := strings.TrimSpace(input[:nlIdx])
		valueStr := strings.TrimSpace(input[nlIdx+1:])

		// Parse directive
		ctx, isDef, err := ParseSchemaDirective(directive)
		if err != nil {
			return nil, nil, fmt.Errorf("parse schema directive: %w", err)
		}

		// Handle @schema.clear
		if directive == "@schema.clear" {
			if registry != nil {
				registry.ClearActive()
			}
			// Parse the value without a schema
			val, err := parseLooseValue(valueStr)
			return val, nil, err
		}

		// If defining, register the schema
		if isDef && registry != nil {
			registry.Define(ctx)
		} else if !isDef && registry != nil {
			// Reference only - lookup from registry
			existing := registry.Get(ctx.ID)
			if existing != nil {
				ctx = existing
				registry.SetActive(ctx.ID)
			} else {
				return nil, nil, fmt.Errorf("schema not found: %s", ctx.ID)
			}
		}

		// Parse value with schema context
		val, err := parseLooseValueWithSchema(valueStr, ctx)
		return val, ctx, err
	}

	// No schema directive - parse normally
	var ctx *SchemaContext
	if registry != nil {
		ctx = registry.Active()
	}

	if ctx != nil {
		val, err := parseLooseValueWithSchema(input, ctx)
		return val, ctx, err
	}

	val, err := parseLooseValue(input)
	return val, nil, err
}

// parseLooseValueWithSchema parses a loose value with schema context for key resolution.
func parseLooseValueWithSchema(s string, schema *SchemaContext) (*GValue, error) {
	var keyDict []string
	if schema != nil {
		keyDict = schema.Keys
	}
	return parseLooseValueWithDict(s, keyDict)
}

// parseLooseValueWithDict parses a loose value with optional key dictionary.
func parseLooseValueWithDict(s string, keyDict []string) (*GValue, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("empty input")
	}

	// Nested map with key dictionary
	if strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
		return parseLooseMapWithDict(s, keyDict)
	}

	// Nested list
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return parseLooseListWithDict(s, keyDict)
	}

	// Everything else - no key dictionary needed
	return parseLooseValue(s)
}

// parseLooseListWithSchema parses a list with schema context.
func parseLooseListWithSchema(s string, schema *SchemaContext) (*GValue, error) {
	var keyDict []string
	if schema != nil {
		keyDict = schema.Keys
	}
	return parseLooseListWithDict(s, keyDict)
}

// parseLooseListWithDict parses a list with optional key dictionary.
func parseLooseListWithDict(s string, keyDict []string) (*GValue, error) {
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return nil, fmt.Errorf("invalid list: %s", s)
	}

	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return List(), nil
	}

	var elements []*GValue
	for len(inner) > 0 {
		valEnd := findValueEnd(inner)
		valStr := strings.TrimSpace(inner[:valEnd])

		val, err := parseLooseValueWithDict(valStr, keyDict)
		if err != nil {
			return nil, err
		}

		elements = append(elements, val)
		inner = strings.TrimSpace(inner[valEnd:])
	}

	return List(elements...), nil
}
