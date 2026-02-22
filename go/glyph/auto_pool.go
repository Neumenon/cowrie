package glyph

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// AutoPoolOpts configures automatic value pooling for encoding.
type AutoPoolOpts struct {
	MinLength  int            // Minimum string length to consider (default: 20)
	MinOccurs  int            // Minimum occurrences to pool (default: 2)
	LooseOpts  LooseCanonOpts // Underlying encoding options
	PoolByType bool           // Create separate pools by semantic type (tool names, roles, etc.)
}

// DefaultAutoPoolOpts returns sensible defaults for auto-pooling.
func DefaultAutoPoolOpts() AutoPoolOpts {
	return AutoPoolOpts{
		MinLength:  20,
		MinOccurs:  2,
		LooseOpts:  LLMLooseCanonOpts(),
		PoolByType: false, // Single pool is simpler and sufficient
	}
}

// AutoPoolResult contains the pooled encoding and statistics.
type AutoPoolResult struct {
	Output          string            // The encoded output with pool definitions
	PoolDefinitions string            // Just the pool definitions
	ValueOutput     string            // Just the value (without pool defs)
	Stats           AutoPoolStats     // Statistics about pooling
	RefMap          map[string]string // String -> pool ref mapping (for debugging)
}

// AutoPoolStats tracks pooling statistics.
type AutoPoolStats struct {
	TotalStrings     int // Total strings in input
	UniqueStrings    int // Unique strings
	PooledStrings    int // Strings that were pooled
	PoolEntries      int // Number of pool entries created
	BytesSaved       int // Estimated bytes saved by pooling
	OriginalBytes    int // Size without pooling
	PooledBytes      int // Size with pooling
	SavingsPercent   float64
	OccurrenceCounts map[string]int // String -> occurrence count
}

// AutoPoolEncode encodes a GValue with automatic string pooling.
// It performs two passes:
// 1. Walk the tree and count string occurrences
// 2. Build pool from repeated strings, emit pool + value with refs
func AutoPoolEncode(gv *GValue, opts AutoPoolOpts) (*AutoPoolResult, error) {
	if gv == nil {
		return nil, fmt.Errorf("nil value")
	}

	// Pass 1: Count string occurrences
	counts := make(map[string]int)
	walkAndCount(gv, counts, opts.MinLength)

	// Build pool from strings that appear >= MinOccurs times
	pool := &Pool{ID: "S1", Kind: PoolKindString}
	refMap := make(map[string]string) // string -> "^S1:N"

	// Sort strings by occurrence count (most frequent first) for better compression
	type stringCount struct {
		s     string
		count int
	}
	var candidates []stringCount
	for s, count := range counts {
		if count >= opts.MinOccurs {
			candidates = append(candidates, stringCount{s, count})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		// Most frequent first, then by length (longer first for more savings)
		if candidates[i].count != candidates[j].count {
			return candidates[i].count > candidates[j].count
		}
		return len(candidates[i].s) > len(candidates[j].s)
	})

	// Add to pool
	for _, sc := range candidates {
		idx := pool.Add(Str(sc.s))
		refMap[sc.s] = fmt.Sprintf("^S1:%d", idx)
	}

	// Calculate stats
	stats := AutoPoolStats{
		OccurrenceCounts: counts,
		UniqueStrings:    len(counts),
		PooledStrings:    len(refMap),
		PoolEntries:      len(pool.Entries),
	}

	// Count total strings
	walkAndCountAll(gv, &stats.TotalStrings)

	// Pass 2: Emit with pool references
	var poolDef strings.Builder
	var valueOut strings.Builder

	if len(pool.Entries) > 0 {
		poolDef.WriteString(pool.String())
		poolDef.WriteString("\n\n")
	}

	// Emit value with refs substituted
	writeValueWithRefs(&valueOut, gv, refMap, opts.LooseOpts, 0)

	// Calculate byte savings
	originalOut := CanonicalizeLooseWithOpts(gv, opts.LooseOpts)
	stats.OriginalBytes = len(originalOut)
	stats.PooledBytes = poolDef.Len() + valueOut.Len()
	stats.BytesSaved = stats.OriginalBytes - stats.PooledBytes
	if stats.OriginalBytes > 0 {
		stats.SavingsPercent = float64(stats.BytesSaved) / float64(stats.OriginalBytes) * 100
	}

	// Combine output
	var fullOutput strings.Builder
	fullOutput.WriteString(poolDef.String())
	fullOutput.WriteString(valueOut.String())

	return &AutoPoolResult{
		Output:          fullOutput.String(),
		PoolDefinitions: poolDef.String(),
		ValueOutput:     valueOut.String(),
		Stats:           stats,
		RefMap:          refMap,
	}, nil
}

// AutoPoolEncodeJSON is a convenience function that parses JSON and encodes with pooling.
func AutoPoolEncodeJSON(jsonData []byte, opts AutoPoolOpts) (*AutoPoolResult, error) {
	gv, err := FromJSONLoose(jsonData)
	if err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}
	return AutoPoolEncode(gv, opts)
}

// walkAndCount recursively counts string occurrences in a GValue tree.
func walkAndCount(gv *GValue, counts map[string]int, minLength int) {
	if gv == nil {
		return
	}

	switch gv.typ {
	case TypeStr:
		s := gv.strVal
		if len(s) >= minLength {
			counts[s]++
		}

	case TypeList:
		for _, item := range gv.listVal {
			walkAndCount(item, counts, minLength)
		}

	case TypeMap:
		for _, entry := range gv.mapVal {
			// Don't pool keys - they're usually short
			walkAndCount(entry.Value, counts, minLength)
		}

	case TypeStruct:
		if gv.structVal != nil {
			for _, entry := range gv.structVal.Fields {
				walkAndCount(entry.Value, counts, minLength)
			}
		}

	case TypeSum:
		if gv.sumVal != nil {
			walkAndCount(gv.sumVal.Value, counts, minLength)
		}
	}
}

// walkAndCountAll counts all strings (for stats).
func walkAndCountAll(gv *GValue, count *int) {
	if gv == nil {
		return
	}

	switch gv.typ {
	case TypeStr:
		*count++

	case TypeList:
		for _, item := range gv.listVal {
			walkAndCountAll(item, count)
		}

	case TypeMap:
		for _, entry := range gv.mapVal {
			walkAndCountAll(entry.Value, count)
		}

	case TypeStruct:
		if gv.structVal != nil {
			for _, entry := range gv.structVal.Fields {
				walkAndCountAll(entry.Value, count)
			}
		}

	case TypeSum:
		if gv.sumVal != nil {
			walkAndCountAll(gv.sumVal.Value, count)
		}
	}
}

// isLLMMode checks if opts use LLM-friendly null style.
func isLLMMode(opts LooseCanonOpts) bool {
	return opts.NullStyle == NullStyleUnderscore
}

// writeValueWithRefs writes a GValue with pool references substituted.
func writeValueWithRefs(b *strings.Builder, gv *GValue, refMap map[string]string, opts LooseCanonOpts, depth int) {
	if gv == nil {
		if isLLMMode(opts) {
			b.WriteString("_")
		} else {
			b.WriteString("∅")
		}
		return
	}

	switch gv.typ {
	case TypeNull:
		if isLLMMode(opts) {
			b.WriteString("_")
		} else {
			b.WriteString("∅")
		}

	case TypeBool:
		if gv.boolVal {
			b.WriteString("t")
		} else {
			b.WriteString("f")
		}

	case TypeInt:
		b.WriteString(strconv.FormatInt(gv.intVal, 10))

	case TypeFloat:
		if gv.floatVal == 0 {
			b.WriteByte('0')
		} else {
			s := strconv.FormatFloat(gv.floatVal, 'g', -1, 64)
			s = strings.ReplaceAll(s, "E", "e")
			if s == "-0" {
				b.WriteByte('0')
			} else {
				b.WriteString(s)
			}
		}

	case TypeStr:
		// Check if this string should be a ref
		if ref, ok := refMap[gv.strVal]; ok {
			b.WriteString(ref)
		} else {
			writeCanonString(b, gv.strVal)
		}

	case TypeBytes:
		b.WriteString("b64\"")
		b.WriteString(base64Encode(gv.bytesVal))
		b.WriteString("\"")

	case TypeTime:
		b.WriteString("@")
		b.WriteString(gv.timeVal.Format("2006-01-02T15:04:05Z07:00"))

	case TypeID:
		writeCanonRef(b, gv.idVal)

	case TypeList:
		writeListWithRefs(b, gv.listVal, refMap, opts, depth)

	case TypeMap:
		writeMapWithRefs(b, gv.mapVal, refMap, opts, depth)

	case TypeStruct:
		if gv.structVal != nil {
			b.WriteString(gv.structVal.TypeName)
			writeMapWithRefs(b, gv.structVal.Fields, refMap, opts, depth)
		}

	case TypeSum:
		if gv.sumVal != nil {
			b.WriteString(gv.sumVal.Tag)
			b.WriteString("(")
			writeValueWithRefs(b, gv.sumVal.Value, refMap, opts, depth)
			b.WriteString(")")
		}

	case TypeBlob:
		if gv.blobVal != nil {
			b.WriteString(gv.blobVal.String())
		}

	case TypePoolRef:
		if gv.poolRef != nil {
			b.WriteString(gv.poolRef.String())
		}
	}
}

// writeListWithRefs writes a list with refs, using tabular if appropriate.
func writeListWithRefs(b *strings.Builder, items []*GValue, refMap map[string]string, opts LooseCanonOpts, depth int) {
	// Check for tabular mode
	if opts.AutoTabular && len(items) >= opts.MinRows {
		keys, uniform := detectTabularKeysForPool(items)
		if uniform && len(keys) > 0 && len(keys) <= opts.MaxCols {
			writeTabularWithRefs(b, items, keys, refMap, opts)
			return
		}
	}

	// Regular list
	b.WriteString("[")
	for i, item := range items {
		if i > 0 {
			b.WriteString(" ")
		}
		writeValueWithRefs(b, item, refMap, opts, depth+1)
	}
	b.WriteString("]")
}

// writeMapWithRefs writes a map with refs.
func writeMapWithRefs(b *strings.Builder, entries []MapEntry, refMap map[string]string, opts LooseCanonOpts, depth int) {
	// Sort entries by key for canonical form
	sorted := make([]MapEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Key < sorted[j].Key
	})

	b.WriteString("{")
	for i, entry := range sorted {
		if i > 0 {
			b.WriteString(" ")
		}
		// Write key (bare if safe, quoted otherwise)
		if isBareKeyForPool(entry.Key) {
			b.WriteString(entry.Key)
		} else {
			b.WriteString("\"")
			b.WriteString(escapeStringForPool(entry.Key))
			b.WriteString("\"")
		}
		b.WriteString("=")
		writeValueWithRefs(b, entry.Value, refMap, opts, depth+1)
	}
	b.WriteString("}")
}

// writeTabularWithRefs writes tabular format with refs.
func writeTabularWithRefs(b *strings.Builder, items []*GValue, keys []string, refMap map[string]string, opts LooseCanonOpts) {
	// Sort keys for canonical order
	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	sort.Strings(sortedKeys)

	// Header
	b.WriteString(fmt.Sprintf("@tab _ rows=%d cols=%d [", len(items), len(sortedKeys)))
	for i, k := range sortedKeys {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(k)
	}
	b.WriteString("]\n")

	// Rows
	for _, item := range items {
		b.WriteString("|")
		// Build key->value map for this row
		rowMap := make(map[string]*GValue)
		if item.typ == TypeMap {
			for _, e := range item.mapVal {
				rowMap[e.Key] = e.Value
			}
		}

		for i, k := range sortedKeys {
			if i > 0 {
				b.WriteString("|")
			}
			v := rowMap[k]
			writeCellWithRefs(b, v, refMap, opts)
		}
		b.WriteString("|\n")
	}
}

// writeCellWithRefs writes a single cell value with refs.
func writeCellWithRefs(b *strings.Builder, gv *GValue, refMap map[string]string, opts LooseCanonOpts) {
	if gv == nil {
		if isLLMMode(opts) {
			b.WriteString("_")
		} else {
			b.WriteString("∅")
		}
		return
	}

	switch gv.typ {
	case TypeNull:
		if isLLMMode(opts) {
			b.WriteString("_")
		} else {
			b.WriteString("∅")
		}

	case TypeBool:
		if gv.boolVal {
			b.WriteString("t")
		} else {
			b.WriteString("f")
		}

	case TypeInt:
		b.WriteString(strconv.FormatInt(gv.intVal, 10))

	case TypeFloat:
		if gv.floatVal == 0 {
			b.WriteByte('0')
		} else {
			s := strconv.FormatFloat(gv.floatVal, 'g', -1, 64)
			s = strings.ReplaceAll(s, "E", "e")
			if s == "-0" {
				b.WriteByte('0')
			} else {
				b.WriteString(s)
			}
		}

	case TypeStr:
		if ref, ok := refMap[gv.strVal]; ok {
			b.WriteString(ref)
		} else {
			// Cell strings need escaping for pipes
			s := gv.strVal
			if needsQuotingInCellForPool(s) {
				b.WriteString("\"")
				b.WriteString(escapeCellStringForPool(s))
				b.WriteString("\"")
			} else {
				b.WriteString(s)
			}
		}

	case TypeList:
		// Inline list in cell
		b.WriteString("[")
		for i, item := range gv.listVal {
			if i > 0 {
				b.WriteString(" ")
			}
			writeCellWithRefs(b, item, refMap, opts)
		}
		b.WriteString("]")

	case TypeMap:
		// Inline map in cell
		b.WriteString("{")
		sorted := make([]MapEntry, len(gv.mapVal))
		copy(sorted, gv.mapVal)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Key < sorted[j].Key
		})
		for i, e := range sorted {
			if i > 0 {
				b.WriteString(" ")
			}
			b.WriteString(e.Key)
			b.WriteString("=")
			writeCellWithRefs(b, e.Value, refMap, opts)
		}
		b.WriteString("}")

	default:
		// Fallback to canonical
		b.WriteString(CanonicalizeLooseWithOpts(gv, opts))
	}
}

// detectTabularKeysForPool checks if items are uniform maps and returns their keys.
func detectTabularKeysForPool(items []*GValue) ([]string, bool) {
	if len(items) == 0 {
		return nil, false
	}

	// All items must be maps
	var firstKeys []string
	for i, item := range items {
		if item == nil || item.typ != TypeMap {
			return nil, false
		}

		keys := make([]string, 0, len(item.mapVal))
		for _, e := range item.mapVal {
			keys = append(keys, e.Key)
		}
		sort.Strings(keys)

		if i == 0 {
			firstKeys = keys
		} else {
			// Check same keys
			if len(keys) != len(firstKeys) {
				return nil, false
			}
			for j, k := range keys {
				if k != firstKeys[j] {
					return nil, false
				}
			}
		}
	}

	return firstKeys, true
}

// needsQuotingInCellForPool checks if a string needs quoting in a table cell.
func needsQuotingInCellForPool(s string) bool {
	if s == "" {
		return true
	}
	for _, c := range s {
		if c == '|' || c == '"' || c == '\n' || c == '\r' || c == '\\' || c == ' ' {
			return true
		}
	}
	// Also quote if it looks like a special value
	if s == "t" || s == "f" || s == "_" || s == "∅" || s == "null" {
		return true
	}
	return false
}

// escapeCellStringForPool escapes a string for use in a table cell.
func escapeCellStringForPool(s string) string {
	var b strings.Builder
	for _, c := range s {
		switch c {
		case '\\':
			b.WriteString("\\\\")
		case '"':
			b.WriteString("\\\"")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '|':
			b.WriteString("\\|")
		default:
			b.WriteRune(c)
		}
	}
	return b.String()
}

// isBareKeyForPool checks if a key can be written without quotes.
func isBareKeyForPool(s string) bool {
	if s == "" {
		return false
	}
	for i, c := range s {
		if i == 0 {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
				return false
			}
		} else {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
				return false
			}
		}
	}
	return true
}

// escapeStringForPool escapes a string for quoted output.
func escapeStringForPool(s string) string {
	var b strings.Builder
	for _, c := range s {
		switch c {
		case '\\':
			b.WriteString("\\\\")
		case '"':
			b.WriteString("\\\"")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '\t':
			b.WriteString("\\t")
		default:
			b.WriteRune(c)
		}
	}
	return b.String()
}
