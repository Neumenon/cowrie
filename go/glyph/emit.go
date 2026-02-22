package glyph

import (
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// EmitOptions configures the canonical emitter.
type EmitOptions struct {
	// Schema for wire key compression (optional)
	Schema *Schema

	// UseWireKeys uses short wire keys instead of full field names
	UseWireKeys bool

	// Compact removes optional whitespace
	Compact bool

	// Pretty adds indentation for readability
	Pretty bool

	// Indent string for pretty mode (default: "  ")
	Indent string

	// SortFields sorts struct/map fields alphabetically (for canonical output)
	SortFields bool
}

// DefaultEmitOptions returns sensible defaults.
func DefaultEmitOptions() EmitOptions {
	return EmitOptions{
		UseWireKeys: false,
		Compact:     false,
		Pretty:      false,
		Indent:      "  ",
		SortFields:  true,
	}
}

// CompactEmitOptions returns options for minimal output.
func CompactEmitOptions() EmitOptions {
	return EmitOptions{
		UseWireKeys: true,
		Compact:     true,
		Pretty:      false,
		SortFields:  true,
	}
}

// Emit converts a GValue to GLYPH-T canonical text.
func Emit(v *GValue) string {
	return EmitWithOptions(v, DefaultEmitOptions())
}

// EmitCompact converts a GValue to compact GLYPH-T text.
func EmitCompact(v *GValue) string {
	return EmitWithOptions(v, CompactEmitOptions())
}

// EmitWithOptions converts a GValue with custom options.
func EmitWithOptions(v *GValue, opts EmitOptions) string {
	e := &emitter{opts: opts}
	e.emit(v, 0)
	return e.sb.String()
}

type emitter struct {
	sb   strings.Builder
	opts EmitOptions
}

func (e *emitter) emit(v *GValue, depth int) {
	if v == nil || v.IsNull() {
		e.sb.WriteString("∅")
		return
	}

	switch v.typ {
	case TypeNull:
		e.sb.WriteString("∅")

	case TypeBool:
		if v.boolVal {
			e.sb.WriteString("t")
		} else {
			e.sb.WriteString("f")
		}

	case TypeInt:
		e.sb.WriteString(strconv.FormatInt(v.intVal, 10))

	case TypeFloat:
		e.emitFloat(v.floatVal)

	case TypeStr:
		e.emitString(v.strVal)

	case TypeBytes:
		// Emit as base64
		e.sb.WriteString("b64\"")
		e.sb.WriteString(base64.StdEncoding.EncodeToString(v.bytesVal))
		e.sb.WriteString("\"")

	case TypeTime:
		e.sb.WriteString(v.timeVal.Format("2006-01-02T15:04:05Z07:00"))

	case TypeID:
		e.sb.WriteString("^")
		if v.idVal.Prefix != "" {
			e.sb.WriteString(v.idVal.Prefix)
			e.sb.WriteString(":")
		}
		e.sb.WriteString(v.idVal.Value)

	case TypeList:
		e.emitList(v, depth)

	case TypeMap:
		e.emitMap(v, depth)

	case TypeStruct:
		e.emitStruct(v, depth)

	case TypeSum:
		e.emitSum(v, depth)
	}
}

func (e *emitter) emitFloat(f float64) {
	// Use canonical float representation
	if math.IsNaN(f) {
		e.sb.WriteString("NaN")
		return
	}
	if math.IsInf(f, 1) {
		e.sb.WriteString("Inf")
		return
	}
	if math.IsInf(f, -1) {
		e.sb.WriteString("-Inf")
		return
	}

	// Use shortest representation that round-trips
	s := strconv.FormatFloat(f, 'f', -1, 64)
	// Ensure it has a decimal point to distinguish from int
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	e.sb.WriteString(s)
}

func (e *emitter) emitString(s string) {
	if isValidBareString(s) {
		e.sb.WriteString(s)
	} else {
		e.sb.WriteString("\"")
		e.sb.WriteString(escapeString(s))
		e.sb.WriteString("\"")
	}
}

func (e *emitter) emitList(v *GValue, depth int) {
	e.sb.WriteString("[")

	if e.opts.Pretty && len(v.listVal) > 0 {
		e.sb.WriteString("\n")
	}

	for i, elem := range v.listVal {
		if e.opts.Pretty {
			e.writeIndent(depth + 1)
		}

		e.emit(elem, depth+1)

		if i < len(v.listVal)-1 {
			if e.opts.Compact {
				e.sb.WriteString(" ")
			} else {
				e.sb.WriteString(" ")
			}
		}

		if e.opts.Pretty {
			e.sb.WriteString("\n")
		}
	}

	if e.opts.Pretty && len(v.listVal) > 0 {
		e.writeIndent(depth)
	}
	e.sb.WriteString("]")
}

func (e *emitter) emitMap(v *GValue, depth int) {
	entries := v.mapVal
	if e.opts.SortFields {
		entries = sortMapEntries(entries)
	}

	e.sb.WriteString("{")

	if e.opts.Pretty && len(entries) > 0 {
		e.sb.WriteString("\n")
	}

	for i, entry := range entries {
		if e.opts.Pretty {
			e.writeIndent(depth + 1)
		}

		e.emitString(entry.Key)
		e.sb.WriteString(":")
		e.emit(entry.Value, depth+1)

		if i < len(entries)-1 {
			e.sb.WriteString(" ")
		}

		if e.opts.Pretty {
			e.sb.WriteString("\n")
		}
	}

	if e.opts.Pretty && len(entries) > 0 {
		e.writeIndent(depth)
	}
	e.sb.WriteString("}")
}

func (e *emitter) emitStruct(v *GValue, depth int) {
	sv := v.structVal
	fields := sv.Fields
	if e.opts.SortFields {
		fields = sortMapEntries(fields)
	}

	e.sb.WriteString(sv.TypeName)
	e.sb.WriteString("{")

	if e.opts.Pretty && len(fields) > 0 {
		e.sb.WriteString("\n")
	}

	for i, field := range fields {
		if e.opts.Pretty {
			e.writeIndent(depth + 1)
		}

		// Use wire key if enabled and available
		key := field.Key
		if e.opts.UseWireKeys && e.opts.Schema != nil {
			if wk := e.opts.Schema.GetWireKey(sv.TypeName, field.Key); wk != "" {
				key = wk
			}
		}

		e.sb.WriteString(key)
		e.sb.WriteString("=")
		e.emit(field.Value, depth+1)

		if i < len(fields)-1 {
			e.sb.WriteString(" ")
		}

		if e.opts.Pretty {
			e.sb.WriteString("\n")
		}
	}

	if e.opts.Pretty && len(fields) > 0 {
		e.writeIndent(depth)
	}
	e.sb.WriteString("}")
}

func (e *emitter) emitSum(v *GValue, depth int) {
	sv := v.sumVal
	e.sb.WriteString(sv.Tag)

	if sv.Value == nil || sv.Value.IsNull() {
		e.sb.WriteString("()")
		return
	}

	// For struct values, use Tag{...} syntax
	if sv.Value.typ == TypeStruct {
		e.sb.WriteString("{")
		fields := sv.Value.structVal.Fields
		if e.opts.SortFields {
			fields = sortMapEntries(fields)
		}

		for i, field := range fields {
			e.sb.WriteString(field.Key)
			e.sb.WriteString("=")
			e.emit(field.Value, depth+1)
			if i < len(fields)-1 {
				e.sb.WriteString(" ")
			}
		}
		e.sb.WriteString("}")
		return
	}

	// For other values, use Tag(value) syntax
	e.sb.WriteString("(")
	e.emit(sv.Value, depth)
	e.sb.WriteString(")")
}

func (e *emitter) writeIndent(depth int) {
	for i := 0; i < depth; i++ {
		e.sb.WriteString(e.opts.Indent)
	}
}

// escapeString escapes a string for quoted output.
func escapeString(s string) string {
	var sb strings.Builder
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString("\\\"")
		case '\\':
			sb.WriteString("\\\\")
		case '\n':
			sb.WriteString("\\n")
		case '\r':
			sb.WriteString("\\r")
		case '\t':
			sb.WriteString("\\t")
		default:
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// sortMapEntries returns a sorted copy of map entries.
func sortMapEntries(entries []MapEntry) []MapEntry {
	if len(entries) <= 1 {
		return entries
	}

	sorted := make([]MapEntry, len(entries))
	copy(sorted, entries)

	// Simple insertion sort (good for small lists)
	for i := 1; i < len(sorted); i++ {
		j := i
		for j > 0 && sorted[j].Key < sorted[j-1].Key {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
			j--
		}
	}

	return sorted
}

// ============================================================
// Canonical Hash
// ============================================================

// CanonicalHash returns a hash of the canonical representation.
func CanonicalHash(v *GValue) string {
	opts := EmitOptions{
		UseWireKeys: false,
		Compact:     true,
		SortFields:  true,
	}
	canonical := EmitWithOptions(v, opts)

	// Use FNV-1a for speed (not cryptographic)
	h := uint64(14695981039346656037)
	for i := 0; i < len(canonical); i++ {
		h ^= uint64(canonical[i])
		h *= 1099511628211
	}

	return fmt.Sprintf("%016x", h)
}

// ============================================================
// Schema Emission
// ============================================================

// EmitSchema converts a Schema to GLYPH schema text.
func EmitSchema(s *Schema) string {
	return s.Canonical()
}

// EmitSchemaRef returns a schema reference string.
func EmitSchemaRef(s *Schema) string {
	if s.Hash == "" {
		s.ComputeHash()
	}
	return "@schema#" + s.Hash
}
