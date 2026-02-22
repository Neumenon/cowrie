// Package glyph - Token-Aware Encoding for LLM optimization.
//
// This module provides token-optimized encoding for LLM output by:
// 1. Abbreviating common field names to minimize tokenization
// 2. Using compact number representations
// 3. Providing bidirectional key mappings for common ML/tool schemas
//
// Goal: Reduce LLM output tokens by 15-25% while maintaining round-trip fidelity.
package glyph

import (
	"strconv"
	"strings"
	"sync"
)

// TokenAwareOptions configures token-optimized emission.
type TokenAwareOptions struct {
	// UseAbbreviations enables key abbreviation (default: true)
	UseAbbreviations bool

	// CompactNumbers uses shortest numeric representation
	CompactNumbers bool

	// OmitDefaults skips default values (false, 0, "", null)
	OmitDefaults bool

	// UseCustomDict uses a custom abbreviation dictionary
	CustomDict *KeyDict

	// Schema for additional wire key compression
	Schema *Schema
}

// DefaultTokenAwareOptions returns optimized defaults for LLM output.
func DefaultTokenAwareOptions() TokenAwareOptions {
	return TokenAwareOptions{
		UseAbbreviations: true,
		CompactNumbers:   true,
		OmitDefaults:     false,
		CustomDict:       nil,
	}
}

// KeyDict provides bidirectional key abbreviation mapping.
type KeyDict struct {
	mu         sync.RWMutex
	name       string            // Dictionary name for debugging
	longToAbbr map[string]string // "content" -> "c"
	abbrToLong map[string]string // "c" -> "content"
}

// NewKeyDict creates a new abbreviation dictionary.
func NewKeyDict(name string) *KeyDict {
	return &KeyDict{
		name:       name,
		longToAbbr: make(map[string]string),
		abbrToLong: make(map[string]string),
	}
}

// Add registers a key abbreviation pair.
// Returns false if either key already exists (no overwrite).
func (d *KeyDict) Add(long, abbr string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.longToAbbr[long]; exists {
		return false
	}
	if _, exists := d.abbrToLong[abbr]; exists {
		return false
	}

	d.longToAbbr[long] = abbr
	d.abbrToLong[abbr] = long
	return true
}

// Abbreviate returns the abbreviation for a key, or the key itself if not found.
func (d *KeyDict) Abbreviate(key string) string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if abbr, ok := d.longToAbbr[key]; ok {
		return abbr
	}
	return key
}

// Expand returns the full key for an abbreviation, or the key itself if not found.
func (d *KeyDict) Expand(abbr string) string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if long, ok := d.abbrToLong[abbr]; ok {
		return long
	}
	return abbr
}

// HasAbbreviation checks if a key has an abbreviation.
func (d *KeyDict) HasAbbreviation(key string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.longToAbbr[key]
	return ok
}

// IsAbbreviation checks if a key is an abbreviation.
func (d *KeyDict) IsAbbreviation(key string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.abbrToLong[key]
	return ok
}

// Len returns the number of abbreviation pairs.
func (d *KeyDict) Len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.longToAbbr)
}

// Merge adds all entries from another dictionary (non-conflicting only).
func (d *KeyDict) Merge(other *KeyDict) int {
	other.mu.RLock()
	defer other.mu.RUnlock()

	added := 0
	for long, abbr := range other.longToAbbr {
		if d.Add(long, abbr) {
			added++
		}
	}
	return added
}

// ============================================================
// Pre-built Dictionaries for Common Domains
// ============================================================

// LLMDict contains abbreviations for common LLM API fields.
var LLMDict = func() *KeyDict {
	d := NewKeyDict("llm")
	// Chat/Message fields
	d.Add("content", "c")
	d.Add("role", "r")
	d.Add("messages", "m")
	d.Add("assistant", "a")
	d.Add("system", "s")
	d.Add("user", "u")
	d.Add("function", "fn")
	d.Add("tool", "tl")

	// Request/Response fields
	d.Add("model", "md")
	d.Add("temperature", "tp")
	d.Add("max_tokens", "mx")
	d.Add("top_p", "pp")
	d.Add("frequency_penalty", "fp")
	d.Add("presence_penalty", "pr")
	d.Add("stop", "st")
	d.Add("stream", "sm")
	d.Add("n", "n")

	// Tool calling
	d.Add("tool_calls", "tc")
	d.Add("tool_call_id", "ti")
	d.Add("function_call", "fc")
	d.Add("name", "nm")
	d.Add("arguments", "ag")
	d.Add("type", "ty")

	// Response fields
	d.Add("choices", "ch")
	d.Add("message", "mg")
	d.Add("finish_reason", "fr")
	d.Add("usage", "us")
	d.Add("prompt_tokens", "pt")
	d.Add("completion_tokens", "ct")
	d.Add("total_tokens", "tt")
	d.Add("index", "ix")
	d.Add("delta", "dt")

	// Metadata
	d.Add("id", "id")
	d.Add("object", "ob")
	d.Add("created", "cr")
	d.Add("timestamp", "ts")
	return d
}()

// ToolDict contains abbreviations for tool/function calling schemas.
var ToolDict = func() *KeyDict {
	d := NewKeyDict("tool")
	// JSON Schema fields
	d.Add("type", "ty")
	d.Add("properties", "p")
	d.Add("required", "rq")
	d.Add("description", "d")
	d.Add("default", "df")
	d.Add("enum", "en")
	d.Add("items", "it")
	d.Add("minimum", "mn")
	d.Add("maximum", "mx")
	d.Add("minLength", "ml")
	d.Add("maxLength", "xl")
	d.Add("pattern", "pt")
	d.Add("format", "fm")
	d.Add("oneOf", "oo")
	d.Add("anyOf", "ao")
	d.Add("allOf", "al")
	d.Add("additionalProperties", "ap")

	// Common types (as strings)
	d.Add("string", "s")
	d.Add("number", "n")
	d.Add("integer", "i")
	d.Add("boolean", "b")
	d.Add("array", "a")
	d.Add("object", "o")
	d.Add("null", "z")

	// Function schema fields
	d.Add("name", "nm")
	d.Add("parameters", "pm")
	d.Add("returns", "rt")
	return d
}()

// MLDict contains abbreviations for ML tensor/model fields.
var MLDict = func() *KeyDict {
	d := NewKeyDict("ml")
	// Tensor metadata
	d.Add("shape", "sh")
	d.Add("dtype", "dt")
	d.Add("data", "da")
	d.Add("strides", "st")
	d.Add("offset", "of")
	d.Add("device", "dv")

	// Common dtypes
	d.Add("float32", "f32")
	d.Add("float64", "f64")
	d.Add("float16", "f16")
	d.Add("bfloat16", "bf16")
	d.Add("int32", "i32")
	d.Add("int64", "i64")
	d.Add("int8", "i8")
	d.Add("uint8", "u8")

	// Model fields
	d.Add("weights", "w")
	d.Add("bias", "bi")
	d.Add("input", "in")
	d.Add("output", "ou")
	d.Add("hidden", "hi")
	d.Add("embedding", "em")
	d.Add("attention", "at")
	d.Add("layer", "ly")
	d.Add("norm", "no")
	d.Add("activation", "ac")
	d.Add("dropout", "dr")
	d.Add("gradient", "gr")
	d.Add("loss", "lo")
	d.Add("optimizer", "op")
	d.Add("learning_rate", "lr")
	d.Add("batch_size", "bs")
	d.Add("epochs", "ep")
	return d
}()

// CombinedDict merges LLM + Tool + ML dictionaries.
var CombinedDict = func() *KeyDict {
	d := NewKeyDict("combined")
	d.Merge(LLMDict)
	d.Merge(ToolDict)
	d.Merge(MLDict)
	return d
}()

// ============================================================
// Token-Aware Emitter
// ============================================================

// EmitTokenAware converts a GValue to token-optimized GLYPH-T text.
func EmitTokenAware(v *GValue) string {
	return EmitTokenAwareWithOptions(v, DefaultTokenAwareOptions())
}

// EmitTokenAwareWithOptions converts with custom options.
func EmitTokenAwareWithOptions(v *GValue, opts TokenAwareOptions) string {
	dict := opts.CustomDict
	if dict == nil {
		dict = CombinedDict
	}

	e := &tokenEmitter{
		opts: opts,
		dict: dict,
	}
	e.emit(v, 0)
	return e.sb.String()
}

type tokenEmitter struct {
	sb   strings.Builder
	opts TokenAwareOptions
	dict *KeyDict
}

func (e *tokenEmitter) emit(v *GValue, depth int) {
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
		e.emitInt(v.intVal)

	case TypeFloat:
		e.emitFloat(v.floatVal)

	case TypeStr:
		e.emitString(v.strVal)

	case TypeBytes:
		// Same as regular emit
		e.sb.WriteString("b64\"")
		e.sb.WriteString(encodeBase64(v.bytesVal))
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

func (e *tokenEmitter) emitInt(n int64) {
	if e.opts.CompactNumbers {
		// Use compact representation for small numbers
		if n >= 0 && n <= 9 {
			e.sb.WriteByte('0' + byte(n))
			return
		}
	}
	e.sb.WriteString(strconv.FormatInt(n, 10))
}

func (e *tokenEmitter) emitFloat(f float64) {
	if e.opts.CompactNumbers {
		// Use integer representation if no precision loss
		if f == float64(int64(f)) && f >= -1e15 && f <= 1e15 {
			e.sb.WriteString(strconv.FormatInt(int64(f), 10))
			e.sb.WriteString(".0")
			return
		}

		// Use shortest representation
		s := strconv.FormatFloat(f, 'g', -1, 64)
		// Ensure it has decimal or exponent to distinguish from int
		if !strings.ContainsAny(s, ".eE") {
			s += ".0"
		}
		e.sb.WriteString(s)
		return
	}

	// Default: same as regular emit
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	e.sb.WriteString(s)
}

func (e *tokenEmitter) emitString(s string) {
	if isValidBareString(s) {
		e.sb.WriteString(s)
	} else {
		e.sb.WriteString("\"")
		e.sb.WriteString(escapeString(s))
		e.sb.WriteString("\"")
	}
}

func (e *tokenEmitter) emitKey(key string) {
	if e.opts.UseAbbreviations && e.dict != nil {
		key = e.dict.Abbreviate(key)
	}
	e.emitString(key)
}

func (e *tokenEmitter) emitList(v *GValue, depth int) {
	e.sb.WriteString("[")

	for i, elem := range v.listVal {
		// Skip defaults if enabled
		if e.opts.OmitDefaults && isDefaultValue(elem) {
			continue
		}

		e.emit(elem, depth+1)

		if i < len(v.listVal)-1 {
			e.sb.WriteString(" ")
		}
	}

	e.sb.WriteString("]")
}

func (e *tokenEmitter) emitMap(v *GValue, depth int) {
	entries := sortMapEntries(v.mapVal)

	e.sb.WriteString("{")

	first := true
	for _, entry := range entries {
		// Skip defaults if enabled
		if e.opts.OmitDefaults && isDefaultValue(entry.Value) {
			continue
		}

		if !first {
			e.sb.WriteString(" ")
		}
		first = false

		e.emitKey(entry.Key)
		e.sb.WriteString(":")
		e.emit(entry.Value, depth+1)
	}

	e.sb.WriteString("}")
}

func (e *tokenEmitter) emitStruct(v *GValue, depth int) {
	sv := v.structVal
	fields := sortMapEntries(sv.Fields)

	// Abbreviate type name if in dict
	typeName := sv.TypeName
	if e.opts.UseAbbreviations && e.dict != nil {
		typeName = e.dict.Abbreviate(typeName)
	}

	e.sb.WriteString(typeName)
	e.sb.WriteString("{")

	first := true
	for _, field := range fields {
		// Skip defaults if enabled
		if e.opts.OmitDefaults && isDefaultValue(field.Value) {
			continue
		}

		if !first {
			e.sb.WriteString(" ")
		}
		first = false

		e.emitKey(field.Key)
		e.sb.WriteString("=")
		e.emit(field.Value, depth+1)
	}

	e.sb.WriteString("}")
}

func (e *tokenEmitter) emitSum(v *GValue, depth int) {
	sv := v.sumVal

	// Abbreviate tag if in dict
	tag := sv.Tag
	if e.opts.UseAbbreviations && e.dict != nil {
		tag = e.dict.Abbreviate(tag)
	}

	e.sb.WriteString(tag)

	if sv.Value == nil || sv.Value.IsNull() {
		e.sb.WriteString("()")
		return
	}

	// For struct values, use Tag{...} syntax
	if sv.Value.typ == TypeStruct {
		e.sb.WriteString("{")
		fields := sortMapEntries(sv.Value.structVal.Fields)
		first := true
		for _, field := range fields {
			if e.opts.OmitDefaults && isDefaultValue(field.Value) {
				continue
			}
			if !first {
				e.sb.WriteString(" ")
			}
			first = false
			e.emitKey(field.Key)
			e.sb.WriteString("=")
			e.emit(field.Value, depth+1)
		}
		e.sb.WriteString("}")
		return
	}

	e.sb.WriteString("(")
	e.emit(sv.Value, depth)
	e.sb.WriteString(")")
}

// isDefaultValue checks if a value is a "default" (false, 0, "", null).
func isDefaultValue(v *GValue) bool {
	if v == nil {
		return true
	}
	switch v.typ {
	case TypeNull:
		return true
	case TypeBool:
		return !v.boolVal
	case TypeInt:
		return v.intVal == 0
	case TypeFloat:
		return v.floatVal == 0
	case TypeStr:
		return v.strVal == ""
	case TypeList:
		return len(v.listVal) == 0
	case TypeMap:
		return len(v.mapVal) == 0
	case TypeStruct:
		return v.structVal == nil || len(v.structVal.Fields) == 0
	default:
		return false
	}
}

// ============================================================
// Expansion (Decode Abbreviations)
// ============================================================

// ExpandAbbreviations expands abbreviated keys in a GValue tree.
// Modifies the value in-place.
func ExpandAbbreviations(v *GValue, dict *KeyDict) {
	if v == nil || dict == nil {
		return
	}
	expandAbbrevHelper(v, dict)
}

func expandAbbrevHelper(v *GValue, dict *KeyDict) {
	switch v.typ {
	case TypeList:
		for _, elem := range v.listVal {
			expandAbbrevHelper(elem, dict)
		}

	case TypeMap:
		for i := range v.mapVal {
			v.mapVal[i].Key = dict.Expand(v.mapVal[i].Key)
			expandAbbrevHelper(v.mapVal[i].Value, dict)
		}

	case TypeStruct:
		if v.structVal != nil {
			v.structVal.TypeName = dict.Expand(v.structVal.TypeName)
			for i := range v.structVal.Fields {
				v.structVal.Fields[i].Key = dict.Expand(v.structVal.Fields[i].Key)
				expandAbbrevHelper(v.structVal.Fields[i].Value, dict)
			}
		}

	case TypeSum:
		if v.sumVal != nil {
			v.sumVal.Tag = dict.Expand(v.sumVal.Tag)
			if v.sumVal.Value != nil {
				expandAbbrevHelper(v.sumVal.Value, dict)
			}
		}
	}
}

// ============================================================
// Token Counting (Estimation)
// ============================================================

// EstimateTokens estimates the number of LLM tokens in a string.
// Uses a simple heuristic: ~4 chars per token for English.
func EstimateTokens(s string) int {
	// Simple heuristic for GPT-style tokenizers
	return (len(s) + 3) / 4
}

// TokenSavings calculates token savings from abbreviation.
// Returns (original, abbreviated, savings_percent).
func TokenSavings(v *GValue, dict *KeyDict) (int, int, float64) {
	// Emit without abbreviation
	origOpts := TokenAwareOptions{
		UseAbbreviations: false,
		CompactNumbers:   false,
	}
	original := EmitTokenAwareWithOptions(v, origOpts)
	origTokens := EstimateTokens(original)

	// Emit with abbreviation
	abbrOpts := TokenAwareOptions{
		UseAbbreviations: true,
		CompactNumbers:   true,
		CustomDict:       dict,
	}
	abbreviated := EmitTokenAwareWithOptions(v, abbrOpts)
	abbrTokens := EstimateTokens(abbreviated)

	savings := 0.0
	if origTokens > 0 {
		savings = float64(origTokens-abbrTokens) / float64(origTokens) * 100
	}

	return origTokens, abbrTokens, savings
}

// Helper for base64 encoding
func encodeBase64(data []byte) string {
	const base64Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	var sb strings.Builder
	sb.Grow(((len(data) + 2) / 3) * 4)

	for i := 0; i < len(data); i += 3 {
		var b0, b1, b2 byte
		b0 = data[i]
		if i+1 < len(data) {
			b1 = data[i+1]
		}
		if i+2 < len(data) {
			b2 = data[i+2]
		}

		sb.WriteByte(base64Chars[b0>>2])
		sb.WriteByte(base64Chars[((b0&0x03)<<4)|(b1>>4)])
		if i+1 < len(data) {
			sb.WriteByte(base64Chars[((b1&0x0f)<<2)|(b2>>6)])
		} else {
			sb.WriteByte('=')
		}
		if i+2 < len(data) {
			sb.WriteByte(base64Chars[b2&0x3f])
		} else {
			sb.WriteByte('=')
		}
	}

	return sb.String()
}
