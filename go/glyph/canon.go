package glyph

import (
	"math"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ============================================================
// Canonical Scalar Encoding (LYPH v2)
// ============================================================

// canonNull returns the canonical null representation.
func canonNull() string {
	return "∅"
}

// canonBool returns the canonical boolean representation.
func canonBool(b bool) string {
	if b {
		return "t"
	}
	return "f"
}

// canonInt returns the canonical integer representation.
// No leading zeros, -0 → 0.
func canonInt(n int64) string {
	if n == 0 {
		return "0"
	}
	return strconv.FormatInt(n, 10)
}

// canonFloat returns the canonical float representation.
// Uses shortest-roundtrip format, E→e, -0→0.
func canonFloat(f float64) string {
	// Handle -0 using bit representation (more reliable)
	if math.Float64bits(f) == 0x8000000000000000 {
		return "0"
	}
	if f == 0 {
		return "0"
	}

	// Small integers use integer format for clarity
	// Limit to 10^6 to avoid very long digit strings like "10000000000"
	if f == math.Trunc(f) && math.Abs(f) < 1e6 {
		return strconv.FormatInt(int64(f), 10)
	}

	// Use 'g' format for shortest representation
	s := strconv.FormatFloat(f, 'g', -1, 64)
	s = strings.ReplaceAll(s, "E", "e")

	// Verify round-trip to catch precision loss
	if parsed, _ := strconv.ParseFloat(s, 64); parsed != f {
		s = strconv.FormatFloat(f, 'e', 17, 64)
		s = strings.ReplaceAll(s, "E", "e")
	}

	return s
}

// canonString returns the canonical string representation.
// Uses bare form if safe, otherwise quoted with minimal escapes.
func canonString(s string) string {
	if isBareSafeV2(s) {
		return s
	}
	return quoteString(s)
}

// canonRef returns the canonical reference representation.
// Format: ^prefix:value or ^"quoted:value" if unsafe.
func canonRef(r RefID) string {
	full := r.String()[1:] // Remove leading ^ from String()
	if isRefSafe(full) {
		return "^" + full
	}
	return "^" + quoteString(full)
}

// ============================================================
// Safety Checks
// ============================================================

// isBareSafeV2 checks if a string can be represented without quotes.
// Pattern: ^[A-Za-z_][A-Za-z0-9_\-./]*$
// Must not be a reserved word: t, f, ∅, null, none, nil, true, false
func isBareSafeV2(s string) bool {
	if len(s) == 0 {
		return false
	}

	// Check reserved words
	switch s {
	case "t", "f", "true", "false", "null", "none", "nil":
		return false
	}

	// Check first character: must be letter or underscore
	r, size := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError {
		return false
	}
	if !unicode.IsLetter(r) && r != '_' {
		return false
	}

	// Check remaining characters: letter, digit, _, -, ., /
	for i := size; i < len(s); {
		r, size = utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError {
			return false
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) &&
			r != '_' && r != '-' && r != '.' && r != '/' {
			return false
		}
		i += size
	}

	return true
}

// isRefSafe checks if a ref string can be used without quoting.
// Refs allow: letters, digits, _, -, ., /, :
func isRefSafe(s string) bool {
	if len(s) == 0 {
		return false
	}

	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError {
			return false
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) &&
			r != '_' && r != '-' && r != '.' && r != '/' && r != ':' {
			return false
		}
		i += size
	}

	return true
}

// ============================================================
// String Quoting
// ============================================================

// quoteString returns a quoted string with minimal escapes.
func quoteString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 10)
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
				// Control character: use \u00XX
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
	return b.String()
}

// ============================================================
// Canonical Value Encoding
// ============================================================

// canonValue returns the canonical string representation of any GValue.
// This is used for scalar values; containers use specialized encoders.
func canonValue(v *GValue) string {
	if v == nil {
		return canonNull()
	}

	switch v.typ {
	case TypeNull:
		return canonNull()
	case TypeBool:
		return canonBool(v.boolVal)
	case TypeInt:
		return canonInt(v.intVal)
	case TypeFloat:
		return canonFloat(v.floatVal)
	case TypeStr:
		return canonString(v.strVal)
	case TypeID:
		return canonRef(v.idVal)
	case TypeTime:
		// ISO-8601 format
		return v.timeVal.UTC().Format("2006-01-02T15:04:05Z")
	case TypeBytes:
		// Base64 encoded
		return "b64" + quoteString(string(v.bytesVal))
	default:
		// Container types handled by specialized encoders
		return ""
	}
}

// ============================================================
// Bitmap Encoding
// ============================================================

// maskToBinary converts a boolean mask to "0bXXX" format.
// LSB = first element (lowest fid optional field).
// Returns "0b0" for empty or all-false mask.
func maskToBinary(mask []bool) string {
	// Find highest set bit
	hi := -1
	for i := len(mask) - 1; i >= 0; i-- {
		if mask[i] {
			hi = i
			break
		}
	}

	if hi == -1 {
		return "0b0"
	}

	var b strings.Builder
	b.WriteString("0b")

	// Write bits from MSB (highest index) to LSB (index 0)
	for i := hi; i >= 0; i-- {
		if mask[i] {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
	}

	return b.String()
}

// binaryToMask parses a "0bXXX" string back to a boolean mask.
func binaryToMask(s string) ([]bool, error) {
	if !strings.HasPrefix(s, "0b") {
		return nil, &ParseError{Message: "bitmap must start with 0b"}
	}

	bits := s[2:]
	if len(bits) == 0 {
		return nil, &ParseError{Message: "empty bitmap"}
	}

	// Bits are written MSB first, so reverse to get LSB-first mask
	mask := make([]bool, len(bits))
	for i, c := range bits {
		idx := len(bits) - 1 - i // Reverse index
		switch c {
		case '1':
			mask[idx] = true
		case '0':
			mask[idx] = false
		default:
			return nil, &ParseError{Message: "invalid bitmap character"}
		}
	}

	return mask, nil
}
