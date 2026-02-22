package glyph

import (
	"fmt"
	"strconv"
	"strings"
)

// ============================================================
// LYPH v2 Patch Parser
// ============================================================
//
// Parses patch blocks in the format:
//
//   @patch @schema#abc123 @keys=wire @target=m:123
//   = home.ft_h 2
//   = away.ft_a 1
//   + events "Goal!"
//   - odds
//   ~ home.rating +0.15
//   @end
//
// This complements emit_patch.go which handles encoding.

// ParsePatch parses a patch block from text.
// Input should include the @patch header and @end footer.
func ParsePatch(input string, schema *Schema) (*Patch, error) {
	return ParsePatchWithOptions(input, schema, KeyModeWire)
}

// ParsePatchWithOptions parses a patch with explicit key mode.
func ParsePatchWithOptions(input string, schema *Schema, defaultKeyMode KeyMode) (*Patch, error) {
	lines := strings.Split(input, "\n")
	if len(lines) == 0 {
		return nil, &ParseError{Message: "empty patch input"}
	}

	// Parse header line
	headerLine := strings.TrimSpace(lines[0])
	header, err := parsePatchHeader(headerLine)
	if err != nil {
		return nil, err
	}

	// Use header's key mode if specified, else default
	keyMode := defaultKeyMode
	if header.KeyMode != KeyModeWire || strings.Contains(headerLine, "@keys=") {
		keyMode = header.KeyMode
	}

	patch := &Patch{
		Target:          header.Target,
		SchemaID:        header.SchemaID,
		BaseFingerprint: header.BaseFingerprint,
		Ops:             make([]*PatchOp, 0),
	}

	// Parse operations
	for i := 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// End marker
		if line == "@end" {
			break
		}

		// Parse operation
		op, err := parsePatchOp(line, keyMode, schema)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}

		patch.Ops = append(patch.Ops, op)
	}

	return patch, nil
}

// parsePatchHeader parses the @patch header line.
func parsePatchHeader(line string) (*Header, error) {
	if !strings.HasPrefix(line, "@patch") {
		return nil, &ParseError{Message: "patch must start with @patch"}
	}

	h := &Header{
		Version: "v2",
		Mode:    ModePatch,
		KeyMode: KeyModeWire,
		Raw:     line,
	}

	tokens := tokenizeHeader(line)

	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]

		switch {
		case tok == "@patch":
			// Already handled

		case strings.HasPrefix(tok, "@schema#"):
			h.SchemaID = tok[8:]

		case strings.HasPrefix(tok, "@keys="):
			keys := tok[6:]
			switch keys {
			case "wire":
				h.KeyMode = KeyModeWire
			case "name":
				h.KeyMode = KeyModeName
			case "fid":
				h.KeyMode = KeyModeFID
			default:
				return nil, &ParseError{Message: fmt.Sprintf("unknown key mode: %s", keys)}
			}

		case strings.HasPrefix(tok, "@target="):
			target := tok[8:]
			h.Target = parseRefIDFromTarget(target)

		case strings.HasPrefix(tok, "@base="):
			h.BaseFingerprint = tok[6:]
		}
	}

	return h, nil
}

// parsePatchOp parses a single patch operation line.
// Format: <op> <path> [value]
// Examples:
//
//	= home.score 2
//	+ events "Goal!"
//	- odds
//	~ rating +0.15
func parsePatchOp(line string, keyMode KeyMode, schema *Schema) (*PatchOp, error) {
	if len(line) == 0 {
		return nil, &ParseError{Message: "empty operation line"}
	}

	// First character is the operation
	opChar := rune(line[0])
	var opKind PatchOpKind

	switch opChar {
	case '=':
		opKind = OpSet
	case '+':
		opKind = OpAppend
	case '-':
		opKind = OpDelete
	case '~':
		opKind = OpDelta
	default:
		return nil, &ParseError{Message: fmt.Sprintf("unknown operation: %c", opChar)}
	}

	// Rest of line after op character
	rest := strings.TrimSpace(line[1:])
	if rest == "" {
		return nil, &ParseError{Message: "missing path in operation"}
	}

	// Split into path and value
	// Path ends at first space (unless inside quotes/brackets)
	pathEnd := findPathEnd(rest)
	pathStr := rest[:pathEnd]
	valueStr := ""
	if pathEnd < len(rest) {
		valueStr = strings.TrimSpace(rest[pathEnd:])
	}

	// Parse path
	path := parsePathToSegs(pathStr)

	op := &PatchOp{
		Op:    opKind,
		Path:  path,
		Index: -1, // Default: append to end
	}

	// Parse value based on operation type
	switch opKind {
	case OpSet, OpAppend:
		if valueStr != "" {
			// Check for @idx= suffix (insert at index)
			if idx := strings.Index(valueStr, " @idx="); idx >= 0 {
				idxStr := valueStr[idx+6:]
				op.Index, _ = strconv.Atoi(idxStr)
				valueStr = valueStr[:idx]
			}

			val, err := parseInlineValue(valueStr, schema)
			if err != nil {
				return nil, err
			}
			op.Value = val
		}

	case OpDelta:
		if valueStr == "" {
			return nil, &ParseError{Message: "delta operation requires a value"}
		}
		delta, err := parseDeltaValue(valueStr)
		if err != nil {
			return nil, err
		}
		op.Value = delta

	case OpDelete:
		// No value needed
	}

	return op, nil
}

// findPathEnd finds where the path ends in the string.
// Path ends at first unquoted, unbracket space.
func findPathEnd(s string) int {
	inQuote := false
	bracketDepth := 0

	for i, r := range s {
		switch r {
		case '"':
			inQuote = !inQuote
		case '[':
			if !inQuote {
				bracketDepth++
			}
		case ']':
			if !inQuote && bracketDepth > 0 {
				bracketDepth--
			}
		case ' ', '\t':
			if !inQuote && bracketDepth == 0 {
				return i
			}
		}
	}

	return len(s)
}

// parseInlineValue parses a value from the operation line.
// This handles primitives and packed structs inline.
func parseInlineValue(s string, schema *Schema) (*GValue, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}

	// Check for packed format: Type@(...) or Type@{bm=...}(...)
	if isPackedFormat(s) && schema != nil {
		return ParsePacked(s, schema)
	}

	// Use the main parser for value parsing
	result, err := ParseWithOptions(s, ParseOptions{Schema: schema, Tolerant: false})
	if err != nil {
		return nil, err
	}
	if result.HasErrors() {
		return nil, &ParseError{Message: result.Errors[0].Message}
	}

	return result.Value, nil
}

// isPackedFormat checks if a string looks like packed format.
func isPackedFormat(s string) bool {
	// Look for Type@( or Type@{bm=
	atIdx := strings.Index(s, "@")
	if atIdx <= 0 {
		return false
	}
	// Check character after @
	if atIdx+1 >= len(s) {
		return false
	}
	next := s[atIdx+1]
	return next == '(' || next == '{'
}

// parseDeltaValue parses a delta value like "+0.15" or "-3".
func parseDeltaValue(s string) (*GValue, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, &ParseError{Message: "empty delta value"}
	}

	// Parse as float (handles +/- prefix)
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		// Try as int
		n, err2 := strconv.ParseInt(s, 10, 64)
		if err2 != nil {
			return nil, &ParseError{Message: fmt.Sprintf("invalid delta value: %s", s)}
		}
		return Int(n), nil
	}

	return Float(f), nil
}

// ============================================================
// Patch Round-Trip Helpers
// ============================================================

// ParsePatchRoundTrip parses a patch, applies it, then re-emits.
// Useful for testing canonical form preservation.
func ParsePatchRoundTrip(input string, schema *Schema) (string, error) {
	patch, err := ParsePatch(input, schema)
	if err != nil {
		return "", err
	}

	return EmitPatch(patch, schema)
}
