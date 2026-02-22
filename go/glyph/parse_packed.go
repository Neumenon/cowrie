package glyph

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ============================================================
// LYPH v2 Packed Form Parser
// ============================================================
//
// Packed formats:
//   Dense:  Type@(v1 v2 v3 ∅ v5)
//   Bitmap: Type@{bm=0b101}(v1 v3)
//
// This parser requires a schema to decode values by FID order.

// ParsePacked parses a packed struct expression.
func ParsePacked(input string, schema *Schema) (*GValue, error) {
	p := &packedParser{
		input:  input,
		schema: schema,
		pos:    0,
	}
	return p.parse()
}

type packedParser struct {
	input  string
	schema *Schema
	pos    int
}

func (p *packedParser) parse() (*GValue, error) {
	p.skipWhitespace()

	// Expect Type@(...) or Type@{bm=...}(...)
	typeName, err := p.parseTypeName()
	if err != nil {
		return nil, err
	}

	if !p.expect('@') {
		return nil, fmt.Errorf("expected '@' after type name at pos %d", p.pos)
	}

	td := p.schema.GetType(typeName)
	if td == nil {
		return nil, fmt.Errorf("unknown type: %s", typeName)
	}

	// Check for bitmap header
	var mask []bool
	hasBitmap := false

	if p.peek() == '{' {
		// Parse bitmap: {bm=0bXXX}
		mask, err = p.parseBitmapHeader(td)
		if err != nil {
			return nil, err
		}
		hasBitmap = true
	}

	// Parse values: (v1 v2 v3 ...)
	if !p.expect('(') {
		return nil, fmt.Errorf("expected '(' at pos %d", p.pos)
	}

	var value *GValue
	if hasBitmap {
		value, err = p.parseBitmapValues(td, mask)
	} else {
		value, err = p.parseDenseValues(td)
	}

	if err != nil {
		return nil, err
	}

	if !p.expect(')') {
		return nil, fmt.Errorf("expected ')' at pos %d, got %q", p.pos, string(p.peek()))
	}

	return value, nil
}

func (p *packedParser) parseTypeName() (string, error) {
	p.skipWhitespace()
	start := p.pos

	// Type names: [A-Za-z_][A-Za-z0-9_]*
	if p.pos >= len(p.input) {
		return "", fmt.Errorf("unexpected end of input")
	}

	c := p.input[p.pos]
	if !isTypeNameStart(c) {
		return "", fmt.Errorf("expected type name at pos %d", p.pos)
	}

	for p.pos < len(p.input) && isTypeNameCont(p.input[p.pos]) {
		p.pos++
	}

	return p.input[start:p.pos], nil
}

func isTypeNameStart(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_'
}

func isTypeNameCont(c byte) bool {
	return isTypeNameStart(c) || (c >= '0' && c <= '9')
}

func (p *packedParser) parseBitmapHeader(td *TypeDef) ([]bool, error) {
	if !p.expect('{') {
		return nil, fmt.Errorf("expected '{' for bitmap")
	}

	p.skipWhitespace()

	// Expect "bm="
	if !p.expectLiteral("bm=") {
		return nil, fmt.Errorf("expected 'bm=' in bitmap header")
	}

	// Parse binary: 0bXXX
	if !p.expectLiteral("0b") {
		return nil, fmt.Errorf("expected '0b' prefix for bitmap")
	}

	start := p.pos
	for p.pos < len(p.input) && (p.input[p.pos] == '0' || p.input[p.pos] == '1') {
		p.pos++
	}

	if p.pos == start {
		return nil, fmt.Errorf("empty bitmap")
	}

	bits := p.input[start:p.pos]
	mask, err := binaryToMask("0b" + bits)
	if err != nil {
		return nil, err
	}

	p.skipWhitespace()
	if !p.expect('}') {
		return nil, fmt.Errorf("expected '}' after bitmap")
	}

	return mask, nil
}

func (p *packedParser) parseDenseValues(td *TypeDef) (*GValue, error) {
	fields := td.FieldsByFID()
	entries := make([]MapEntry, 0, len(fields))

	for i, fd := range fields {
		p.skipWhitespace()

		if p.peek() == ')' {
			// Remaining fields are null/optional
			for j := i; j < len(fields); j++ {
				if !fields[j].Optional {
					return nil, fmt.Errorf("missing required field: %s", fields[j].Name)
				}
				entries = append(entries, MapEntry{Key: fields[j].Name, Value: Null()})
			}
			break
		}

		val, err := p.parseValue(fd)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", fd.Name, err)
		}

		entries = append(entries, MapEntry{Key: fd.Name, Value: val})
	}

	return &GValue{
		typ: TypeStruct,
		structVal: &StructValue{
			TypeName: td.Name,
			Fields:   entries,
		},
	}, nil
}

func (p *packedParser) parseBitmapValues(td *TypeDef, mask []bool) (*GValue, error) {
	reqFields := td.RequiredFieldsByFID()
	optFields := td.OptionalFieldsByFID()
	entries := make([]MapEntry, 0, len(reqFields)+len(optFields))

	// Parse required fields first
	for _, fd := range reqFields {
		p.skipWhitespace()
		val, err := p.parseValue(fd)
		if err != nil {
			return nil, fmt.Errorf("required field %s: %w", fd.Name, err)
		}
		entries = append(entries, MapEntry{Key: fd.Name, Value: val})
	}

	// Parse optional fields based on bitmap
	maskIdx := 0
	for _, fd := range optFields {
		isPresent := maskIdx < len(mask) && mask[maskIdx]
		maskIdx++

		if isPresent {
			p.skipWhitespace()
			val, err := p.parseValue(fd)
			if err != nil {
				return nil, fmt.Errorf("optional field %s: %w", fd.Name, err)
			}
			entries = append(entries, MapEntry{Key: fd.Name, Value: val})
		} else {
			entries = append(entries, MapEntry{Key: fd.Name, Value: Null()})
		}
	}

	return &GValue{
		typ: TypeStruct,
		structVal: &StructValue{
			TypeName: td.Name,
			Fields:   entries,
		},
	}, nil
}

func (p *packedParser) parseValue(fd *FieldDef) (*GValue, error) {
	p.skipWhitespace()

	c := p.peek()

	// Check for null
	if c == 0xe2 && p.peekRune() == '∅' {
		p.consumeRune()
		return Null(), nil
	}

	switch c {
	case 't':
		// true or bare string starting with t
		if p.tryLiteral("true") || (p.pos < len(p.input) && p.input[p.pos] == 't' && (p.pos+1 >= len(p.input) || !isTypeNameCont(p.input[p.pos+1]))) {
			if p.input[p.pos:p.pos+1] == "t" && (p.pos+1 >= len(p.input) || !isTypeNameCont(p.input[p.pos+1])) {
				p.pos++
				return Bool(true), nil
			}
			if p.tryLiteral("true") {
				return Bool(true), nil
			}
		}
		return p.parseBareOrQuotedString()

	case 'f':
		// false or bare string starting with f
		if p.pos+1 < len(p.input) && !isTypeNameCont(p.input[p.pos+1]) {
			p.pos++
			return Bool(false), nil
		}
		if p.tryLiteral("false") {
			return Bool(false), nil
		}
		return p.parseBareOrQuotedString()

	case '"':
		return p.parseQuotedString()

	case '^':
		return p.parseRef()

	case '[':
		return p.parseList()

	case '{':
		return p.parseMap()

	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return p.parseNumberOrTime()

	default:
		// Check if it's a nested packed struct
		if isTypeNameStart(c) {
			// Could be a bare string OR a nested Type@(...)
			saved := p.pos
			typeName, err := p.parseTypeName()
			if err == nil && p.peek() == '@' {
				// It's a nested packed struct - restore and parse fully
				p.pos = saved
				return p.parseNestedPacked()
			}
			// It's a bare string - but we already consumed it
			return Str(typeName), nil
		}

		return nil, fmt.Errorf("unexpected character at pos %d: %c", p.pos, c)
	}
}

func (p *packedParser) parseNestedPacked() (*GValue, error) {
	typeName, err := p.parseTypeName()
	if err != nil {
		return nil, err
	}

	if !p.expect('@') {
		// Just a bare string
		return Str(typeName), nil
	}

	td := p.schema.GetType(typeName)
	if td == nil {
		return nil, fmt.Errorf("unknown nested type: %s", typeName)
	}

	var mask []bool
	hasBitmap := false

	if p.peek() == '{' {
		mask, err = p.parseBitmapHeader(td)
		if err != nil {
			return nil, err
		}
		hasBitmap = true
	}

	if !p.expect('(') {
		return nil, fmt.Errorf("expected '(' for nested packed")
	}

	var value *GValue
	if hasBitmap {
		value, err = p.parseBitmapValues(td, mask)
	} else {
		value, err = p.parseDenseValues(td)
	}

	if err != nil {
		return nil, err
	}

	if !p.expect(')') {
		return nil, fmt.Errorf("expected ')' for nested packed")
	}

	return value, nil
}

func (p *packedParser) parseNumberOrTime() (*GValue, error) {
	// Check if it looks like an ISO-8601 timestamp: 2025-12-19T...
	// Pattern: YYYY-MM-DDT...Z or similar
	if p.pos+10 < len(p.input) && p.input[p.pos+4] == '-' && p.input[p.pos+7] == '-' && p.input[p.pos+10] == 'T' {
		return p.parseTime()
	}
	return p.parseNumber()
}

func (p *packedParser) parseTime() (*GValue, error) {
	start := p.pos

	// Parse until whitespace or delimiter
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		if c == ' ' || c == ')' || c == ']' || c == '}' || c == '\n' {
			break
		}
		p.pos++
	}

	timeStr := p.input[start:p.pos]
	t, err := time.Parse("2006-01-02T15:04:05Z", timeStr)
	if err != nil {
		// Try other ISO formats
		t, err = time.Parse(time.RFC3339, timeStr)
		if err != nil {
			return nil, fmt.Errorf("invalid time format: %s", timeStr)
		}
	}
	return Time(t), nil
}

func (p *packedParser) parseNumber() (*GValue, error) {
	start := p.pos

	// Optional minus
	if p.pos < len(p.input) && p.input[p.pos] == '-' {
		p.pos++
	}

	// Integer part
	for p.pos < len(p.input) && p.input[p.pos] >= '0' && p.input[p.pos] <= '9' {
		p.pos++
	}

	isFloat := false

	// Decimal part
	if p.pos < len(p.input) && p.input[p.pos] == '.' {
		isFloat = true
		p.pos++
		for p.pos < len(p.input) && p.input[p.pos] >= '0' && p.input[p.pos] <= '9' {
			p.pos++
		}
	}

	// Exponent part
	if p.pos < len(p.input) && (p.input[p.pos] == 'e' || p.input[p.pos] == 'E') {
		isFloat = true
		p.pos++
		if p.pos < len(p.input) && (p.input[p.pos] == '+' || p.input[p.pos] == '-') {
			p.pos++
		}
		for p.pos < len(p.input) && p.input[p.pos] >= '0' && p.input[p.pos] <= '9' {
			p.pos++
		}
	}

	numStr := p.input[start:p.pos]

	if isFloat {
		f, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return nil, err
		}
		return Float(f), nil
	}

	n, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return nil, err
	}
	return Int(n), nil
}

func (p *packedParser) parseQuotedString() (*GValue, error) {
	if !p.expect('"') {
		return nil, fmt.Errorf("expected '\"'")
	}

	var sb strings.Builder
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		if c == '"' {
			p.pos++
			return Str(sb.String()), nil
		}
		if c == '\\' && p.pos+1 < len(p.input) {
			p.pos++
			switch p.input[p.pos] {
			case 'n':
				sb.WriteByte('\n')
			case 'r':
				sb.WriteByte('\r')
			case 't':
				sb.WriteByte('\t')
			case '\\':
				sb.WriteByte('\\')
			case '"':
				sb.WriteByte('"')
			default:
				sb.WriteByte(p.input[p.pos])
			}
		} else {
			sb.WriteByte(c)
		}
		p.pos++
	}

	return nil, fmt.Errorf("unterminated string")
}

func (p *packedParser) parseBareOrQuotedString() (*GValue, error) {
	if p.peek() == '"' {
		return p.parseQuotedString()
	}

	start := p.pos
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		if c == ' ' || c == ')' || c == ']' || c == '}' || c == '\n' {
			break
		}
		p.pos++
	}

	return Str(p.input[start:p.pos]), nil
}

func (p *packedParser) parseRef() (*GValue, error) {
	if !p.expect('^') {
		return nil, fmt.Errorf("expected '^'")
	}

	// Could be quoted: ^"prefix:value"
	if p.peek() == '"' {
		s, err := p.parseQuotedString()
		if err != nil {
			return nil, err
		}
		str, err := s.AsStr()
		if err != nil {
			return nil, err
		}
		ref := parseRefIDFromTarget(str)
		return IDFromRef(ref), nil
	}

	// Bare: ^prefix:value
	start := p.pos
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		if c == ' ' || c == ')' || c == ']' || c == '}' || c == '\n' {
			break
		}
		p.pos++
	}

	ref := parseRefIDFromTarget(p.input[start:p.pos])
	return IDFromRef(ref), nil
}

func (p *packedParser) parseList() (*GValue, error) {
	if !p.expect('[') {
		return nil, fmt.Errorf("expected '['")
	}

	var items []*GValue
	for {
		p.skipWhitespace()
		if p.peek() == ']' {
			p.pos++
			return List(items...), nil
		}

		val, err := p.parseValue(nil)
		if err != nil {
			return nil, err
		}
		items = append(items, val)
	}
}

func (p *packedParser) parseMap() (*GValue, error) {
	if !p.expect('{') {
		return nil, fmt.Errorf("expected '{'")
	}

	var entries []MapEntry
	for {
		p.skipWhitespace()
		if p.peek() == '}' {
			p.pos++
			return Map(entries...), nil
		}

		// Parse key
		key, err := p.parseBareOrQuotedString()
		if err != nil {
			return nil, err
		}

		p.skipWhitespace()
		if p.peek() != ':' && p.peek() != '=' {
			return nil, fmt.Errorf("expected ':' or '=' after map key")
		}
		p.pos++

		// Parse value
		val, err := p.parseValue(nil)
		if err != nil {
			return nil, err
		}

		keyStr, err := key.AsStr()
		if err != nil {
			return nil, err
		}
		entries = append(entries, MapEntry{Key: keyStr, Value: val})
	}
}

func (p *packedParser) skipWhitespace() {
	for p.pos < len(p.input) && (p.input[p.pos] == ' ' || p.input[p.pos] == '\t' || p.input[p.pos] == '\n' || p.input[p.pos] == '\r') {
		p.pos++
	}
}

func (p *packedParser) peek() byte {
	if p.pos >= len(p.input) {
		return 0
	}
	return p.input[p.pos]
}

func (p *packedParser) peekRune() rune {
	if p.pos >= len(p.input) {
		return 0
	}
	r, _ := parseRune(p.input[p.pos:])
	return r
}

func (p *packedParser) consumeRune() {
	if p.pos >= len(p.input) {
		return
	}
	_, size := parseRune(p.input[p.pos:])
	p.pos += size
}

func parseRune(s string) (rune, int) {
	if len(s) == 0 {
		return 0, 0
	}
	b := s[0]
	if b < 0x80 {
		return rune(b), 1
	}
	if b < 0xC0 {
		return 0, 1
	}
	if b < 0xE0 && len(s) >= 2 {
		return rune(b&0x1F)<<6 | rune(s[1]&0x3F), 2
	}
	if b < 0xF0 && len(s) >= 3 {
		return rune(b&0x0F)<<12 | rune(s[1]&0x3F)<<6 | rune(s[2]&0x3F), 3
	}
	if len(s) >= 4 {
		return rune(b&0x07)<<18 | rune(s[1]&0x3F)<<12 | rune(s[2]&0x3F)<<6 | rune(s[3]&0x3F), 4
	}
	return 0, 1
}

func (p *packedParser) expect(c byte) bool {
	p.skipWhitespace()
	if p.pos < len(p.input) && p.input[p.pos] == c {
		p.pos++
		return true
	}
	return false
}

func (p *packedParser) expectLiteral(s string) bool {
	if p.pos+len(s) <= len(p.input) && p.input[p.pos:p.pos+len(s)] == s {
		p.pos += len(s)
		return true
	}
	return false
}

func (p *packedParser) tryLiteral(s string) bool {
	if p.pos+len(s) <= len(p.input) && p.input[p.pos:p.pos+len(s)] == s {
		// Check it's not followed by identifier characters
		if p.pos+len(s) < len(p.input) && isTypeNameCont(p.input[p.pos+len(s)]) {
			return false
		}
		p.pos += len(s)
		return true
	}
	return false
}
