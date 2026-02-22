package glyph

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"time"
)

// ============================================================
// LYPH v2 Tabular Mode Parser (Streaming)
// ============================================================
//
// Tabular format:
//   @tab Type [col1 col2 col3]
//   val1a val1b val1c
//   val2a val2b val2c
//   @end
//
// TabularReader provides streaming access to tabular data.

// TabularReader reads tabular data row by row.
type TabularReader struct {
	scanner  *bufio.Scanner
	schema   *Schema
	typeName string
	td       *TypeDef
	columns  []string    // Column names from header
	fields   []*FieldDef // Field definitions in column order
	started  bool
	finished bool
	rowNum   int
}

const (
	tabularScannerBufSize = 64 * 1024
	tabularScannerMaxSize = 4 * 1024 * 1024
)

// NewTabularReader creates a streaming tabular reader.
func NewTabularReader(r io.Reader, schema *Schema) *TabularReader {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, tabularScannerBufSize), tabularScannerMaxSize)
	return &TabularReader{
		scanner: scanner,
		schema:  schema,
	}
}

// NewTabularReaderFromString creates a reader from a string.
func NewTabularReaderFromString(input string, schema *Schema) *TabularReader {
	return NewTabularReader(strings.NewReader(input), schema)
}

// ReadHeader reads and parses the @tab header line.
// Returns the type name and column names.
// Must be called before Next().
func (tr *TabularReader) ReadHeader() (typeName string, columns []string, err error) {
	if tr.started {
		return tr.typeName, tr.columns, nil
	}
	if tr.schema == nil {
		return "", nil, fmt.Errorf("schema is required for tabular parsing")
	}

	// Read lines until we find @tab
	for tr.scanner.Scan() {
		line := strings.TrimSpace(tr.scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse @tab header
		if strings.HasPrefix(line, "@tab ") {
			if err := tr.parseHeader(line); err != nil {
				return "", nil, err
			}
			tr.started = true
			return tr.typeName, tr.columns, nil
		}

		return "", nil, fmt.Errorf("expected @tab header, got: %s", line)
	}

	if err := tr.scanner.Err(); err != nil {
		return "", nil, err
	}

	return "", nil, io.EOF
}

// parseHeader parses: @tab Type [col1 col2 col3]
func (tr *TabularReader) parseHeader(line string) error {
	// Remove @tab prefix
	rest := strings.TrimPrefix(line, "@tab ")
	rest = strings.TrimSpace(rest)

	// Find type name (everything before '[')
	bracketIdx := strings.Index(rest, "[")
	if bracketIdx == -1 {
		return fmt.Errorf("missing column list in @tab header")
	}

	tr.typeName = strings.TrimSpace(rest[:bracketIdx])

	// Get type definition
	tr.td = tr.schema.GetType(tr.typeName)
	if tr.td == nil {
		return fmt.Errorf("unknown type: %s", tr.typeName)
	}

	// Parse column list: [col1 col2 col3]
	colPart := rest[bracketIdx:]
	if !strings.HasPrefix(colPart, "[") || !strings.HasSuffix(colPart, "]") {
		return fmt.Errorf("invalid column list format")
	}

	colPart = strings.TrimPrefix(colPart, "[")
	colPart = strings.TrimSuffix(colPart, "]")
	colPart = strings.TrimSpace(colPart)

	tr.columns = strings.Fields(colPart)
	if len(tr.columns) == 0 {
		return fmt.Errorf("empty column list")
	}

	// Map columns to field definitions
	tr.fields = make([]*FieldDef, len(tr.columns))
	for i, col := range tr.columns {
		fd := tr.findField(col)
		if fd == nil {
			return fmt.Errorf("unknown column: %s", col)
		}
		tr.fields[i] = fd
	}

	return nil
}

// findField finds a field by name, wire key, or FID.
func (tr *TabularReader) findField(col string) *FieldDef {
	// Check for FID format: #123
	if strings.HasPrefix(col, "#") {
		fidStr := strings.TrimPrefix(col, "#")
		var fid int
		if _, err := fmt.Sscanf(fidStr, "%d", &fid); err == nil {
			for _, fd := range tr.td.Struct.Fields {
				if fd.FID == fid {
					return fd
				}
			}
		}
		return nil
	}

	// Check field name first
	for _, fd := range tr.td.Struct.Fields {
		if fd.Name == col {
			return fd
		}
	}

	// Check wire key
	for _, fd := range tr.td.Struct.Fields {
		if fd.WireKey == col {
			return fd
		}
	}

	return nil
}

// TypeName returns the struct type name from the header.
func (tr *TabularReader) TypeName() string {
	return tr.typeName
}

// Columns returns the column names from the header.
func (tr *TabularReader) Columns() []string {
	return tr.columns
}

// Next reads and parses the next data row.
// Returns io.EOF when @end is reached.
func (tr *TabularReader) Next() (*GValue, error) {
	if !tr.started {
		if _, _, err := tr.ReadHeader(); err != nil {
			return nil, err
		}
	}

	if tr.finished {
		return nil, io.EOF
	}

	for tr.scanner.Scan() {
		line := strings.TrimSpace(tr.scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Check for @end
		if line == "@end" {
			tr.finished = true
			return nil, io.EOF
		}

		// Parse data row
		tr.rowNum++
		return tr.parseRow(line)
	}

	if err := tr.scanner.Err(); err != nil {
		return nil, err
	}

	// No @end found - that's an error
	return nil, fmt.Errorf("unexpected end of input (missing @end)")
}

// parseRow parses a single data row.
func (tr *TabularReader) parseRow(line string) (*GValue, error) {
	p := &tabularRowParser{
		input:  line,
		schema: tr.schema,
		pos:    0,
	}

	entries := make([]MapEntry, 0, len(tr.fields))

	for i, fd := range tr.fields {
		p.skipWhitespace()

		if p.pos >= len(p.input) {
			// Missing values at end - treat as null if optional
			if !fd.Optional {
				return nil, fmt.Errorf("row %d: missing required field %s", tr.rowNum, fd.Name)
			}
			entries = append(entries, MapEntry{Key: fd.Name, Value: Null()})
			continue
		}

		val, err := p.parseValue(fd)
		if err != nil {
			return nil, fmt.Errorf("row %d, column %d (%s): %w", tr.rowNum, i+1, tr.columns[i], err)
		}

		entries = append(entries, MapEntry{Key: fd.Name, Value: val})
	}
	p.skipWhitespace()
	if p.pos < len(p.input) {
		return nil, fmt.Errorf("row %d: extra columns starting at %d", tr.rowNum, p.pos+1)
	}

	return &GValue{
		typ: TypeStruct,
		structVal: &StructValue{
			TypeName: tr.typeName,
			Fields:   entries,
		},
	}, nil
}

// RowNum returns the number of data rows read so far.
func (tr *TabularReader) RowNum() int {
	return tr.rowNum
}

// ReadAll reads all remaining rows into a slice.
func (tr *TabularReader) ReadAll() ([]*GValue, error) {
	var rows []*GValue

	for {
		row, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// ============================================================
// Tabular Row Parser
// ============================================================

type tabularRowParser struct {
	input  string
	schema *Schema
	pos    int
}

func (p *tabularRowParser) parseValue(fd *FieldDef) (*GValue, error) {
	p.skipWhitespace()

	if p.pos >= len(p.input) {
		return Null(), nil
	}

	c := p.peek()

	// Check for null: ∅
	if c == 0xe2 && p.peekRune() == '∅' {
		p.consumeRune()
		return Null(), nil
	}
	if c == '_' && p.isTokenBoundary(p.pos+1) {
		p.pos++
		return Null(), nil
	}
	if c == 'n' && strings.HasPrefix(p.input[p.pos:], "null") && p.isTokenBoundary(p.pos+4) {
		p.pos += 4
		return Null(), nil
	}

	switch c {
	case 't':
		// true or bare string
		if p.pos+1 >= len(p.input) || !isTypeNameCont(p.input[p.pos+1]) {
			p.pos++
			return Bool(true), nil
		}
		return p.parseBareOrQuotedString()

	case 'f':
		// false or bare string
		if p.pos+1 >= len(p.input) || !isTypeNameCont(p.input[p.pos+1]) {
			p.pos++
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
		// Check if it's a nested packed struct: Type@(...)
		if isTypeNameStart(c) {
			return p.parseNestedPackedOrBareString()
		}

		return nil, fmt.Errorf("unexpected character at pos %d: %c", p.pos, c)
	}
}

func (p *tabularRowParser) parseNestedPackedOrBareString() (*GValue, error) {
	saved := p.pos

	// Try to parse as Type@(...)
	typeName, err := p.parseTypeName()
	if err != nil {
		return nil, err
	}

	if p.peek() == '@' {
		// It's a nested packed struct
		p.pos++ // consume '@'

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

		pp := &packedParser{
			input:  p.input,
			schema: p.schema,
			pos:    p.pos,
		}

		var value *GValue
		if hasBitmap {
			value, err = pp.parseBitmapValues(td, mask)
		} else {
			value, err = pp.parseDenseValues(td)
		}
		p.pos = pp.pos

		if err != nil {
			return nil, err
		}

		if !p.expect(')') {
			return nil, fmt.Errorf("expected ')' for nested packed")
		}

		return value, nil
	}

	// It's a bare string - restore and parse normally
	p.pos = saved
	return p.parseBareString()
}

func (p *tabularRowParser) parseTypeName() (string, error) {
	p.skipWhitespace()
	start := p.pos

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

func (p *tabularRowParser) parseBitmapHeader(td *TypeDef) ([]bool, error) {
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

func (p *tabularRowParser) parseNumberOrTime() (*GValue, error) {
	// Check if it looks like an ISO-8601 timestamp
	if looksLikeTime(p.input, p.pos) {
		return p.parseTime()
	}
	return p.parseNumber()
}

func (p *tabularRowParser) parseTime() (*GValue, error) {
	start := p.pos

	for p.pos < len(p.input) {
		c := p.input[p.pos]
		if c == ' ' || c == ')' || c == ']' || c == '}' || c == '\n' || c == '|' {
			break
		}
		p.pos++
	}

	timeStr := p.input[start:p.pos]

	// Try common ISO-8601 formats
	t, err := time.Parse("2006-01-02T15:04:05Z", timeStr)
	if err != nil {
		t, err = time.Parse(time.RFC3339, timeStr)
		if err != nil {
			t, err = time.Parse("2006-01-02T15:04:05.000Z", timeStr)
			if err != nil {
				t, err = time.Parse("2006-01-02", timeStr)
				if err != nil {
					return nil, fmt.Errorf("invalid time format: %s", timeStr)
				}
			}
		}
	}

	return Time(t), nil
}

func (p *tabularRowParser) parseNumber() (*GValue, error) {
	start := p.pos

	if p.pos < len(p.input) && p.input[p.pos] == '-' {
		p.pos++
	}

	for p.pos < len(p.input) && p.input[p.pos] >= '0' && p.input[p.pos] <= '9' {
		p.pos++
	}

	isFloat := false

	if p.pos < len(p.input) && p.input[p.pos] == '.' {
		isFloat = true
		p.pos++
		for p.pos < len(p.input) && p.input[p.pos] >= '0' && p.input[p.pos] <= '9' {
			p.pos++
		}
	}

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
		f, err := parseFloatValue(numStr)
		if err != nil {
			return nil, err
		}
		return Float(f), nil
	}

	n, err := parseIntValue(numStr)
	if err != nil {
		return nil, err
	}
	return Int(n), nil
}

func (p *tabularRowParser) parseQuotedString() (*GValue, error) {
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

func (p *tabularRowParser) parseBareString() (*GValue, error) {
	start := p.pos
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		if c == ' ' || c == ')' || c == ']' || c == '}' || c == '\n' || c == '|' {
			break
		}
		p.pos++
	}
	return Str(p.input[start:p.pos]), nil
}

func (p *tabularRowParser) parseBareOrQuotedString() (*GValue, error) {
	if p.peek() == '"' {
		return p.parseQuotedString()
	}
	return p.parseBareString()
}

func (p *tabularRowParser) parseRef() (*GValue, error) {
	if !p.expect('^') {
		return nil, fmt.Errorf("expected '^'")
	}

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

	start := p.pos
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		if c == ' ' || c == ')' || c == ']' || c == '}' || c == '\n' || c == '|' {
			break
		}
		p.pos++
	}

	ref := parseRefIDFromTarget(p.input[start:p.pos])
	return IDFromRef(ref), nil
}

func (p *tabularRowParser) parseList() (*GValue, error) {
	if !p.expect('[') {
		return nil, fmt.Errorf("expected '['")
	}

	var items []*GValue
	for {
		p.skipWhitespace()
		if p.peek() == ',' {
			p.pos++
			continue
		}
		if p.peek() == ']' {
			p.pos++
			return List(items...), nil
		}

		val, err := p.parseValue(nil)
		if err != nil {
			return nil, err
		}
		items = append(items, val)
		p.skipWhitespace()
		if p.peek() == ',' {
			p.pos++
		}
	}
}

func (p *tabularRowParser) parseMap() (*GValue, error) {
	if !p.expect('{') {
		return nil, fmt.Errorf("expected '{'")
	}

	var entries []MapEntry
	for {
		p.skipWhitespace()
		if p.peek() == ',' {
			p.pos++
			continue
		}
		if p.peek() == '}' {
			p.pos++
			return Map(entries...), nil
		}

		keyStr, err := p.parseMapKey()
		if err != nil {
			return nil, err
		}

		p.skipWhitespace()
		if p.peek() != ':' && p.peek() != '=' {
			return nil, fmt.Errorf("expected ':' or '=' after map key")
		}
		p.pos++

		val, err := p.parseValue(nil)
		if err != nil {
			return nil, err
		}

		entries = append(entries, MapEntry{Key: keyStr, Value: val})
		p.skipWhitespace()
		if p.peek() == ',' {
			p.pos++
		}
	}
}

func (p *tabularRowParser) skipWhitespace() {
	for p.pos < len(p.input) && (p.input[p.pos] == ' ' || p.input[p.pos] == '\t') {
		p.pos++
	}
}

func (p *tabularRowParser) peek() byte {
	if p.pos >= len(p.input) {
		return 0
	}
	return p.input[p.pos]
}

func (p *tabularRowParser) peekRune() rune {
	if p.pos >= len(p.input) {
		return 0
	}
	r, _ := parseRune(p.input[p.pos:])
	return r
}

func (p *tabularRowParser) consumeRune() {
	if p.pos >= len(p.input) {
		return
	}
	_, size := parseRune(p.input[p.pos:])
	p.pos += size
}

func (p *tabularRowParser) expect(c byte) bool {
	p.skipWhitespace()
	if p.pos < len(p.input) && p.input[p.pos] == c {
		p.pos++
		return true
	}
	return false
}

func (p *tabularRowParser) expectLiteral(s string) bool {
	if p.pos+len(s) <= len(p.input) && p.input[p.pos:p.pos+len(s)] == s {
		p.pos += len(s)
		return true
	}
	return false
}

func (p *tabularRowParser) parseMapKey() (string, error) {
	p.skipWhitespace()
	if p.peek() == '"' {
		v, err := p.parseQuotedString()
		if err != nil {
			return "", err
		}
		return v.AsStr()
	}

	start := p.pos
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		if c == '=' || c == ':' || c == ' ' || c == '\t' || c == '\n' || c == ')' || c == ']' || c == '}' || c == '|' || c == ',' {
			break
		}
		p.pos++
	}
	if p.pos == start {
		return "", fmt.Errorf("expected map key")
	}
	return p.input[start:p.pos], nil
}

func (p *tabularRowParser) isTokenBoundary(pos int) bool {
	if pos >= len(p.input) {
		return true
	}
	switch p.input[pos] {
	case ' ', '\t', '\n', ')', ']', '}', '|':
		return true
	default:
		return false
	}
}

// ============================================================
// Inline Tabular Parser
// ============================================================

// ParseInlineTabular parses inline tabular format:
// @tab Type [cols] v1 v2 | v3 v4 | ... @end
func ParseInlineTabular(input string, schema *Schema) ([]*GValue, error) {
	input = strings.TrimSpace(input)
	if schema == nil {
		return nil, fmt.Errorf("schema is required for tabular parsing")
	}

	if !strings.HasPrefix(input, "@tab ") {
		return nil, fmt.Errorf("expected @tab header")
	}

	// Find @end
	endIdx := strings.LastIndex(input, "@end")
	if endIdx == -1 {
		return nil, fmt.Errorf("missing @end marker")
	}

	// Extract content between header and @end
	headerEnd := strings.Index(input, "]")
	if headerEnd == -1 {
		return nil, fmt.Errorf("missing column list ']'")
	}

	// Parse header
	headerPart := input[:headerEnd+1]
	rest := strings.TrimSpace(input[headerEnd+1 : endIdx])

	// Extract type name and columns from header
	typeName, columns, err := parseInlineHeader(headerPart)
	if err != nil {
		return nil, err
	}

	td := schema.GetType(typeName)
	if td == nil {
		return nil, fmt.Errorf("unknown type: %s", typeName)
	}

	// Map columns to fields
	fields := make([]*FieldDef, len(columns))
	for i, col := range columns {
		fd := findFieldByColumnName(td, col)
		if fd == nil {
			return nil, fmt.Errorf("unknown column: %s", col)
		}
		fields[i] = fd
	}

	// Split by | for rows (respect quoted strings)
	rowStrs := splitInlineRows(rest)
	var rows []*GValue

	for _, rowStr := range rowStrs {
		rowStr = strings.TrimSpace(rowStr)
		if rowStr == "" {
			continue
		}

		p := &tabularRowParser{
			input:  rowStr,
			schema: schema,
			pos:    0,
		}

		entries := make([]MapEntry, 0, len(fields))
		for _, fd := range fields {
			p.skipWhitespace()

			if p.pos >= len(p.input) {
				if !fd.Optional {
					return nil, fmt.Errorf("missing required field %s", fd.Name)
				}
				entries = append(entries, MapEntry{Key: fd.Name, Value: Null()})
				continue
			}

			val, err := p.parseValue(fd)
			if err != nil {
				return nil, err
			}
			entries = append(entries, MapEntry{Key: fd.Name, Value: val})
		}
		p.skipWhitespace()
		if p.pos < len(p.input) {
			return nil, fmt.Errorf("extra columns in inline row starting at %d", p.pos+1)
		}

		rows = append(rows, &GValue{
			typ: TypeStruct,
			structVal: &StructValue{
				TypeName: typeName,
				Fields:   entries,
			},
		})
	}

	return rows, nil
}

func looksLikeTime(input string, pos int) bool {
	if pos+9 >= len(input) {
		return false
	}
	if input[pos+4] != '-' || input[pos+7] != '-' {
		return false
	}
	// Date-time with T
	if pos+10 < len(input) && input[pos+10] == 'T' {
		return true
	}
	// Date-only if boundary after YYYY-MM-DD
	if pos+10 == len(input) {
		return true
	}
	return isTimeDelimiter(input[pos+10])
}

func isTimeDelimiter(c byte) bool {
	switch c {
	case ' ', '\t', '\n', ')', ']', '}', '|':
		return true
	default:
		return false
	}
}

func splitInlineRows(input string) []string {
	var rows []string
	var b strings.Builder
	inString := false
	escaped := false

	for i := 0; i < len(input); i++ {
		c := input[i]
		if inString {
			b.WriteByte(c)
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == '"' {
				inString = false
			}
			continue
		}

		switch c {
		case '"':
			inString = true
			b.WriteByte(c)
		case '|':
			rows = append(rows, strings.TrimSpace(b.String()))
			b.Reset()
		default:
			b.WriteByte(c)
		}
	}

	if b.Len() > 0 {
		rows = append(rows, strings.TrimSpace(b.String()))
	}

	return rows
}

func parseInlineHeader(header string) (typeName string, columns []string, err error) {
	// Format: @tab Type [col1 col2 col3]
	rest := strings.TrimPrefix(header, "@tab ")
	rest = strings.TrimSpace(rest)

	bracketIdx := strings.Index(rest, "[")
	if bracketIdx == -1 {
		return "", nil, fmt.Errorf("missing column list")
	}

	typeName = strings.TrimSpace(rest[:bracketIdx])

	colPart := rest[bracketIdx:]
	colPart = strings.TrimPrefix(colPart, "[")
	colPart = strings.TrimSuffix(colPart, "]")
	colPart = strings.TrimSpace(colPart)

	columns = strings.Fields(colPart)
	return typeName, columns, nil
}

func findFieldByColumnName(td *TypeDef, col string) *FieldDef {
	// Check for FID format: #123
	if strings.HasPrefix(col, "#") {
		fidStr := strings.TrimPrefix(col, "#")
		var fid int
		if _, err := fmt.Sscanf(fidStr, "%d", &fid); err == nil {
			for _, fd := range td.Struct.Fields {
				if fd.FID == fid {
					return fd
				}
			}
		}
		return nil
	}

	// Check field name
	for _, fd := range td.Struct.Fields {
		if fd.Name == col {
			return fd
		}
	}

	// Check wire key
	for _, fd := range td.Struct.Fields {
		if fd.WireKey == col {
			return fd
		}
	}

	return nil
}

// ============================================================
// Helper functions for parsing values
// ============================================================

func parseFloatValue(s string) (float64, error) {
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	return f, err
}

func parseIntValue(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
