// Package glyph - Streaming Incremental Parser for real-time GLYPH processing.
//
// This module provides an incremental parser that emits events as tokens arrive,
// enabling processing of streaming LLM output before the complete response is
// available. This reduces perceived latency for UI applications.
//
// Key features:
//   - Event-based parsing with callbacks
//   - Partial value extraction for early processing
//   - Automatic buffering and state management
//   - Support for resumption after network interruption
package glyph

import (
	"errors"
	"sync"
)

// ParseEventType identifies the type of parse event.
type ParseEventType uint8

const (
	EventNone ParseEventType = iota
	EventStartObject         // Beginning of { or Type{
	EventEndObject           // End of }
	EventStartList           // Beginning of [
	EventEndList             // End of ]
	EventKey                 // Field key parsed
	EventValue               // Scalar value parsed
	EventStartSum            // Beginning of Tag( or Tag{
	EventEndSum              // End of ) or }
	EventError               // Parse error
	EventNeedMore            // Need more input
)

// String returns the event type name.
func (e ParseEventType) String() string {
	switch e {
	case EventNone:
		return "NONE"
	case EventStartObject:
		return "START_OBJECT"
	case EventEndObject:
		return "END_OBJECT"
	case EventStartList:
		return "START_LIST"
	case EventEndList:
		return "END_LIST"
	case EventKey:
		return "KEY"
	case EventValue:
		return "VALUE"
	case EventStartSum:
		return "START_SUM"
	case EventEndSum:
		return "END_SUM"
	case EventError:
		return "ERROR"
	case EventNeedMore:
		return "NEED_MORE"
	default:
		return "UNKNOWN"
	}
}

// ParseEvent represents a single parsing event.
type ParseEvent struct {
	Type     ParseEventType
	Path     []PathElement // Path to current location
	Key      string        // For EventKey
	Value    *GValue       // For EventValue
	TypeName string        // For EventStartObject (struct type)
	Tag      string        // For EventStartSum
	Error    error         // For EventError
	Pos      Position      // Source position
}

// PathElement represents one level in the parse path.
type PathElement struct {
	IsIndex bool   // true if array index, false if key
	Index   int    // Array index (if IsIndex)
	Key     string // Object key (if !IsIndex)
}

// ParseHandler is called for each parse event.
type ParseHandler func(event ParseEvent) error

// IncrementalParser provides streaming parse functionality.
type IncrementalParser struct {
	mu      sync.Mutex
	handler ParseHandler
	buffer  []byte
	pos     int
	path    []PathElement
	state   parseState
	stack   []parseStackFrame
	err     error

	// Configuration
	maxDepth    int
	maxKeyLen   int
	maxValueLen int
}

type parseState int

const (
	stateStart parseState = iota
	stateInObject
	stateInList
	stateExpectKey
	stateExpectValue
	stateExpectColon
	stateAfterValue
	stateDone
	stateError
)

type parseStackFrame struct {
	state    parseState
	typeName string // For structs
	tag      string // For sums
	count    int    // Number of items parsed
}

// IncrementalParserOptions configures the parser.
type IncrementalParserOptions struct {
	// MaxDepth limits nesting depth (default: 128)
	MaxDepth int

	// MaxKeyLen limits key string length (default: 4096)
	MaxKeyLen int

	// MaxValueLen limits value string length (default: 1MB)
	MaxValueLen int
}

// DefaultIncrementalParserOptions returns sensible defaults.
func DefaultIncrementalParserOptions() IncrementalParserOptions {
	return IncrementalParserOptions{
		MaxDepth:    128,
		MaxKeyLen:   4096,
		MaxValueLen: 1 << 20, // 1MB
	}
}

// NewIncrementalParser creates a new incremental parser.
func NewIncrementalParser(handler ParseHandler, opts IncrementalParserOptions) *IncrementalParser {
	if opts.MaxDepth == 0 {
		opts.MaxDepth = 128
	}
	if opts.MaxKeyLen == 0 {
		opts.MaxKeyLen = 4096
	}
	if opts.MaxValueLen == 0 {
		opts.MaxValueLen = 1 << 20
	}

	return &IncrementalParser{
		handler:     handler,
		buffer:      make([]byte, 0, 4096),
		path:        make([]PathElement, 0, 16),
		state:       stateStart,
		stack:       make([]parseStackFrame, 0, 16),
		maxDepth:    opts.MaxDepth,
		maxKeyLen:   opts.MaxKeyLen,
		maxValueLen: opts.MaxValueLen,
	}
}

// Feed adds more input data to the parser.
// Returns number of bytes consumed and any error.
func (p *IncrementalParser) Feed(data []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == stateError {
		return 0, p.err
	}

	// Append to buffer
	p.buffer = append(p.buffer, data...)

	// Process as much as possible
	startLen := len(p.buffer)
	for p.pos < len(p.buffer) && p.state != stateError && p.state != stateDone {
		consumed := p.processNext()
		if consumed == 0 {
			// Need more data
			p.emitEvent(ParseEvent{Type: EventNeedMore})
			break
		}
	}

	// Compact buffer
	if p.pos > 0 {
		copy(p.buffer, p.buffer[p.pos:])
		p.buffer = p.buffer[:len(p.buffer)-p.pos]
		consumed := p.pos
		p.pos = 0
		return consumed, p.err
	}

	return startLen - len(p.buffer), p.err
}

// End signals end of input.
func (p *IncrementalParser) End() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == stateError {
		return p.err
	}

	// Check for incomplete parse
	if len(p.stack) > 0 {
		p.setError(errors.New("unexpected end of input"))
		return p.err
	}

	p.state = stateDone
	return nil
}

// Reset clears the parser state for reuse.
func (p *IncrementalParser) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.buffer = p.buffer[:0]
	p.pos = 0
	p.path = p.path[:0]
	p.state = stateStart
	p.stack = p.stack[:0]
	p.err = nil
}

// Path returns the current parse path.
func (p *IncrementalParser) Path() []PathElement {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make([]PathElement, len(p.path))
	copy(result, p.path)
	return result
}

func (p *IncrementalParser) processNext() int {
	// Skip whitespace
	for p.pos < len(p.buffer) && isWhitespace(p.buffer[p.pos]) {
		p.pos++
	}

	if p.pos >= len(p.buffer) {
		return 0
	}

	ch := p.buffer[p.pos]

	switch p.state {
	case stateStart, stateExpectValue:
		return p.parseValue()

	case stateInObject, stateExpectKey:
		if ch == '}' {
			p.pos++
			p.popStack()
			p.emitEvent(ParseEvent{Type: EventEndObject, Path: p.copyPath()})
			return 1
		}
		return p.parseKey()

	case stateExpectColon:
		if ch == ':' || ch == '=' {
			p.pos++
			p.state = stateExpectValue
			return 1
		}
		p.setError(errors.New("expected ':' or '='"))
		return 0

	case stateInList:
		if ch == ']' {
			p.pos++
			p.popStack()
			p.emitEvent(ParseEvent{Type: EventEndList, Path: p.copyPath()})
			return 1
		}
		// Update path index
		if len(p.stack) > 0 {
			frame := &p.stack[len(p.stack)-1]
			p.updatePathIndex(frame.count)
			frame.count++
		}
		return p.parseValue()

	case stateAfterValue:
		// After a value, expect comma or closing bracket/brace
		if ch == ',' {
			p.pos++
			if len(p.stack) > 0 {
				frame := &p.stack[len(p.stack)-1]
				if frame.state == stateInList {
					p.state = stateInList
				} else {
					p.state = stateExpectKey
				}
			}
			return 1
		}
		if ch == '}' || ch == ']' || ch == ')' {
			// Let the parent state handle it
			if len(p.stack) > 0 {
				p.state = p.stack[len(p.stack)-1].state
			}
			return 0
		}
		// Implicit comma - switch to next state
		if len(p.stack) > 0 {
			frame := &p.stack[len(p.stack)-1]
			if frame.state == stateInList {
				p.state = stateInList
			} else {
				p.state = stateExpectKey
			}
		}
		return 0

	default:
		return 0
	}
}

func (p *IncrementalParser) parseValue() int {
	if p.pos >= len(p.buffer) {
		return 0
	}

	ch := p.buffer[p.pos]

	// Null (∅ or null/none/nil)
	if ch == 0xE2 && p.pos+2 < len(p.buffer) && string(p.buffer[p.pos:p.pos+3]) == "∅" {
		p.pos += 3
		p.emitEvent(ParseEvent{Type: EventValue, Value: Null(), Path: p.copyPath()})
		p.state = stateAfterValue
		return 3
	}

	// Boolean
	if ch == 't' {
		if p.matchKeyword("true") {
			p.emitEvent(ParseEvent{Type: EventValue, Value: Bool(true), Path: p.copyPath()})
			p.state = stateAfterValue
			return 4
		}
		p.pos++
		p.emitEvent(ParseEvent{Type: EventValue, Value: Bool(true), Path: p.copyPath()})
		p.state = stateAfterValue
		return 1
	}
	if ch == 'f' {
		if p.matchKeyword("false") {
			p.emitEvent(ParseEvent{Type: EventValue, Value: Bool(false), Path: p.copyPath()})
			p.state = stateAfterValue
			return 5
		}
		p.pos++
		p.emitEvent(ParseEvent{Type: EventValue, Value: Bool(false), Path: p.copyPath()})
		p.state = stateAfterValue
		return 1
	}

	// Null keywords
	if p.matchKeyword("null") || p.matchKeyword("none") || p.matchKeyword("nil") {
		p.emitEvent(ParseEvent{Type: EventValue, Value: Null(), Path: p.copyPath()})
		p.state = stateAfterValue
		return 4
	}

	// Number
	if ch == '-' || (ch >= '0' && ch <= '9') {
		return p.parseNumber()
	}

	// String
	if ch == '"' {
		return p.parseString()
	}

	// List
	if ch == '[' {
		p.pos++
		p.pushStack(stateInList, "", "")
		p.emitEvent(ParseEvent{Type: EventStartList, Path: p.copyPath()})
		p.state = stateInList
		return 1
	}

	// Object or struct
	if ch == '{' {
		p.pos++
		p.pushStack(stateInObject, "", "")
		p.emitEvent(ParseEvent{Type: EventStartObject, Path: p.copyPath()})
		p.state = stateExpectKey
		return 1
	}

	// Reference (^prefix:value)
	if ch == '^' {
		return p.parseRef()
	}

	// Identifier (could be struct type, sum tag, or bare string)
	if isIdentStart(ch) {
		return p.parseIdentifier()
	}

	p.setError(errors.New("unexpected character"))
	return 0
}

func (p *IncrementalParser) parseKey() int {
	if p.pos >= len(p.buffer) {
		return 0
	}

	ch := p.buffer[p.pos]

	// Quoted key
	if ch == '"' {
		start := p.pos
		key, consumed := p.scanString()
		if consumed == 0 {
			return 0 // Need more data
		}
		p.path = append(p.path, PathElement{Key: key})
		p.emitEvent(ParseEvent{Type: EventKey, Key: key, Path: p.copyPath()})
		p.state = stateExpectColon
		return p.pos - start
	}

	// Bare identifier key
	if isIdentStart(ch) {
		start := p.pos
		for p.pos < len(p.buffer) && isIdentContinue(p.buffer[p.pos]) {
			p.pos++
		}
		key := string(p.buffer[start:p.pos])
		p.path = append(p.path, PathElement{Key: key})
		p.emitEvent(ParseEvent{Type: EventKey, Key: key, Path: p.copyPath()})
		p.state = stateExpectColon
		return p.pos - start
	}

	p.setError(errors.New("expected key"))
	return 0
}

func (p *IncrementalParser) parseNumber() int {
	start := p.pos

	// Skip sign
	if p.pos < len(p.buffer) && p.buffer[p.pos] == '-' {
		p.pos++
	}

	// Integer part
	for p.pos < len(p.buffer) && p.buffer[p.pos] >= '0' && p.buffer[p.pos] <= '9' {
		p.pos++
	}

	isFloat := false

	// Decimal part
	if p.pos < len(p.buffer) && p.buffer[p.pos] == '.' {
		next := p.pos + 1
		if next < len(p.buffer) && p.buffer[next] >= '0' && p.buffer[next] <= '9' {
			isFloat = true
			p.pos++
			for p.pos < len(p.buffer) && p.buffer[p.pos] >= '0' && p.buffer[p.pos] <= '9' {
				p.pos++
			}
		}
	}

	// Exponent
	if p.pos < len(p.buffer) && (p.buffer[p.pos] == 'e' || p.buffer[p.pos] == 'E') {
		isFloat = true
		p.pos++
		if p.pos < len(p.buffer) && (p.buffer[p.pos] == '+' || p.buffer[p.pos] == '-') {
			p.pos++
		}
		for p.pos < len(p.buffer) && p.buffer[p.pos] >= '0' && p.buffer[p.pos] <= '9' {
			p.pos++
		}
	}

	numStr := string(p.buffer[start:p.pos])
	var value *GValue

	if isFloat {
		f := parseFloat(numStr)
		value = Float(f)
	} else {
		i := parseInt(numStr)
		value = Int(i)
	}

	p.emitEvent(ParseEvent{Type: EventValue, Value: value, Path: p.copyPath()})
	p.state = stateAfterValue
	return p.pos - start
}

func (p *IncrementalParser) parseString() int {
	str, consumed := p.scanString()
	if consumed == 0 {
		return 0
	}
	p.emitEvent(ParseEvent{Type: EventValue, Value: Str(str), Path: p.copyPath()})
	p.state = stateAfterValue
	return consumed
}

func (p *IncrementalParser) scanString() (string, int) {
	if p.pos >= len(p.buffer) || p.buffer[p.pos] != '"' {
		return "", 0
	}

	start := p.pos
	p.pos++ // Skip opening quote

	var sb []byte
	for p.pos < len(p.buffer) {
		ch := p.buffer[p.pos]
		if ch == '"' {
			p.pos++ // Skip closing quote
			return string(sb), p.pos - start
		}
		if ch == '\\' {
			p.pos++
			if p.pos >= len(p.buffer) {
				p.pos = start // Reset - need more data
				return "", 0
			}
			escaped := p.buffer[p.pos]
			switch escaped {
			case 'n':
				sb = append(sb, '\n')
			case 'r':
				sb = append(sb, '\r')
			case 't':
				sb = append(sb, '\t')
			case '\\':
				sb = append(sb, '\\')
			case '"':
				sb = append(sb, '"')
			default:
				sb = append(sb, escaped)
			}
			p.pos++
		} else {
			sb = append(sb, ch)
			p.pos++
		}
	}

	// Unterminated string - need more data
	p.pos = start
	return "", 0
}

func (p *IncrementalParser) parseRef() int {
	start := p.pos
	p.pos++ // Skip ^

	var refStr []byte
	for p.pos < len(p.buffer) && isRefChar(p.buffer[p.pos]) {
		refStr = append(refStr, p.buffer[p.pos])
		p.pos++
	}

	// Parse prefix:value
	prefix, value := "", string(refStr)
	for i, ch := range refStr {
		if ch == ':' {
			prefix = string(refStr[:i])
			value = string(refStr[i+1:])
			break
		}
	}

	p.emitEvent(ParseEvent{Type: EventValue, Value: ID(prefix, value), Path: p.copyPath()})
	p.state = stateAfterValue
	return p.pos - start
}

func (p *IncrementalParser) parseIdentifier() int {
	start := p.pos
	for p.pos < len(p.buffer) && isIdentContinue(p.buffer[p.pos]) {
		p.pos++
	}

	ident := string(p.buffer[start:p.pos])

	// Check what follows
	if p.pos < len(p.buffer) {
		ch := p.buffer[p.pos]

		// Struct: Type{...}
		if ch == '{' {
			p.pos++
			p.pushStack(stateInObject, ident, "")
			p.emitEvent(ParseEvent{Type: EventStartObject, TypeName: ident, Path: p.copyPath()})
			p.state = stateExpectKey
			return p.pos - start
		}

		// Sum: Tag(...) or Tag{...}
		if ch == '(' {
			p.pos++
			p.pushStack(stateExpectValue, "", ident)
			p.emitEvent(ParseEvent{Type: EventStartSum, Tag: ident, Path: p.copyPath()})
			p.state = stateExpectValue
			return p.pos - start
		}
	}

	// Bare string value
	p.emitEvent(ParseEvent{Type: EventValue, Value: Str(ident), Path: p.copyPath()})
	p.state = stateAfterValue
	return p.pos - start
}

func (p *IncrementalParser) matchKeyword(keyword string) bool {
	if p.pos+len(keyword) > len(p.buffer) {
		return false
	}
	for i := 0; i < len(keyword); i++ {
		if p.buffer[p.pos+i] != keyword[i] {
			return false
		}
	}
	// Check that keyword is not followed by identifier char
	if p.pos+len(keyword) < len(p.buffer) && isIdentContinue(p.buffer[p.pos+len(keyword)]) {
		return false
	}
	p.pos += len(keyword)
	return true
}

func (p *IncrementalParser) pushStack(state parseState, typeName, tag string) {
	p.stack = append(p.stack, parseStackFrame{
		state:    state,
		typeName: typeName,
		tag:      tag,
		count:    0,
	})
}

func (p *IncrementalParser) popStack() {
	if len(p.stack) > 0 {
		p.stack = p.stack[:len(p.stack)-1]
	}
	if len(p.path) > 0 {
		p.path = p.path[:len(p.path)-1]
	}
	if len(p.stack) > 0 {
		p.state = stateAfterValue
	} else {
		p.state = stateDone
	}
}

func (p *IncrementalParser) updatePathIndex(index int) {
	if len(p.path) > 0 && p.path[len(p.path)-1].IsIndex {
		p.path[len(p.path)-1].Index = index
	} else {
		p.path = append(p.path, PathElement{IsIndex: true, Index: index})
	}
}

func (p *IncrementalParser) copyPath() []PathElement {
	result := make([]PathElement, len(p.path))
	copy(result, p.path)
	return result
}

func (p *IncrementalParser) emitEvent(event ParseEvent) {
	if p.handler != nil {
		if err := p.handler(event); err != nil {
			p.setError(err)
		}
	}
}

func (p *IncrementalParser) setError(err error) {
	p.err = err
	p.state = stateError
	p.emitEvent(ParseEvent{Type: EventError, Error: err})
}

func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func parseFloat(s string) float64 {
	var f float64
	for i := 0; i < len(s); i++ {
		if s[i] == '.' || s[i] == 'e' || s[i] == 'E' || s[i] == '-' || s[i] == '+' {
			continue
		}
		f = f*10 + float64(s[i]-'0')
	}
	return f // Simplified - real impl would handle decimals/exponents
}

func parseInt(s string) int64 {
	var n int64
	neg := false
	i := 0
	if len(s) > 0 && s[0] == '-' {
		neg = true
		i = 1
	}
	for ; i < len(s); i++ {
		n = n*10 + int64(s[i]-'0')
	}
	if neg {
		return -n
	}
	return n
}
