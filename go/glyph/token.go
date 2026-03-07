package glyph

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// TokenType represents the type of a lexer token.
type TokenType uint8

const (
	TokenEOF TokenType = iota
	TokenError

	// Literals
	TokenNull    // ∅, null, none, nil
	TokenTrue    // t, true
	TokenFalse   // f, false
	TokenInt     // 123, -456
	TokenFloat   // 1.23, -4.56e7
	TokenString  // "quoted string"
	TokenBareStr // bare_identifier
	TokenRef     // ^prefix:value
	TokenTime    // 2025-12-19T20:00Z

	// Structural
	TokenLBrace   // {
	TokenRBrace   // }
	TokenLBracket // [
	TokenRBracket // ]
	TokenLParen   // (
	TokenRParen   // )
	TokenEq       // = or :
	TokenComma    // , (optional)
	TokenPipe     // |

	// Schema-related
	TokenAt   // @
	TokenHash // #
	TokenLT   // <
	TokenGT   // >

	// Identifiers (for type names, field names)
	TokenIdent // Match, Team, fieldName
)

// String returns the token type name.
func (t TokenType) String() string {
	switch t {
	case TokenEOF:
		return "EOF"
	case TokenError:
		return "ERROR"
	case TokenNull:
		return "NULL"
	case TokenTrue:
		return "TRUE"
	case TokenFalse:
		return "FALSE"
	case TokenInt:
		return "INT"
	case TokenFloat:
		return "FLOAT"
	case TokenString:
		return "STRING"
	case TokenBareStr:
		return "BARESTR"
	case TokenRef:
		return "REF"
	case TokenTime:
		return "TIME"
	case TokenLBrace:
		return "{"
	case TokenRBrace:
		return "}"
	case TokenLBracket:
		return "["
	case TokenRBracket:
		return "]"
	case TokenLParen:
		return "("
	case TokenRParen:
		return ")"
	case TokenEq:
		return "EQ"
	case TokenComma:
		return ","
	case TokenPipe:
		return "|"
	case TokenAt:
		return "@"
	case TokenHash:
		return "#"
	case TokenLT:
		return "<"
	case TokenGT:
		return ">"
	case TokenIdent:
		return "IDENT"
	default:
		return "UNKNOWN"
	}
}

// Token represents a lexer token.
type Token struct {
	Type  TokenType
	Value string
	Pos   Position
}

// String returns a debug representation of the token.
func (t Token) String() string {
	if t.Value == "" {
		return t.Type.String()
	}
	return fmt.Sprintf("%s(%q)", t.Type, t.Value)
}

// Lexer tokenizes GLYPH-T text.
type Lexer struct {
	input  string
	pos    int // Current position in input
	line   int // Current line number (1-based)
	col    int // Current column number (1-based)
	start  int // Start position of current token
	tokens []Token
	err    error
}

// NewLexer creates a new lexer for the given input.
func NewLexer(input string) *Lexer {
	return &Lexer{
		input: input,
		pos:   0,
		line:  1,
		col:   1,
	}
}

// Tokenize returns all tokens from the input.
func (l *Lexer) Tokenize() ([]Token, error) {
	for {
		tok := l.nextToken()
		l.tokens = append(l.tokens, tok)
		if tok.Type == TokenEOF || tok.Type == TokenError {
			break
		}
	}
	if l.err != nil {
		return l.tokens, l.err
	}
	return l.tokens, nil
}

// nextToken returns the next token.
func (l *Lexer) nextToken() Token {
	l.skipWhitespaceAndComments()

	if l.pos >= len(l.input) {
		return l.makeToken(TokenEOF, "")
	}

	l.start = l.pos
	startPos := l.currentPos()

	ch := l.peek()

	// Single character tokens
	switch ch {
	case '{':
		l.advance()
		return Token{Type: TokenLBrace, Value: "{", Pos: startPos}
	case '}':
		l.advance()
		return Token{Type: TokenRBrace, Value: "}", Pos: startPos}
	case '[':
		l.advance()
		return Token{Type: TokenLBracket, Value: "[", Pos: startPos}
	case ']':
		l.advance()
		return Token{Type: TokenRBracket, Value: "]", Pos: startPos}
	case '(':
		l.advance()
		return Token{Type: TokenLParen, Value: "(", Pos: startPos}
	case ')':
		l.advance()
		return Token{Type: TokenRParen, Value: ")", Pos: startPos}
	case '=', ':':
		l.advance()
		return Token{Type: TokenEq, Value: string(ch), Pos: startPos}
	case ',':
		l.advance()
		return Token{Type: TokenComma, Value: ",", Pos: startPos}
	case '|':
		l.advance()
		return Token{Type: TokenPipe, Value: "|", Pos: startPos}
	case '@':
		l.advance()
		return Token{Type: TokenAt, Value: "@", Pos: startPos}
	case '#':
		l.advance()
		return Token{Type: TokenHash, Value: "#", Pos: startPos}
	case '<':
		l.advance()
		return Token{Type: TokenLT, Value: "<", Pos: startPos}
	case '>':
		l.advance()
		return Token{Type: TokenGT, Value: ">", Pos: startPos}
	case '"':
		return l.scanString()
	case '^':
		return l.scanRef()
	}

	// Null symbol (∅ is multi-byte UTF-8)
	if l.pos+2 < len(l.input) && l.input[l.pos:l.pos+3] == "∅" {
		l.pos += 3
		l.col += 1
		return Token{Type: TokenNull, Value: "∅", Pos: startPos}
	}

	// Numbers (including negative)
	if ch == '-' || (ch >= '0' && ch <= '9') {
		return l.scanNumber()
	}

	// Identifiers and keywords
	if isIdentStart(ch) {
		return l.scanIdentOrKeyword()
	}

	// Unknown character
	l.advance()
	l.err = fmt.Errorf("unexpected character %q at %s", ch, startPos)
	return Token{Type: TokenError, Value: string(ch), Pos: startPos}
}

// scanString scans a quoted string.
func (l *Lexer) scanString() Token {
	startPos := l.currentPos()
	l.advance() // consume opening "

	var sb strings.Builder
	for {
		if l.pos >= len(l.input) {
			l.err = fmt.Errorf("unterminated string at %s", startPos)
			return Token{Type: TokenError, Value: sb.String(), Pos: startPos}
		}

		ch := l.peek()
		if ch == '"' {
			l.advance() // consume closing "
			break
		}

		if ch == '\\' {
			l.advance()
			if l.pos >= len(l.input) {
				l.err = fmt.Errorf("unterminated escape at %s", l.currentPos())
				return Token{Type: TokenError, Value: sb.String(), Pos: startPos}
			}
			escaped := l.peek()
			l.advance()
			switch escaped {
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
				sb.WriteByte(escaped)
			}
		} else {
			sb.WriteByte(ch)
			l.advance()
		}
	}

	str := sb.String()
	// Sanitize invalid UTF-8 sequences (lone surrogates, overlong encodings,
	// truncated multi-byte, invalid continuation bytes). Replace with U+FFFD
	// so downstream consumers (json.Marshal, C interop) never see broken bytes.
	if !utf8.ValidString(str) {
		str = strings.ToValidUTF8(str, "\uFFFD")
	}
	return Token{Type: TokenString, Value: str, Pos: startPos}
}

// scanRef scans a reference (^prefix:value).
func (l *Lexer) scanRef() Token {
	startPos := l.currentPos()
	l.advance() // consume ^

	var sb strings.Builder
	for l.pos < len(l.input) {
		ch := l.peek()
		if isRefChar(ch) {
			sb.WriteByte(ch)
			l.advance()
		} else {
			break
		}
	}

	return Token{Type: TokenRef, Value: sb.String(), Pos: startPos}
}

// scanNumber scans an integer or float.
func (l *Lexer) scanNumber() Token {
	startPos := l.currentPos()
	start := l.pos

	// Optional negative sign
	if l.peek() == '-' {
		l.advance()

		// Check for -Inf (with word boundary: next char must not be ident-continue)
		if l.pos+2 < len(l.input) && l.input[l.pos:l.pos+3] == "Inf" &&
			(l.pos+3 >= len(l.input) || !isIdentContinue(l.input[l.pos+3])) {
			l.pos += 3
			l.col += 3
			return Token{Type: TokenFloat, Value: l.input[start:l.pos], Pos: startPos}
		}
	}

	// Integer part
	for l.pos < len(l.input) && isDigit(l.peek()) {
		l.advance()
	}

	isFloat := false

	// Decimal part
	if l.pos < len(l.input) && l.peek() == '.' {
		nextPos := l.pos + 1
		if nextPos < len(l.input) && isDigit(l.input[nextPos]) {
			isFloat = true
			l.advance() // consume .
			for l.pos < len(l.input) && isDigit(l.peek()) {
				l.advance()
			}
		}
	}

	// Exponent part
	if l.pos < len(l.input) && (l.peek() == 'e' || l.peek() == 'E') {
		isFloat = true
		l.advance()
		if l.pos < len(l.input) && (l.peek() == '+' || l.peek() == '-') {
			l.advance()
		}
		for l.pos < len(l.input) && isDigit(l.peek()) {
			l.advance()
		}
	}

	value := l.input[start:l.pos]

	// Check if this might be a time value (starts with 4 digits followed by -)
	if !isFloat && len(value) >= 4 && l.pos < len(l.input) && l.peek() == '-' {
		// Could be an ISO date, scan the rest
		return l.scanTimeFromNumber(startPos, start)
	}

	if isFloat {
		return Token{Type: TokenFloat, Value: value, Pos: startPos}
	}
	return Token{Type: TokenInt, Value: value, Pos: startPos}
}

// scanTimeFromNumber continues scanning a time value that started with digits.
func (l *Lexer) scanTimeFromNumber(startPos Position, start int) Token {
	// Continue scanning time-like characters
	for l.pos < len(l.input) {
		ch := l.peek()
		if isTimeChar(ch) {
			l.advance()
		} else {
			break
		}
	}
	value := l.input[start:l.pos]
	return Token{Type: TokenTime, Value: value, Pos: startPos}
}

// scanIdentOrKeyword scans an identifier or keyword.
func (l *Lexer) scanIdentOrKeyword() Token {
	startPos := l.currentPos()
	start := l.pos

	for l.pos < len(l.input) && isIdentContinue(l.peek()) {
		l.advance()
	}

	value := l.input[start:l.pos]

	// Check for keywords
	switch value {
	case "null", "none", "nil":
		return Token{Type: TokenNull, Value: value, Pos: startPos}
	case "true", "t":
		return Token{Type: TokenTrue, Value: value, Pos: startPos}
	case "false", "f":
		return Token{Type: TokenFalse, Value: value, Pos: startPos}
	case "NaN", "Inf":
		return Token{Type: TokenFloat, Value: value, Pos: startPos}
	}

	return Token{Type: TokenIdent, Value: value, Pos: startPos}
}

// skipWhitespaceAndComments skips whitespace and // comments.
func (l *Lexer) skipWhitespaceAndComments() {
	for l.pos < len(l.input) {
		ch := l.peek()

		if ch == ' ' || ch == '\t' || ch == '\r' {
			l.advance()
			continue
		}

		if ch == '\n' {
			l.advance()
			l.line++
			l.col = 1
			continue
		}

		// Skip // comments
		if ch == '/' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '/' {
			l.advance()
			l.advance()
			for l.pos < len(l.input) && l.peek() != '\n' {
				l.advance()
			}
			continue
		}

		break
	}
}

// Helper methods

func (l *Lexer) peek() byte {
	if l.pos >= len(l.input) {
		return 0
	}
	return l.input[l.pos]
}

func (l *Lexer) advance() {
	if l.pos < len(l.input) {
		if l.input[l.pos] == '\n' {
			l.line++
			l.col = 1
		} else {
			l.col++
		}
		l.pos++
	}
}

func (l *Lexer) currentPos() Position {
	return Position{Line: l.line, Column: l.col, Offset: l.pos}
}

func (l *Lexer) makeToken(typ TokenType, value string) Token {
	return Token{Type: typ, Value: value, Pos: l.currentPos()}
}

// Character classification

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentContinue(ch byte) bool {
	return isIdentStart(ch) || isDigit(ch)
}

func isRefChar(ch byte) bool {
	return isIdentContinue(ch) || ch == ':' || ch == '-' || ch == '.'
}

func isTimeChar(ch byte) bool {
	return isDigit(ch) || ch == '-' || ch == ':' || ch == 'T' || ch == 'Z' || ch == '+' || ch == '.'
}

// isValidBareString checks if a string can be represented without quotes.
func isValidBareString(s string) bool {
	if len(s) == 0 {
		return false
	}

	// Check first character
	r, size := utf8.DecodeRuneInString(s)
	if !unicode.IsLetter(r) && r != '_' {
		return false
	}

	// Check remaining characters
	for i := size; i < len(s); {
		r, size = utf8.DecodeRuneInString(s[i:])
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '-' {
			return false
		}
		i += size
	}

	// Check it's not a keyword
	switch s {
	case "null", "none", "nil", "true", "false", "t", "f", "struct", "sum", "list", "map",
		"NaN", "Inf":
		return false
	}

	return true
}

// TokenStream provides a stream interface over tokens.
type TokenStream struct {
	tokens []Token
	pos    int
}

// NewTokenStream creates a token stream from tokens.
func NewTokenStream(tokens []Token) *TokenStream {
	return &TokenStream{tokens: tokens, pos: 0}
}

// Peek returns the current token without advancing.
func (ts *TokenStream) Peek() Token {
	if ts.pos >= len(ts.tokens) {
		return Token{Type: TokenEOF}
	}
	return ts.tokens[ts.pos]
}

// PeekN returns the token N positions ahead.
func (ts *TokenStream) PeekN(n int) Token {
	idx := ts.pos + n
	if idx >= len(ts.tokens) {
		return Token{Type: TokenEOF}
	}
	return ts.tokens[idx]
}

// Advance moves to the next token and returns the current one.
func (ts *TokenStream) Advance() Token {
	tok := ts.Peek()
	if ts.pos < len(ts.tokens) {
		ts.pos++
	}
	return tok
}

// Expect advances if the current token matches, otherwise returns error.
func (ts *TokenStream) Expect(typ TokenType) (Token, error) {
	tok := ts.Peek()
	if tok.Type != typ {
		return tok, fmt.Errorf("expected %s, got %s at %s", typ, tok.Type, tok.Pos)
	}
	ts.Advance()
	return tok, nil
}

// Match returns true and advances if the current token matches.
func (ts *TokenStream) Match(typ TokenType) bool {
	if ts.Peek().Type == typ {
		ts.Advance()
		return true
	}
	return false
}

// AtEnd returns true if at end of stream.
func (ts *TokenStream) AtEnd() bool {
	return ts.Peek().Type == TokenEOF
}

// Position returns the current position in the stream.
func (ts *TokenStream) Position() int {
	return ts.pos
}

// Reset resets to a previous position.
func (ts *TokenStream) Reset(pos int) {
	ts.pos = pos
}
