package glyph

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParseError represents a parsing error with location.
type ParseError struct {
	Message string
	Pos     Position
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("%s at %s", e.Message, e.Pos)
}

// ParseResult contains the parsed value and any errors/warnings.
type ParseResult struct {
	Value    *GValue
	Errors   []ParseError
	Warnings []ParseError
	Schema   *Schema // Schema used for parsing (if any)
}

// HasErrors returns true if there were any errors.
func (r *ParseResult) HasErrors() bool {
	return len(r.Errors) > 0
}

// Parser parses GLYPH-T text into GValues.
type Parser struct {
	stream   *TokenStream
	schema   *Schema
	errors   []ParseError
	warnings []ParseError
	tolerant bool // Enable tolerant parsing mode
}

// ParseOptions configures the parser behavior.
type ParseOptions struct {
	Schema   *Schema // Schema for type-aware parsing
	Tolerant bool    // Enable tolerant/repair mode
}

// Parse parses GLYPH-T text into a GValue.
func Parse(input string) (*ParseResult, error) {
	return ParseWithOptions(input, ParseOptions{Tolerant: true})
}

// ParseWithSchema parses with a schema for validation and repair.
func ParseWithSchema(input string, schema *Schema) (*ParseResult, error) {
	return ParseWithOptions(input, ParseOptions{Schema: schema, Tolerant: true})
}

// ParseWithOptions parses with full options.
func ParseWithOptions(input string, opts ParseOptions) (*ParseResult, error) {
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, err
	}

	p := &Parser{
		stream:   NewTokenStream(tokens),
		schema:   opts.Schema,
		tolerant: opts.Tolerant,
	}

	value := p.parseValue()

	return &ParseResult{
		Value:    value,
		Errors:   p.errors,
		Warnings: p.warnings,
		Schema:   p.schema,
	}, nil
}

// parseValue parses any value.
func (p *Parser) parseValue() *GValue {
	tok := p.stream.Peek()

	switch tok.Type {
	case TokenNull:
		p.stream.Advance()
		return Null()

	case TokenTrue:
		p.stream.Advance()
		return Bool(true)

	case TokenFalse:
		p.stream.Advance()
		return Bool(false)

	case TokenInt:
		p.stream.Advance()
		v, _ := strconv.ParseInt(tok.Value, 10, 64)
		return Int(v)

	case TokenFloat:
		p.stream.Advance()
		v, _ := strconv.ParseFloat(tok.Value, 64)
		return Float(v)

	case TokenString:
		p.stream.Advance()
		return Str(tok.Value)

	case TokenRef:
		p.stream.Advance()
		return p.parseRef(tok.Value)

	case TokenTime:
		p.stream.Advance()
		return p.parseTime(tok.Value, tok.Pos)

	case TokenLBracket:
		return p.parseList()

	case TokenLBrace:
		return p.parseMap()

	case TokenIdent:
		// Could be: TypeName{...}, Tag(value), Tag{...}, or bare string
		return p.parseIdentValue()

	case TokenBareStr:
		p.stream.Advance()
		return Str(tok.Value)

	case TokenAt:
		// Schema block or annotation - skip for value parsing
		return p.parseSchemaAnnotatedValue()

	default:
		if p.tolerant {
			// Try to recover
			p.addWarning(tok.Pos, "unexpected token %s, skipping", tok.Type)
			p.stream.Advance()
			return Null()
		}
		p.addError(tok.Pos, "unexpected token %s", tok.Type)
		return nil
	}
}

// parseRef parses a reference ID (^prefix:value).
func (p *Parser) parseRef(value string) *GValue {
	// Parse prefix:value format
	if idx := strings.Index(value, ":"); idx > 0 {
		return ID(value[:idx], value[idx+1:])
	}
	return ID("", value)
}

// parseTime parses an ISO-8601 time value.
func (p *Parser) parseTime(value string, pos Position) *GValue {
	// Try common formats
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04Z",
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, value); err == nil {
			return Time(t)
		}
	}

	if p.tolerant {
		p.addWarning(pos, "invalid time format, treating as string: %s", value)
		return Str(value)
	}
	p.addError(pos, "invalid time format: %s", value)
	return Str(value)
}

// parseList parses a list: [v1 v2 v3] or [v1, v2, v3]
func (p *Parser) parseList() *GValue {
	p.stream.Advance() // consume [

	var elements []*GValue
	for {
		tok := p.stream.Peek()

		if tok.Type == TokenRBracket {
			p.stream.Advance()
			break
		}

		if tok.Type == TokenEOF {
			if p.tolerant {
				p.addWarning(tok.Pos, "unterminated list, auto-closing")
				break
			}
			p.addError(tok.Pos, "unterminated list")
			break
		}

		// Skip optional commas
		if tok.Type == TokenComma {
			p.stream.Advance()
			continue
		}

		elem := p.parseValue()
		if elem != nil {
			elements = append(elements, elem)
		}
	}

	return List(elements...)
}

// parseMap parses a map: {k:v k2:v2} or {k=v, k2=v2}
func (p *Parser) parseMap() *GValue {
	p.stream.Advance() // consume {

	var entries []MapEntry
	for {
		tok := p.stream.Peek()

		if tok.Type == TokenRBrace {
			p.stream.Advance()
			break
		}

		if tok.Type == TokenEOF {
			if p.tolerant {
				p.addWarning(tok.Pos, "unterminated map, auto-closing")
				break
			}
			p.addError(tok.Pos, "unterminated map")
			break
		}

		// Skip optional commas
		if tok.Type == TokenComma {
			p.stream.Advance()
			continue
		}

		entry := p.parseMapEntry()
		if entry != nil {
			entries = append(entries, *entry)
		}
	}

	return Map(entries...)
}

// parseMapEntry parses a single key:value or key=value pair.
func (p *Parser) parseMapEntry() *MapEntry {
	// Get key (identifier or string)
	keyTok := p.stream.Peek()
	var key string

	switch keyTok.Type {
	case TokenIdent:
		key = keyTok.Value
		p.stream.Advance()
	case TokenString:
		key = keyTok.Value
		p.stream.Advance()
	default:
		if p.tolerant {
			p.addWarning(keyTok.Pos, "expected key, got %s", keyTok.Type)
			p.stream.Advance()
			return nil
		}
		p.addError(keyTok.Pos, "expected key, got %s", keyTok.Type)
		return nil
	}

	// Expect = or :
	if !p.stream.Match(TokenEq) {
		if p.tolerant {
			// Try to continue - maybe the value follows directly
			p.addWarning(p.stream.Peek().Pos, "expected = or :, continuing")
		} else {
			p.addError(p.stream.Peek().Pos, "expected = or :")
			return nil
		}
	}

	// Parse value
	value := p.parseValue()

	return &MapEntry{Key: key, Value: value}
}

// parseIdentValue handles identifiers which could be:
// - TypeName{...} (struct)
// - Tag(value) or Tag{...} (sum)
// - bare string value
func (p *Parser) parseIdentValue() *GValue {
	identTok := p.stream.Advance()
	name := identTok.Value

	next := p.stream.Peek()

	switch next.Type {
	case TokenLBrace:
		// TypeName{...} - struct or inline sum variant
		return p.parseStruct(name)

	case TokenLParen:
		// Tag(value) - sum type
		return p.parseSum(name)

	default:
		// Just a bare string
		return Str(name)
	}
}

// parseStruct parses a typed struct: TypeName{field=value ...}
func (p *Parser) parseStruct(typeName string) *GValue {
	p.stream.Advance() // consume {

	var fields []MapEntry
	for {
		tok := p.stream.Peek()

		if tok.Type == TokenRBrace {
			p.stream.Advance()
			break
		}

		if tok.Type == TokenEOF {
			if p.tolerant {
				p.addWarning(tok.Pos, "unterminated struct, auto-closing")
				break
			}
			p.addError(tok.Pos, "unterminated struct")
			break
		}

		// Skip optional commas
		if tok.Type == TokenComma {
			p.stream.Advance()
			continue
		}

		entry := p.parseStructField(typeName)
		if entry != nil {
			fields = append(fields, *entry)
		}
	}

	return Struct(typeName, fields...)
}

// parseStructField parses a struct field, resolving wire keys if schema present.
func (p *Parser) parseStructField(typeName string) *MapEntry {
	// Get field key
	keyTok := p.stream.Peek()
	var key string

	switch keyTok.Type {
	case TokenIdent:
		key = keyTok.Value
		p.stream.Advance()
	case TokenString:
		key = keyTok.Value
		p.stream.Advance()
	default:
		if p.tolerant {
			p.addWarning(keyTok.Pos, "expected field name, got %s", keyTok.Type)
			p.stream.Advance()
			return nil
		}
		p.addError(keyTok.Pos, "expected field name, got %s", keyTok.Type)
		return nil
	}

	// Resolve wire key to full name if schema present
	if p.schema != nil {
		fullName := p.schema.ResolveWireKey(typeName, key)
		if fullName != key {
			key = fullName
		}
	}

	// Expect = or :
	if !p.stream.Match(TokenEq) {
		if p.tolerant {
			p.addWarning(p.stream.Peek().Pos, "expected = or :, continuing")
		} else {
			p.addError(p.stream.Peek().Pos, "expected = or :")
			return nil
		}
	}

	// Parse value
	value := p.parseValue()

	return &MapEntry{Key: key, Value: value}
}

// parseSum parses a sum type: Tag(value)
func (p *Parser) parseSum(tag string) *GValue {
	p.stream.Advance() // consume (

	var value *GValue

	// Check for empty: Tag()
	if p.stream.Peek().Type == TokenRParen {
		p.stream.Advance()
		value = Null()
	} else {
		value = p.parseValue()

		if !p.stream.Match(TokenRParen) {
			if p.tolerant {
				p.addWarning(p.stream.Peek().Pos, "expected ), auto-closing sum")
			} else {
				p.addError(p.stream.Peek().Pos, "expected )")
			}
		}
	}

	return Sum(tag, value)
}

// parseSchemaAnnotatedValue handles @schema{...} or @schema#hash references.
func (p *Parser) parseSchemaAnnotatedValue() *GValue {
	p.stream.Advance() // consume @

	tok := p.stream.Peek()

	if tok.Type == TokenIdent && tok.Value == "schema" {
		p.stream.Advance()

		next := p.stream.Peek()
		if next.Type == TokenHash {
			// @schema#hash - schema reference
			p.stream.Advance()
			hashTok := p.stream.Peek()
			if hashTok.Type == TokenIdent || hashTok.Type == TokenBareStr {
				p.stream.Advance()
				// Store schema hash for later lookup
				p.addWarning(hashTok.Pos, "schema reference: %s (lookup not implemented)", hashTok.Value)
			}
		} else if next.Type == TokenLBrace {
			// @schema{...} - inline schema
			p.skipSchemaBlock()
		}
	}

	// Continue parsing the actual value
	return p.parseValue()
}

// skipSchemaBlock skips over a @schema{...} block.
func (p *Parser) skipSchemaBlock() {
	if !p.stream.Match(TokenLBrace) {
		return
	}

	depth := 1
	for depth > 0 && !p.stream.AtEnd() {
		tok := p.stream.Advance()
		switch tok.Type {
		case TokenLBrace:
			depth++
		case TokenRBrace:
			depth--
		}
	}
}

// Error handling

func (p *Parser) addError(pos Position, format string, args ...interface{}) {
	p.errors = append(p.errors, ParseError{
		Message: fmt.Sprintf(format, args...),
		Pos:     pos,
	})
}

func (p *Parser) addWarning(pos Position, format string, args ...interface{}) {
	p.warnings = append(p.warnings, ParseError{
		Message: fmt.Sprintf(format, args...),
		Pos:     pos,
	})
}

// ============================================================
// Schema Parsing
// ============================================================

// ParseSchema parses a GLYPH schema definition.
func ParseSchema(input string) (*Schema, error) {
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, err
	}

	p := &schemaParser{
		stream: NewTokenStream(tokens),
	}

	return p.parseSchema()
}

type schemaParser struct {
	stream *TokenStream
	errors []ParseError
}

func (p *schemaParser) parseSchema() (*Schema, error) {
	schema := &Schema{Types: make(map[string]*TypeDef)}

	// Expect @schema{
	if !p.stream.Match(TokenAt) {
		return nil, fmt.Errorf("expected @schema")
	}

	tok := p.stream.Peek()
	if tok.Type != TokenIdent || tok.Value != "schema" {
		return nil, fmt.Errorf("expected @schema, got @%s", tok.Value)
	}
	p.stream.Advance()

	if !p.stream.Match(TokenLBrace) {
		return nil, fmt.Errorf("expected { after @schema")
	}

	// Parse type definitions
	for {
		tok := p.stream.Peek()
		if tok.Type == TokenRBrace {
			p.stream.Advance()
			break
		}
		if tok.Type == TokenEOF {
			return nil, fmt.Errorf("unterminated schema")
		}

		typeDef, err := p.parseTypeDef()
		if err != nil {
			return nil, err
		}
		if typeDef != nil {
			schema.Types[typeDef.Name] = typeDef
		}
	}

	schema.ComputeHash()
	return schema, nil
}

func (p *schemaParser) parseTypeDef() (*TypeDef, error) {
	// Name[:version] struct{...} or Name sum{...}
	nameTok, err := p.stream.Expect(TokenIdent)
	if err != nil {
		return nil, err
	}

	name := nameTok.Value
	version := ""

	// Check for :version
	if p.stream.Match(TokenEq) { // : was lexed as TokenEq
		verTok := p.stream.Peek()
		if verTok.Type == TokenIdent {
			version = verTok.Value
			p.stream.Advance()
		}
	}

	// struct or sum
	kindTok, err := p.stream.Expect(TokenIdent)
	if err != nil {
		return nil, err
	}

	td := &TypeDef{
		Name:    name,
		Version: version,
	}

	switch kindTok.Value {
	case "struct":
		td.Kind = TypeDefStruct
		structDef, err := p.parseStructDef()
		if err != nil {
			return nil, err
		}
		td.Struct = structDef

	case "sum":
		td.Kind = TypeDefSum
		sumDef, err := p.parseSumDef()
		if err != nil {
			return nil, err
		}
		td.Sum = sumDef

	default:
		return nil, fmt.Errorf("expected struct or sum, got %s", kindTok.Value)
	}

	return td, nil
}

func (p *schemaParser) parseStructDef() (*StructDef, error) {
	if !p.stream.Match(TokenLBrace) {
		return nil, fmt.Errorf("expected { after struct")
	}

	var fields []*FieldDef
	for {
		tok := p.stream.Peek()
		if tok.Type == TokenRBrace {
			p.stream.Advance()
			break
		}
		if tok.Type == TokenEOF {
			return nil, fmt.Errorf("unterminated struct definition")
		}

		field, err := p.parseFieldDef()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}

	return &StructDef{Fields: fields}, nil
}

func (p *schemaParser) parseFieldDef() (*FieldDef, error) {
	// name: Type [constraints] @k(wireKey)
	nameTok, err := p.stream.Expect(TokenIdent)
	if err != nil {
		return nil, err
	}

	if !p.stream.Match(TokenEq) { // : was lexed as TokenEq
		return nil, fmt.Errorf("expected : after field name")
	}

	typeSpec, err := p.parseTypeSpec()
	if err != nil {
		return nil, err
	}

	field := &FieldDef{
		Name: nameTok.Value,
		Type: typeSpec,
	}

	// Parse optional constraints and annotations
	for {
		tok := p.stream.Peek()

		if tok.Type == TokenLBracket {
			constraint, err := p.parseConstraint()
			if err != nil {
				return nil, err
			}
			if constraint.Kind == ConstraintOptional {
				field.Optional = true
			} else {
				field.Constraints = append(field.Constraints, constraint)
			}
			continue
		}

		if tok.Type == TokenAt {
			p.stream.Advance()
			annot := p.stream.Peek()
			if annot.Type == TokenIdent && annot.Value == "k" {
				p.stream.Advance()
				if !p.stream.Match(TokenLParen) {
					return nil, fmt.Errorf("expected ( after @k")
				}
				keyTok := p.stream.Peek()
				if keyTok.Type == TokenIdent {
					field.WireKey = keyTok.Value
					p.stream.Advance()
				}
				if !p.stream.Match(TokenRParen) {
					return nil, fmt.Errorf("expected ) after wire key")
				}
			}
			continue
		}

		break
	}

	return field, nil
}

func (p *schemaParser) parseTypeSpec() (TypeSpec, error) {
	tok := p.stream.Peek()

	if tok.Type != TokenIdent {
		return TypeSpec{}, fmt.Errorf("expected type name, got %s", tok.Type)
	}

	name := tok.Value
	p.stream.Advance()

	// Check for parameterized types: list<T>, map<K,V>
	if name == "list" {
		if _, err := p.stream.Expect(TokenLT); err != nil {
			return TypeSpec{}, fmt.Errorf("expected < after list")
		}
		elem, err := p.parseTypeSpec()
		if err != nil {
			return TypeSpec{}, err
		}
		if _, err := p.stream.Expect(TokenGT); err != nil {
			return TypeSpec{}, fmt.Errorf("expected > after list type")
		}
		return ListType(elem), nil
	}

	if name == "map" {
		if _, err := p.stream.Expect(TokenLT); err != nil {
			return TypeSpec{}, fmt.Errorf("expected < after map")
		}
		keyType, err := p.parseTypeSpec()
		if err != nil {
			return TypeSpec{}, err
		}
		if _, err := p.stream.Expect(TokenComma); err != nil {
			return TypeSpec{}, fmt.Errorf("expected , in map type")
		}
		valType, err := p.parseTypeSpec()
		if err != nil {
			return TypeSpec{}, err
		}
		if _, err := p.stream.Expect(TokenGT); err != nil {
			return TypeSpec{}, fmt.Errorf("expected > after map type")
		}
		return MapType(keyType, valType), nil
	}

	if name == "struct" {
		structDef, err := p.parseStructDef()
		if err != nil {
			return TypeSpec{}, err
		}
		return TypeSpec{Kind: TypeSpecInlineStruct, Struct: structDef}, nil
	}

	return PrimitiveType(name), nil
}

func (p *schemaParser) parseConstraint() (Constraint, error) {
	p.stream.Advance() // consume [

	var constraint Constraint

	tok := p.stream.Peek()
	switch tok.Type {
	case TokenIdent:
		switch tok.Value {
		case "optional":
			p.stream.Advance()
			constraint = OptionalConstraint()
		case "nonempty":
			p.stream.Advance()
			constraint = NonEmptyConstraint()
		case "min":
			p.stream.Advance()
			p.stream.Match(TokenEq)
			numTok := p.stream.Advance()
			if v, err := strconv.ParseFloat(numTok.Value, 64); err == nil {
				constraint = MinConstraint(v)
			}
		case "max":
			p.stream.Advance()
			p.stream.Match(TokenEq)
			numTok := p.stream.Advance()
			if v, err := strconv.ParseFloat(numTok.Value, 64); err == nil {
				constraint = MaxConstraint(v)
			}
		case "len":
			p.stream.Advance()
			p.stream.Match(TokenEq)
			numTok := p.stream.Advance()
			if v, err := strconv.Atoi(numTok.Value); err == nil {
				constraint = LenConstraint(v)
			}
		default:
			p.stream.Advance()
		}

	case TokenInt, TokenFloat:
		// Range constraint: [0..10] or [min=0 max=10]
		v1, _ := strconv.ParseFloat(tok.Value, 64)
		p.stream.Advance()
		// Check for ..
		if p.stream.Peek().Value == ".." || (p.stream.Peek().Type == TokenIdent) {
			// Skip ..
			p.stream.Advance()
			v2Tok := p.stream.Advance()
			if v2, err := strconv.ParseFloat(v2Tok.Value, 64); err == nil {
				constraint = RangeConstraint(v1, v2)
			}
		}
	}

	if !p.stream.Match(TokenRBracket) {
		return constraint, fmt.Errorf("expected ] after constraint")
	}

	return constraint, nil
}

func (p *schemaParser) parseSumDef() (*SumDef, error) {
	if !p.stream.Match(TokenLBrace) {
		return nil, fmt.Errorf("expected { after sum")
	}

	var variants []*VariantDef
	for {
		tok := p.stream.Peek()
		if tok.Type == TokenRBrace {
			p.stream.Advance()
			break
		}
		if tok.Type == TokenEOF {
			return nil, fmt.Errorf("unterminated sum definition")
		}
		if tok.Type == TokenPipe {
			p.stream.Advance()
			continue
		}

		// Tag: Type
		tagTok, err := p.stream.Expect(TokenIdent)
		if err != nil {
			return nil, err
		}

		if !p.stream.Match(TokenEq) { // :
			return nil, fmt.Errorf("expected : after variant tag")
		}

		typeSpec, err := p.parseTypeSpec()
		if err != nil {
			return nil, err
		}

		variants = append(variants, &VariantDef{
			Tag:  tagTok.Value,
			Type: typeSpec,
		})
	}

	return &SumDef{Variants: variants}, nil
}
