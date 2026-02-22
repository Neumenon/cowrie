/**
 * LYPH v2 Parser
 * 
 * Parses LYPH format back to GValue.
 */

import { GValue, RefID } from './types';
import { Schema } from './schema';

// ============================================================
// Packed Parser
// ============================================================

export interface ParseOptions {
  schema?: Schema;
  tolerant?: boolean;
}

export function parsePacked(input: string, schema: Schema): GValue {
  const parser = new PackedParser(input, schema);
  return parser.parse();
}

class PackedParser {
  private input: string;
  private pos: number = 0;
  private schema: Schema;

  constructor(input: string, schema: Schema) {
    this.input = input;
    this.schema = schema;
  }

  parse(): GValue {
    this.skipWhitespace();
    
    // Expect Type@(...) or Type@{bm=...}(...)
    const typeName = this.parseTypeName();
    this.expect('@');
    
    const td = this.schema.getType(typeName);
    if (!td) {
      throw new Error(`unknown type: ${typeName}`);
    }
    
    // Check for bitmap header
    let mask: boolean[] | null = null;
    if (this.peek() === '{') {
      mask = this.parseBitmapHeader();
    }
    
    this.expect('(');
    
    let value: GValue;
    if (mask) {
      value = this.parseBitmapValues(typeName, mask);
    } else {
      value = this.parseDenseValues(typeName);
    }
    
    this.expect(')');
    return value;
  }

  private parseTypeName(): string {
    this.skipWhitespace();
    const start = this.pos;
    
    if (this.pos >= this.input.length) {
      throw new Error('unexpected end of input');
    }
    
    if (!this.isTypeNameStart(this.input.charCodeAt(this.pos))) {
      throw new Error(`expected type name at pos ${this.pos}`);
    }
    
    while (this.pos < this.input.length && this.isTypeNameCont(this.input.charCodeAt(this.pos))) {
      this.pos++;
    }
    
    return this.input.slice(start, this.pos);
  }

  private isTypeNameStart(c: number): boolean {
    return (c >= 65 && c <= 90) || (c >= 97 && c <= 122) || c === 95;
  }

  private isTypeNameCont(c: number): boolean {
    return this.isTypeNameStart(c) || (c >= 48 && c <= 57);
  }

  private parseBitmapHeader(): boolean[] {
    this.expect('{');
    this.skipWhitespace();
    this.expectLiteral('bm=');
    this.expectLiteral('0b');
    
    const start = this.pos;
    while (this.pos < this.input.length && (this.input[this.pos] === '0' || this.input[this.pos] === '1')) {
      this.pos++;
    }
    
    const bits = this.input.slice(start, this.pos);
    if (bits.length === 0) {
      throw new Error('empty bitmap');
    }
    
    // Convert bits to mask (LSB first)
    const mask: boolean[] = [];
    for (let i = bits.length - 1; i >= 0; i--) {
      mask.push(bits[i] === '1');
    }
    
    this.skipWhitespace();
    this.expect('}');
    
    return mask;
  }

  private parseDenseValues(typeName: string): GValue {
    const fields = this.schema.fieldsByFid(typeName);
    const entries: { key: string; value: GValue }[] = [];
    
    for (let i = 0; i < fields.length; i++) {
      const fd = fields[i];
      this.skipWhitespace();
      
      if (this.peek() === ')') {
        // Remaining fields are null
        for (let j = i; j < fields.length; j++) {
          entries.push({ key: fields[j].name, value: GValue.null() });
        }
        break;
      }
      
      const val = this.parseValue(fd.type.kind === 'ref' ? fd.type.name : undefined);
      entries.push({ key: fd.name, value: val });
    }
    
    return GValue.struct(typeName, ...entries);
  }

  private parseBitmapValues(typeName: string, mask: boolean[]): GValue {
    const reqFields = this.schema.requiredFieldsByFid(typeName);
    const optFields = this.schema.optionalFieldsByFid(typeName);
    const entries: { key: string; value: GValue }[] = [];
    
    // Parse required fields
    for (const fd of reqFields) {
      this.skipWhitespace();
      const val = this.parseValue(fd.type.kind === 'ref' ? fd.type.name : undefined);
      entries.push({ key: fd.name, value: val });
    }
    
    // Parse optional fields based on mask
    for (let i = 0; i < optFields.length; i++) {
      const fd = optFields[i];
      if (i < mask.length && mask[i]) {
        this.skipWhitespace();
        const val = this.parseValue(fd.type.kind === 'ref' ? fd.type.name : undefined);
        entries.push({ key: fd.name, value: val });
      } else {
        entries.push({ key: fd.name, value: GValue.null() });
      }
    }
    
    return GValue.struct(typeName, ...entries);
  }

  private parseValue(typeHint?: string): GValue {
    this.skipWhitespace();
    const c = this.peek();
    
    // Null
    if (c === '∅') {
      this.pos++;
      return GValue.null();
    }
    
    // Boolean
    if (c === 't') {
      if (this.tryLiteral('true') || this.tryLiteral('t')) {
        return GValue.bool(true);
      }
      return this.parseBareString();
    }
    if (c === 'f') {
      if (this.tryLiteral('false') || this.tryLiteral('f')) {
        return GValue.bool(false);
      }
      return this.parseBareString();
    }
    
    // String
    if (c === '"') {
      return this.parseQuotedString();
    }
    
    // Ref
    if (c === '^') {
      return this.parseRef();
    }
    
    // List
    if (c === '[') {
      return this.parseList();
    }
    
    // Map
    if (c === '{') {
      return this.parseMap();
    }
    
    // Number or time
    if (c === '-' || (c >= '0' && c <= '9')) {
      return this.parseNumberOrTime();
    }
    
    // Nested packed struct or bare string
    if (this.isTypeNameStart(c.charCodeAt(0))) {
      const saved = this.pos;
      const name = this.parseTypeName();
      if (this.peek() === '@') {
        // Nested packed struct
        this.pos = saved;
        return this.parseNestedPacked();
      }
      // Bare string
      return GValue.str(name);
    }
    
    throw new Error(`unexpected character at pos ${this.pos}: ${c}`);
  }

  private parseNestedPacked(): GValue {
    const typeName = this.parseTypeName();
    this.expect('@');
    
    const td = this.schema.getType(typeName);
    if (!td) {
      throw new Error(`unknown nested type: ${typeName}`);
    }
    
    let mask: boolean[] | null = null;
    if (this.peek() === '{') {
      mask = this.parseBitmapHeader();
    }
    
    this.expect('(');
    
    let value: GValue;
    if (mask) {
      value = this.parseBitmapValues(typeName, mask);
    } else {
      value = this.parseDenseValues(typeName);
    }
    
    this.expect(')');
    return value;
  }

  private parseNumberOrTime(): GValue {
    // Check for ISO time pattern
    if (this.pos + 10 < this.input.length) {
      const ahead = this.input.slice(this.pos, this.pos + 11);
      if (/^\d{4}-\d{2}-\d{2}T/.test(ahead)) {
        return this.parseTime();
      }
    }
    return this.parseNumber();
  }

  private parseTime(): GValue {
    const start = this.pos;
    while (this.pos < this.input.length) {
      const c = this.input[this.pos];
      if (c === ' ' || c === ')' || c === ']' || c === '}' || c === '\n') {
        break;
      }
      this.pos++;
    }
    const timeStr = this.input.slice(start, this.pos);
    return GValue.time(new Date(timeStr));
  }

  private parseNumber(): GValue {
    const start = this.pos;
    
    // Optional minus
    if (this.input[this.pos] === '-') this.pos++;
    
    // Integer part
    while (this.pos < this.input.length && this.input[this.pos] >= '0' && this.input[this.pos] <= '9') {
      this.pos++;
    }
    
    let isFloat = false;
    
    // Decimal part
    if (this.pos < this.input.length && this.input[this.pos] === '.') {
      isFloat = true;
      this.pos++;
      while (this.pos < this.input.length && this.input[this.pos] >= '0' && this.input[this.pos] <= '9') {
        this.pos++;
      }
    }
    
    // Exponent
    if (this.pos < this.input.length && (this.input[this.pos] === 'e' || this.input[this.pos] === 'E')) {
      isFloat = true;
      this.pos++;
      if (this.input[this.pos] === '+' || this.input[this.pos] === '-') this.pos++;
      while (this.pos < this.input.length && this.input[this.pos] >= '0' && this.input[this.pos] <= '9') {
        this.pos++;
      }
    }
    
    const numStr = this.input.slice(start, this.pos);
    if (isFloat) {
      return GValue.float(parseFloat(numStr));
    }
    return GValue.int(parseInt(numStr, 10));
  }

  private parseQuotedString(): GValue {
    this.expect('"');
    let result = '';
    
    while (this.pos < this.input.length) {
      const c = this.input[this.pos];
      if (c === '"') {
        this.pos++;
        return GValue.str(result);
      }
      if (c === '\\' && this.pos + 1 < this.input.length) {
        this.pos++;
        switch (this.input[this.pos]) {
          case 'n': result += '\n'; break;
          case 'r': result += '\r'; break;
          case 't': result += '\t'; break;
          case '\\': result += '\\'; break;
          case '"': result += '"'; break;
          default: result += this.input[this.pos];
        }
      } else {
        result += c;
      }
      this.pos++;
    }
    
    throw new Error('unterminated string');
  }

  private parseBareString(): GValue {
    const start = this.pos;
    while (this.pos < this.input.length) {
      const c = this.input[this.pos];
      if (c === ' ' || c === ')' || c === ']' || c === '}' || c === '\n') {
        break;
      }
      this.pos++;
    }
    return GValue.str(this.input.slice(start, this.pos));
  }

  private parseRef(): GValue {
    this.expect('^');
    
    // Quoted ref
    if (this.peek() === '"') {
      const s = this.parseQuotedString().asStr();
      const colonIdx = s.indexOf(':');
      if (colonIdx > 0) {
        return GValue.id(s.slice(0, colonIdx), s.slice(colonIdx + 1));
      }
      return GValue.id('', s);
    }
    
    // Bare ref
    const start = this.pos;
    while (this.pos < this.input.length) {
      const c = this.input[this.pos];
      if (c === ' ' || c === ')' || c === ']' || c === '}' || c === '\n') {
        break;
      }
      this.pos++;
    }
    
    const refStr = this.input.slice(start, this.pos);
    const colonIdx = refStr.indexOf(':');
    if (colonIdx > 0) {
      return GValue.id(refStr.slice(0, colonIdx), refStr.slice(colonIdx + 1));
    }
    return GValue.id('', refStr);
  }

  private parseList(): GValue {
    this.expect('[');
    const items: GValue[] = [];
    
    while (true) {
      this.skipWhitespace();
      if (this.peek() === ']') {
        this.pos++;
        return GValue.list(...items);
      }
      items.push(this.parseValue());
    }
  }

  private parseMap(): GValue {
    this.expect('{');
    const entries: { key: string; value: GValue }[] = [];
    
    while (true) {
      this.skipWhitespace();
      if (this.peek() === '}') {
        this.pos++;
        return GValue.map(...entries);
      }
      
      // Parse key
      const key = this.parseValue().asStr();
      this.skipWhitespace();
      
      if (this.peek() !== ':' && this.peek() !== '=') {
        throw new Error(`expected ':' or '=' after map key`);
      }
      this.pos++;
      
      // Parse value
      const value = this.parseValue();
      entries.push({ key, value });
    }
  }

  private skipWhitespace(): void {
    while (this.pos < this.input.length) {
      const c = this.input[this.pos];
      if (c !== ' ' && c !== '\t' && c !== '\n' && c !== '\r') break;
      this.pos++;
    }
  }

  private peek(): string {
    return this.pos < this.input.length ? this.input[this.pos] : '';
  }

  private expect(c: string): void {
    this.skipWhitespace();
    if (this.pos >= this.input.length || this.input[this.pos] !== c) {
      throw new Error(`expected '${c}' at pos ${this.pos}`);
    }
    this.pos++;
  }

  private expectLiteral(s: string): void {
    if (this.input.slice(this.pos, this.pos + s.length) !== s) {
      throw new Error(`expected '${s}' at pos ${this.pos}`);
    }
    this.pos += s.length;
  }

  private tryLiteral(s: string): boolean {
    if (this.input.slice(this.pos, this.pos + s.length) === s) {
      // Check not followed by identifier char
      const next = this.input.charCodeAt(this.pos + s.length);
      if (this.isTypeNameCont(next)) {
        return false;
      }
      this.pos += s.length;
      return true;
    }
    return false;
  }
}

// ============================================================
// Header Parser
// ============================================================

export interface Header {
  version: string;
  schemaId?: string;
  mode?: 'auto' | 'struct' | 'packed' | 'tabular' | 'patch';
  keyMode?: 'wire' | 'name' | 'fid';
  target?: RefID;
}

export function parseHeader(input: string): Header | null {
  const trimmed = input.trim();
  
  if (!trimmed.startsWith('@lyph') && !trimmed.startsWith('@glyph')) {
    return null;
  }
  
  const header: Header = { version: 'v2' };
  const tokens = tokenizeHeader(trimmed);
  
  for (let i = 0; i < tokens.length; i++) {
    const tok = tokens[i];
    
    if (tok === '@lyph' || tok === '@glyph') {
      if (i + 1 < tokens.length && !tokens[i + 1].startsWith('@')) {
        header.version = tokens[++i];
      }
      continue;
    }
    
    if (tok.startsWith('@schema#')) {
      header.schemaId = tok.slice(8);
      continue;
    }
    
    if (tok.startsWith('@mode=')) {
      header.mode = tok.slice(6) as Header['mode'];
      continue;
    }
    
    if (tok.startsWith('@keys=')) {
      header.keyMode = tok.slice(6) as Header['keyMode'];
      continue;
    }
    
    if (tok.startsWith('@target=')) {
      const ref = tok.slice(8);
      const colonIdx = ref.indexOf(':');
      if (colonIdx > 0) {
        header.target = { prefix: ref.slice(0, colonIdx), value: ref.slice(colonIdx + 1) };
      } else {
        header.target = { prefix: '', value: ref };
      }
      continue;
    }
  }
  
  return header;
}

function tokenizeHeader(input: string): string[] {
  const tokens: string[] = [];
  let current = '';
  let inQuote = false;
  
  for (const c of input) {
    if (c === '"') {
      inQuote = !inQuote;
      current += c;
    } else if (c === ' ' && !inQuote) {
      if (current) {
        tokens.push(current);
        current = '';
      }
    } else {
      current += c;
    }
  }
  
  if (current) tokens.push(current);
  return tokens;
}

// ============================================================
// Tabular Parser
// ============================================================

export interface TabularParseResult {
  typeName: string;
  columns: string[];
  rows: GValue[];
}

/**
 * Parse a tabular format block.
 * 
 * Format:
 *   @tab Type [col1 col2 col3]
 *   value1 value2 value3
 *   value4 value5 value6
 *   @end
 */
export function parseTabular(input: string, schema: Schema): TabularParseResult {
  const lines = input.split('\n');
  if (lines.length === 0) {
    throw new Error('empty tabular input');
  }

  // Parse header: @tab Type [cols]
  const headerLine = lines[0].trim();
  const { typeName, columns } = parseTabularHeader(headerLine);

  const td = schema.getType(typeName);
  if (!td) {
    throw new Error(`unknown type: ${typeName}`);
  }
  if (!td.fields || td.fields.length === 0) {
    throw new Error(`type ${typeName} has no fields`);
  }

  // Map columns to field definitions
  type FieldType = typeof td.fields[number];
  const fieldMap = new Map<string, FieldType>();
  for (const fd of td.fields) {
    fieldMap.set(fd.name, fd);
    if (fd.wireKey) fieldMap.set(fd.wireKey, fd);
    fieldMap.set(`#${fd.fid}`, fd);
  }

  const columnFields = columns.map(col => {
    const fd = fieldMap.get(col);
    if (!fd) {
      throw new Error(`unknown column: ${col}`);
    }
    return fd;
  });

  // Parse rows
  const rows: GValue[] = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    
    // Skip empty lines and comments
    if (line === '' || line.startsWith('#')) continue;
    
    // Stop at @end
    if (line === '@end') break;

    // Parse row
    const row = parseTabularRow(line, typeName, columnFields, schema);
    rows.push(row);
  }

  return { typeName, columns, rows };
}

function parseTabularHeader(line: string): { typeName: string; columns: string[] } {
  // @tab Type [col1 col2 col3]
  if (!line.startsWith('@tab')) {
    throw new Error('tabular must start with @tab');
  }

  const rest = line.slice(4).trim();
  
  // Parse type name
  let pos = 0;
  while (pos < rest.length && rest[pos] !== ' ' && rest[pos] !== '[') {
    pos++;
  }
  const typeName = rest.slice(0, pos);
  
  if (!typeName) {
    throw new Error('missing type name after @tab');
  }

  // Skip to [
  while (pos < rest.length && rest[pos] !== '[') pos++;
  if (pos >= rest.length) {
    throw new Error('missing column list in tabular header');
  }
  
  // Parse columns
  pos++; // skip [
  const colStart = pos;
  while (pos < rest.length && rest[pos] !== ']') pos++;
  const colStr = rest.slice(colStart, pos);
  
  const columns = colStr.trim().split(/\s+/).filter(c => c.length > 0);
  
  return { typeName, columns };
}

function parseTabularRow(
  line: string,
  typeName: string,
  columnFields: Array<{ name: string; type: { kind: string; name?: string } }>,
  schema: Schema
): GValue {
  // Tokenize the row (respecting quoted strings, brackets, packed structs)
  const tokens = tokenizeRow(line);
  
  if (tokens.length !== columnFields.length) {
    throw new Error(`row has ${tokens.length} values, expected ${columnFields.length}`);
  }

  const entries: { key: string; value: GValue }[] = [];
  
  for (let i = 0; i < tokens.length; i++) {
    const fd = columnFields[i];
    const token = tokens[i];
    
    let value: GValue;
    if (isPackedFormat(token)) {
      value = parsePacked(token, schema);
    } else {
      value = parseScalarValue(token);
    }
    
    entries.push({ key: fd.name, value });
  }

  return GValue.struct(typeName, ...entries);
}

function tokenizeRow(line: string): string[] {
  const tokens: string[] = [];
  let pos = 0;
  
  while (pos < line.length) {
    // Skip whitespace
    while (pos < line.length && (line[pos] === ' ' || line[pos] === '\t')) pos++;
    if (pos >= line.length) break;
    
    const start = pos;
    const c = line[pos];
    
    if (c === '"') {
      // Quoted string
      pos++;
      while (pos < line.length && line[pos] !== '"') {
        if (line[pos] === '\\') pos++;
        pos++;
      }
      pos++; // closing quote
    } else if (c === '[') {
      // List
      let depth = 1;
      pos++;
      while (pos < line.length && depth > 0) {
        if (line[pos] === '[') depth++;
        else if (line[pos] === ']') depth--;
        pos++;
      }
    } else if (c === '{') {
      // Map or bitmap header
      let depth = 1;
      pos++;
      while (pos < line.length && depth > 0) {
        if (line[pos] === '{') depth++;
        else if (line[pos] === '}') depth--;
        pos++;
      }
    } else {
      // Bare token - handle packed structs Type@(...)
      while (pos < line.length) {
        const ch = line[pos];
        if (ch === ' ' || ch === '\t') break;
        if (ch === '(') {
          // Start of packed values - consume until matching )
          let depth = 1;
          pos++;
          while (pos < line.length && depth > 0) {
            if (line[pos] === '(') depth++;
            else if (line[pos] === ')') depth--;
            pos++;
          }
          break;
        }
        pos++;
      }
    }
    
    tokens.push(line.slice(start, pos));
  }
  
  return tokens;
}

function isPackedFormat(s: string): boolean {
  const atIdx = s.indexOf('@');
  if (atIdx <= 0) return false;
  if (atIdx + 1 >= s.length) return false;
  const next = s[atIdx + 1];
  return next === '(' || next === '{';
}

function parseScalarValue(s: string): GValue {
  s = s.trim();
  
  // Null
  if (s === '∅' || s === 'null' || s === 'nil' || s === 'none') {
    return GValue.null();
  }
  
  // Boolean
  if (s === 't' || s === 'true') return GValue.bool(true);
  if (s === 'f' || s === 'false') return GValue.bool(false);
  
  // Ref
  if (s.startsWith('^')) {
    const ref = s.slice(1);
    // Handle quoted ref
    if (ref.startsWith('"')) {
      const inner = ref.slice(1, -1);
      const colonIdx = inner.indexOf(':');
      if (colonIdx > 0) {
        return GValue.id(inner.slice(0, colonIdx), inner.slice(colonIdx + 1));
      }
      return GValue.id('', inner);
    }
    const colonIdx = ref.indexOf(':');
    if (colonIdx > 0) {
      return GValue.id(ref.slice(0, colonIdx), ref.slice(colonIdx + 1));
    }
    return GValue.id('', ref);
  }
  
  // Quoted string
  if (s.startsWith('"')) {
    return parseQuotedScalar(s);
  }
  
  // Time (ISO format)
  if (/^\d{4}-\d{2}-\d{2}T/.test(s)) {
    return GValue.time(new Date(s));
  }
  
  // Number
  if (/^-?\d/.test(s)) {
    if (s.includes('.') || s.includes('e') || s.includes('E')) {
      return GValue.float(parseFloat(s));
    }
    return GValue.int(parseInt(s, 10));
  }
  
  // List
  if (s.startsWith('[')) {
    return parseListScalar(s);
  }
  
  // Map
  if (s.startsWith('{')) {
    return parseMapScalar(s);
  }
  
  // Bare string
  return GValue.str(s);
}

function parseQuotedScalar(s: string): GValue {
  let result = '';
  for (let i = 1; i < s.length - 1; i++) {
    if (s[i] === '\\' && i + 1 < s.length - 1) {
      i++;
      switch (s[i]) {
        case 'n': result += '\n'; break;
        case 'r': result += '\r'; break;
        case 't': result += '\t'; break;
        case '\\': result += '\\'; break;
        case '"': result += '"'; break;
        default: result += s[i];
      }
    } else {
      result += s[i];
    }
  }
  return GValue.str(result);
}

function parseListScalar(s: string): GValue {
  // Simple list parsing - tokenize content
  const inner = s.slice(1, -1).trim();
  if (!inner) return GValue.list();
  
  const tokens = tokenizeRow(inner);
  return GValue.list(...tokens.map(t => parseScalarValue(t)));
}

function parseMapScalar(s: string): GValue {
  // Simple map parsing
  const inner = s.slice(1, -1).trim();
  if (!inner) return GValue.map();
  
  const entries: { key: string; value: GValue }[] = [];
  const tokens = tokenizeRow(inner);
  
  for (const token of tokens) {
    const eqIdx = token.indexOf('=');
    const colonIdx = token.indexOf(':');
    const sepIdx = eqIdx > 0 ? eqIdx : colonIdx;
    
    if (sepIdx > 0) {
      const key = token.slice(0, sepIdx).trim();
      const valStr = token.slice(sepIdx + 1).trim();
      entries.push({ key, value: parseScalarValue(valStr) });
    }
  }
  
  return GValue.map(...entries);
}
