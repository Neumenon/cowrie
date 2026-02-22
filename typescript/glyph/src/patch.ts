/**
 * LYPH v2 Patch System
 * 
 * Implements patch emit, parse, and apply for cross-implementation parity with Go.
 */

import { GValue, RefID, MapEntry } from './types';
import { Schema } from './schema';
import { canonicalizeLoose } from './loose';
import { stateHashLooseSync, hashToHex } from './stream/hash';

// ============================================================
// Patch Types (Match Go's emit_patch.go)
// ============================================================

export type PatchOpKind = '=' | '+' | '-' | '~';

export type PathSegKind = 'field' | 'listIdx' | 'mapKey';

export interface PathSeg {
  kind: PathSegKind;
  field?: string;   // For field: canonical field name
  fid?: number;     // For field: resolved FID
  listIdx?: number; // For listIdx: index
  mapKey?: string;  // For mapKey: key
}

export interface PatchOp {
  op: PatchOpKind;
  path: PathSeg[];
  value?: GValue;    // For =, +, ~
  index?: number;    // For +: insert at index (-1 = append)
}

export interface Patch {
  target: RefID;
  schemaId?: string;
  targetType?: string;
  baseFingerprint?: string;  // v2.4.0: Base state fingerprint for validation
  ops: PatchOp[];
}

// ============================================================
// Path Segment Constructors
// ============================================================

export function fieldSeg(name: string, fid?: number): PathSeg {
  return { kind: 'field', field: name, fid };
}

export function listIdxSeg(idx: number): PathSeg {
  return { kind: 'listIdx', listIdx: idx };
}

export function mapKeySeg(key: string): PathSeg {
  return { kind: 'mapKey', mapKey: key };
}

// ============================================================
// Path Parsing
// ============================================================

/**
 * Parse a path string into segments.
 * Supports: .fieldName, .#fid, [N], ["key"]
 */
export function parsePathToSegs(path: string): PathSeg[] {
  if (!path) return [];
  
  const segs: PathSeg[] = [];
  let i = 0;
  
  while (i < path.length) {
    // Skip leading dots
    if (path[i] === '.') {
      i++;
      continue;
    }
    
    // List index: [N] or map key: ["key"]
    if (path[i] === '[') {
      const end = path.indexOf(']', i);
      if (end === -1) {
        // Malformed, treat rest as field
        segs.push(fieldSeg(path.slice(i)));
        break;
      }
      
      const inner = path.slice(i + 1, end);
      if (inner.startsWith('"')) {
        // Map key
        segs.push(mapKeySeg(inner.slice(1, -1)));
      } else {
        // List index
        segs.push(listIdxSeg(parseInt(inner, 10)));
      }
      i = end + 1;
      continue;
    }
    
    // FID reference: #N
    if (path[i] === '#') {
      let j = i + 1;
      while (j < path.length && path[j] >= '0' && path[j] <= '9') {
        j++;
      }
      if (j > i + 1) {
        const fid = parseInt(path.slice(i + 1, j), 10);
        segs.push({ kind: 'field', fid });
      }
      i = j;
      continue;
    }
    
    // Field name: until . or [ or end
    let j = i;
    let inQuote = false;
    while (j < path.length) {
      const c = path[j];
      if (c === '"') {
        inQuote = !inQuote;
      } else if (!inQuote && (c === '.' || c === '[')) {
        break;
      }
      j++;
    }
    
    if (j > i) {
      let field = path.slice(i, j);
      // Remove quotes if present
      if (field.startsWith('"') && field.endsWith('"')) {
        field = field.slice(1, -1);
      }
      segs.push(fieldSeg(field));
    }
    i = j;
  }
  
  return segs;
}

// ============================================================
// Patch Builder
// ============================================================

export class PatchBuilder {
  private patch: Patch;
  private schema?: Schema;

  constructor(target: RefID) {
    this.patch = {
      target,
      ops: [],
    };
  }

  withSchema(schema: Schema): this {
    this.schema = schema;
    this.patch.schemaId = schema.hash;
    return this;
  }

  withSchemaId(id: string): this {
    this.patch.schemaId = id;
    return this;
  }

  withTargetType(typeName: string): this {
    this.patch.targetType = typeName;
    return this;
  }

  /**
   * Set the base state fingerprint for validation.
   * The fingerprint should be the first 16 chars of the SHA-256 hash
   * of the canonical form of the base state.
   */
  withBaseFingerprint(fingerprint: string): this {
    this.patch.baseFingerprint = fingerprint;
    return this;
  }

  /**
   * Compute and set the base fingerprint from a GValue.
   * Uses the SHA-256 hash of the loose canonical form (first 16 hex chars).
   */
  withBaseValue(base: GValue): this {
    const hash = stateHashLooseSync(base);
    const hex = hashToHex(hash);
    this.patch.baseFingerprint = hex.slice(0, 16);
    return this;
  }

  set(path: string, value: GValue): this {
    this.patch.ops.push({
      op: '=',
      path: parsePathToSegs(path),
      value,
    });
    return this;
  }

  setWithSegs(path: PathSeg[], value: GValue): this {
    this.patch.ops.push({
      op: '=',
      path,
      value,
    });
    return this;
  }

  append(path: string, value: GValue): this {
    this.patch.ops.push({
      op: '+',
      path: parsePathToSegs(path),
      value,
      index: -1,
    });
    return this;
  }

  delete(path: string): this {
    this.patch.ops.push({
      op: '-',
      path: parsePathToSegs(path),
    });
    return this;
  }

  delta(path: string, amount: number): this {
    this.patch.ops.push({
      op: '~',
      path: parsePathToSegs(path),
      value: GValue.float(amount),
    });
    return this;
  }

  insertAt(path: string, index: number, value: GValue): this {
    this.patch.ops.push({
      op: '+',
      path: parsePathToSegs(path),
      value,
      index,
    });
    return this;
  }

  build(): Patch {
    return this.patch;
  }
}

// ============================================================
// Patch Emit
// ============================================================

export type KeyMode = 'wire' | 'name' | 'fid';

export interface PatchEmitOptions {
  schema?: Schema;
  keyMode?: KeyMode;
  sortOps?: boolean;
  indentPrefix?: string;
}

export function emitPatch(patch: Patch, options: PatchEmitOptions = {}): string {
  const keyMode = options.keyMode || 'wire';
  const sortOps = options.sortOps !== false;
  
  const lines: string[] = [];
  
  // Header
  let header = '@patch';
  if (patch.schemaId) {
    header += ` @schema#${patch.schemaId}`;
  }
  header += ` @keys=${keyMode}`;
  header += ` @target=${patch.target.prefix}:${patch.target.value}`;
  // v2.4.0: Base fingerprint for state validation
  if (patch.baseFingerprint) {
    header += ` @base=${patch.baseFingerprint}`;
  }
  lines.push(header);
  
  // Operations
  let ops = patch.ops;
  if (sortOps) {
    ops = [...ops].sort((a, b) => {
      const pa = pathSegsToString(a.path, keyMode);
      const pb = pathSegsToString(b.path, keyMode);
      if (pa !== pb) return pa < pb ? -1 : 1;
      return a.op < b.op ? -1 : a.op > b.op ? 1 : 0;
    });
  }
  
  const prefix = options.indentPrefix || '';
  
  for (const op of ops) {
    let line = prefix + op.op + ' ';
    line += emitPathSegs(op.path, keyMode);
    
    if (op.op === '=' || op.op === '+') {
      if (op.value) {
        line += ' ' + emitValue(op.value, options.schema);
      }
      if (op.op === '+' && op.index !== undefined && op.index >= 0) {
        line += ` @idx=${op.index}`;
      }
    } else if (op.op === '~') {
      if (op.value) {
        const num = op.value.type === 'float' ? op.value.asFloat() : op.value.asInt();
        line += ' ' + (num >= 0 ? '+' : '') + canonFloat(num);
      }
    }
    // OpDelete has no value
    
    lines.push(line);
  }
  
  lines.push('@end');
  
  return lines.join('\n');
}

function pathSegsToString(path: PathSeg[], keyMode: KeyMode): string {
  let result = '';
  for (let i = 0; i < path.length; i++) {
    const seg = path[i];
    if (seg.kind === 'field') {
      if (i > 0) result += '.';
      if (keyMode === 'fid' && seg.fid) {
        result += '#' + seg.fid;
      } else {
        result += seg.field || '';
      }
    } else if (seg.kind === 'listIdx') {
      result += `[${seg.listIdx}]`;
    } else if (seg.kind === 'mapKey') {
      result += `["${seg.mapKey}"]`;
    }
  }
  return result;
}

function emitPathSegs(path: PathSeg[], keyMode: KeyMode): string {
  return pathSegsToString(path, keyMode);
}

function emitValue(gv: GValue, schema?: Schema): string {
  switch (gv.type) {
    case 'null': return '∅';
    case 'bool': return gv.asBool() ? 't' : 'f';
    case 'int': return canonInt(gv.asInt());
    case 'float': return canonFloat(gv.asFloat());
    case 'str': return canonString(gv.asStr());
    case 'id': return canonRef(gv.asId());
    case 'time': return gv.asTime().toISOString().replace('.000Z', 'Z');
    case 'list': {
      const items = gv.asList().map(v => emitValue(v, schema));
      return '[' + items.join(' ') + ']';
    }
    case 'map': {
      const parts: string[] = [];
      for (const e of gv.asMap()) {
        parts.push(`${canonString(e.key)}:${emitValue(e.value, schema)}`);
      }
      return '{' + parts.join(' ') + '}';
    }
    case 'struct': {
      const sv = gv.asStruct();
      // For structs, emit in packed form if schema available
      // Otherwise fall back to struct form
      const parts: string[] = [];
      for (const f of sv.fields) {
        parts.push(`${canonString(f.key)}=${emitValue(f.value, schema)}`);
      }
      return `${sv.typeName}{${parts.join(' ')}}`;
    }
    case 'sum': {
      const sum = gv.asSum();
      if (!sum.value) return `${sum.tag}()`;
      return `${sum.tag}(${emitValue(sum.value, schema)})`;
    }
    default:
      return '∅';
  }
}

// ============================================================
// Patch Parse
// ============================================================

export function parsePatch(input: string, schema?: Schema): Patch {
  const lines = input.split('\n');
  if (lines.length === 0) {
    throw new Error('empty patch input');
  }
  
  // Parse header
  const headerLine = lines[0].trim();
  const header = parsePatchHeader(headerLine);
  
  const patch: Patch = {
    target: header.target,
    schemaId: header.schemaId,
    baseFingerprint: header.baseFingerprint,
    ops: [],
  };
  
  // Parse operations
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    
    if (!line || line.startsWith('#')) continue;
    if (line === '@end') break;
    
    const op = parsePatchOp(line, schema);
    patch.ops.push(op);
  }
  
  return patch;
}

interface ParsedHeader {
  target: RefID;
  schemaId?: string;
  keyMode: KeyMode;
  baseFingerprint?: string;  // v2.4.0
}

function parsePatchHeader(line: string): ParsedHeader {
  if (!line.startsWith('@patch')) {
    throw new Error('patch must start with @patch');
  }
  
  const result: ParsedHeader = {
    target: { prefix: '', value: '' },
    keyMode: 'wire',
  };
  
  const tokens = tokenizeHeader(line);
  
  for (const tok of tokens) {
    if (tok.startsWith('@schema#')) {
      result.schemaId = tok.slice(8);
    } else if (tok.startsWith('@keys=')) {
      result.keyMode = tok.slice(6) as KeyMode;
    } else if (tok.startsWith('@target=')) {
      const ref = tok.slice(8);
      const colonIdx = ref.indexOf(':');
      if (colonIdx > 0) {
        result.target = { prefix: ref.slice(0, colonIdx), value: ref.slice(colonIdx + 1) };
      } else {
        result.target = { prefix: '', value: ref };
      }
    } else if (tok.startsWith('@base=')) {
      // v2.4.0: Base fingerprint
      result.baseFingerprint = tok.slice(6);
    }
  }
  
  return result;
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

function parsePatchOp(line: string, schema?: Schema): PatchOp {
  if (!line) {
    throw new Error('empty operation line');
  }
  
  const opChar = line[0] as PatchOpKind;
  if (!['=', '+', '-', '~'].includes(opChar)) {
    throw new Error(`unknown operation: ${opChar}`);
  }
  
  const rest = line.slice(1).trim();
  if (!rest) {
    throw new Error('missing path in operation');
  }
  
  // Split into path and value
  const pathEnd = findPathEnd(rest);
  const pathStr = rest.slice(0, pathEnd);
  let valueStr = rest.slice(pathEnd).trim();
  
  const path = parsePathToSegs(pathStr);
  
  const op: PatchOp = {
    op: opChar,
    path,
    index: -1,
  };
  
  switch (opChar) {
    case '=':
    case '+': {
      if (valueStr) {
        // Check for @idx= suffix
        const idxMatch = valueStr.match(/ @idx=(\d+)$/);
        if (idxMatch) {
          op.index = parseInt(idxMatch[1], 10);
          valueStr = valueStr.slice(0, -idxMatch[0].length);
        }
        op.value = parseInlineValue(valueStr, schema);
      }
      break;
    }
    case '~': {
      if (!valueStr) {
        throw new Error('delta operation requires a value');
      }
      const num = parseFloat(valueStr);
      op.value = GValue.float(num);
      break;
    }
    case '-':
      // No value needed
      break;
  }
  
  return op;
}

function findPathEnd(s: string): number {
  let inQuote = false;
  let bracketDepth = 0;
  
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c === '"') {
      inQuote = !inQuote;
    } else if (c === '[' && !inQuote) {
      bracketDepth++;
    } else if (c === ']' && !inQuote && bracketDepth > 0) {
      bracketDepth--;
    } else if ((c === ' ' || c === '\t') && !inQuote && bracketDepth === 0) {
      return i;
    }
  }
  
  return s.length;
}

function parseInlineValue(s: string, schema?: Schema): GValue {
  s = s.trim();
  if (!s) return GValue.null();
  
  // Null
  if (s === '∅' || s === 'null') return GValue.null();
  
  // Bool
  if (s === 't' || s === 'true') return GValue.bool(true);
  if (s === 'f' || s === 'false') return GValue.bool(false);
  
  // Ref
  if (s.startsWith('^')) {
    const ref = s.slice(1);
    const colonIdx = ref.indexOf(':');
    if (colonIdx > 0) {
      return GValue.id(ref.slice(0, colonIdx), ref.slice(colonIdx + 1));
    }
    return GValue.id('', ref);
  }
  
  // Quoted string
  if (s.startsWith('"')) {
    return parseQuotedString(s);
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
    return parseList(s);
  }
  
  // Struct (Type{...})
  if (/^[A-Za-z_]\w*\{/.test(s)) {
    return parseStruct(s);
  }
  
  // Bare string
  return GValue.str(s);
}

function parseQuotedString(s: string): GValue {
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

function parseList(s: string): GValue {
  // Simple tokenized list parsing
  const inner = s.slice(1, -1).trim();
  if (!inner) return GValue.list();
  
  const items: GValue[] = [];
  const tokens = tokenizeValues(inner);
  for (const tok of tokens) {
    items.push(parseInlineValue(tok));
  }
  return GValue.list(...items);
}

function parseStruct(s: string): GValue {
  const braceIdx = s.indexOf('{');
  const typeName = s.slice(0, braceIdx);
  const inner = s.slice(braceIdx + 1, -1).trim();
  
  if (!inner) return GValue.struct(typeName);
  
  const entries: { key: string; value: GValue }[] = [];
  const tokens = tokenizeValues(inner);
  
  for (const tok of tokens) {
    const eqIdx = tok.indexOf('=');
    if (eqIdx > 0) {
      const key = tok.slice(0, eqIdx).trim();
      const valStr = tok.slice(eqIdx + 1).trim();
      entries.push({ key, value: parseInlineValue(valStr) });
    }
  }
  
  return GValue.struct(typeName, ...entries);
}

function tokenizeValues(s: string): string[] {
  const tokens: string[] = [];
  let current = '';
  let inQuote = false;
  let depth = 0;
  
  for (const c of s) {
    if (c === '"') {
      inQuote = !inQuote;
      current += c;
    } else if (!inQuote) {
      if (c === '[' || c === '{' || c === '(') {
        depth++;
        current += c;
      } else if (c === ']' || c === '}' || c === ')') {
        depth--;
        current += c;
      } else if (c === ' ' && depth === 0) {
        if (current) {
          tokens.push(current);
          current = '';
        }
      } else {
        current += c;
      }
    } else {
      current += c;
    }
  }
  
  if (current) tokens.push(current);
  return tokens;
}

// ============================================================
// Patch Apply
// ============================================================

export function applyPatch(value: GValue, patch: Patch): GValue {
  let result = value.clone();
  
  for (const op of patch.ops) {
    result = applyOp(result, op);
  }
  
  return result;
}

function applyOp(value: GValue, op: PatchOp): GValue {
  if (op.path.length === 0) {
    // Root-level operation
    if (op.op === '=') {
      return op.value || GValue.null();
    }
    throw new Error(`cannot apply ${op.op} to root`);
  }
  
  return applyAtPath(value, op.path, op);
}

function applyAtPath(value: GValue, path: PathSeg[], op: PatchOp): GValue {
  if (path.length === 1) {
    return applyToParent(value, path[0], op);
  }
  
  const seg = path[0];
  const rest = path.slice(1);
  
  if (seg.kind === 'field') {
    const key = seg.field!;
    if (value.type !== 'struct') {
      throw new Error(`cannot navigate into ${value.type} with field`);
    }
    
    const sv = value.asStruct();
    for (let i = 0; i < sv.fields.length; i++) {
      if (sv.fields[i].key === key) {
        sv.fields[i].value = applyAtPath(sv.fields[i].value, rest, op);
        return value;
      }
    }
    throw new Error(`field not found: ${key}`);
  }
  
  if (seg.kind === 'listIdx') {
    if (value.type !== 'list') {
      throw new Error(`cannot index into ${value.type}`);
    }
    const list = value.asList();
    const idx = seg.listIdx!;
    if (idx < 0 || idx >= list.length) {
      throw new Error(`index out of bounds: ${idx}`);
    }
    list[idx] = applyAtPath(list[idx], rest, op);
    return value;
  }
  
  if (seg.kind === 'mapKey') {
    if (value.type !== 'map') {
      throw new Error(`cannot access map key in ${value.type}`);
    }
    const entries = value.asMap();
    const key = seg.mapKey!;
    for (let i = 0; i < entries.length; i++) {
      if (entries[i].key === key) {
        entries[i].value = applyAtPath(entries[i].value, rest, op);
        return value;
      }
    }
    throw new Error(`key not found: ${key}`);
  }
  
  throw new Error('unknown path segment kind');
}

function applyToParent(value: GValue, seg: PathSeg, op: PatchOp): GValue {
  const key = seg.kind === 'mapKey' ? seg.mapKey! : seg.field!;
  
  switch (op.op) {
    case '=':
      value.set(key, op.value || GValue.null());
      return value;
    
    case '+': {
      const existing = value.get(key);
      if (!existing || existing.isNull()) {
        value.set(key, GValue.list(op.value || GValue.null()));
      } else if (existing.type === 'list') {
        const list = existing.asList();
        if (op.index !== undefined && op.index >= 0 && op.index <= list.length) {
          list.splice(op.index, 0, op.value || GValue.null());
        } else {
          list.push(op.value || GValue.null());
        }
      } else {
        throw new Error(`cannot append to ${existing.type}`);
      }
      return value;
    }
    
    case '-': {
      if (value.type === 'struct') {
        const sv = value.asStruct();
        sv.fields = sv.fields.filter(f => f.key !== key);
      } else if (value.type === 'map') {
        const entries = value.asMap();
        const idx = entries.findIndex(e => e.key === key);
        if (idx >= 0) entries.splice(idx, 1);
      } else {
        throw new Error(`cannot delete from ${value.type}`);
      }
      return value;
    }
    
    case '~': {
      const existing = value.get(key);
      if (!existing) {
        throw new Error(`field not found for delta: ${key}`);
      }
      
      const delta = op.value?.type === 'float' ? op.value.asFloat() : op.value?.asInt() || 0;
      
      if (existing.type === 'int') {
        value.set(key, GValue.int(existing.asInt() + delta));
      } else if (existing.type === 'float') {
        value.set(key, GValue.float(existing.asFloat() + delta));
      } else {
        throw new Error(`cannot apply delta to ${existing.type}`);
      }
      return value;
    }
  }
  
  throw new Error(`unknown operation: ${op.op}`);
}

// ============================================================
// Canonical Helpers
// ============================================================

function canonInt(n: number): string {
  return String(Math.floor(n));
}

function canonFloat(f: number): string {
  if (f === 0) return '0';
  return String(f).replace('E', 'e');
}

function canonString(s: string): string {
  if (isBareSafe(s)) return s;
  return quoteString(s);
}

function canonRef(ref: RefID): string {
  const full = ref.prefix ? `${ref.prefix}:${ref.value}` : ref.value;
  return `^${full}`;
}

function isBareSafe(s: string): boolean {
  if (!s) return false;
  if (['t', 'f', 'true', 'false', 'null', 'none', 'nil'].includes(s)) return false;
  
  const first = s.charCodeAt(0);
  if (!isLetter(first) && first !== 95) return false;
  
  for (let i = 1; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (!isLetter(c) && !isDigit(c) && c !== 95 && c !== 45 && c !== 46 && c !== 47) {
      return false;
    }
  }
  
  return true;
}

function isLetter(c: number): boolean {
  return (c >= 65 && c <= 90) || (c >= 97 && c <= 122);
}

function isDigit(c: number): boolean {
  return c >= 48 && c <= 57;
}

function quoteString(s: string): string {
  let result = '"';
  for (const ch of s) {
    switch (ch) {
      case '\\': result += '\\\\'; break;
      case '"': result += '\\"'; break;
      case '\n': result += '\\n'; break;
      case '\r': result += '\\r'; break;
      case '\t': result += '\\t'; break;
      default: result += ch;
    }
  }
  return result + '"';
}
