/**
 * LYPH v2 JSON Conversion
 * 
 * Converts between JSON and GValue representations.
 */

import { GValue, RefID, MapEntry } from './types';
import { Schema, TypeDef } from './schema';

// ============================================================
// JSON to GValue Conversion
// ============================================================

export interface FromJsonOptions {
  /** Schema for type hints */
  schema?: Schema;
  /** Type name hint for root value */
  typeName?: string;
  /** Parse ISO date strings as time values */
  parseDates?: boolean;
  /** Parse ^prefix:value strings as refs */
  parseRefs?: boolean;
}

/**
 * Convert JSON value to GValue
 */
export function fromJson(json: unknown, options: FromJsonOptions = {}): GValue {
  const { schema, typeName, parseDates = true, parseRefs = true } = options;
  
  return convertValue(json, schema, typeName, parseDates, parseRefs);
}

function convertValue(
  v: unknown,
  schema: Schema | undefined,
  typeName: string | undefined,
  parseDates: boolean,
  parseRefs: boolean
): GValue {
  // Null
  if (v === null || v === undefined) {
    return GValue.null();
  }

  // Boolean
  if (typeof v === 'boolean') {
    return GValue.bool(v);
  }

  // Number
  if (typeof v === 'number') {
    if (Number.isInteger(v)) {
      return GValue.int(v);
    }
    return GValue.float(v);
  }

  // String
  if (typeof v === 'string') {
    // Check for ref pattern: ^prefix:value
    if (parseRefs && v.startsWith('^')) {
      const rest = v.slice(1);
      const colonIdx = rest.indexOf(':');
      if (colonIdx > 0) {
        return GValue.id(rest.slice(0, colonIdx), rest.slice(colonIdx + 1));
      }
      return GValue.id('', rest);
    }

    // Check for ISO date pattern
    if (parseDates && isIsoDateString(v)) {
      const date = new Date(v);
      if (!isNaN(date.getTime())) {
        return GValue.time(date);
      }
    }

    return GValue.str(v);
  }

  // Array
  if (Array.isArray(v)) {
    const items = v.map(item => convertValue(item, schema, undefined, parseDates, parseRefs));
    return GValue.list(...items);
  }

  // Object
  if (typeof v === 'object') {
    const obj = v as Record<string, unknown>;
    
    // Check for special type markers
    if ('$type' in obj && typeof obj.$type === 'string') {
      // Typed struct: { $type: "TypeName", field1: ..., field2: ... }
      const structTypeName = obj.$type;
      const td = schema?.getType(structTypeName);
      const fields: MapEntry[] = [];
      
      for (const [key, val] of Object.entries(obj)) {
        if (key === '$type') continue;
        
        // Get field type hint from schema
        const fieldDef = td?.fields?.find(f => f.name === key || f.wireKey === key);
        const fieldTypeName = fieldDef?.type.kind === 'ref' ? fieldDef.type.name : undefined;
        
        fields.push({
          key,
          value: convertValue(val, schema, fieldTypeName, parseDates, parseRefs),
        });
      }
      
      return GValue.struct(structTypeName, ...fields);
    }

    // Check for ref marker
    if ('$ref' in obj && typeof obj.$ref === 'string') {
      const ref = obj.$ref;
      const colonIdx = ref.indexOf(':');
      if (colonIdx > 0) {
        return GValue.id(ref.slice(0, colonIdx), ref.slice(colonIdx + 1));
      }
      return GValue.id('', ref);
    }

    // Check for time marker
    if ('$time' in obj && typeof obj.$time === 'string') {
      return GValue.time(new Date(obj.$time));
    }

    // Check for bytes marker
    if ('$bytes' in obj && typeof obj.$bytes === 'string') {
      return GValue.bytes(base64ToBytes(obj.$bytes));
    }

    // Check for sum type marker
    if ('$tag' in obj && typeof obj.$tag === 'string') {
      const value = '$value' in obj 
        ? convertValue(obj.$value, schema, undefined, parseDates, parseRefs)
        : null;
      return GValue.sum(obj.$tag, value);
    }

    // Regular object -> struct with typeName or map
    if (typeName) {
      const td = schema?.getType(typeName);
      const fields: MapEntry[] = [];
      
      for (const [key, val] of Object.entries(obj)) {
        const fieldDef = td?.fields?.find(f => f.name === key || f.wireKey === key);
        const fieldTypeName = fieldDef?.type.kind === 'ref' ? fieldDef.type.name : undefined;
        
        fields.push({
          key,
          value: convertValue(val, schema, fieldTypeName, parseDates, parseRefs),
        });
      }
      
      return GValue.struct(typeName, ...fields);
    }

    // Map
    const entries: MapEntry[] = [];
    for (const [key, val] of Object.entries(obj)) {
      entries.push({
        key,
        value: convertValue(val, schema, undefined, parseDates, parseRefs),
      });
    }
    return GValue.map(...entries);
  }

  throw new Error(`Unsupported JSON value type: ${typeof v}`);
}

function isIsoDateString(s: string): boolean {
  // Simple ISO-8601 detection: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
  return /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?/.test(s);
}

function base64ToBytes(b64: string): Uint8Array {
  if (typeof atob === 'function') {
    const binary = atob(b64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
  }
  // Node.js fallback
  return new Uint8Array(Buffer.from(b64, 'base64'));
}

// ============================================================
// GValue to JSON Conversion
// ============================================================

export interface ToJsonOptions {
  /** Use $type markers for structs */
  includeTypeMarkers?: boolean;
  /** Use compact ref format (^prefix:value) instead of $ref */
  compactRefs?: boolean;
  /** Format dates as ISO strings */
  formatDates?: boolean;
  /** Use wire keys instead of field names */
  useWireKeys?: boolean;
  /** Schema for wire key lookup */
  schema?: Schema;
}

/**
 * Convert GValue to JSON-compatible value
 */
export function toJson(gv: GValue, options: ToJsonOptions = {}): unknown {
  const { 
    includeTypeMarkers = false, 
    compactRefs = true,
    formatDates = true,
    useWireKeys = false,
    schema,
  } = options;

  return convertToJson(gv, includeTypeMarkers, compactRefs, formatDates, useWireKeys, schema);
}

function convertToJson(
  gv: GValue,
  includeTypeMarkers: boolean,
  compactRefs: boolean,
  formatDates: boolean,
  useWireKeys: boolean,
  schema: Schema | undefined
): unknown {
  switch (gv.type) {
    case 'null':
      return null;

    case 'bool':
      return gv.asBool();

    case 'int':
      return gv.asInt();

    case 'float':
      return gv.asFloat();

    case 'str':
      return gv.asStr();

    case 'bytes': {
      const bytes = gv.asBytes();
      const b64 = bytesToBase64(bytes);
      return { $bytes: b64 };
    }

    case 'time': {
      const date = gv.asTime();
      if (formatDates) {
        return date.toISOString();
      }
      return { $time: date.toISOString() };
    }

    case 'id': {
      const ref = gv.asId();
      const refStr = ref.prefix ? `${ref.prefix}:${ref.value}` : ref.value;
      if (compactRefs) {
        return `^${refStr}`;
      }
      return { $ref: refStr };
    }

    case 'list': {
      return gv.asList().map(item => 
        convertToJson(item, includeTypeMarkers, compactRefs, formatDates, useWireKeys, schema)
      );
    }

    case 'map': {
      const result: Record<string, unknown> = {};
      for (const entry of gv.asMap()) {
        result[entry.key] = convertToJson(
          entry.value, includeTypeMarkers, compactRefs, formatDates, useWireKeys, schema
        );
      }
      return result;
    }

    case 'struct': {
      const sv = gv.asStruct();
      const result: Record<string, unknown> = {};
      
      if (includeTypeMarkers) {
        result.$type = sv.typeName;
      }

      const td = schema?.getType(sv.typeName);

      for (const field of sv.fields) {
        let key = field.key;
        
        if (useWireKeys && td) {
          const fd = td.fields?.find(f => f.name === field.key);
          if (fd?.wireKey) {
            key = fd.wireKey;
          }
        }

        result[key] = convertToJson(
          field.value, includeTypeMarkers, compactRefs, formatDates, useWireKeys, schema
        );
      }
      
      return result;
    }

    case 'sum': {
      const sum = gv.asSum();
      if (sum.value === null) {
        return { $tag: sum.tag };
      }
      return {
        $tag: sum.tag,
        $value: convertToJson(
          sum.value, includeTypeMarkers, compactRefs, formatDates, useWireKeys, schema
        ),
      };
    }
  }
}

function bytesToBase64(bytes: Uint8Array): string {
  if (typeof btoa === 'function') {
    let binary = '';
    for (let i = 0; i < bytes.length; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
  }
  // Node.js fallback
  return Buffer.from(bytes).toString('base64');
}

// ============================================================
// Convenience Functions
// ============================================================

/**
 * Parse JSON string to GValue
 */
export function parseJson(jsonStr: string, options: FromJsonOptions = {}): GValue {
  const json = JSON.parse(jsonStr);
  return fromJson(json, options);
}

/**
 * Stringify GValue to JSON string
 */
export function stringifyJson(gv: GValue, options: ToJsonOptions = {}, indent?: number): string {
  const json = toJson(gv, options);
  return JSON.stringify(json, null, indent);
}

/**
 * Round-trip convert: JSON -> GValue -> JSON
 * Useful for normalizing JSON to LYPH conventions
 */
export function normalizeJson(
  json: unknown, 
  fromOptions: FromJsonOptions = {},
  toOptions: ToJsonOptions = {}
): unknown {
  const gv = fromJson(json, fromOptions);
  return toJson(gv, toOptions);
}
