#!/usr/bin/env node
/**
 * LYPH Cross-Implementation Canonicalization Script
 * 
 * This script is called by Go tests to verify that the JS and Go
 * implementations produce identical output for the same input.
 * 
 * Usage:
 *   node canon.mjs <command> [args...]
 * 
 * Commands:
 *   parse-packed <lyph-string> <schema-json>
 *     Parse packed LYPH and emit canonical form
 * 
 *   emit-packed <json-value> <schema-json>
 *     Convert JSON to packed LYPH
 * 
 *   roundtrip <lyph-string> <schema-json>
 *     Parse then re-emit to verify roundtrip
 * 
 * Output: JSON with { success: bool, result?: string, error?: string }
 */

import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { readFileSync } from 'fs';

// Import the built glyph-js library
const __dirname = dirname(fileURLToPath(import.meta.url));
const glyphPath = join(__dirname, '..', '..', '..', 'glyph-js', 'dist', 'index.js');

// Dynamic import of the built module
let glyph;
try {
  glyph = await import(glyphPath);
} catch (e) {
  console.error(JSON.stringify({ 
    success: false, 
    error: `Failed to import glyph-js: ${e.message}` 
  }));
  process.exit(1);
}

const { 
  SchemaBuilder, 
  t,
  g,
  field,
  parsePacked, 
  emitPacked,
  parseTabular,
  emitTabular,
  parsePatch,
  emitPatch,
  applyPatch,
  fromJson,
  toJson,
  // Loose mode
  canonicalizeLoose,
  canonicalizeLooseWithOpts,
  canonicalizeLooseWithSchema,
  llmLooseCanonOpts,
  defaultLooseCanonOpts,
  buildKeyDictFromValue,
  parseSchemaHeader,
  parseTabularLooseHeaderWithMeta,
  fromJsonLoose,
  toJsonLoose,
  equalLoose,
} = glyph;

/**
 * Build a Schema from JSON schema definition.
 * Format: { types: { TypeName: { fields: [...], packed: bool } } }
 */
function buildSchemaFromJson(schemaJson) {
  const def = JSON.parse(schemaJson);
  const builder = new SchemaBuilder();
  
  for (const [typeName, typeDef] of Object.entries(def.types || {})) {
    const fields = typeDef.fields || [];
    
    if (typeDef.packed) {
      builder.addPackedStruct(typeName, typeDef.version || 'v2');
    } else {
      builder.addStruct(typeName, typeDef.version || 'v1');
    }
    
    for (const fd of fields) {
      const typeSpec = resolveTypeSpec(fd.type);
      builder.field(fd.name, typeSpec, {
        fid: fd.fid || 0,
        wireKey: fd.wireKey || '',
        optional: fd.optional || false,
      });
    }
  }
  
  return builder.build();
}

function resolveTypeSpec(typeStr) {
  switch (typeStr) {
    case 'null': return t.null();
    case 'bool': return t.bool();
    case 'int': return t.int();
    case 'float': return t.float();
    case 'str': case 'string': return t.str();
    case 'bytes': return t.bytes();
    case 'time': return t.time();
    case 'id': return t.id();
    default:
      // Check for list<T>
      if (typeStr.startsWith('list<') && typeStr.endsWith('>')) {
        const inner = typeStr.slice(5, -1);
        return t.list(resolveTypeSpec(inner));
      }
      // Reference to another type
      return t.ref(typeStr);
  }
}

/**
 * Commands
 */

function cmdParsePacked(lyphStr, schemaJson) {
  try {
    const schema = buildSchemaFromJson(schemaJson);
    const gv = parsePacked(lyphStr, schema);
    const json = toJson(gv, { includeTypeMarkers: true });
    return { success: true, result: JSON.stringify(json) };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function cmdEmitPacked(valueJson, schemaJson) {
  try {
    const schema = buildSchemaFromJson(schemaJson);
    const value = JSON.parse(valueJson);
    const gv = fromJson(value, { schema });
    const lyph = emitPacked(gv, schema);
    return { success: true, result: lyph };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function cmdRoundtrip(lyphStr, schemaJson) {
  try {
    const schema = buildSchemaFromJson(schemaJson);
    
    // Parse
    const gv = parsePacked(lyphStr, schema);
    
    // Re-emit
    const reemitted = emitPacked(gv, schema);
    
    return { 
      success: true, 
      result: reemitted,
      matches: lyphStr.trim() === reemitted.trim(),
    };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function cmdCanonical(lyphStr, schemaJson) {
  try {
    const schema = buildSchemaFromJson(schemaJson);
    
    // Parse and re-emit to get canonical form
    const gv = parsePacked(lyphStr, schema);
    const canonical = emitPacked(gv, schema);
    
    return { success: true, result: canonical };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function cmdParsePatch(patchStr, schemaJson) {
  try {
    const schema = schemaJson ? buildSchemaFromJson(schemaJson) : null;
    const patch = parsePatch(patchStr, schema);
    return { 
      success: true, 
      result: JSON.stringify({
        target: patch.target,
        schemaId: patch.schemaId,
        opsCount: patch.ops.length,
      })
    };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function cmdEmitPatch(patchStr, schemaJson) {
  try {
    const schema = schemaJson ? buildSchemaFromJson(schemaJson) : null;
    // Parse and re-emit to get canonical form
    const patch = parsePatch(patchStr, schema);
    const canonical = emitPatch(patch);
    return { success: true, result: canonical };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function cmdPatchRoundtrip(patchStr, schemaJson) {
  try {
    const schema = schemaJson ? buildSchemaFromJson(schemaJson) : null;
    
    // Parse
    const patch = parsePatch(patchStr, schema);
    
    // Re-emit
    const reemitted = emitPatch(patch);
    
    return { 
      success: true, 
      result: reemitted,
      matches: patchStr.trim() === reemitted.trim(),
    };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function cmdParseTabular(tabularStr, schemaJson) {
  try {
    const schema = buildSchemaFromJson(schemaJson);
    const result = parseTabular(tabularStr, schema);
    return { 
      success: true, 
      result: JSON.stringify({
        typeName: result.typeName,
        columns: result.columns,
        rowCount: result.rows.length,
      })
    };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// ============================================================
// Loose Mode Commands
// ============================================================

function cmdCanonicalizeLoose(jsonStr) {
  try {
    const value = JSON.parse(jsonStr);
    const gv = fromJsonLoose(value);
    const canonical = canonicalizeLoose(gv);
    return { success: true, result: canonical };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function cmdFromJsonLoose(jsonStr, optsJson) {
  try {
    const value = JSON.parse(jsonStr);
    const opts = optsJson ? JSON.parse(optsJson) : {};
    const gv = fromJsonLoose(value, opts);
    const result = toJsonLoose(gv, opts);
    return { success: true, result: JSON.stringify(result) };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function cmdJsonRoundtripLoose(jsonStr, optsJson) {
  try {
    const original = JSON.parse(jsonStr);
    const opts = optsJson ? JSON.parse(optsJson) : {};
    
    // JSON -> GValue -> JSON
    const gv = fromJsonLoose(original, opts);
    const roundtripped = toJsonLoose(gv, opts);
    
    // Check equality
    const equal = JSON.stringify(original) === JSON.stringify(roundtripped);
    
    return { 
      success: true, 
      result: JSON.stringify(roundtripped),
      equal: equal,
    };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// ============================================================
// v2.4.0 Commands: LLM Mode, Schema Headers, Compact Keys
// ============================================================

function cmdCanonicalizeLooseLLM(jsonStr) {
  try {
    const value = JSON.parse(jsonStr);
    const gv = fromJsonLoose(value);
    const opts = llmLooseCanonOpts();
    const canonical = canonicalizeLooseWithOpts(gv, opts);
    return { success: true, result: canonical };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function cmdCanonicalizeLooseWithSchema(jsonStr, optsJson) {
  try {
    const value = JSON.parse(jsonStr);
    const opts = optsJson ? JSON.parse(optsJson) : {};
    const gv = fromJsonLoose(value);
    const canonical = canonicalizeLooseWithSchema(gv, opts);
    return { success: true, result: canonical };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function cmdBuildKeyDict(jsonStr) {
  try {
    const value = JSON.parse(jsonStr);
    const gv = fromJsonLoose(value);
    const keyDict = buildKeyDictFromValue(gv);
    return { success: true, result: JSON.stringify(keyDict) };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function cmdParseSchemaHeader(line) {
  try {
    const result = parseSchemaHeader(line);
    return { 
      success: true, 
      result: JSON.stringify({
        schemaRef: result.schemaRef,
        keyDict: result.keyDict,
      })
    };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

function cmdParseTabularHeaderWithMeta(line) {
  try {
    const result = parseTabularLooseHeaderWithMeta(line);
    return { 
      success: true, 
      result: JSON.stringify({
        rows: result.rows,
        cols: result.cols,
        keys: result.keys,
      })
    };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

// Main
const args = process.argv.slice(2);
const command = args[0];

let result;

switch (command) {
  case 'parse-packed':
    result = cmdParsePacked(args[1], args[2]);
    break;
    
  case 'emit-packed':
    result = cmdEmitPacked(args[1], args[2]);
    break;
    
  case 'roundtrip':
    result = cmdRoundtrip(args[1], args[2]);
    break;
    
  case 'canonical':
    result = cmdCanonical(args[1], args[2]);
    break;
    
  case 'parse-patch':
    result = cmdParsePatch(args[1], args[2]);
    break;
    
  case 'emit-patch':
    result = cmdEmitPatch(args[1], args[2]);
    break;
    
  case 'patch-roundtrip':
    result = cmdPatchRoundtrip(args[1], args[2]);
    break;
    
  case 'parse-tabular':
    result = cmdParseTabular(args[1], args[2]);
    break;
    
  case 'version':
    result = { success: true, result: '0.4.0' };
    break;
    
  // Loose mode commands
  case 'canonicalize-loose':
    result = cmdCanonicalizeLoose(args[1]);
    break;
    
  case 'from-json-loose':
    result = cmdFromJsonLoose(args[1], args[2]);
    break;
    
  case 'json-roundtrip-loose':
    result = cmdJsonRoundtripLoose(args[1], args[2]);
    break;
  
  // v2.4.0 commands
  case 'canonicalize-loose-llm':
    result = cmdCanonicalizeLooseLLM(args[1]);
    break;
    
  case 'canonicalize-loose-with-schema':
    result = cmdCanonicalizeLooseWithSchema(args[1], args[2]);
    break;
    
  case 'build-key-dict':
    result = cmdBuildKeyDict(args[1]);
    break;
    
  case 'parse-schema-header':
    result = cmdParseSchemaHeader(args[1]);
    break;
    
  case 'parse-tabular-header-with-meta':
    result = cmdParseTabularHeaderWithMeta(args[1]);
    break;
    
  default:
    result = { 
      success: false, 
      error: `Unknown command: ${command}. Use: parse-packed, emit-packed, roundtrip, canonical, parse-patch, emit-patch, patch-roundtrip, parse-tabular, canonicalize-loose, canonicalize-loose-llm, canonicalize-loose-with-schema, build-key-dict, parse-schema-header, parse-tabular-header-with-meta, from-json-loose, json-roundtrip-loose, version` 
    };
}

console.log(JSON.stringify(result));
