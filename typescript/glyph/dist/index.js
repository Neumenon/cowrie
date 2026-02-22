"use strict";
/**
 * LYPH v2 JavaScript/TypeScript Codec
 *
 * A token-efficient serialization format for LLM communication.
 *
 * @example
 * ```typescript
 * import { g, field, SchemaBuilder, t, fromJson, toJson, emitPacked, parsePacked } from 'glyph-js';
 *
 * // Define a schema
 * const schema = new SchemaBuilder()
 *   .addPackedStruct('Team', 'v2')
 *     .field('id', t.id(), { fid: 1, wireKey: 't' })
 *     .field('name', t.str(), { fid: 2, wireKey: 'n' })
 *     .field('league', t.str(), { fid: 3, wireKey: 'l' })
 *   .build();
 *
 * // Create values
 * const team = g.struct('Team',
 *   field('id', g.id('t', 'ARS')),
 *   field('name', g.str('Arsenal')),
 *   field('league', g.str('EPL'))
 * );
 *
 * // Emit as packed LYPH
 * const packed = emitPacked(team, schema);
 * // => "Team@(^t:ARS Arsenal EPL)"
 *
 * // Parse back
 * const parsed = parsePacked(packed, schema);
 *
 * // Convert from JSON
 * const json = { $type: 'Team', id: '^t:ARS', name: 'Arsenal', league: 'EPL' };
 * const fromJsonValue = fromJson(json, { schema, typeName: 'Team' });
 *
 * // Convert to JSON
 * const backToJson = toJson(team, { includeTypeMarkers: true });
 * ```
 */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.Decimal128 = exports.stream = exports.parseTabularLooseHeaderWithMeta = exports.parseSchemaHeader = exports.buildKeyDictFromValue = exports.canonicalizeLooseWithSchema = exports.llmLooseCanonOpts = exports.tabularLooseCanonOpts = exports.noTabularLooseCanonOpts = exports.defaultLooseCanonOpts = exports.unescapeTabularCell = exports.parseTabularLoose = exports.jsonEqual = exports.stringifyJsonLoose = exports.parseJsonLoose = exports.toJsonLoose = exports.fromJsonLoose = exports.equalLoose = exports.fingerprintLoose = exports.canonicalizeLooseTabular = exports.canonicalizeLooseWithOpts = exports.canonicalizeLooseNoTabular = exports.canonicalizeLoose = exports.mapKeySeg = exports.listIdxSeg = exports.fieldSeg = exports.parsePathToSegs = exports.applyPatch = exports.parsePatch = exports.emitPatch = exports.PatchBuilder = exports.parseHeader = exports.parseTabular = exports.parsePacked = exports.emitHeader = exports.emitV2 = exports.emitTabular = exports.emitPacked = exports.emit = exports.normalizeJson = exports.stringifyJson = exports.parseJson = exports.toJson = exports.fromJson = exports.t = exports.SchemaBuilder = exports.Schema = exports.field = exports.g = exports.GValue = void 0;
exports.defaultToolRegistry = exports.ErrorCode = exports.ValidatorState = exports.ToolRegistry = exports.StreamingValidator = exports.versionedSchema = exports.formatVersionHeader = exports.parseVersionHeader = exports.compareVersions = exports.EvolutionMode = exports.EvolvingField = exports.VersionSchema = exports.VersionedSchema = exports.parseDecimalLiteral = exports.isDecimalLiteral = exports.decimal = exports.DecimalError = void 0;
exports.jsonToPacked = jsonToPacked;
exports.jsonToTabular = jsonToTabular;
exports.jsonToLyph = jsonToLyph;
exports.estimateTokens = estimateTokens;
exports.compareTokens = compareTokens;
// Core types
var types_1 = require("./types");
Object.defineProperty(exports, "GValue", { enumerable: true, get: function () { return types_1.GValue; } });
Object.defineProperty(exports, "g", { enumerable: true, get: function () { return types_1.g; } });
Object.defineProperty(exports, "field", { enumerable: true, get: function () { return types_1.field; } });
// Schema
var schema_1 = require("./schema");
Object.defineProperty(exports, "Schema", { enumerable: true, get: function () { return schema_1.Schema; } });
Object.defineProperty(exports, "SchemaBuilder", { enumerable: true, get: function () { return schema_1.SchemaBuilder; } });
Object.defineProperty(exports, "t", { enumerable: true, get: function () { return schema_1.t; } });
// JSON conversion
var json_1 = require("./json");
Object.defineProperty(exports, "fromJson", { enumerable: true, get: function () { return json_1.fromJson; } });
Object.defineProperty(exports, "toJson", { enumerable: true, get: function () { return json_1.toJson; } });
Object.defineProperty(exports, "parseJson", { enumerable: true, get: function () { return json_1.parseJson; } });
Object.defineProperty(exports, "stringifyJson", { enumerable: true, get: function () { return json_1.stringifyJson; } });
Object.defineProperty(exports, "normalizeJson", { enumerable: true, get: function () { return json_1.normalizeJson; } });
// Emitters
var emit_1 = require("./emit");
Object.defineProperty(exports, "emit", { enumerable: true, get: function () { return emit_1.emit; } });
Object.defineProperty(exports, "emitPacked", { enumerable: true, get: function () { return emit_1.emitPacked; } });
Object.defineProperty(exports, "emitTabular", { enumerable: true, get: function () { return emit_1.emitTabular; } });
Object.defineProperty(exports, "emitV2", { enumerable: true, get: function () { return emit_1.emitV2; } });
Object.defineProperty(exports, "emitHeader", { enumerable: true, get: function () { return emit_1.emitHeader; } });
// Parsers
var parse_1 = require("./parse");
Object.defineProperty(exports, "parsePacked", { enumerable: true, get: function () { return parse_1.parsePacked; } });
Object.defineProperty(exports, "parseTabular", { enumerable: true, get: function () { return parse_1.parseTabular; } });
Object.defineProperty(exports, "parseHeader", { enumerable: true, get: function () { return parse_1.parseHeader; } });
// Patch system
var patch_1 = require("./patch");
Object.defineProperty(exports, "PatchBuilder", { enumerable: true, get: function () { return patch_1.PatchBuilder; } });
Object.defineProperty(exports, "emitPatch", { enumerable: true, get: function () { return patch_1.emitPatch; } });
Object.defineProperty(exports, "parsePatch", { enumerable: true, get: function () { return patch_1.parsePatch; } });
Object.defineProperty(exports, "applyPatch", { enumerable: true, get: function () { return patch_1.applyPatch; } });
Object.defineProperty(exports, "parsePathToSegs", { enumerable: true, get: function () { return patch_1.parsePathToSegs; } });
Object.defineProperty(exports, "fieldSeg", { enumerable: true, get: function () { return patch_1.fieldSeg; } });
Object.defineProperty(exports, "listIdxSeg", { enumerable: true, get: function () { return patch_1.listIdxSeg; } });
Object.defineProperty(exports, "mapKeySeg", { enumerable: true, get: function () { return patch_1.mapKeySeg; } });
// Loose mode (schema-optional)
var loose_1 = require("./loose");
Object.defineProperty(exports, "canonicalizeLoose", { enumerable: true, get: function () { return loose_1.canonicalizeLoose; } });
Object.defineProperty(exports, "canonicalizeLooseNoTabular", { enumerable: true, get: function () { return loose_1.canonicalizeLooseNoTabular; } });
Object.defineProperty(exports, "canonicalizeLooseWithOpts", { enumerable: true, get: function () { return loose_1.canonicalizeLooseWithOpts; } });
Object.defineProperty(exports, "canonicalizeLooseTabular", { enumerable: true, get: function () { return loose_1.canonicalizeLooseTabular; } });
Object.defineProperty(exports, "fingerprintLoose", { enumerable: true, get: function () { return loose_1.fingerprintLoose; } });
Object.defineProperty(exports, "equalLoose", { enumerable: true, get: function () { return loose_1.equalLoose; } });
Object.defineProperty(exports, "fromJsonLoose", { enumerable: true, get: function () { return loose_1.fromJsonLoose; } });
Object.defineProperty(exports, "toJsonLoose", { enumerable: true, get: function () { return loose_1.toJsonLoose; } });
Object.defineProperty(exports, "parseJsonLoose", { enumerable: true, get: function () { return loose_1.parseJsonLoose; } });
Object.defineProperty(exports, "stringifyJsonLoose", { enumerable: true, get: function () { return loose_1.stringifyJsonLoose; } });
Object.defineProperty(exports, "jsonEqual", { enumerable: true, get: function () { return loose_1.jsonEqual; } });
Object.defineProperty(exports, "parseTabularLoose", { enumerable: true, get: function () { return loose_1.parseTabularLoose; } });
Object.defineProperty(exports, "unescapeTabularCell", { enumerable: true, get: function () { return loose_1.unescapeTabularCell; } });
Object.defineProperty(exports, "defaultLooseCanonOpts", { enumerable: true, get: function () { return loose_1.defaultLooseCanonOpts; } });
Object.defineProperty(exports, "noTabularLooseCanonOpts", { enumerable: true, get: function () { return loose_1.noTabularLooseCanonOpts; } });
Object.defineProperty(exports, "tabularLooseCanonOpts", { enumerable: true, get: function () { return loose_1.tabularLooseCanonOpts; } });
// v2.4.0: LLM mode, schema headers, compact keys
Object.defineProperty(exports, "llmLooseCanonOpts", { enumerable: true, get: function () { return loose_1.llmLooseCanonOpts; } });
Object.defineProperty(exports, "canonicalizeLooseWithSchema", { enumerable: true, get: function () { return loose_1.canonicalizeLooseWithSchema; } });
Object.defineProperty(exports, "buildKeyDictFromValue", { enumerable: true, get: function () { return loose_1.buildKeyDictFromValue; } });
Object.defineProperty(exports, "parseSchemaHeader", { enumerable: true, get: function () { return loose_1.parseSchemaHeader; } });
Object.defineProperty(exports, "parseTabularLooseHeaderWithMeta", { enumerable: true, get: function () { return loose_1.parseTabularLooseHeaderWithMeta; } });
// GS1 Stream (streaming transport)
exports.stream = __importStar(require("./stream/index"));
// Decimal128 - High-precision decimal type
var decimal128_1 = require("./decimal128");
Object.defineProperty(exports, "Decimal128", { enumerable: true, get: function () { return decimal128_1.Decimal128; } });
Object.defineProperty(exports, "DecimalError", { enumerable: true, get: function () { return decimal128_1.DecimalError; } });
Object.defineProperty(exports, "decimal", { enumerable: true, get: function () { return decimal128_1.decimal; } });
Object.defineProperty(exports, "isDecimalLiteral", { enumerable: true, get: function () { return decimal128_1.isDecimalLiteral; } });
Object.defineProperty(exports, "parseDecimalLiteral", { enumerable: true, get: function () { return decimal128_1.parseDecimalLiteral; } });
// Schema Evolution - Safe API versioning
var schema_evolution_1 = require("./schema_evolution");
Object.defineProperty(exports, "VersionedSchema", { enumerable: true, get: function () { return schema_evolution_1.VersionedSchema; } });
Object.defineProperty(exports, "VersionSchema", { enumerable: true, get: function () { return schema_evolution_1.VersionSchema; } });
Object.defineProperty(exports, "EvolvingField", { enumerable: true, get: function () { return schema_evolution_1.EvolvingField; } });
Object.defineProperty(exports, "EvolutionMode", { enumerable: true, get: function () { return schema_evolution_1.EvolutionMode; } });
Object.defineProperty(exports, "compareVersions", { enumerable: true, get: function () { return schema_evolution_1.compareVersions; } });
Object.defineProperty(exports, "parseVersionHeader", { enumerable: true, get: function () { return schema_evolution_1.parseVersionHeader; } });
Object.defineProperty(exports, "formatVersionHeader", { enumerable: true, get: function () { return schema_evolution_1.formatVersionHeader; } });
Object.defineProperty(exports, "versionedSchema", { enumerable: true, get: function () { return schema_evolution_1.versionedSchema; } });
// Streaming Validator - Incremental validation
var stream_validator_1 = require("./stream_validator");
Object.defineProperty(exports, "StreamingValidator", { enumerable: true, get: function () { return stream_validator_1.StreamingValidator; } });
Object.defineProperty(exports, "ToolRegistry", { enumerable: true, get: function () { return stream_validator_1.ToolRegistry; } });
Object.defineProperty(exports, "ValidatorState", { enumerable: true, get: function () { return stream_validator_1.ValidatorState; } });
Object.defineProperty(exports, "ErrorCode", { enumerable: true, get: function () { return stream_validator_1.ErrorCode; } });
Object.defineProperty(exports, "defaultToolRegistry", { enumerable: true, get: function () { return stream_validator_1.defaultToolRegistry; } });
// ============================================================
// Convenience: Convert JSON directly to LYPH
// ============================================================
const json_2 = require("./json");
const emit_2 = require("./emit");
/**
 * Convert JSON directly to packed LYPH format
 */
function jsonToPacked(json, schema, options = {}) {
    const gv = (0, json_2.fromJson)(json, { ...options, schema });
    return (0, emit_2.emitPacked)(gv, schema);
}
/**
 * Convert JSON directly to tabular LYPH format
 */
function jsonToTabular(json, schema, options = {}) {
    const gv = (0, json_2.fromJson)(json, { ...options, schema });
    return (0, emit_2.emitTabular)(gv, schema);
}
/**
 * Convert JSON directly to LYPH v2 with auto mode selection
 */
function jsonToLyph(json, schema, options = {}) {
    const gv = (0, json_2.fromJson)(json, { ...options, schema });
    return (0, emit_2.emitV2)(gv, schema, options);
}
// ============================================================
// Token Counting Utilities
// ============================================================
/**
 * Estimate token count for a string (simple whitespace-based estimate)
 */
function estimateTokens(s) {
    return s.split(/\s+/).filter(Boolean).length;
}
/**
 * Compare token counts between JSON and LYPH representations
 */
function compareTokens(json, schema, options = {}) {
    const jsonStr = JSON.stringify(json);
    const lyphStr = jsonToLyph(json, schema, options);
    const jsonTokens = estimateTokens(jsonStr);
    const lyphTokens = estimateTokens(lyphStr);
    const savings = jsonTokens - lyphTokens;
    const savingsPercent = jsonTokens > 0 ? (savings / jsonTokens) * 100 : 0;
    return { json: jsonTokens, lyph: lyphTokens, savings, savingsPercent };
}
//# sourceMappingURL=index.js.map