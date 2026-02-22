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

// Core types
export { 
  GValue, 
  GType, 
  RefID, 
  MapEntry, 
  StructValue, 
  SumValue,
  g,
  field,
} from './types';

// Schema
export {
  Schema,
  SchemaBuilder,
  TypeDef,
  FieldDef,
  TypeSpec,
  TypeSpecKind,
  Constraint,
  VariantDef,
  t,
} from './schema';

// JSON conversion
export {
  fromJson,
  toJson,
  parseJson,
  stringifyJson,
  normalizeJson,
  FromJsonOptions,
  ToJsonOptions,
} from './json';

// Emitters
export {
  emit,
  emitPacked,
  emitTabular,
  emitV2,
  emitHeader,
  EmitOptions,
  PackedOptions,
  TabularOptions,
  HeaderOptions,
  V2Options,
  KeyMode,
} from './emit';

// Parsers
export {
  parsePacked,
  parseTabular,
  parseHeader,
  ParseOptions,
  Header,
  TabularParseResult,
} from './parse';

// Patch system
export {
  Patch,
  PatchOp,
  PatchOpKind,
  PathSeg,
  PathSegKind,
  PatchBuilder,
  PatchEmitOptions,
  emitPatch,
  parsePatch,
  applyPatch,
  parsePathToSegs,
  fieldSeg,
  listIdxSeg,
  mapKeySeg,
} from './patch';

// Loose mode (schema-optional)
export {
  canonicalizeLoose,
  canonicalizeLooseNoTabular,
  canonicalizeLooseWithOpts,
  canonicalizeLooseTabular,
  fingerprintLoose,
  equalLoose,
  fromJsonLoose,
  toJsonLoose,
  parseJsonLoose,
  stringifyJsonLoose,
  jsonEqual,
  parseTabularLoose,
  unescapeTabularCell,
  BridgeOpts,
  LooseCanonOpts,
  TabularParseResult as LooseTabularParseResult,
  defaultLooseCanonOpts,
  noTabularLooseCanonOpts,
  tabularLooseCanonOpts,
  // v2.4.0: LLM mode, schema headers, compact keys
  llmLooseCanonOpts,
  canonicalizeLooseWithSchema,
  buildKeyDictFromValue,
  parseSchemaHeader,
  parseTabularLooseHeaderWithMeta,
  NullStyle,
  SchemaHeaderResult,
  TabularMetadata,
} from './loose';

// GS1 Stream (streaming transport)
export * as stream from './stream/index';

// Decimal128 - High-precision decimal type
export {
  Decimal128,
  DecimalError,
  decimal,
  isDecimalLiteral,
  parseDecimalLiteral,
} from './decimal128';

// Schema Evolution - Safe API versioning
export {
  VersionedSchema,
  VersionSchema,
  EvolvingField,
  EvolvingFieldConfig,
  EvolutionMode,
  FieldType,
  FieldValue as EvolutionFieldValue,
  ParseResult as EvolutionParseResult,
  EmitResult as EvolutionEmitResult,
  ChangelogEntry,
  compareVersions,
  parseVersionHeader,
  formatVersionHeader,
  versionedSchema,
} from './schema_evolution';

// Streaming Validator - Incremental validation
export {
  StreamingValidator,
  ToolRegistry,
  ToolSchema,
  ArgSchema,
  ValidationResult,
  ValidationError,
  ValidatorState,
  ErrorCode,
  TimelineEvent,
  FieldValue as ValidatorFieldValue,
  ValidatorLimits,
  DEFAULT_MAX_BUFFER,
  DEFAULT_MAX_FIELDS,
  DEFAULT_MAX_ERRORS,
  defaultToolRegistry,
} from './stream_validator';

// ============================================================
// Convenience: Convert JSON directly to LYPH
// ============================================================

import { fromJson, FromJsonOptions } from './json';
import { emitPacked, emitTabular, emitV2, V2Options } from './emit';
import { Schema } from './schema';
import { GValue } from './types';

/**
 * Convert JSON directly to packed LYPH format
 */
export function jsonToPacked(
  json: unknown, 
  schema: Schema, 
  options: FromJsonOptions & { typeName?: string } = {}
): string {
  const gv = fromJson(json, { ...options, schema });
  return emitPacked(gv, schema);
}

/**
 * Convert JSON directly to tabular LYPH format
 */
export function jsonToTabular(
  json: unknown,
  schema: Schema,
  options: FromJsonOptions = {}
): string {
  const gv = fromJson(json, { ...options, schema });
  return emitTabular(gv, schema);
}

/**
 * Convert JSON directly to LYPH v2 with auto mode selection
 */
export function jsonToLyph(
  json: unknown,
  schema: Schema,
  options: FromJsonOptions & V2Options = {}
): string {
  const gv = fromJson(json, { ...options, schema });
  return emitV2(gv, schema, options);
}

// ============================================================
// Token Counting Utilities
// ============================================================

/**
 * Estimate token count for a string (simple whitespace-based estimate)
 */
export function estimateTokens(s: string): number {
  return s.split(/\s+/).filter(Boolean).length;
}

/**
 * Compare token counts between JSON and LYPH representations
 */
export function compareTokens(
  json: unknown,
  schema: Schema,
  options: FromJsonOptions & V2Options = {}
): { json: number; lyph: number; savings: number; savingsPercent: number } {
  const jsonStr = JSON.stringify(json);
  const lyphStr = jsonToLyph(json, schema, options);
  
  const jsonTokens = estimateTokens(jsonStr);
  const lyphTokens = estimateTokens(lyphStr);
  const savings = jsonTokens - lyphTokens;
  const savingsPercent = jsonTokens > 0 ? (savings / jsonTokens) * 100 : 0;
  
  return { json: jsonTokens, lyph: lyphTokens, savings, savingsPercent };
}
