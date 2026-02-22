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
export { GValue, GType, RefID, MapEntry, StructValue, SumValue, g, field, } from './types';
export { Schema, SchemaBuilder, TypeDef, FieldDef, TypeSpec, TypeSpecKind, Constraint, VariantDef, t, } from './schema';
export { fromJson, toJson, parseJson, stringifyJson, normalizeJson, FromJsonOptions, ToJsonOptions, } from './json';
export { emit, emitPacked, emitTabular, emitV2, emitHeader, EmitOptions, PackedOptions, TabularOptions, HeaderOptions, V2Options, KeyMode, } from './emit';
export { parsePacked, parseTabular, parseHeader, ParseOptions, Header, TabularParseResult, } from './parse';
export { Patch, PatchOp, PatchOpKind, PathSeg, PathSegKind, PatchBuilder, PatchEmitOptions, emitPatch, parsePatch, applyPatch, parsePathToSegs, fieldSeg, listIdxSeg, mapKeySeg, } from './patch';
export { canonicalizeLoose, canonicalizeLooseNoTabular, canonicalizeLooseWithOpts, canonicalizeLooseTabular, fingerprintLoose, equalLoose, fromJsonLoose, toJsonLoose, parseJsonLoose, stringifyJsonLoose, jsonEqual, parseTabularLoose, unescapeTabularCell, BridgeOpts, LooseCanonOpts, TabularParseResult as LooseTabularParseResult, defaultLooseCanonOpts, noTabularLooseCanonOpts, tabularLooseCanonOpts, llmLooseCanonOpts, canonicalizeLooseWithSchema, buildKeyDictFromValue, parseSchemaHeader, parseTabularLooseHeaderWithMeta, NullStyle, SchemaHeaderResult, TabularMetadata, } from './loose';
export * as stream from './stream/index';
export { Decimal128, DecimalError, decimal, isDecimalLiteral, parseDecimalLiteral, } from './decimal128';
export { VersionedSchema, VersionSchema, EvolvingField, EvolvingFieldConfig, EvolutionMode, FieldType, FieldValue as EvolutionFieldValue, ParseResult as EvolutionParseResult, EmitResult as EvolutionEmitResult, ChangelogEntry, compareVersions, parseVersionHeader, formatVersionHeader, versionedSchema, } from './schema_evolution';
export { StreamingValidator, ToolRegistry, ToolSchema, ArgSchema, ValidationResult, ValidationError, ValidatorState, ErrorCode, TimelineEvent, FieldValue as ValidatorFieldValue, defaultToolRegistry, } from './stream_validator';
import { FromJsonOptions } from './json';
import { V2Options } from './emit';
import { Schema } from './schema';
/**
 * Convert JSON directly to packed LYPH format
 */
export declare function jsonToPacked(json: unknown, schema: Schema, options?: FromJsonOptions & {
    typeName?: string;
}): string;
/**
 * Convert JSON directly to tabular LYPH format
 */
export declare function jsonToTabular(json: unknown, schema: Schema, options?: FromJsonOptions): string;
/**
 * Convert JSON directly to LYPH v2 with auto mode selection
 */
export declare function jsonToLyph(json: unknown, schema: Schema, options?: FromJsonOptions & V2Options): string;
/**
 * Estimate token count for a string (simple whitespace-based estimate)
 */
export declare function estimateTokens(s: string): number;
/**
 * Compare token counts between JSON and LYPH representations
 */
export declare function compareTokens(json: unknown, schema: Schema, options?: FromJsonOptions & V2Options): {
    json: number;
    lyph: number;
    savings: number;
    savingsPercent: number;
};
//# sourceMappingURL=index.d.ts.map