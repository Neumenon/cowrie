/**
 * GLYPH-Loose Mode
 *
 * Schema-optional canonicalization for GLYPH values.
 * Provides deterministic string representation for hashing, comparison, and deduplication.
 *
 * Canonical rules:
 * - null → "∅"
 * - bool → "t" / "f"
 * - int → decimal, no leading zeros, -0 → 0
 * - float → shortest roundtrip, E→e, -0→0
 * - string → bare if safe, otherwise quoted
 * - bytes → "b64" + quoted base64
 * - time → ISO-8601 UTC
 * - id → ^prefix:value or ^"quoted"
 * - list → "[" + space-separated elements + "]"
 * - map → "{" + sorted key=value pairs + "}"
 *   Keys sorted by bytewise UTF-8 of canonString(key)
 *
 * Auto-Tabular (v2.3.0):
 * - Homogeneous lists of objects can be emitted as @tab _ [cols]...|row|...@end
 * - Opt-in via LooseCanonOpts.autoTabular
 */
import { GValue } from './types';
/**
 * NullStyle controls how null values are emitted.
 */
export type NullStyle = 'symbol' | 'underscore';
/**
 * Options for loose canonicalization with auto-tabular support.
 */
export interface LooseCanonOpts {
    /**
     * Enable auto-tabular mode for homogeneous lists of objects.
     * When true, lists of 3+ maps/structs with shared keys are emitted as @tab _ blocks.
     */
    autoTabular?: boolean;
    /**
     * Minimum number of rows to trigger tabular output (default: 3).
     */
    minRows?: number;
    /**
     * Maximum number of columns to allow in tabular output (default: 20).
     */
    maxCols?: number;
    /**
     * Allow missing keys in rows (emit ∅ for missing values) (default: true).
     */
    allowMissing?: boolean;
    /**
     * How to emit null values.
     * 'symbol' = ∅ (default, human-readable)
     * 'underscore' = _ (LLM-friendly, ASCII-safe)
     */
    nullStyle?: NullStyle;
    /**
     * Optional schema hash/id for @schema header.
     */
    schemaRef?: string;
    /**
     * Optional key dictionary for compact keys.
     */
    keyDict?: string[];
    /**
     * Emit #N instead of field names when keyDict is set.
     */
    useCompactKeys?: boolean;
}
/**
 * Default options for loose canonicalization with smart auto-tabular ENABLED.
 * Lists of 3+ homogeneous objects are automatically emitted as @tab blocks.
 * Non-eligible data gracefully falls back to standard format.
 * Uses _ for null (ASCII-safe, LLM-friendly - matches Go/Python).
 */
export declare function defaultLooseCanonOpts(): LooseCanonOpts;
/**
 * Options optimized for LLM output.
 * Uses _ for null (ASCII-safe, single token), auto-tabular enabled.
 */
export declare function llmLooseCanonOpts(): LooseCanonOpts;
/**
 * Options with auto-tabular DISABLED.
 * Use for backward compatibility or when tabular format is not desired.
 */
export declare function noTabularLooseCanonOpts(): LooseCanonOpts;
/**
 * Options for human-readable "pretty" output.
 * Uses ∅ for null (unicode symbol) for nicer visual appearance.
 */
export declare function prettyLooseCanonOpts(): LooseCanonOpts;
/**
 * Options preset for tabular-enabled canonicalization.
 * @deprecated auto-tabular is now the default.
 */
export declare function tabularLooseCanonOpts(): LooseCanonOpts;
/**
 * Returns a deterministic canonical string for any GValue.
 * This function produces identical output for semantically equal values,
 * making it suitable for hashing, comparison, and deduplication.
 *
 * Smart auto-tabular is ENABLED by default (v2.3.0+):
 * - Lists of 3+ homogeneous objects → @tab blocks (35-65% token savings)
 * - All other data → standard format (unchanged)
 *
 * Use canonicalizeLooseNoTabular for backward-compatible output.
 */
export declare function canonicalizeLoose(v: GValue): string;
/**
 * Returns canonical form WITHOUT auto-tabular.
 * Use for v2.2.x backward compatibility or when tabular format is not desired.
 */
export declare function canonicalizeLooseNoTabular(v: GValue): string;
/**
 * Canonicalize with options (including auto-tabular support).
 */
export declare function canonicalizeLooseWithOpts(v: GValue, opts: LooseCanonOpts): string;
/**
 * Convenience function: canonicalize with auto-tabular enabled.
 * @deprecated auto-tabular is now the default. Use canonicalizeLoose instead.
 */
export declare function canonicalizeLooseTabular(v: GValue): string;
/**
 * Unescape pipe characters in a tabular cell.
 */
export declare function unescapeTabularCell(s: string): string;
/**
 * Result of parsing a tabular block.
 */
export interface TabularParseResult {
    columns: string[];
    rows: Array<Record<string, unknown>>;
}
/**
 * Parse a @tab _ block into a list of maps.
 * Input format:
 *   @tab _ [col1 col2 col3]
 *   |val1|val2|val3|
 *   |val4|val5|val6|
 *   @end
 */
export declare function parseTabularLoose(input: string): TabularParseResult;
/**
 * Tabular metadata from header parsing.
 */
export interface TabularMetadata {
    rows: number;
    cols: number;
    keys: string[];
}
/**
 * Parse header with full metadata.
 */
export declare function parseTabularLooseHeaderWithMeta(line: string): TabularMetadata;
/**
 * Returns a deterministic fingerprint string for a GValue.
 * Useful for caching, deduplication, and equality checks.
 */
export declare function fingerprintLoose(v: GValue): string;
/**
 * Checks if two GValues are semantically equal using loose canonicalization.
 */
export declare function equalLoose(a: GValue, b: GValue): boolean;
/**
 * Returns canonical form with schema header.
 * If opts.keyDict is set and opts.useCompactKeys is true, keys are emitted as #N.
 * If opts.schemaRef is set, a @schema header is prepended.
 */
export declare function canonicalizeLooseWithSchema(v: GValue, opts: LooseCanonOpts): string;
/**
 * Extracts all unique keys from a value.
 * Useful for auto-generating a key dictionary for repeated objects.
 */
export declare function buildKeyDictFromValue(v: GValue): string[];
/**
 * Parse result from a schema header.
 */
export interface SchemaHeaderResult {
    schemaRef: string;
    keyDict: string[];
}
/**
 * Parses a @schema header line.
 * Returns schemaRef and keyDict, or throws on error.
 */
export declare function parseSchemaHeader(line: string): SchemaHeaderResult;
export interface BridgeOpts {
    /**
     * Extended enables $glyph markers for lossless round-trip of time/id/bytes.
     * When false (default), these types are converted to plain strings.
     */
    extended?: boolean;
}
/**
 * Convert JSON value to GValue using loose mode.
 * Rejects NaN and Infinity for JSON compatibility.
 */
export declare function fromJsonLoose(json: unknown, opts?: BridgeOpts): GValue;
/**
 * Convert GValue to JSON-compatible value using loose mode.
 * Rejects NaN and Infinity.
 */
export declare function toJsonLoose(gv: GValue, opts?: BridgeOpts): unknown;
/**
 * Parse JSON string to GValue using loose mode.
 */
export declare function parseJsonLoose(jsonStr: string, opts?: BridgeOpts): GValue;
/**
 * Stringify GValue to JSON string using loose mode.
 */
export declare function stringifyJsonLoose(gv: GValue, opts?: BridgeOpts, indent?: number): string;
/**
 * Check if two JSON byte arrays represent equal values.
 */
export declare function jsonEqual(a: string, b: string): boolean;
//# sourceMappingURL=loose.d.ts.map