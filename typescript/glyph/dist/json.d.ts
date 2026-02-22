/**
 * LYPH v2 JSON Conversion
 *
 * Converts between JSON and GValue representations.
 */
import { GValue } from './types';
import { Schema } from './schema';
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
export declare function fromJson(json: unknown, options?: FromJsonOptions): GValue;
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
export declare function toJson(gv: GValue, options?: ToJsonOptions): unknown;
/**
 * Parse JSON string to GValue
 */
export declare function parseJson(jsonStr: string, options?: FromJsonOptions): GValue;
/**
 * Stringify GValue to JSON string
 */
export declare function stringifyJson(gv: GValue, options?: ToJsonOptions, indent?: number): string;
/**
 * Round-trip convert: JSON -> GValue -> JSON
 * Useful for normalizing JSON to LYPH conventions
 */
export declare function normalizeJson(json: unknown, fromOptions?: FromJsonOptions, toOptions?: ToJsonOptions): unknown;
//# sourceMappingURL=json.d.ts.map