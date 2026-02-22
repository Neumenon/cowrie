/**
 * LYPH v2 Parser
 *
 * Parses LYPH format back to GValue.
 */
import { GValue, RefID } from './types';
import { Schema } from './schema';
export interface ParseOptions {
    schema?: Schema;
    tolerant?: boolean;
}
export declare function parsePacked(input: string, schema: Schema): GValue;
export interface Header {
    version: string;
    schemaId?: string;
    mode?: 'auto' | 'struct' | 'packed' | 'tabular' | 'patch';
    keyMode?: 'wire' | 'name' | 'fid';
    target?: RefID;
}
export declare function parseHeader(input: string): Header | null;
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
export declare function parseTabular(input: string, schema: Schema): TabularParseResult;
//# sourceMappingURL=parse.d.ts.map