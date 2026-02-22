/**
 * LYPH v2 Encoders
 *
 * Emits LYPH format from GValue.
 */
import { GValue, RefID } from './types';
import { Schema } from './schema';
export type KeyMode = 'wire' | 'name' | 'fid';
export interface EmitOptions {
    schema?: Schema;
    keyMode?: KeyMode;
    useBitmap?: boolean;
}
export declare function emit(gv: GValue, options?: EmitOptions): string;
export interface PackedOptions extends EmitOptions {
    useBitmap?: boolean;
}
export declare function emitPacked(gv: GValue, schema: Schema, options?: PackedOptions): string;
export interface TabularOptions extends EmitOptions {
    indentPrefix?: string;
}
export declare function emitTabular(gv: GValue, schema: Schema, options?: TabularOptions): string;
export interface HeaderOptions {
    version?: string;
    schemaId?: string;
    mode?: 'auto' | 'struct' | 'packed' | 'tabular' | 'patch';
    keyMode?: KeyMode;
    target?: RefID;
}
export declare function emitHeader(options?: HeaderOptions): string;
export interface V2Options extends EmitOptions {
    mode?: 'auto' | 'struct' | 'packed' | 'tabular';
    tabThreshold?: number;
    includeHeader?: boolean;
}
export declare function emitV2(gv: GValue, schema: Schema, options?: V2Options): string;
//# sourceMappingURL=emit.d.ts.map