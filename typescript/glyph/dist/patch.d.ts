/**
 * LYPH v2 Patch System
 *
 * Implements patch emit, parse, and apply for cross-implementation parity with Go.
 */
import { GValue, RefID } from './types';
import { Schema } from './schema';
export type PatchOpKind = '=' | '+' | '-' | '~';
export type PathSegKind = 'field' | 'listIdx' | 'mapKey';
export interface PathSeg {
    kind: PathSegKind;
    field?: string;
    fid?: number;
    listIdx?: number;
    mapKey?: string;
}
export interface PatchOp {
    op: PatchOpKind;
    path: PathSeg[];
    value?: GValue;
    index?: number;
}
export interface Patch {
    target: RefID;
    schemaId?: string;
    targetType?: string;
    baseFingerprint?: string;
    ops: PatchOp[];
}
export declare function fieldSeg(name: string, fid?: number): PathSeg;
export declare function listIdxSeg(idx: number): PathSeg;
export declare function mapKeySeg(key: string): PathSeg;
/**
 * Parse a path string into segments.
 * Supports: .fieldName, .#fid, [N], ["key"]
 */
export declare function parsePathToSegs(path: string): PathSeg[];
export declare class PatchBuilder {
    private patch;
    private schema?;
    constructor(target: RefID);
    withSchema(schema: Schema): this;
    withSchemaId(id: string): this;
    withTargetType(typeName: string): this;
    /**
     * Set the base state fingerprint for validation.
     * The fingerprint should be the first 16 chars of the SHA-256 hash
     * of the canonical form of the base state.
     */
    withBaseFingerprint(fingerprint: string): this;
    /**
     * Compute and set the base fingerprint from a GValue.
     * Uses the SHA-256 hash of the loose canonical form (first 16 hex chars).
     */
    withBaseValue(base: GValue): this;
    set(path: string, value: GValue): this;
    setWithSegs(path: PathSeg[], value: GValue): this;
    append(path: string, value: GValue): this;
    delete(path: string): this;
    delta(path: string, amount: number): this;
    insertAt(path: string, index: number, value: GValue): this;
    build(): Patch;
}
export type KeyMode = 'wire' | 'name' | 'fid';
export interface PatchEmitOptions {
    schema?: Schema;
    keyMode?: KeyMode;
    sortOps?: boolean;
    indentPrefix?: string;
}
export declare function emitPatch(patch: Patch, options?: PatchEmitOptions): string;
export declare function parsePatch(input: string, schema?: Schema): Patch;
export declare function applyPatch(value: GValue, patch: Patch): GValue;
//# sourceMappingURL=patch.d.ts.map