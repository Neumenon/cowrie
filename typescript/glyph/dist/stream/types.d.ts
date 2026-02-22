/**
 * GS1 (GLYPH Stream v1) Types
 *
 * GS1 is a transport envelope for GLYPH payloads, providing:
 * - Message boundaries and resync
 * - Multiplexing via stream IDs (sid)
 * - Ordering via sequence numbers (seq)
 * - Integrity via optional CRC-32
 * - Patch safety via optional state hash (base)
 *
 * GS1 headers are NOT part of GLYPH canonicalization.
 */
/** Protocol version */
export declare const VERSION = 1;
/** Frame kind indicates the semantic category of the payload */
export type FrameKind = 'doc' | 'patch' | 'row' | 'ui' | 'ack' | 'err' | 'ping' | 'pong';
/** Kind value mapping */
export declare const KIND_VALUES: Record<FrameKind, number>;
/** Reverse mapping from number to kind */
export declare const VALUE_KINDS: Record<number, FrameKind>;
/** Parse kind from string or number */
export declare function parseKind(s: string): FrameKind | number;
/** Get kind string for output */
export declare function kindToString(kind: FrameKind | number): string;
/** Flag bits */
export declare const FLAGS: {
    readonly HAS_CRC: 1;
    readonly HAS_BASE: 2;
    readonly FINAL: 4;
    readonly COMPRESSED: 8;
};
/** A GS1 frame */
export interface Frame {
    /** Protocol version (must be 1) */
    version: number;
    /** Stream identifier */
    sid: bigint;
    /** Sequence number (per-SID, monotonic) */
    seq: bigint;
    /** Frame kind */
    kind: FrameKind | number;
    /** GLYPH payload bytes (UTF-8) */
    payload: Uint8Array;
    /** CRC-32 of payload (optional) */
    crc?: number;
    /** SHA-256 state hash (optional, 32 bytes) */
    base?: Uint8Array;
    /** Flag bits */
    flags?: number;
    /** End-of-stream marker */
    final?: boolean;
}
/** Maximum payload size (64 MiB) */
export declare const MAX_PAYLOAD_SIZE: number;
/** GS1 parse error */
export declare class ParseError extends Error {
    reason: string;
    offset: number;
    constructor(reason: string, offset?: number);
}
/** CRC mismatch error */
export declare class CRCMismatchError extends Error {
    expected: number;
    got: number;
    constructor(expected: number, got: number);
}
/** Base hash mismatch error */
export declare class BaseMismatchError extends Error {
    constructor();
}
//# sourceMappingURL=types.d.ts.map