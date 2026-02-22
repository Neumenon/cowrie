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
export const VERSION = 1;

/** Frame kind indicates the semantic category of the payload */
export type FrameKind = 
  | 'doc'    // Snapshot or general GLYPH document
  | 'patch'  // GLYPH patch doc (@patch ... @end)
  | 'row'    // Single row value (streaming tabular)
  | 'ui'     // UI event (progress/log/artifact)
  | 'ack'    // Acknowledgement
  | 'err'    // Error event
  | 'ping'   // Keepalive
  | 'pong';  // Ping response

/** Kind value mapping */
export const KIND_VALUES: Record<FrameKind, number> = {
  doc: 0,
  patch: 1,
  row: 2,
  ui: 3,
  ack: 4,
  err: 5,
  ping: 6,
  pong: 7,
};

/** Reverse mapping from number to kind */
export const VALUE_KINDS: Record<number, FrameKind> = {
  0: 'doc',
  1: 'patch',
  2: 'row',
  3: 'ui',
  4: 'ack',
  5: 'err',
  6: 'ping',
  7: 'pong',
};

/** Parse kind from string or number */
export function parseKind(s: string): FrameKind | number {
  if (s in KIND_VALUES) {
    return s as FrameKind;
  }
  const n = parseInt(s, 10);
  if (!isNaN(n) && n >= 0 && n <= 255) {
    return VALUE_KINDS[n] ?? n;
  }
  throw new Error(`Invalid kind: ${s}`);
}

/** Get kind string for output */
export function kindToString(kind: FrameKind | number): string {
  if (typeof kind === 'string') {
    return kind;
  }
  return VALUE_KINDS[kind] ?? `unknown(${kind})`;
}

/** Flag bits */
export const FLAGS = {
  HAS_CRC: 0x01,
  HAS_BASE: 0x02,
  FINAL: 0x04,
  COMPRESSED: 0x08, // Reserved for GS1.1
} as const;

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
export const MAX_PAYLOAD_SIZE = 64 * 1024 * 1024;

/** GS1 parse error */
export class ParseError extends Error {
  constructor(
    public reason: string,
    public offset: number = -1
  ) {
    super(offset >= 0 ? `gs1: ${reason} at offset ${offset}` : `gs1: ${reason}`);
    this.name = 'ParseError';
  }
}

/** CRC mismatch error */
export class CRCMismatchError extends Error {
  constructor(
    public expected: number,
    public got: number
  ) {
    super(`gs1: CRC mismatch: expected ${expected.toString(16).padStart(8, '0')}, got ${got.toString(16).padStart(8, '0')}`);
    this.name = 'CRCMismatchError';
  }
}

/** Base hash mismatch error */
export class BaseMismatchError extends Error {
  constructor() {
    super('gs1: base hash mismatch');
    this.name = 'BaseMismatchError';
  }
}
