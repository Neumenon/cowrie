"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.BaseMismatchError = exports.CRCMismatchError = exports.ParseError = exports.MAX_PAYLOAD_SIZE = exports.FLAGS = exports.VALUE_KINDS = exports.KIND_VALUES = exports.VERSION = void 0;
exports.parseKind = parseKind;
exports.kindToString = kindToString;
/** Protocol version */
exports.VERSION = 1;
/** Kind value mapping */
exports.KIND_VALUES = {
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
exports.VALUE_KINDS = {
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
function parseKind(s) {
    if (s in exports.KIND_VALUES) {
        return s;
    }
    const n = parseInt(s, 10);
    if (!isNaN(n) && n >= 0 && n <= 255) {
        return exports.VALUE_KINDS[n] ?? n;
    }
    throw new Error(`Invalid kind: ${s}`);
}
/** Get kind string for output */
function kindToString(kind) {
    if (typeof kind === 'string') {
        return kind;
    }
    return exports.VALUE_KINDS[kind] ?? `unknown(${kind})`;
}
/** Flag bits */
exports.FLAGS = {
    HAS_CRC: 0x01,
    HAS_BASE: 0x02,
    FINAL: 0x04,
    COMPRESSED: 0x08, // Reserved for GS1.1
};
/** Maximum payload size (64 MiB) */
exports.MAX_PAYLOAD_SIZE = 64 * 1024 * 1024;
/** GS1 parse error */
class ParseError extends Error {
    constructor(reason, offset = -1) {
        super(offset >= 0 ? `gs1: ${reason} at offset ${offset}` : `gs1: ${reason}`);
        this.reason = reason;
        this.offset = offset;
        this.name = 'ParseError';
    }
}
exports.ParseError = ParseError;
/** CRC mismatch error */
class CRCMismatchError extends Error {
    constructor(expected, got) {
        super(`gs1: CRC mismatch: expected ${expected.toString(16).padStart(8, '0')}, got ${got.toString(16).padStart(8, '0')}`);
        this.expected = expected;
        this.got = got;
        this.name = 'CRCMismatchError';
    }
}
exports.CRCMismatchError = CRCMismatchError;
/** Base hash mismatch error */
class BaseMismatchError extends Error {
    constructor() {
        super('gs1: base hash mismatch');
        this.name = 'BaseMismatchError';
    }
}
exports.BaseMismatchError = BaseMismatchError;
//# sourceMappingURL=types.js.map