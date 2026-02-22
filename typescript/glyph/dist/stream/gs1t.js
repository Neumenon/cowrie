"use strict";
/**
 * GS1-T (Text framing) reader and writer.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.Reader = void 0;
exports.encodeFrame = encodeFrame;
exports.encodeFrames = encodeFrames;
exports.decodeFrames = decodeFrames;
exports.decodeFrame = decodeFrame;
exports.docFrame = docFrame;
exports.patchFrame = patchFrame;
exports.rowFrame = rowFrame;
exports.uiFrame = uiFrame;
exports.ackFrame = ackFrame;
exports.errFrame = errFrame;
exports.pingFrame = pingFrame;
exports.pongFrame = pongFrame;
const types_1 = require("./types");
const crc_1 = require("./crc");
const hash_1 = require("./hash");
const encoder = new TextEncoder();
const decoder = new TextDecoder();
/**
 * Encode a frame as GS1-T bytes.
 *
 * Format:
 *   @frame{v=1 sid=N seq=N kind=K len=N [crc=X] [base=sha256:X] [final=true]}\n
 *   <payload bytes>\n
 */
function encodeFrame(frame, options = {}) {
    const parts = [];
    // Required fields
    parts.push(`v=${frame.version || types_1.VERSION}`);
    parts.push(`sid=${frame.sid}`);
    parts.push(`seq=${frame.seq}`);
    parts.push(`kind=${(0, types_1.kindToString)(frame.kind)}`);
    parts.push(`len=${frame.payload.length}`);
    // Optional CRC
    let crc = frame.crc;
    if (crc === undefined && options.withCRC && frame.payload.length > 0) {
        crc = (0, crc_1.computeCRC)(frame.payload);
    }
    if (crc !== undefined) {
        parts.push(`crc=${(0, crc_1.crcToHex)(crc)}`);
    }
    // Optional base hash
    if (frame.base) {
        parts.push(`base=sha256:${(0, hash_1.hashToHex)(frame.base)}`);
    }
    // Optional final flag
    if (frame.final) {
        parts.push('final=true');
    }
    const header = `@frame{${parts.join(' ')}}\n`;
    const headerBytes = encoder.encode(header);
    // Combine: header + payload + newline
    const result = new Uint8Array(headerBytes.length + frame.payload.length + 1);
    result.set(headerBytes, 0);
    result.set(frame.payload, headerBytes.length);
    result[result.length - 1] = 0x0a; // newline
    return result;
}
/**
 * Encode multiple frames.
 */
function encodeFrames(frames, options = {}) {
    const encoded = frames.map(f => encodeFrame(f, options));
    const totalLength = encoded.reduce((sum, arr) => sum + arr.length, 0);
    const result = new Uint8Array(totalLength);
    let offset = 0;
    for (const arr of encoded) {
        result.set(arr, offset);
        offset += arr.length;
    }
    return result;
}
/**
 * GS1-T stream reader.
 * Incrementally reads frames from a byte buffer.
 */
class Reader {
    constructor(options = {}) {
        this.buffer = new Uint8Array(0);
        this.offset = 0;
        this.maxPayload = options.maxPayload ?? types_1.MAX_PAYLOAD_SIZE;
        this.verifyCRC = options.verifyCRC ?? true;
    }
    /**
     * Add data to the internal buffer.
     */
    push(data) {
        if (this.offset > 0) {
            // Compact buffer
            this.buffer = this.buffer.slice(this.offset);
            this.offset = 0;
        }
        const newBuffer = new Uint8Array(this.buffer.length + data.length);
        newBuffer.set(this.buffer, 0);
        newBuffer.set(data, this.buffer.length);
        this.buffer = newBuffer;
    }
    /**
     * Try to read the next frame.
     * Returns null if not enough data is available.
     * Throws ParseError or CRCMismatchError on errors.
     */
    next() {
        // Find header line ending
        const headerEnd = this.findNewline(this.offset);
        if (headerEnd < 0) {
            return null; // Need more data
        }
        const headerLine = decoder.decode(this.buffer.slice(this.offset, headerEnd));
        // Parse header
        const header = this.parseHeader(headerLine);
        if (header.payloadLen > this.maxPayload) {
            throw new types_1.ParseError(`payload too large: ${header.payloadLen} > ${this.maxPayload}`);
        }
        // Check if we have enough data for payload + trailing newline
        const payloadStart = headerEnd + 1;
        const frameEnd = payloadStart + header.payloadLen + 1;
        if (this.buffer.length < payloadStart + header.payloadLen) {
            return null; // Need more data
        }
        // Extract payload
        const payload = this.buffer.slice(payloadStart, payloadStart + header.payloadLen);
        // Advance offset (consume trailing newline if present)
        if (this.buffer.length > payloadStart + header.payloadLen &&
            this.buffer[payloadStart + header.payloadLen] === 0x0a) {
            this.offset = payloadStart + header.payloadLen + 1;
        }
        else {
            this.offset = payloadStart + header.payloadLen;
        }
        // Verify CRC if present
        if (this.verifyCRC && header.crc !== undefined) {
            const computed = (0, crc_1.computeCRC)(payload);
            if (computed !== header.crc) {
                throw new types_1.CRCMismatchError(header.crc, computed);
            }
        }
        return {
            version: header.version,
            sid: header.sid,
            seq: header.seq,
            kind: header.kind,
            payload,
            crc: header.crc,
            base: header.base,
            flags: header.flags,
            final: header.final,
        };
    }
    /**
     * Read all available frames.
     */
    readAll() {
        const frames = [];
        let frame;
        while ((frame = this.next()) !== null) {
            frames.push(frame);
        }
        return frames;
    }
    findNewline(start) {
        for (let i = start; i < this.buffer.length; i++) {
            if (this.buffer[i] === 0x0a) {
                return i;
            }
        }
        return -1;
    }
    parseHeader(line) {
        line = line.trim();
        if (!line.startsWith('@frame{')) {
            throw new types_1.ParseError('expected @frame{', 0);
        }
        const endIdx = line.lastIndexOf('}');
        if (endIdx < 0) {
            throw new types_1.ParseError('missing closing }');
        }
        const content = line.slice(7, endIdx);
        const pairs = this.tokenize(content);
        const header = {
            version: 1,
            sid: 0n,
            seq: 0n,
            kind: 'doc',
            payloadLen: 0,
        };
        for (const pair of pairs) {
            const eqIdx = pair.indexOf('=');
            if (eqIdx < 0)
                continue;
            const key = pair.slice(0, eqIdx);
            const val = pair.slice(eqIdx + 1);
            switch (key) {
                case 'v':
                    header.version = parseInt(val, 10);
                    break;
                case 'sid':
                    header.sid = BigInt(val);
                    break;
                case 'seq':
                    header.seq = BigInt(val);
                    break;
                case 'kind':
                    header.kind = (0, types_1.parseKind)(val);
                    break;
                case 'len':
                    header.payloadLen = parseInt(val, 10);
                    break;
                case 'crc':
                    header.crc = (0, crc_1.parseCRC)(val) ?? undefined;
                    break;
                case 'base':
                    header.base = (0, hash_1.hexToHash)(val) ?? undefined;
                    break;
                case 'final':
                    header.final = val === 'true' || val === '1';
                    break;
                case 'flags':
                    header.flags = parseInt(val, 16);
                    break;
            }
        }
        return header;
    }
    tokenize(s) {
        const tokens = [];
        let current = '';
        let inQuote = false;
        for (let i = 0; i < s.length; i++) {
            const c = s[i];
            if (c === '"') {
                inQuote = !inQuote;
                current += c;
            }
            else if ((c === ' ' || c === ',' || c === '\t') && !inQuote) {
                if (current.length > 0) {
                    tokens.push(current);
                    current = '';
                }
            }
            else {
                current += c;
            }
        }
        if (current.length > 0) {
            tokens.push(current);
        }
        return tokens;
    }
}
exports.Reader = Reader;
// ============================================================
// Convenience functions
// ============================================================
/**
 * Decode frames from a complete byte buffer.
 */
function decodeFrames(data, options) {
    const reader = new Reader(options);
    reader.push(data);
    return reader.readAll();
}
/**
 * Decode a single frame from bytes.
 */
function decodeFrame(data, options) {
    const reader = new Reader(options);
    reader.push(data);
    return reader.next();
}
// ============================================================
// Helper frame constructors
// ============================================================
/**
 * Create a doc frame.
 */
function docFrame(sid, seq, payload) {
    return {
        version: types_1.VERSION,
        sid,
        seq,
        kind: 'doc',
        payload: typeof payload === 'string' ? encoder.encode(payload) : payload,
    };
}
/**
 * Create a patch frame.
 */
function patchFrame(sid, seq, payload, base) {
    return {
        version: types_1.VERSION,
        sid,
        seq,
        kind: 'patch',
        payload: typeof payload === 'string' ? encoder.encode(payload) : payload,
        base,
    };
}
/**
 * Create a row frame.
 */
function rowFrame(sid, seq, payload) {
    return {
        version: types_1.VERSION,
        sid,
        seq,
        kind: 'row',
        payload: typeof payload === 'string' ? encoder.encode(payload) : payload,
    };
}
/**
 * Create a UI event frame.
 */
function uiFrame(sid, seq, payload) {
    return {
        version: types_1.VERSION,
        sid,
        seq,
        kind: 'ui',
        payload: typeof payload === 'string' ? encoder.encode(payload) : payload,
    };
}
/**
 * Create an ack frame.
 */
function ackFrame(sid, seq) {
    return {
        version: types_1.VERSION,
        sid,
        seq,
        kind: 'ack',
        payload: new Uint8Array(0),
    };
}
/**
 * Create an error frame.
 */
function errFrame(sid, seq, payload) {
    return {
        version: types_1.VERSION,
        sid,
        seq,
        kind: 'err',
        payload: typeof payload === 'string' ? encoder.encode(payload) : payload,
    };
}
/**
 * Create a ping frame.
 */
function pingFrame(sid, seq) {
    return {
        version: types_1.VERSION,
        sid,
        seq,
        kind: 'ping',
        payload: new Uint8Array(0),
    };
}
/**
 * Create a pong frame.
 */
function pongFrame(sid, seq) {
    return {
        version: types_1.VERSION,
        sid,
        seq,
        kind: 'pong',
        payload: new Uint8Array(0),
    };
}
//# sourceMappingURL=gs1t.js.map