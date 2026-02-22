/**
 * GS1-T (Text framing) reader and writer.
 */

import { 
  Frame, FrameKind, VERSION, MAX_PAYLOAD_SIZE,
  ParseError, CRCMismatchError,
  parseKind, kindToString
} from './types';
import { computeCRC, crcToHex, parseCRC } from './crc';
import { hashToHex, hexToHash } from './hash';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

// ============================================================
// Writer
// ============================================================

export interface WriterOptions {
  /** Whether to compute and include CRC */
  withCRC?: boolean;
}

/**
 * Encode a frame as GS1-T bytes.
 * 
 * Format:
 *   @frame{v=1 sid=N seq=N kind=K len=N [crc=X] [base=sha256:X] [final=true]}\n
 *   <payload bytes>\n
 */
export function encodeFrame(frame: Frame, options: WriterOptions = {}): Uint8Array {
  const parts: string[] = [];
  
  // Required fields
  parts.push(`v=${frame.version || VERSION}`);
  parts.push(`sid=${frame.sid}`);
  parts.push(`seq=${frame.seq}`);
  parts.push(`kind=${kindToString(frame.kind)}`);
  parts.push(`len=${frame.payload.length}`);
  
  // Optional CRC
  let crc = frame.crc;
  if (crc === undefined && options.withCRC && frame.payload.length > 0) {
    crc = computeCRC(frame.payload);
  }
  if (crc !== undefined) {
    parts.push(`crc=${crcToHex(crc)}`);
  }
  
  // Optional base hash
  if (frame.base) {
    parts.push(`base=sha256:${hashToHex(frame.base)}`);
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
export function encodeFrames(frames: Frame[], options: WriterOptions = {}): Uint8Array {
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

// ============================================================
// Reader
// ============================================================

export interface ReaderOptions {
  /** Maximum payload size (default: 64 MiB) */
  maxPayload?: number;
  /** Whether to verify CRC (default: true) */
  verifyCRC?: boolean;
}

/**
 * GS1-T stream reader.
 * Incrementally reads frames from a byte buffer.
 */
export class Reader {
  private buffer: Uint8Array = new Uint8Array(0);
  private offset = 0;
  private readonly maxPayload: number;
  private readonly verifyCRC: boolean;
  
  constructor(options: ReaderOptions = {}) {
    this.maxPayload = options.maxPayload ?? MAX_PAYLOAD_SIZE;
    this.verifyCRC = options.verifyCRC ?? true;
  }
  
  /**
   * Add data to the internal buffer.
   */
  push(data: Uint8Array): void {
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
  next(): Frame | null {
    // Find header line ending
    const headerEnd = this.findNewline(this.offset);
    if (headerEnd < 0) {
      return null; // Need more data
    }
    
    const headerLine = decoder.decode(this.buffer.slice(this.offset, headerEnd));
    
    // Parse header
    const header = this.parseHeader(headerLine);
    if (header.payloadLen > this.maxPayload) {
      throw new ParseError(`payload too large: ${header.payloadLen} > ${this.maxPayload}`);
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
    } else {
      this.offset = payloadStart + header.payloadLen;
    }
    
    // Verify CRC if present
    if (this.verifyCRC && header.crc !== undefined) {
      const computed = computeCRC(payload);
      if (computed !== header.crc) {
        throw new CRCMismatchError(header.crc, computed);
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
  readAll(): Frame[] {
    const frames: Frame[] = [];
    let frame: Frame | null;
    while ((frame = this.next()) !== null) {
      frames.push(frame);
    }
    return frames;
  }
  
  private findNewline(start: number): number {
    for (let i = start; i < this.buffer.length; i++) {
      if (this.buffer[i] === 0x0a) {
        return i;
      }
    }
    return -1;
  }
  
  private parseHeader(line: string): ParsedHeader {
    line = line.trim();
    
    if (!line.startsWith('@frame{')) {
      throw new ParseError('expected @frame{', 0);
    }
    
    const endIdx = line.lastIndexOf('}');
    if (endIdx < 0) {
      throw new ParseError('missing closing }');
    }
    
    const content = line.slice(7, endIdx);
    const pairs = this.tokenize(content);
    
    const header: ParsedHeader = {
      version: 1,
      sid: 0n,
      seq: 0n,
      kind: 'doc',
      payloadLen: 0,
    };
    
    for (const pair of pairs) {
      const eqIdx = pair.indexOf('=');
      if (eqIdx < 0) continue;
      
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
          header.kind = parseKind(val);
          break;
        case 'len':
          header.payloadLen = parseInt(val, 10);
          break;
        case 'crc':
          header.crc = parseCRC(val) ?? undefined;
          break;
        case 'base':
          header.base = hexToHash(val) ?? undefined;
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
  
  private tokenize(s: string): string[] {
    const tokens: string[] = [];
    let current = '';
    let inQuote = false;
    
    for (let i = 0; i < s.length; i++) {
      const c = s[i];
      if (c === '"') {
        inQuote = !inQuote;
        current += c;
      } else if ((c === ' ' || c === ',' || c === '\t') && !inQuote) {
        if (current.length > 0) {
          tokens.push(current);
          current = '';
        }
      } else {
        current += c;
      }
    }
    if (current.length > 0) {
      tokens.push(current);
    }
    return tokens;
  }
}

interface ParsedHeader {
  version: number;
  sid: bigint;
  seq: bigint;
  kind: FrameKind | number;
  payloadLen: number;
  crc?: number;
  base?: Uint8Array;
  flags?: number;
  final?: boolean;
}

// ============================================================
// Convenience functions
// ============================================================

/**
 * Decode frames from a complete byte buffer.
 */
export function decodeFrames(data: Uint8Array, options?: ReaderOptions): Frame[] {
  const reader = new Reader(options);
  reader.push(data);
  return reader.readAll();
}

/**
 * Decode a single frame from bytes.
 */
export function decodeFrame(data: Uint8Array, options?: ReaderOptions): Frame | null {
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
export function docFrame(sid: bigint, seq: bigint, payload: Uint8Array | string): Frame {
  return {
    version: VERSION,
    sid,
    seq,
    kind: 'doc',
    payload: typeof payload === 'string' ? encoder.encode(payload) : payload,
  };
}

/**
 * Create a patch frame.
 */
export function patchFrame(
  sid: bigint, 
  seq: bigint, 
  payload: Uint8Array | string,
  base?: Uint8Array
): Frame {
  return {
    version: VERSION,
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
export function rowFrame(sid: bigint, seq: bigint, payload: Uint8Array | string): Frame {
  return {
    version: VERSION,
    sid,
    seq,
    kind: 'row',
    payload: typeof payload === 'string' ? encoder.encode(payload) : payload,
  };
}

/**
 * Create a UI event frame.
 */
export function uiFrame(sid: bigint, seq: bigint, payload: Uint8Array | string): Frame {
  return {
    version: VERSION,
    sid,
    seq,
    kind: 'ui',
    payload: typeof payload === 'string' ? encoder.encode(payload) : payload,
  };
}

/**
 * Create an ack frame.
 */
export function ackFrame(sid: bigint, seq: bigint): Frame {
  return {
    version: VERSION,
    sid,
    seq,
    kind: 'ack',
    payload: new Uint8Array(0),
  };
}

/**
 * Create an error frame.
 */
export function errFrame(sid: bigint, seq: bigint, payload: Uint8Array | string): Frame {
  return {
    version: VERSION,
    sid,
    seq,
    kind: 'err',
    payload: typeof payload === 'string' ? encoder.encode(payload) : payload,
  };
}

/**
 * Create a ping frame.
 */
export function pingFrame(sid: bigint, seq: bigint): Frame {
  return {
    version: VERSION,
    sid,
    seq,
    kind: 'ping',
    payload: new Uint8Array(0),
  };
}

/**
 * Create a pong frame.
 */
export function pongFrame(sid: bigint, seq: bigint): Frame {
  return {
    version: VERSION,
    sid,
    seq,
    kind: 'pong',
    payload: new Uint8Array(0),
  };
}
