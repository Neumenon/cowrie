/**
 * Gen1: Lightweight binary JSON codec with proto-tensor support.
 *
 * Gen1 provides a compact binary JSON format that's simpler than Gen2:
 * - 11 core types (null, bool, int64, float64, string, bytes, object, arrays)
 * - Proto-tensor support for efficient numeric arrays
 * - 6 graph types (Node, Edge, AdjList, batches)
 * - No dictionary coding (simpler implementation)
 *
 * @example
 * ```typescript
 * import { encode, decode } from 'sjson/gen1';
 *
 * const data = encode({ name: 'Alice', scores: [1.0, 2.0, 3.0] });
 * const decoded = decode(data);
 * ```
 */

// Type tags
export const Tags = {
  NULL: 0x00,
  FALSE: 0x01,
  TRUE: 0x02,
  INT64: 0x03,
  FLOAT64: 0x04,
  STRING: 0x05,
  BYTES: 0x06,
  ARRAY: 0x07,
  OBJECT: 0x08,
  // Proto-tensor types
  INT64_ARRAY: 0x09,
  FLOAT64_ARRAY: 0x0a,
  STRING_ARRAY: 0x0b,
  // Graph types
  NODE: 0x10,
  EDGE: 0x11,
  ADJLIST: 0x12,
  NODE_BATCH: 0x13,
  EDGE_BATCH: 0x14,
  GRAPH_SHARD: 0x15,
} as const;

// Security limits - prevent DoS from malicious input
export const Limits = {
  MAX_DEPTH: 1000,
  MAX_ARRAY_LEN: 100_000_000,  // 100M elements
  MAX_OBJECT_LEN: 10_000_000,  // 10M fields
  MAX_STRING_LEN: 500_000_000, // 500MB
} as const;

export class SecurityLimitExceeded extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SecurityLimitExceeded';
  }
}

export type JsonValue =
  | null
  | boolean
  | number
  | bigint
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

/**
 * Encode a JavaScript value to Gen1 binary format.
 */
export function encode(value: JsonValue): Uint8Array {
  const buf: number[] = [];
  encodeValue(buf, value);
  return new Uint8Array(buf);
}

/**
 * Decode Gen1 binary data to a JavaScript value.
 * @throws SecurityLimitExceeded if security limits are exceeded
 */
export function decode(data: Uint8Array): JsonValue {
  const reader = { data, pos: 0 };
  return decodeValue(reader, 0);
}

// Encoding helpers

function writeUvarint(buf: number[], n: number): void {
  while (n >= 0x80) {
    buf.push((n & 0x7f) | 0x80);
    n >>>= 7;
  }
  buf.push(n);
}

function writeUvarintBigInt(buf: number[], n: bigint): void {
  while (n >= 0x80n) {
    buf.push(Number(n & 0x7fn) | 0x80);
    n >>= 7n;
  }
  buf.push(Number(n));
}

function zigzagEncode(n: number): number {
  return n >= 0 ? n * 2 : (-n * 2 - 1);
}

function zigzagEncodeBigInt(n: bigint): bigint {
  return n >= 0n ? n * 2n : (-n * 2n - 1n);
}

function isHomogeneousNumberArray(arr: unknown[]): arr is number[] {
  if (arr.length < 4) return false;
  return arr.every((x) => typeof x === 'number');
}

function isHomogeneousStringArray(arr: unknown[]): arr is string[] {
  if (arr.length < 4) return false;
  return arr.every((x) => typeof x === 'string');
}

function encodeValue(buf: number[], value: JsonValue): void {
  if (value === null) {
    buf.push(Tags.NULL);
  } else if (typeof value === 'boolean') {
    buf.push(value ? Tags.TRUE : Tags.FALSE);
  } else if (typeof value === 'bigint') {
    // BigInt: always encode as INT64 using zigzag + varint
    buf.push(Tags.INT64);
    writeUvarintBigInt(buf, zigzagEncodeBigInt(value));
  } else if (typeof value === 'number') {
    // For integers within safe range, use INT64; otherwise FLOAT64
    if (Number.isInteger(value) && Number.isSafeInteger(value)) {
      buf.push(Tags.INT64);
      // Use BigInt for large integers to avoid precision loss in zigzag encoding
      if (value >= -2147483648 && value <= 2147483647) {
        writeUvarint(buf, zigzagEncode(value));
      } else {
        writeUvarintBigInt(buf, zigzagEncodeBigInt(BigInt(value)));
      }
    } else {
      buf.push(Tags.FLOAT64);
      const view = new DataView(new ArrayBuffer(8));
      view.setFloat64(0, value, true);
      for (let i = 0; i < 8; i++) {
        buf.push(view.getUint8(i));
      }
    }
  } else if (typeof value === 'string') {
    buf.push(Tags.STRING);
    const encoded = new TextEncoder().encode(value);
    writeUvarint(buf, encoded.length);
    for (const byte of encoded) {
      buf.push(byte);
    }
  } else if (Array.isArray(value)) {
    // Check for proto-tensor opportunity
    if (isHomogeneousNumberArray(value)) {
      // Check if all integers or all floats
      const allIntegers = value.every((x) => Number.isInteger(x));
      if (allIntegers) {
        buf.push(Tags.INT64_ARRAY);
        writeUvarint(buf, value.length);
        for (const n of value) {
          const view = new DataView(new ArrayBuffer(8));
          view.setBigInt64(0, BigInt(n), true);
          for (let i = 0; i < 8; i++) {
            buf.push(view.getUint8(i));
          }
        }
      } else {
        buf.push(Tags.FLOAT64_ARRAY);
        writeUvarint(buf, value.length);
        for (const n of value) {
          const view = new DataView(new ArrayBuffer(8));
          view.setFloat64(0, n, true);
          for (let i = 0; i < 8; i++) {
            buf.push(view.getUint8(i));
          }
        }
      }
    } else if (isHomogeneousStringArray(value)) {
      buf.push(Tags.STRING_ARRAY);
      writeUvarint(buf, value.length);
      for (const s of value) {
        const encoded = new TextEncoder().encode(s);
        writeUvarint(buf, encoded.length);
        for (const byte of encoded) {
          buf.push(byte);
        }
      }
    } else {
      buf.push(Tags.ARRAY);
      writeUvarint(buf, value.length);
      for (const item of value) {
        encodeValue(buf, item);
      }
    }
  } else if (typeof value === 'object') {
    buf.push(Tags.OBJECT);
    const keys = Object.keys(value).sort(); // Sort for determinism
    writeUvarint(buf, keys.length);
    for (const key of keys) {
      const encodedKey = new TextEncoder().encode(key);
      writeUvarint(buf, encodedKey.length);
      for (const byte of encodedKey) {
        buf.push(byte);
      }
      encodeValue(buf, (value as Record<string, JsonValue>)[key]);
    }
  }
}

// Decoding helpers

interface Reader {
  data: Uint8Array;
  pos: number;
}

function readByte(r: Reader): number {
  if (r.pos >= r.data.length) throw new Error('Unexpected end of data');
  return r.data[r.pos++];
}

function readBytes(r: Reader, n: number): Uint8Array {
  if (r.pos + n > r.data.length) throw new Error('Unexpected end of data');
  const result = r.data.slice(r.pos, r.pos + n);
  r.pos += n;
  return result;
}

function readUvarint(r: Reader): number {
  let result = 0;
  let shift = 0;
  while (true) {
    const byte = readByte(r);
    result |= (byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) break;
    shift += 7;
    if (shift > 63) throw new Error('Varint overflow');
  }
  return result;
}

function readUvarintBigInt(r: Reader): bigint {
  let result = 0n;
  let shift = 0n;
  while (true) {
    const byte = readByte(r);
    result |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) break;
    shift += 7n;
    if (shift > 63n) throw new Error('Varint overflow');
  }
  return result;
}

function zigzagDecode(n: number): number {
  return (n >>> 1) ^ -(n & 1);
}

function zigzagDecodeBigInt(n: bigint): bigint {
  return (n >> 1n) ^ -(n & 1n);
}

function decodeValue(r: Reader, depth: number): JsonValue {
  // Security: check depth limit
  if (depth > Limits.MAX_DEPTH) {
    throw new SecurityLimitExceeded(`Maximum nesting depth exceeded: ${Limits.MAX_DEPTH}`);
  }

  const tag = readByte(r);

  switch (tag) {
    case Tags.NULL:
      return null;
    case Tags.FALSE:
      return false;
    case Tags.TRUE:
      return true;
    case Tags.INT64: {
      // Use BigInt for decoding to preserve precision for large integers
      const decoded = zigzagDecodeBigInt(readUvarintBigInt(r));
      // Return number for safe integers, BigInt for large values
      if (decoded >= -9007199254740991n && decoded <= 9007199254740991n) {
        return Number(decoded);
      }
      return decoded;
    }
    case Tags.FLOAT64: {
      const bytes = readBytes(r, 8);
      const view = new DataView(bytes.buffer, bytes.byteOffset);
      return view.getFloat64(0, true);
    }
    case Tags.STRING: {
      const len = readUvarint(r);
      if (len > Limits.MAX_STRING_LEN) {
        throw new SecurityLimitExceeded(`String too long: ${len} > ${Limits.MAX_STRING_LEN}`);
      }
      const bytes = readBytes(r, len);
      return new TextDecoder().decode(bytes);
    }
    case Tags.BYTES: {
      const len = readUvarint(r);
      if (len > Limits.MAX_STRING_LEN) {
        throw new SecurityLimitExceeded(`Bytes too long: ${len} > ${Limits.MAX_STRING_LEN}`);
      }
      return Array.from(readBytes(r, len));
    }
    case Tags.ARRAY: {
      const count = readUvarint(r);
      if (count > Limits.MAX_ARRAY_LEN) {
        throw new SecurityLimitExceeded(`Array too large: ${count} > ${Limits.MAX_ARRAY_LEN}`);
      }
      const arr: JsonValue[] = [];
      for (let i = 0; i < count; i++) {
        arr.push(decodeValue(r, depth + 1));
      }
      return arr;
    }
    case Tags.OBJECT: {
      const count = readUvarint(r);
      if (count > Limits.MAX_OBJECT_LEN) {
        throw new SecurityLimitExceeded(`Object too large: ${count} > ${Limits.MAX_OBJECT_LEN}`);
      }
      const obj: Record<string, JsonValue> = {};
      for (let i = 0; i < count; i++) {
        const keyLen = readUvarint(r);
        if (keyLen > Limits.MAX_STRING_LEN) {
          throw new SecurityLimitExceeded(`Key too long: ${keyLen} > ${Limits.MAX_STRING_LEN}`);
        }
        const keyBytes = readBytes(r, keyLen);
        const key = new TextDecoder().decode(keyBytes);
        obj[key] = decodeValue(r, depth + 1);
      }
      return obj;
    }
    case Tags.INT64_ARRAY: {
      const count = readUvarint(r);
      if (count > Limits.MAX_ARRAY_LEN) {
        throw new SecurityLimitExceeded(`Int64 array too large: ${count} > ${Limits.MAX_ARRAY_LEN}`);
      }
      const arr: number[] = [];
      for (let i = 0; i < count; i++) {
        const bytes = readBytes(r, 8);
        const view = new DataView(bytes.buffer, bytes.byteOffset);
        arr.push(Number(view.getBigInt64(0, true)));
      }
      return arr;
    }
    case Tags.FLOAT64_ARRAY: {
      const count = readUvarint(r);
      if (count > Limits.MAX_ARRAY_LEN) {
        throw new SecurityLimitExceeded(`Float64 array too large: ${count} > ${Limits.MAX_ARRAY_LEN}`);
      }
      const arr: number[] = [];
      for (let i = 0; i < count; i++) {
        const bytes = readBytes(r, 8);
        const view = new DataView(bytes.buffer, bytes.byteOffset);
        arr.push(view.getFloat64(0, true));
      }
      return arr;
    }
    case Tags.STRING_ARRAY: {
      const count = readUvarint(r);
      if (count > Limits.MAX_ARRAY_LEN) {
        throw new SecurityLimitExceeded(`String array too large: ${count} > ${Limits.MAX_ARRAY_LEN}`);
      }
      const arr: string[] = [];
      for (let i = 0; i < count; i++) {
        const len = readUvarint(r);
        if (len > Limits.MAX_STRING_LEN) {
          throw new SecurityLimitExceeded(`String too long: ${len} > ${Limits.MAX_STRING_LEN}`);
        }
        const bytes = readBytes(r, len);
        arr.push(new TextDecoder().decode(bytes));
      }
      return arr;
    }
    default:
      throw new Error(`Invalid tag: 0x${tag.toString(16)}`);
  }
}

/**
 * Encode JSON string to Gen1 binary.
 */
export function encodeJson(json: string): Uint8Array {
  return encode(JSON.parse(json));
}

/**
 * Decode Gen1 binary to JSON string.
 */
export function decodeJson(data: Uint8Array): string {
  return JSON.stringify(decode(data));
}
