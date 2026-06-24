/**
 * Cowrie v2 - "JSON++" Binary Codec for TypeScript/JavaScript
 *
 * A binary format that extends JSON with:
 * - Explicit integer types (int64, uint64 via BigInt)
 * - Decimal128 for high-precision decimals
 * - Native binary data (no base64)
 * - Datetime64 (nanosecond timestamps)
 * - UUID128 (native UUIDs)
 * - BigInt (arbitrary precision)
 * - Dictionary-coded object keys
 */

// Wire format constants
const MAGIC = new Uint8Array([0x43, 0x4f, 0x57, 0x52]); // 'COWR' (SPEC-v1 §2.2)
const VERSION = 1;

// Compression
export enum Compression {
  NONE = 0,
  GZIP = 1,
  ZSTD = 2,
}

const FLAG_COMPRESSED = 0x01;
// bit 3 (0x08) is reserved — was FLAG_HAS_COLUMN_HINTS, now silently ignored on decode
const COMPRESS_THRESHOLD = 256;

// Canonical 64-byte alignment epoch (SPEC-v1 §2.5 + §7): tensor data and file frames
// begin at a 64-byte boundary, position-determined so there is still exactly one
// canonical byte-string. Mirrors model.TENSOR_ALIGN in the Python reference.
const ALIGN = 64;

// Security limits - aligned with Go reference implementation
export const Limits = {
  MAX_DEPTH: 1000,               // Maximum nesting depth
  MAX_ARRAY_LEN: 1_000_000,     // 1M elements (tightened: was 100M)
  MAX_OBJECT_LEN: 1_000_000,    // 1M fields (tightened: was 10M)
  MAX_STRING_LEN: 10_000_000,   // 10MB (tightened: was 500MB)
  MAX_BYTES_LEN: 50_000_000,    // 50MB (tightened: was 1GB)
  MAX_EXT_LEN: 1_000_000,       // 1MB extension payload (tightened: was 100MB)
  MAX_RANK: 32,                  // Maximum tensor rank (dimensions)
  MAX_DICT_LEN: 1_000_000,      // 1M dictionary entries (tightened: was 10M)
  MAX_DECOMPRESSED_SIZE: 256 * 1024 * 1024, // 256MB decompressed limit
};

export class SecurityLimitExceeded extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SecurityLimitExceeded';
  }
}

/**
 * A decode failure carrying a stable SPEC-v1 §2.6 error code. Strict decode raises this with
 * code ERR_NON_CANONICAL when well-formed-but-non-canonical input is encountered (§5.3).
 */
export class CowrieError extends Error {
  code: string;
  constructor(code: string, detail = '') {
    super(detail ? `${code}: ${detail}` : code);
    this.name = 'CowrieError';
    this.code = code;
  }
}

// SPEC-v1 §2.6 canonical error codes. The conformance gate extracts the first /ERR_[A-Z_]+/
// token from stderr, so every decode failure must surface one of these.
export const ERR_NON_CANONICAL = 'ERR_NON_CANONICAL';
export const ERR_INVALID_MAGIC = 'ERR_INVALID_MAGIC';
export const ERR_INVALID_VERSION = 'ERR_INVALID_VERSION';
export const ERR_TRUNCATED = 'ERR_TRUNCATED';
export const ERR_RESERVED_TAG = 'ERR_RESERVED_TAG';
export const ERR_INVALID_VARINT = 'ERR_INVALID_VARINT';
export const ERR_INVALID_FIELD_ID = 'ERR_INVALID_FIELD_ID';
export const ERR_TRAILING_DATA = 'ERR_TRAILING_DATA';
export const ERR_DUPLICATE_KEY = 'ERR_DUPLICATE_KEY';
export const ERR_TOO_LARGE = 'ERR_TOO_LARGE';
export const ERR_TOO_DEEP = 'ERR_TOO_DEEP';

// Strict-mode constants (SPEC-v1 §5.3), mirroring tools/cowrie_ref/model.py.
const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;
const UINT64_MAX = 2n ** 64n - 1n;
const CANONICAL_NAN_BYTES = new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x7f]);
const NEG_ZERO_BYTES = new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80]);

/**
 * Encode a float64 to its 8 canonical little-endian bytes (§3). Normalizes on
 * encode so the output is something the strict decoder accepts: -0.0 (and +0.0)
 * become all-zero bytes, and ANY NaN (including signaling / non-canonical bit
 * patterns) becomes the one canonical quiet NaN 0x000000000000f87f. Mirrors the
 * Python reference encode.py.
 */
function encodeFloat64Bytes(value: number): Uint8Array {
  if (value === 0) {
    // covers both +0.0 and -0.0 (=== treats them as equal) -> +0.0
    return new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
  }
  if (Number.isNaN(value)) {
    return CANONICAL_NAN_BYTES.slice();
  }
  const buf = new ArrayBuffer(8);
  new DataView(buf).setFloat64(0, value, true);
  return new Uint8Array(buf);
}
// Bits-per-element for each DTYPE wire value (mirrors model.DTYPE_BITS); used for the
// tensor sub-byte trailing-padding check. Sub-byte types are the ones whose bit width is < 8.
const DTYPE_BITS: Record<number, number> = {
  0x01: 32, 0x02: 16, 0x03: 16, 0x04: 8, 0x05: 16, 0x06: 32, 0x07: 64, 0x08: 8,
  0x09: 16, 0x0a: 32, 0x0b: 64, 0x0c: 64, 0x0d: 8,
  0x10: 4, 0x11: 2, 0x12: 3, 0x13: 2, 0x14: 1,
};

/** Minimal-length little-endian two's-complement bytes for a signed BigInt (model.min_twos_complement_le). */
function minTwosComplementLE(n: bigint): Uint8Array {
  const bitLen = (n < 0n ? -n - 1n : n).toString(2).length; // bit_length()
  let length = Math.max(1, Math.floor((bitLen + 8) / 8));
  // bigintToBytes here would be big-endian/sign-extended; build LE two's complement directly.
  for (;;) {
    const min = -(2n ** BigInt(length * 8 - 1));
    const max = 2n ** BigInt(length * 8 - 1) - 1n;
    if (n >= min && n <= max) {
      const mask = (1n << BigInt(length * 8)) - 1n;
      const u = n < 0n ? (n & mask) : n;
      const out = new Uint8Array(length);
      let v = u;
      for (let i = 0; i < length; i++) {
        out[i] = Number(v & 0xffn);
        v >>= 8n;
      }
      return out;
    }
    length += 1;
  }
}

/** Lexicographic byte comparison (like Python `bytes` comparison). <0, 0, >0. */
function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    if (a[i] !== b[i]) return a[i] - b[i];
  }
  return a.length - b.length;
}

/** Decode little-endian two's-complement bytes to a signed BigInt (Python int.from_bytes(.., 'little', signed=True)). */
function bytesToBigintLESigned(bytes: Uint8Array): bigint {
  if (bytes.length === 0) return 0n;
  let result = 0n;
  for (let i = bytes.length - 1; i >= 0; i--) {
    result = (result << 8n) | BigInt(bytes[i]);
  }
  const signBit = 1n << BigInt(bytes.length * 8 - 1);
  if (result & signBit) {
    result -= 1n << BigInt(bytes.length * 8);
  }
  return result;
}

export enum UnknownExtBehavior {
  KEEP = 0,
  SKIP_AS_NULL = 1,
  ERROR = 2,
}

export interface DecodeOptions {
  onUnknownExt?: UnknownExtBehavior;
  /**
   * Strict canonical decode (SPEC-v1 §5.3). When true, well-formed-but-non-canonical
   * input is rejected with ERR_NON_CANONICAL instead of silently accepted. This mirrors
   * the Python reference (tools/cowrie_ref/decode.py) exactly. Default: false (lenient).
   */
  strict?: boolean;
  maxDepth?: number;
  maxArrayLen?: number;
  maxObjectLen?: number;
  maxStringLen?: number;
  maxBytesLen?: number;
  maxExtLen?: number;
  maxDictLen?: number;
  maxRank?: number;
  maxDecompressedSize?: number;
}

/** Fully resolved limits with no undefined fields */
interface ResolvedLimits {
  maxDepth: number;
  maxArrayLen: number;
  maxObjectLen: number;
  maxStringLen: number;
  maxBytesLen: number;
  maxExtLen: number;
  maxDictLen: number;
  maxRank: number;
  maxDecompressedSize: number;
}

function resolveLimits(opts?: DecodeOptions): ResolvedLimits {
  return {
    maxDepth: opts?.maxDepth ?? Limits.MAX_DEPTH,
    maxArrayLen: opts?.maxArrayLen ?? Limits.MAX_ARRAY_LEN,
    maxObjectLen: opts?.maxObjectLen ?? Limits.MAX_OBJECT_LEN,
    maxStringLen: opts?.maxStringLen ?? Limits.MAX_STRING_LEN,
    maxBytesLen: opts?.maxBytesLen ?? Limits.MAX_BYTES_LEN,
    maxExtLen: opts?.maxExtLen ?? Limits.MAX_EXT_LEN,
    maxDictLen: opts?.maxDictLen ?? Limits.MAX_DICT_LEN,
    maxRank: opts?.maxRank ?? Limits.MAX_RANK,
    maxDecompressedSize: opts?.maxDecompressedSize ?? Limits.MAX_DECOMPRESSED_SIZE,
  };
}

// Type tags
enum Tag {
  NULL = 0x00,
  FALSE = 0x01,
  TRUE = 0x02,
  INT64 = 0x03,
  FLOAT64 = 0x04,
  STRING = 0x05,
  ARRAY = 0x06,
  OBJECT = 0x07,
  BYTES = 0x08,
  UINT64 = 0x09,
  DECIMAL128 = 0x0a,
  DATETIME64 = 0x0b,
  UUID128 = 0x0c,
  BIGINT = 0x0d,
  EXT = 0x0e,
  // 0x0f was a scalar FLOAT32 value tag (removed pre-v1); it is now a reserved tag and
  // decodes to ERR_RESERVED_TAG. (DType.FLOAT32 below is the unrelated tensor dtype.)
  // ML/Multimodal extensions (0x20-0x2F)
  TENSOR = 0x20,
  TENSOR_REF = 0x21,
  IMAGE = 0x22,
  AUDIO = 0x23,
  BITMASK = 0x24, // v3: packed boolean bitmask
  // 0x30-0x32 reserved (AdjList/RichText/Delta — moved to dedicated packages)
  // Graph types (v2.1)
  NODE = 0x35,
  EDGE = 0x36,
  NODE_BATCH = 0x37,
  EDGE_BATCH = 0x38,
  // 0x39 reserved (GraphShard — moved to dedicated package)
  // v3 inline types (0x40-0xFF)
  FIXINT_BASE = 0x40,
  FIXINT_MAX = 0xbf,
  FIXARRAY_BASE = 0xc0,
  FIXARRAY_MAX = 0xcf,
  FIXMAP_BASE = 0xd0,
  FIXMAP_MAX = 0xdf,
  FIXNEG_BASE = 0xe0,
  FIXNEG_MAX = 0xef,
}

// Cowrie value types
export enum Type {
  NULL,
  BOOL,
  INT64,
  UINT64,
  FLOAT64,
  DECIMAL128,
  STRING,
  BYTES,
  DATETIME64,
  UUID128,
  BIGINT,
  ARRAY,
  OBJECT,
  TENSOR,
  IMAGE,
  AUDIO,
  TENSOR_REF,
  // Graph types (v2.1)
  NODE,
  EDGE,
  NODE_BATCH,
  EDGE_BATCH,
  BITMASK,
  UNKNOWN_EXT,
}

// Image format enum - aligned with Go reference implementation
export enum ImageFormat {
  JPEG = 0x01,
  PNG = 0x02,
  WEBP = 0x03,
  AVIF = 0x04,
  BMP = 0x05,
}

// Audio encoding enum - aligned with Go reference implementation
export enum AudioEncoding {
  PCM_INT16 = 0x01,
  PCM_FLOAT32 = 0x02,
  OPUS = 0x03,
  AAC = 0x04,
}

// Decimal128 type
export interface Decimal128 {
  scale: number; // -127 to 127
  coef: Uint8Array; // 16 bytes, two's complement big-endian
}

// Image data type
export interface ImageData {
  format: ImageFormat;
  width: number;
  height: number;
  data: Uint8Array;
}

// Audio data type
export interface AudioData {
  encoding: AudioEncoding;
  sampleRate: number;
  channels: number;
  data: Uint8Array;
}

// Tensor reference data type (reference to externally stored tensor)
export interface TensorRefData {
  storeId: number; // Which store/shard (0-255)
  key: Uint8Array; // Lookup key (UUID, hash, etc.)
}

// Graph types (v2.1)

// Graph node with ID, labels, and properties
export interface NodeData {
  id: string;
  labels: string[];
  props: Record<string, Value>;
}

// Graph edge with source, destination, type, and properties
export interface EdgeData {
  fromId: string;
  toId: string;
  edgeType: string;
  props: Record<string, Value>;
}

// Batch of nodes for streaming/bulk operations
export interface NodeBatchData {
  nodes: NodeData[];
}

// Batch of edges for streaming/bulk operations
export interface EdgeBatchData {
  edges: EdgeData[];
}

// Bitmask data type (v3: packed boolean bitmask)
// Bit ordering: LSB-first within each byte. Bit i is at bytes[i/8] & (1 << (i%8)).
export interface BitmaskData {
  count: number; // Number of boolean values
  bits: Uint8Array; // Packed bits, ceil(count/8) bytes
}

export interface UnknownExtData {
  extType: bigint;
  payload: Uint8Array;
}

// Cowrie Value type
export interface Value {
  type: Type;
  data: unknown;
}

// Value constructors
export const SJ = {
  null(): Value {
    return { type: Type.NULL, data: null };
  },
  bool(b: boolean): Value {
    return { type: Type.BOOL, data: b };
  },
  int64(i: bigint | number): Value {
    return { type: Type.INT64, data: BigInt(i) };
  },
  uint64(u: bigint | number): Value {
    const v = BigInt(u);
    if (v < 0n || v > 0xFFFFFFFFFFFFFFFFn) {
      throw new RangeError(`UINT64 value out of range: ${v} (must be 0..2^64-1)`);
    }
    return { type: Type.UINT64, data: v };
  },
  float64(f: number): Value {
    return { type: Type.FLOAT64, data: f };
  },
  decimal128(scale: number, coef: Uint8Array): Value {
    return { type: Type.DECIMAL128, data: { scale, coef } as Decimal128 };
  },
  string(s: string): Value {
    return { type: Type.STRING, data: s };
  },
  bytes(b: Uint8Array): Value {
    return { type: Type.BYTES, data: new Uint8Array(b) };
  },
  datetime64(nanos: bigint): Value {
    return { type: Type.DATETIME64, data: nanos };
  },
  datetime(d: Date): Value {
    return { type: Type.DATETIME64, data: BigInt(d.getTime()) * 1000000n };
  },
  uuid128(uuid: Uint8Array | string): Value {
    if (typeof uuid === "string") {
      uuid = parseUUID(uuid);
    }
    return { type: Type.UUID128, data: uuid };
  },
  bigint(b: Uint8Array | bigint): Value {
    if (typeof b === "bigint") {
      b = bigintToBytes(b);
    }
    return { type: Type.BIGINT, data: b };
  },
  array(items: Value[]): Value {
    return { type: Type.ARRAY, data: items };
  },
  object(members: Record<string, Value>): Value {
    return { type: Type.OBJECT, data: members };
  },
  tensor(dtype: DType, shape: number[], data: Uint8Array): Value {
    return { type: Type.TENSOR, data: { dtype, shape, data } as TensorData };
  },
  image(format: ImageFormat, width: number, height: number, data: Uint8Array): Value {
    return { type: Type.IMAGE, data: { format, width, height, data } as ImageData };
  },
  audio(encoding: AudioEncoding, sampleRate: number, channels: number, data: Uint8Array): Value {
    // sampleRate is a u32 on the wire, channels a u8 with >=1 frames. Validate at
    // this single choke point so every path (direct construction, binary decode,
    // JSON bridge) rejects out-of-range values instead of accepting channels=0 or
    // silently truncating.
    if (!Number.isInteger(sampleRate) || sampleRate < 0 || sampleRate > 0xffffffff) {
      throw new RangeError(`cowrie: audio sampleRate out of range [0, 4294967295] (got ${sampleRate})`);
    }
    if (!Number.isInteger(channels) || channels < 1 || channels > 255) {
      throw new RangeError(`cowrie: audio channels out of range [1, 255] (got ${channels})`);
    }
    return { type: Type.AUDIO, data: { encoding, sampleRate, channels, data } as AudioData };
  },
  tensorRef(storeId: number, key: Uint8Array): Value {
    return { type: Type.TENSOR_REF, data: { storeId, key } as TensorRefData };
  },
  // Graph types (v2.1)
  node(id: string, labels: string[], props: Record<string, Value>): Value {
    return { type: Type.NODE, data: { id, labels, props } as NodeData };
  },
  edge(fromId: string, toId: string, edgeType: string, props: Record<string, Value>): Value {
    return { type: Type.EDGE, data: { fromId, toId, edgeType, props } as EdgeData };
  },
  nodeBatch(nodes: NodeData[]): Value {
    return { type: Type.NODE_BATCH, data: { nodes } as NodeBatchData };
  },
  edgeBatch(edges: EdgeData[]): Value {
    return { type: Type.EDGE_BATCH, data: { edges } as EdgeBatchData };
  },
  bitmask(count: number, bits: Uint8Array): Value {
    return { type: Type.BITMASK, data: { count, bits } as BitmaskData };
  },
  bitmaskFromBools(bools: boolean[]): Value {
    const count = bools.length;
    const byteLen = Math.ceil(count / 8);
    const bits = new Uint8Array(byteLen);
    for (let i = 0; i < count; i++) {
      if (bools[i]) {
        bits[Math.floor(i / 8)] |= 1 << (i % 8);
      }
    }
    return { type: Type.BITMASK, data: { count, bits } as BitmaskData };
  },
  unknownExt(extType: bigint | number, payload: Uint8Array): Value {
    return { type: Type.UNKNOWN_EXT, data: { extType: BigInt(extType), payload } as UnknownExtData };
  },
};

// Helper functions
function parseUUID(s: string): Uint8Array {
  const hex = s.replace(/-/g, "");
  if (hex.length !== 32) throw new Error("Invalid UUID");
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i++) {
    bytes[i] = parseInt(hex.substr(i * 2, 2), 16);
  }
  return bytes;
}

function formatUUID(bytes: Uint8Array): string {
  const hex = Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function bigintToBytes(n: bigint): Uint8Array {
  if (n === 0n) return new Uint8Array([0]);
  const negative = n < 0n;
  if (negative) n = -n;

  const bytes: number[] = [];
  while (n > 0n) {
    bytes.unshift(Number(n & 0xffn));
    n >>= 8n;
  }

  // Two's complement for negative
  if (negative) {
    let carry = 1;
    for (let i = bytes.length - 1; i >= 0; i--) {
      bytes[i] = ~bytes[i] & 0xff;
      bytes[i] += carry;
      carry = bytes[i] > 0xff ? 1 : 0;
      bytes[i] &= 0xff;
    }
    if (carry) bytes.unshift(0xff);
    if ((bytes[0] & 0x80) === 0) bytes.unshift(0xff);
  } else if (bytes[0] & 0x80) {
    bytes.unshift(0);
  }

  return new Uint8Array(bytes);
}

function bytesToBigint(bytes: Uint8Array): bigint {
  if (bytes.length === 0) return 0n;
  const negative = (bytes[0] & 0x80) !== 0;
  let result = 0n;

  if (negative) {
    // Two's complement
    const inverted = new Uint8Array(bytes.length);
    let carry = 1;
    for (let i = bytes.length - 1; i >= 0; i--) {
      inverted[i] = ~bytes[i] & 0xff;
      inverted[i] += carry;
      carry = inverted[i] > 0xff ? 1 : 0;
      inverted[i] &= 0xff;
    }
    for (const b of inverted) {
      result = (result << 8n) | BigInt(b);
    }
    return -result;
  }

  for (const b of bytes) {
    result = (result << 8n) | BigInt(b);
  }
  return result;
}

// Varint encoding/decoding
function encodeUvarint(n: bigint | number): Uint8Array {
  let v = BigInt(n);
  if (v < 0n || v > 0xFFFFFFFFFFFFFFFFn) {
    throw new RangeError(`uvarint value out of range: ${v} (must be 0..2^64-1)`);
  }
  const bytes: number[] = [];
  while (v >= 0x80n) {
    bytes.push(Number(v & 0x7fn) | 0x80);
    v >>= 7n;
  }
  bytes.push(Number(v));
  return new Uint8Array(bytes);
}

const MAX_UVARINT_BYTES = 10; // a 64-bit value is at most 10 LEB128 bytes (SPEC-v1 §2.1)

function decodeUvarint(data: Uint8Array, pos: number): [bigint, number] {
  let result = 0n;
  let shift = 0n;
  const start = pos;
  for (;;) {
    if (pos - start >= MAX_UVARINT_BYTES) {
      throw new CowrieError(ERR_INVALID_VARINT, 'varint too long');
    }
    if (pos >= data.length) {
      throw new CowrieError(ERR_TRUNCATED, 'varint');
    }
    const b = data[pos];
    pos++;
    result |= BigInt(b & 0x7f) << shift;
    if ((b & 0x80) === 0) {
      // minimality: the final byte must be non-zero unless the value is the single byte 0x00.
      if (b === 0 && pos - start > 1) {
        throw new CowrieError(ERR_INVALID_VARINT, 'overlong varint');
      }
      // 64-bit overflow: minimality does not catch a 10-byte form with bits past bit 63.
      if (result > UINT64_MAX) {
        throw new CowrieError(ERR_INVALID_VARINT, 'uvarint overflows 64 bits');
      }
      return [result, pos];
    }
    shift += 7n;
  }
}

function zigzagEncode(n: bigint): bigint {
  return (n << 1n) ^ (n >> 63n);
}

function zigzagDecode(n: bigint): bigint {
  return (n >> 1n) ^ -(n & 1n);
}

// Reusable TextEncoder instance (created once, shared by all encoders)
const sharedTextEncoder = new TextEncoder();

// ============================================================
// Tensor validation helpers
// ============================================================

/**
 * Returns the element size in bytes for byte-aligned dtypes.
 * Returns null for sub-byte packed types (BOOL, QINT4, QINT2, QINT3, TERNARY, BINARY)
 * whose packing formula is non-trivial and is not validated here.
 */
function dtypeElemSize(dtype: number): number | null {
  switch (dtype) {
    case 0x01: return 4;  // FLOAT32
    case 0x02: return 2;  // FLOAT16
    case 0x03: return 2;  // BFLOAT16
    case 0x04: return 1;  // INT8
    case 0x05: return 2;  // INT16
    case 0x06: return 4;  // INT32
    case 0x07: return 8;  // INT64
    case 0x08: return 1;  // UINT8
    case 0x09: return 2;  // UINT16
    case 0x0a: return 4;  // UINT32
    case 0x0b: return 8;  // UINT64
    case 0x0c: return 8;  // FLOAT64
    case 0x0d: return 1;  // BOOL (one byte per element; Bitmask is the bit-packed type)
    default:              // Sub-byte packed types (qint4/qint2/qint3/ternary/binary): skip
      return null;
  }
}

/**
 * Computes the expected byte length for a tensor with the given shape and dtype.
 * Returns null when the dtype uses sub-byte packing or when the product overflows
 * Number.MAX_SAFE_INTEGER (skip the check rather than accept corrupt data silently).
 * A rank-0 tensor (no dims) is a scalar — one element (elemSize bytes); a zero
 * dimension yields an empty 0-byte tensor.
 */
function tensorExpectedBytes(dtype: number, shape: number[]): number | null {
  const elemSize = dtypeElemSize(dtype);
  if (elemSize === null) return null;
  if (shape.length === 0) return elemSize;  // rank-0 scalar = one element
  let product = 1;
  for (const dim of shape) {
    if (dim === 0) return 0;
    product *= dim;
    // If product exceeds safe integer range, skip the check to avoid silent overflow.
    if (product > Number.MAX_SAFE_INTEGER) return null;
  }
  const total = product * elemSize;
  if (total > Number.MAX_SAFE_INTEGER) return null;
  return total;
}

// Encoder
class Encoder {
  private buf: number[] = [];
  private dictKeys: string[] = [];
  private dictLookup = new Map<string, number>();

  private addKey(key: string): number {
    const existing = this.dictLookup.get(key);
    if (existing !== undefined) return existing;
    const idx = this.dictKeys.length;
    this.dictKeys.push(key);
    this.dictLookup.set(key, idx);
    return idx;
  }

  private collectKeys(v: Value): void {
    if (v.type === Type.ARRAY) {
      for (const item of v.data as Value[]) {
        this.collectKeys(item);
      }
    } else if (v.type === Type.OBJECT) {
      for (const [key, val] of Object.entries(v.data as Record<string, Value>)) {
        this.addKey(key);
        this.collectKeys(val);
      }
    } else if (v.type === Type.NODE) {
      const node = v.data as NodeData;
      this.collectPropsKeys(node.props);
    } else if (v.type === Type.EDGE) {
      const edge = v.data as EdgeData;
      this.collectPropsKeys(edge.props);
    } else if (v.type === Type.NODE_BATCH) {
      const batch = v.data as NodeBatchData;
      for (const node of batch.nodes) {
        this.collectPropsKeys(node.props);
      }
    } else if (v.type === Type.EDGE_BATCH) {
      const batch = v.data as EdgeBatchData;
      for (const edge of batch.edges) {
        this.collectPropsKeys(edge.props);
      }
    }
  }

  private collectPropsKeys(props: Record<string, Value>): void {
    const keys = Object.keys(props).sort((a, b) => a < b ? -1 : a > b ? 1 : 0);
    for (const key of keys) {
      this.addKey(key);
      this.collectKeys(props[key]);
    }
  }

  private write(data: Uint8Array): void {
    for (const b of data) this.buf.push(b);
  }

  private writeByte(b: number): void {
    this.buf.push(b);
  }

  private writeUvarint(n: bigint | number): void {
    this.write(encodeUvarint(n));
  }

  private writeString(s: string): void {
    const bytes = sharedTextEncoder.encode(s);
    this.writeUvarint(bytes.length);
    this.write(bytes);
  }

  private encodeValue(v: Value): void {
    switch (v.type) {
      case Type.NULL:
        this.writeByte(Tag.NULL);
        break;
      case Type.BOOL:
        this.writeByte(v.data ? Tag.TRUE : Tag.FALSE);
        break;
      case Type.INT64: {
        const i64 = v.data as bigint;
        // v3 inline encoding: FIXINT for 0-127, FIXNEG for -1 to -16
        if (i64 >= 0n && i64 <= 127n) {
          this.writeByte(Tag.FIXINT_BASE + Number(i64));
          break;
        }
        if (i64 >= -16n && i64 <= -1n) {
          this.writeByte(Tag.FIXNEG_BASE + Number(-1n - i64));
          break;
        }
        this.writeByte(Tag.INT64);
        this.writeUvarint(zigzagEncode(i64));
        break;
      }
      case Type.UINT64: {
        const u64 = v.data as bigint;
        if (u64 < 0n || u64 > 0xFFFFFFFFFFFFFFFFn) {
          throw new RangeError(`UINT64 value out of range: ${u64} (must be 0..2^64-1)`);
        }
        this.writeByte(Tag.UINT64);
        this.writeUvarint(u64);
        break;
      }
      case Type.FLOAT64: {
        this.writeByte(Tag.FLOAT64);
        this.write(encodeFloat64Bytes(v.data as number));
        break;
      }
      case Type.DECIMAL128: {
        const d = v.data as Decimal128;
        this.writeByte(Tag.DECIMAL128);
        // §2.3: scale is a SVARINT (zigzag + LEB128), NOT a raw int8 byte. A raw byte
        // desyncs the stream for |scale| > 63 (mirrors cowrie_ref encode.py:60).
        this.write(encodeUvarint(zigzagEncode(BigInt(d.scale))));
        this.write(d.coef);
        break;
      }
      case Type.STRING:
        this.writeByte(Tag.STRING);
        this.writeString(v.data as string);
        break;
      case Type.BYTES: {
        const bytes = v.data as Uint8Array;
        this.writeByte(Tag.BYTES);
        this.writeUvarint(bytes.length);
        this.write(bytes);
        break;
      }
      case Type.DATETIME64: {
        this.writeByte(Tag.DATETIME64);
        const buf = new ArrayBuffer(8);
        new DataView(buf).setBigInt64(0, v.data as bigint, true);
        this.write(new Uint8Array(buf));
        break;
      }
      case Type.UUID128:
        this.writeByte(Tag.UUID128);
        this.write(v.data as Uint8Array);
        break;
      case Type.BIGINT: {
        const bytes = v.data as Uint8Array;
        this.writeByte(Tag.BIGINT);
        this.writeUvarint(bytes.length);
        this.write(bytes);
        break;
      }
      case Type.UNKNOWN_EXT: {
        const ext = v.data as UnknownExtData;
        this.writeByte(Tag.EXT);
        this.writeUvarint(ext.extType);
        this.writeUvarint(ext.payload.length);
        this.write(ext.payload);
        break;
      }
      case Type.ARRAY: {
        const arr = v.data as Value[];
        // v3 inline encoding: FIXARRAY for length 0-15
        if (arr.length <= 15) {
          this.writeByte(Tag.FIXARRAY_BASE + arr.length);
        } else {
          this.writeByte(Tag.ARRAY);
          this.writeUvarint(arr.length);
        }
        for (const item of arr) this.encodeValue(item);
        break;
      }
      case Type.OBJECT: {
        const obj = v.data as Record<string, Value>;
        // Emit fields in ascending dictIdx order (§2.4). MUST sort explicitly: JS object
        // iteration hoists integer-like keys ("0","9") to the front in numeric order, which
        // is NOT the canonical (global byte-sorted dict) order. dictLookup is UTF-8-canonical.
        const entries = Object.entries(obj).sort(
          (a, b) => (this.dictLookup.get(a[0]) ?? 0) - (this.dictLookup.get(b[0]) ?? 0)
        );
        // v3 inline encoding: FIXMAP for count 0-15
        if (entries.length <= 15) {
          this.writeByte(Tag.FIXMAP_BASE + entries.length);
        } else {
          this.writeByte(Tag.OBJECT);
          this.writeUvarint(entries.length);
        }
        for (const [key, val] of entries) {
          const idx = this.dictLookup.get(key);
          if (idx === undefined) {
            throw new Error(`Internal: object key "${key}" missing from dictionary`);
          }
          this.writeUvarint(idx);
          this.encodeValue(val);
        }
        break;
      }
      case Type.TENSOR: {
        const t = v.data as TensorData;
        const rank = t.shape.length;
        if (rank > Limits.MAX_RANK || rank > 255) {
          throw new SecurityLimitExceeded(`Tensor rank too large: ${rank} > ${Limits.MAX_RANK}`);
        }
        // Validate dataLen against shape/dtype on encode too, so we never emit
        // bytes the decoder would reject (symmetric with decode).
        const expectedBytes = tensorExpectedBytes(t.dtype, t.shape);
        if (expectedBytes !== null && t.data.length !== expectedBytes) {
          throw new Error(`cowrie: tensor dataLen ${t.data.length} does not match shape/dtype (expected ${expectedBytes} bytes)`);
        }
        this.writeByte(Tag.TENSOR);
        this.writeByte(t.dtype);
        this.writeByte(rank);
        for (const dim of t.shape) {
          this.writeUvarint(dim);
        }
        this.writeUvarint(t.data.length);
        // Zero-pad so the data starts at a 64-byte boundary relative to byte 0 of the
        // message (§2.5). this.buf.length is the absolute offset right after dataLen —
        // this is how the encoder threads the absolute 'base' (cf. cowrie_ref encode.py).
        const pad = ((ALIGN - (this.buf.length % ALIGN)) % ALIGN); // (-offset) mod 64
        for (let i = 0; i < pad; i++) this.writeByte(0);
        this.write(t.data);
        break;
      }
      case Type.IMAGE: {
        const img = v.data as ImageData;
        this.writeByte(Tag.IMAGE);
        this.writeByte(img.format);
        // width and height as u16 LE
        const buf = new ArrayBuffer(4);
        const view = new DataView(buf);
        view.setUint16(0, img.width, true);
        view.setUint16(2, img.height, true);
        this.write(new Uint8Array(buf));
        this.writeUvarint(img.data.length);
        this.write(img.data);
        break;
      }
      case Type.AUDIO: {
        const aud = v.data as AudioData;
        this.writeByte(Tag.AUDIO);
        this.writeByte(aud.encoding);
        // sampleRate as u32 LE
        const buf = new ArrayBuffer(4);
        new DataView(buf).setUint32(0, aud.sampleRate, true);
        this.write(new Uint8Array(buf));
        this.writeByte(aud.channels);
        this.writeUvarint(aud.data.length);
        this.write(aud.data);
        break;
      }
      case Type.TENSOR_REF: {
        const ref = v.data as TensorRefData;
        this.writeByte(Tag.TENSOR_REF);
        this.writeByte(ref.storeId);
        this.writeUvarint(ref.key.length);
        this.write(ref.key);
        break;
      }
      // Graph types
      case Type.NODE: {
        const node = v.data as NodeData;
        this.writeByte(Tag.NODE);
        this.encodeNode(node);
        break;
      }
      case Type.EDGE: {
        const edge = v.data as EdgeData;
        this.writeByte(Tag.EDGE);
        this.encodeEdge(edge);
        break;
      }
      case Type.NODE_BATCH: {
        const batch = v.data as NodeBatchData;
        this.writeByte(Tag.NODE_BATCH);
        this.writeUvarint(batch.nodes.length);
        for (const node of batch.nodes) {
          this.encodeNode(node);
        }
        break;
      }
      case Type.EDGE_BATCH: {
        const batch = v.data as EdgeBatchData;
        this.writeByte(Tag.EDGE_BATCH);
        this.writeUvarint(batch.edges.length);
        for (const edge of batch.edges) {
          this.encodeEdge(edge);
        }
        break;
      }
      case Type.BITMASK: {
        const bm = v.data as BitmaskData;
        this.writeByte(Tag.BITMASK);
        this.writeUvarint(bm.count);
        this.write(bm.bits);
        break;
      }
    }
  }

  private encodeNode(node: NodeData): void {
    this.writeString(node.id);
    this.writeUvarint(node.labels.length);
    for (const label of node.labels) {
      this.writeString(label);
    }
    this.encodeProps(node.props);
  }

  private encodeEdge(edge: EdgeData): void {
    this.writeString(edge.fromId);
    this.writeString(edge.toId);
    this.writeString(edge.edgeType);
    this.encodeProps(edge.props);
  }

  private encodeProps(props: Record<string, Value>): void {
    const entries = Object.entries(props).sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0);
    this.writeUvarint(entries.length);
    for (const [key, val] of entries) {
      const idx = this.dictLookup.get(key);
      if (idx === undefined) {
        throw new Error(`Internal: graph prop key "${key}" missing from dictionary`);
      }
      this.writeUvarint(idx);
      this.encodeValue(val);
    }
  }

  encode(v: Value): Uint8Array {
    this.collectKeys(v);

    // Global byte-sorted canonical dictionary order (SPEC-v1 §2.4), not DFS-discovery order.
    this.dictKeys.sort((a, b) => {
      const ba = sharedTextEncoder.encode(a);
      const bb = sharedTextEncoder.encode(b);
      const n = Math.min(ba.length, bb.length);
      for (let i = 0; i < n; i++) {
        if (ba[i] !== bb[i]) return ba[i] - bb[i];
      }
      return ba.length - bb.length;
    });
    this.dictLookup.clear();
    this.dictKeys.forEach((k, i) => this.dictLookup.set(k, i));

    // Header
    this.write(MAGIC);
    this.writeByte(VERSION);
    this.writeByte(0); // flags

    // Dictionary
    this.writeUvarint(this.dictKeys.length);
    for (const key of this.dictKeys) {
      this.writeString(key);
    }

    // Root value
    this.encodeValue(v);

    return new Uint8Array(this.buf);
  }
}

export function encode(v: Value): Uint8Array {
  return new Encoder().encode(v);
}

// Decoder
// One located tensor data run within a canonical message (Phase 2 zero-copy locator).
// dataOffset is the ABSOLUTE byte offset (from byte 0 of the message) of the tensor's
// contiguous little-endian data run; dataLen is its byte length.
export interface TensorSpan {
  dtype: number;
  shape: number[];
  dataOffset: number;
  dataLen: number;
}

class Decoder {
  private pos = 0;
  private dict: string[] = [];
  private depth = 0;
  private textDecoder = new TextDecoder();
  private limits: ResolvedLimits;
  // Tensor data runs captured during decode, in document order (Phase 2 locator).
  spans: TensorSpan[] = [];

  constructor(
    private data: Uint8Array,
    private onUnknownExt: UnknownExtBehavior = UnknownExtBehavior.KEEP,
    limits?: ResolvedLimits,
    private strict: boolean = false,
  ) {
    this.limits = limits ?? resolveLimits();
  }

  private read(n: number): Uint8Array {
    if (this.pos + n > this.data.length) {
      throw new CowrieError(ERR_TRUNCATED, 'unexpected end of data');
    }
    const result = this.data.slice(this.pos, this.pos + n);
    this.pos += n;
    return result;
  }

  private readByte(): number {
    return this.read(1)[0];
  }

  private readUvarint(): bigint {
    const [val, newPos] = decodeUvarint(this.data, this.pos);
    this.pos = newPos;
    return val;
  }

  /** Read a uvarint and convert to number, throwing if it exceeds safe integer range */
  private readUvarintAsNumber(): number {
    const v = this.readUvarint();
    if (v > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new SecurityLimitExceeded('varint length exceeds safe integer range');
    }
    return Number(v);
  }

  private readString(): string {
    const len = this.readUvarintAsNumber();
    if (len > this.limits.maxStringLen) {
      throw new SecurityLimitExceeded(`String too long: ${len} > ${this.limits.maxStringLen}`);
    }
    return this.textDecoder.decode(this.read(len));
  }

  private enterNested(): void {
    this.depth++;
    // §2.7: nesting depth > MAX_DEPTH is a clean decode error (ERR_TOO_DEEP), checked
    // BEFORE recursing so over-deep input never overflows the native stack.
    if (this.depth > this.limits.maxDepth) {
      throw new CowrieError(ERR_TOO_DEEP, `nesting depth > ${this.limits.maxDepth}`);
    }
  }

  private exitNested(): void {
    this.depth--;
  }

  private decodeValue(): Value {
    const tag = this.readByte();

    switch (tag) {
      case Tag.NULL:
        return SJ.null();
      case Tag.FALSE:
        return SJ.bool(false);
      case Tag.TRUE:
        return SJ.bool(true);
      case Tag.INT64: {
        const v = zigzagDecode(this.readUvarint());
        if (this.strict && v >= -16n && v <= 127n) {
          throw new CowrieError(ERR_NON_CANONICAL, 'int in FIXINT/FIXNEG range');
        }
        return SJ.int64(v);
      }
      case Tag.UINT64: {
        const v = this.readUvarint();
        if (this.strict && v <= INT64_MAX) {
          throw new CowrieError(ERR_NON_CANONICAL, 'uint fits Int');
        }
        return SJ.uint64(v);
      }
      case Tag.FLOAT64: {
        const buf = this.read(8);
        if (this.strict) {
          if (compareBytes(buf, NEG_ZERO_BYTES) === 0) {
            throw new CowrieError(ERR_NON_CANONICAL, 'negative zero');
          }
          const fChk = new DataView(buf.buffer, buf.byteOffset).getFloat64(0, true);
          if (Number.isNaN(fChk) && compareBytes(buf, CANONICAL_NAN_BYTES) !== 0) {
            throw new CowrieError(ERR_NON_CANONICAL, 'non-canonical NaN');
          }
        }
        const f = new DataView(buf.buffer, buf.byteOffset).getFloat64(0, true);
        return SJ.float64(f);
      }
      case Tag.DECIMAL128: {
        // §2.3: scale is a SVARINT (zigzag + LEB128), NOT a raw int8 byte (mirrors
        // cowrie_ref decode.py T_DECIMAL). A raw byte desyncs for |scale| > 63.
        const scale = Number(zigzagDecode(this.readUvarint()));
        const coef = this.read(16);
        if (this.strict) {
          // Lowest-terms check (model.Decimal128.canonical): a non-zero coefficient
          // divisible by 10 is non-canonical (should be scaled down). Additionally,
          // a zero coefficient must use scale 0 — canonical() maps coeff==0 to (0,0),
          // so coeff==0 with scale!=0 is non-canonical (decimal_zero_scale).
          const coeff = bytesToBigintLESigned(coef);
          if (coeff === 0n) {
            if (scale !== 0) {
              throw new CowrieError(ERR_NON_CANONICAL, 'decimal not lowest terms');
            }
          } else if (coeff % 10n === 0n) {
            throw new CowrieError(ERR_NON_CANONICAL, 'decimal not lowest terms');
          }
        }
        return SJ.decimal128(scale, coef);
      }
      case Tag.STRING:
        return SJ.string(this.readString());
      case Tag.BYTES: {
        const len = this.readUvarintAsNumber();
        if (len > this.limits.maxBytesLen) {
          throw new SecurityLimitExceeded(`Bytes too long: ${len} > ${this.limits.maxBytesLen}`);
        }
        return SJ.bytes(this.read(len));
      }
      case Tag.DATETIME64: {
        const buf = this.read(8);
        const nanos = new DataView(buf.buffer, buf.byteOffset).getBigInt64(0, true);
        return SJ.datetime64(nanos);
      }
      case Tag.UUID128:
        return SJ.uuid128(this.read(16));
      case Tag.BIGINT: {
        const len = this.readUvarintAsNumber();
        const raw = this.read(len);
        if (this.strict) {
          const v = bytesToBigintLESigned(raw);
          if (v >= INT64_MIN && v <= UINT64_MAX) {
            throw new CowrieError(ERR_NON_CANONICAL, 'bigint fits Int/Uint');
          }
          const minimal = minTwosComplementLE(v);
          if (compareBytes(raw, minimal) !== 0) {
            throw new CowrieError(ERR_NON_CANONICAL, 'non-minimal bigint');
          }
        }
        return SJ.bigint(raw);
      }
      case Tag.EXT: {
        const extType = this.readUvarint();
        const len = this.readUvarintAsNumber();
        if (len > this.limits.maxExtLen) {
          throw new SecurityLimitExceeded(`Extension payload too large: ${len} > ${this.limits.maxExtLen}`);
        }
        const payload = this.read(len);
        if (this.onUnknownExt === UnknownExtBehavior.ERROR) {
          throw new Error("Unknown extension type");
        }
        if (this.onUnknownExt === UnknownExtBehavior.SKIP_AS_NULL) {
          return SJ.null();
        }
        return SJ.unknownExt(extType, payload);
      }
      case Tag.ARRAY: {
        const count = this.readUvarintAsNumber();
        if (this.strict && count <= 15) {
          throw new CowrieError(ERR_NON_CANONICAL, 'array should use FIXARRAY');
        }
        if (count > this.limits.maxArrayLen) {
          throw new SecurityLimitExceeded(`Array too large: ${count} > ${this.limits.maxArrayLen}`);
        }
        this.enterNested();
        const items: Value[] = [];
        for (let i = 0; i < count; i++) {
          items.push(this.decodeValue());
        }
        this.exitNested();
        return SJ.array(items);
      }
      case Tag.OBJECT: {
        const count = this.readUvarintAsNumber();
        if (this.strict && count <= 15) {
          throw new CowrieError(ERR_NON_CANONICAL, 'object should use FIXMAP');
        }
        if (count > this.limits.maxObjectLen) {
          throw new SecurityLimitExceeded(`Object too large: ${count} > ${this.limits.maxObjectLen}`);
        }
        this.enterNested();
        const members: Record<string, Value> = {};
        let lastIdx = -1;
        for (let i = 0; i < count; i++) {
          const fieldId = this.readUvarintAsNumber();
          if (fieldId >= this.dict.length) {
            throw new Error(`Invalid dictionary index: ${fieldId} >= ${this.dict.length}`);
          }
          if (this.strict && fieldId <= lastIdx) {
            throw new CowrieError(ERR_NON_CANONICAL, 'object fields not in ascending dictIdx');
          }
          lastIdx = fieldId;
          const key = this.dict[fieldId];
          members[key] = this.decodeValue();
        }
        this.exitNested();
        return SJ.object(members);
      }
      case Tag.TENSOR: {
        const dtype = this.readByte() as DType;
        const rank = this.readByte();
        // §2.7: tensor rank > MAX_RANK is a clean decode error (ERR_TOO_LARGE).
        if (rank > this.limits.maxRank) {
          throw new CowrieError(ERR_TOO_LARGE, `tensor rank > ${this.limits.maxRank}`);
        }
        const shape: number[] = [];
        for (let i = 0; i < rank; i++) {
          shape.push(this.readUvarintAsNumber());
        }
        const dataLen = this.readUvarintAsNumber();
        if (dataLen > this.limits.maxBytesLen) {
          throw new SecurityLimitExceeded(`Tensor data too large: ${dataLen} > ${this.limits.maxBytesLen}`);
        }
        // Validate dataLen == product(dims) * elemSize for byte-aligned dtypes.
        // Sub-byte packed types (BOOL, QINT4, etc.) are skipped (tensorExpectedBytes returns null).
        const expected = tensorExpectedBytes(dtype, shape);
        if (expected !== null && dataLen !== expected) {
          throw new Error(`cowrie: tensor dataLen ${dataLen} does not match shape/dtype (expected ${expected} bytes)`);
        }
        // Consume the 64-byte alignment padding (§2.5): data begins at a 64-byte boundary
        // relative to byte 0 of the message. pad = (-pos) mod 64; in strict mode reject any
        // non-zero pad byte. data_offset (the recorded span) is the ALIGNED position.
        const tensorPad = ((ALIGN - (this.pos % ALIGN)) % ALIGN);
        const padBytes = this.read(tensorPad);
        if (this.strict) {
          for (const b of padBytes) {
            if (b !== 0) throw new CowrieError(ERR_NON_CANONICAL, 'tensor alignment padding not zero');
          }
        }
        // Record the tensor's data run: data_offset is the ALIGNED start of the contiguous
        // data bytes, matching the Python oracle (tensor_spans data_offset is 64-aligned).
        this.spans.push({ dtype, shape: shape.slice(), dataOffset: this.pos, dataLen });
        const data = this.read(dataLen);
        if (this.strict) {
          // Sub-byte dtypes (qint4/qint2/qint3/ternary/binary): the unused high bits in the
          // final packed byte must be zero (model: bits % 8 and data[-1] >> (bits % 8)).
          const bitsPer = DTYPE_BITS[dtype];
          if (bitsPer !== undefined && bitsPer < 8) {
            let nelem = 1;
            let safe = true;
            for (const d of shape) {
              nelem *= d;
              if (nelem > Number.MAX_SAFE_INTEGER) { safe = false; break; }
            }
            if (safe) {
              const totalBits = nelem * bitsPer;
              const rem = totalBits % 8;
              if (rem !== 0 && data.length > 0 && (data[data.length - 1] >> rem) !== 0) {
                throw new CowrieError(ERR_NON_CANONICAL, 'tensor sub-byte padding not zero');
              }
            }
          }
        }
        return SJ.tensor(dtype, shape, data);
      }
      // §2.3: TensorRef (0x21), Image (0x22), Audio (0x23), Node/Edge/batches
      // (0x35-0x38) are NOT core tags. Per SPEC §2.3 all non-core tags MUST reject
      // with ERR_RESERVED_TAG (in both lenient and strict). Their decode arms were
      // removed; they now fall through to the reserved-tag default.
      case Tag.BITMASK: {
        const count = this.readUvarintAsNumber();
        const byteLen = Math.ceil(count / 8);
        if (byteLen > this.limits.maxBytesLen) {
          throw new SecurityLimitExceeded(`Bitmask data too large: ${byteLen} > ${this.limits.maxBytesLen}`);
        }
        const bits = count > 0 ? this.read(byteLen) : new Uint8Array(0);
        if (this.strict) {
          const rem = count % 8;
          if (rem !== 0 && bits.length > 0 && (bits[bits.length - 1] >> rem) !== 0) {
            throw new CowrieError(ERR_NON_CANONICAL, 'bitmask trailing bits not zero');
          }
        }
        return SJ.bitmask(count, bits);
      }
      default:
        // v3 inline types
        if (tag >= Tag.FIXINT_BASE && tag <= Tag.FIXINT_MAX) {
          return SJ.int64(BigInt(tag - Tag.FIXINT_BASE));
        }
        if (tag >= Tag.FIXARRAY_BASE && tag <= Tag.FIXARRAY_MAX) {
          const count = tag - Tag.FIXARRAY_BASE;
          if (count > this.limits.maxArrayLen) {
            throw new SecurityLimitExceeded(`Array too large: ${count} > ${this.limits.maxArrayLen}`);
          }
          this.enterNested();
          const items: Value[] = [];
          for (let i = 0; i < count; i++) {
            items.push(this.decodeValue());
          }
          this.exitNested();
          return SJ.array(items);
        }
        if (tag >= Tag.FIXMAP_BASE && tag <= Tag.FIXMAP_MAX) {
          const count = tag - Tag.FIXMAP_BASE;
          if (count > this.limits.maxObjectLen) {
            throw new SecurityLimitExceeded(`Object too large: ${count} > ${this.limits.maxObjectLen}`);
          }
          this.enterNested();
          const members: Record<string, Value> = {};
          let lastIdx = -1;
          for (let i = 0; i < count; i++) {
            const fieldId = this.readUvarintAsNumber();
            if (fieldId >= this.dict.length) {
              throw new CowrieError(ERR_INVALID_FIELD_ID, `${fieldId} >= ${this.dict.length}`);
            }
            if (this.strict && fieldId <= lastIdx) {
              throw new CowrieError(ERR_NON_CANONICAL, 'object fields not in ascending dictIdx');
            }
            lastIdx = fieldId;
            const key = this.dict[fieldId];
            members[key] = this.decodeValue();
          }
          this.exitNested();
          return SJ.object(members);
        }
        if (tag >= Tag.FIXNEG_BASE && tag <= Tag.FIXNEG_MAX) {
          return SJ.int64(BigInt(-1 - (tag - Tag.FIXNEG_BASE)));
        }
        // §2.3: every tag not in the core set rejects with ERR_RESERVED_TAG. This
        // includes the formerly-skipped 0x30-0x34/0x39 (AdjList/RichText/Delta/
        // GraphShard) — no silent skip-as-null.
        throw new CowrieError(ERR_RESERVED_TAG, `0x${tag.toString(16).padStart(2, '0')}`);
    }
  }

  decode(): Value {
    // Header (SPEC-v1 §2.2: COWR + version + compression byte)
    const magic = this.read(4);
    if (magic[0] !== MAGIC[0] || magic[1] !== MAGIC[1] || magic[2] !== MAGIC[2] || magic[3] !== MAGIC[3]) {
      throw new CowrieError(ERR_INVALID_MAGIC, 'invalid magic bytes');
    }
    const version = this.readByte();
    if (version !== VERSION) {
      throw new CowrieError(ERR_INVALID_VERSION, `unsupported version: ${version}`);
    }
    this.readByte(); // compression byte (0x00 = none)

    // Dictionary
    const dictLen = this.readUvarintAsNumber();
    if (dictLen > this.limits.maxDictLen) {
      throw new SecurityLimitExceeded('cowrie: dictionary too large');
    }
    let prevKeyBytes: Uint8Array | null = null;
    for (let i = 0; i < dictLen; i++) {
      const startPos = this.pos;
      const keyLen = this.readUvarintAsNumber();
      if (keyLen > this.limits.maxStringLen) {
        throw new SecurityLimitExceeded(`String too long: ${keyLen} > ${this.limits.maxStringLen}`);
      }
      const rawKey = this.read(keyLen);
      const key = this.textDecoder.decode(rawKey);
      if (this.strict && prevKeyBytes !== null) {
        const cmp = compareBytes(rawKey, prevKeyBytes);
        if (cmp === 0) {
          throw new CowrieError(ERR_DUPLICATE_KEY, 'duplicate dictionary key');
        }
        if (cmp < 0) {
          throw new CowrieError(ERR_NON_CANONICAL, 'dictionary not byte-sorted');
        }
      }
      prevKeyBytes = rawKey;
      this.dict.push(key);
      void startPos;
    }

    let result: Value;
    try {
      result = this.decodeValue();
    } catch (e) {
      // §2.7 native-stack guard: a deeply nested message can overflow the JS engine's
      // call stack (RangeError) before our explicit depth counter trips. Surface that as
      // a clean ERR_TOO_DEEP rather than a crash — the input is over-deep by definition.
      if (e instanceof RangeError && /call stack/i.test(e.message)) {
        throw new CowrieError(ERR_TOO_DEEP, 'nesting depth exceeds native stack');
      }
      throw e;
    }

    // Verify all input consumed — trailing bytes indicate corruption or concatenated data
    if (this.pos < this.data.length) {
      const remaining = this.data.length - this.pos;
      throw new CowrieError(
        ERR_TRAILING_DATA,
        `${remaining} unconsumed bytes at position ${this.pos}`,
      );
    }

    return result;
  }

  private decodeNode(): NodeData {
    const id = this.readString();
    const labelCount = this.readUvarintAsNumber();
    if (labelCount > this.limits.maxArrayLen) {
      throw new SecurityLimitExceeded(`Node label count too large: ${labelCount} > ${this.limits.maxArrayLen}`);
    }
    const labels: string[] = [];
    for (let i = 0; i < labelCount; i++) {
      labels.push(this.readString());
    }
    const props = this.decodeProps();
    return { id, labels, props };
  }

  private decodeEdge(): EdgeData {
    const fromId = this.readString();
    const toId = this.readString();
    const edgeType = this.readString();
    const props = this.decodeProps();
    return { fromId, toId, edgeType, props };
  }

  private decodeProps(): Record<string, Value> {
    const propCount = this.readUvarintAsNumber();
    if (propCount > this.limits.maxObjectLen) {
      throw new SecurityLimitExceeded(`Props count too large: ${propCount} > ${this.limits.maxObjectLen}`);
    }
    const props: Record<string, Value> = {};
    for (let i = 0; i < propCount; i++) {
      const fieldId = this.readUvarintAsNumber();
      if (fieldId >= this.dict.length) {
        throw new Error(`Invalid dictionary index: ${fieldId} >= ${this.dict.length}`);
      }
      const key = this.dict[fieldId];
      props[key] = this.decodeValue();
    }
    return props;
  }
}

export function decode(data: Uint8Array, opts: DecodeOptions = {}): Value {
  return new Decoder(
    data,
    opts.onUnknownExt ?? UnknownExtBehavior.KEEP,
    resolveLimits(opts),
    opts.strict ?? false,
  ).decode();
}

/**
 * Locate every tensor's data run within a canonical message, in document order — the
 * Phase 2 zero-copy locator (mirrors the Python oracle cowrie_ref.tensor_spans). Decodes
 * the message and returns each tensor's { dtype, shape, dataOffset, dataLen }, where
 * dataOffset is the ABSOLUTE byte offset of the tensor's contiguous little-endian data run.
 */
export function tensorSpans(data: Uint8Array, opts: DecodeOptions = {}): TensorSpan[] {
  const dec = new Decoder(
    data,
    opts.onUnknownExt ?? UnknownExtBehavior.KEEP,
    resolveLimits(opts),
    opts.strict ?? false,
  );
  dec.decode();
  return dec.spans;
}

// JSON Bridge
const ISO8601_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;
const UUID_PATTERN = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
const MAX_SAFE_INT = 9007199254740991n;
const MIN_SAFE_INT = -9007199254740991n;

export function fromJSON(json: string): Value {
  return fromAny(JSON.parse(json));
}

export function fromAny(v: unknown, fieldName = ""): Value {
  if (v === null || v === undefined) return SJ.null();
  if (typeof v === "boolean") return SJ.bool(v);
  if (typeof v === "number") {
    if (Number.isInteger(v) && v >= Number.MIN_SAFE_INTEGER && v <= Number.MAX_SAFE_INTEGER) {
      return SJ.int64(BigInt(v));
    }
    return SJ.float64(v);
  }
  if (typeof v === "bigint") {
    return v >= 0n ? SJ.uint64(v) : SJ.int64(v);
  }
  if (typeof v === "string") {
    return inferStringType(v, fieldName);
  }
  if (v instanceof Uint8Array) return SJ.bytes(v);
  if (v instanceof Date) return SJ.datetime(v);
  if (Array.isArray(v)) {
    return SJ.array(v.map((item) => fromAny(item)));
  }
  if (typeof v === "object") {
    const obj = v as Record<string, unknown>;
    const typed = tryReconstructTyped(obj);
    if (typed !== null) return typed;
    // Canonicalize object key order so the same object encodes to identical bytes
    // across Go/Rust/Python/TS (cross-language content-addressing). Matches Go's
    // from_json key sort and Rust's BTreeMap ordering; same comparator the codec
    // uses elsewhere for deterministic key sorting.
    const members: Record<string, Value> = {};
    for (const key of Object.keys(obj).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))) {
      members[key] = fromAny(obj[key], key);
    }
    return SJ.object(members);
  }
  return SJ.string(String(v));
}

/** Helper: decode a base64 string field from a plain-object map. */
function b64FieldFromObj(obj: Record<string, unknown>, key: string): Uint8Array | null {
  const s = obj[key];
  if (typeof s !== "string") return null;
  try {
    if (typeof Buffer !== "undefined") {
      return new Uint8Array(Buffer.from(s, "base64"));
    }
    const bin = atob(s);
    const arr = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
    return arr;
  } catch {
    return null;
  }
}

/** Helper: get an integer from a number or string field.
 * If max is provided, throws if the value is not an integer in [0, max]. */
function intFieldFromObj(obj: Record<string, unknown>, key: string, max?: number): number | null {
  const v = obj[key];
  let n: number | null = null;
  if (typeof v === "number") n = Math.trunc(v);
  else if (typeof v === "bigint") n = Number(v);
  else return null;
  if (max !== undefined) {
    if (!Number.isInteger(n) || n < 0 || n > max) {
      throw new RangeError(`cowrie: ${key} out of range for uint${max < 256 ? 8 : max < 65536 ? 16 : 32} (got ${v})`);
    }
  }
  return n;
}

/**
 * Inspect a plain-object for a "_type" key and reconstruct the original typed Value.
 * Mirrors Go's tryReconstructTyped.  Returns null if not a typed projection.
 */
function tryReconstructTyped(obj: Record<string, unknown>): Value | null {
  const typeStr = obj["_type"];
  if (typeof typeStr !== "string") return null;

  switch (typeStr) {
    case "tensor": {
      const dtypeStr = obj["dtype"];
      if (typeof dtypeStr !== "string") return null;
      const dtype = stringToDtype(dtypeStr);
      if (dtype === null) return null;
      const dimsRaw = obj["dims"];
      if (!Array.isArray(dimsRaw)) return null;
      const shape: number[] = [];
      for (const d of dimsRaw) {
        if (typeof d !== "number") return null;
        shape.push(Math.trunc(d));
      }
      const data = b64FieldFromObj(obj, "data");
      if (data === null) return null;
      return SJ.tensor(dtype, shape, data);
    }

    case "tensor_ref": {
      const store = intFieldFromObj(obj, "store", 255);
      if (store === null) return null;
      const key = b64FieldFromObj(obj, "key");
      if (key === null) return null;
      return SJ.tensorRef(store, key);
    }

    case "image": {
      const fmtStr = obj["format"];
      if (typeof fmtStr !== "string") return null;
      const format = stringToImageFormat(fmtStr);
      if (format === null) return null;
      const width = intFieldFromObj(obj, "width", 65535);
      const height = intFieldFromObj(obj, "height", 65535);
      if (width === null || height === null) return null;
      const data = b64FieldFromObj(obj, "data");
      if (data === null) return null;
      return SJ.image(format, width, height, data);
    }

    case "audio": {
      const encStr = obj["encoding"];
      if (typeof encStr !== "string") return null;
      const encoding = stringToAudioEncoding(encStr);
      if (encoding === null) return null;
      const rate = intFieldFromObj(obj, "rate", 4294967295);
      const channels = intFieldFromObj(obj, "channels", 255);
      if (rate === null || channels === null) return null;
      const data = b64FieldFromObj(obj, "data");
      if (data === null) return null;
      return SJ.audio(encoding, rate, channels, data);
    }

    case "unknown_ext":
    case "ext": {
      const extTypeRaw = obj["ext_type"];
      if (extTypeRaw === undefined) return null;
      const extType = typeof extTypeRaw === "string" ? BigInt(extTypeRaw) : BigInt(extTypeRaw as number);
      const payload = b64FieldFromObj(obj, "payload");
      if (payload === null) return null;
      return SJ.unknownExt(extType, payload);
    }

    case "node": {
      const id = obj["id"];
      if (typeof id !== "string") return null;
      const labelsRaw = obj["labels"];
      if (!Array.isArray(labelsRaw)) return null;
      const labels: string[] = [];
      for (const l of labelsRaw) {
        if (typeof l !== "string") return null;
        labels.push(l);
      }
      const propsRaw = obj["props"];
      const props: Record<string, Value> = {};
      if (propsRaw !== null && propsRaw !== undefined && typeof propsRaw === "object") {
        for (const [k, pv] of Object.entries(propsRaw as Record<string, unknown>)) {
          props[k] = fromAny(pv, k);
        }
      }
      return SJ.node(id, labels, props);
    }

    case "edge": {
      const fromId = obj["fromId"];
      const toId = obj["toId"];
      const edgeType = obj["type"];
      if (typeof fromId !== "string" || typeof toId !== "string" || typeof edgeType !== "string") return null;
      const propsRaw = obj["props"];
      const props: Record<string, Value> = {};
      if (propsRaw !== null && propsRaw !== undefined && typeof propsRaw === "object") {
        for (const [k, pv] of Object.entries(propsRaw as Record<string, unknown>)) {
          props[k] = fromAny(pv, k);
        }
      }
      return SJ.edge(fromId, toId, edgeType, props);
    }

    case "node_batch": {
      const nodesRaw = obj["nodes"];
      if (!Array.isArray(nodesRaw)) return null;
      const nodes: NodeData[] = [];
      for (const nr of nodesRaw) {
        if (typeof nr !== "object" || nr === null) return null;
        const nv = tryReconstructTyped(nr as Record<string, unknown>);
        if (nv === null || nv.type !== Type.NODE) return null;
        nodes.push(nv.data as NodeData);
      }
      return SJ.nodeBatch(nodes);
    }

    case "edge_batch": {
      const edgesRaw = obj["edges"];
      if (!Array.isArray(edgesRaw)) return null;
      const edges: EdgeData[] = [];
      for (const er of edgesRaw) {
        if (typeof er !== "object" || er === null) return null;
        const ev = tryReconstructTyped(er as Record<string, unknown>);
        if (ev === null || ev.type !== Type.EDGE) return null;
        edges.push(ev.data as EdgeData);
      }
      return SJ.edgeBatch(edges);
    }

    case "bitmask": {
      const count = intFieldFromObj(obj, "count");
      if (count === null) return null;
      const bits = b64FieldFromObj(obj, "bits");
      if (bits === null) return null;
      return SJ.bitmask(count, bits);
    }
  }

  return null;
}

/** Map dtype string (as produced by Go) to DType enum. */
function stringToDtype(s: string): DType | null {
  switch (s) {
    case "float32":  return DType.FLOAT32;
    case "float16":  return DType.FLOAT16;
    case "bfloat16": return DType.BFLOAT16;
    case "float64":  return DType.FLOAT64;
    case "int8":     return DType.INT8;
    case "int16":    return DType.INT16;
    case "int32":    return DType.INT32;
    case "int64":    return DType.INT64;
    case "uint8":    return DType.UINT8;
    case "uint16":   return DType.UINT16;
    case "uint32":   return DType.UINT32;
    case "uint64":   return DType.UINT64;
    case "bool":     return DType.BOOL;
    case "qint4":    return DType.QINT4;
    case "qint2":    return DType.QINT2;
    case "qint3":    return DType.QINT3;
    case "ternary":  return DType.TERNARY;
    case "binary":   return DType.BINARY;
    default:         return null;
  }
}

/** Map image format string (as produced by Go) to ImageFormat enum. */
function stringToImageFormat(s: string): ImageFormat | null {
  switch (s) {
    case "jpeg": return ImageFormat.JPEG;
    case "png":  return ImageFormat.PNG;
    case "webp": return ImageFormat.WEBP;
    case "avif": return ImageFormat.AVIF;
    case "bmp":  return ImageFormat.BMP;
    default:     return null;
  }
}

/** Map audio encoding string (as produced by Go) to AudioEncoding enum. */
function stringToAudioEncoding(s: string): AudioEncoding | null {
  switch (s) {
    case "pcm_int16":   return AudioEncoding.PCM_INT16;
    case "pcm_float32": return AudioEncoding.PCM_FLOAT32;
    case "opus":        return AudioEncoding.OPUS;
    case "aac":         return AudioEncoding.AAC;
    default:            return null;
  }
}

function inferStringType(s: string, fieldName: string): Value {
  const lowerField = fieldName.toLowerCase();

  // Date inference. `new Date(s)` returns Invalid Date on bad input (NaN
  // getTime) rather than throwing, so no try/catch is needed.
  if (
    lowerField.includes("time") ||
    lowerField.includes("date") ||
    lowerField.endsWith("_at") ||
    ["created", "updated"].includes(lowerField)
  ) {
    if (ISO8601_PATTERN.test(s)) {
      const d = new Date(s);
      if (!isNaN(d.getTime())) return SJ.datetime(d);
    }
  }

  if (ISO8601_PATTERN.test(s)) {
    const d = new Date(s);
    if (!isNaN(d.getTime())) return SJ.datetime(d);
  }

  if (UUID_PATTERN.test(s)) {
    try {
      return SJ.uuid128(s);
    } catch (e) {
      if (!(e instanceof RangeError || e instanceof TypeError)) throw e;
      // Malformed UUID: fall through to plain string.
    }
  }

  return SJ.string(s);
}

export function toJSON(v: Value): string {
  return JSON.stringify(toAny(v));
}

export function toAny(v: Value): unknown {
  switch (v.type) {
    case Type.NULL:
      return null;
    case Type.BOOL:
      return v.data;
    case Type.INT64: {
      const n = v.data as bigint;
      if (n >= MIN_SAFE_INT && n <= MAX_SAFE_INT) return Number(n);
      return n.toString();
    }
    case Type.UINT64: {
      const n = v.data as bigint;
      if (n <= MAX_SAFE_INT) return Number(n);
      return n.toString();
    }
    case Type.FLOAT64:
      return v.data;
    case Type.DECIMAL128: {
      const d = v.data as Decimal128;
      const coefInt = bytesToBigint(d.coef);
      // Simple decimal string conversion
      const str = coefInt.toString();
      if (d.scale <= 0) return str + "0".repeat(-d.scale);
      if (d.scale >= str.length) return "0." + "0".repeat(d.scale - str.length) + str;
      return str.slice(0, -d.scale) + "." + str.slice(-d.scale);
    }
    case Type.STRING:
      return v.data;
    case Type.BYTES: {
      // Base64 encode
      const bytes = v.data as Uint8Array;
      let binary = "";
      for (const b of bytes) binary += String.fromCharCode(b);
      return btoa(binary);
    }
    case Type.DATETIME64: {
      const nanos = v.data as bigint;
      const ms = Number(nanos / 1000000n);
      return new Date(ms).toISOString();
    }
    case Type.UUID128:
      return formatUUID(v.data as Uint8Array);
    case Type.BIGINT:
      return bytesToBigint(v.data as Uint8Array).toString();
    case Type.ARRAY:
      return (v.data as Value[]).map(toAny);
    case Type.OBJECT: {
      const result: Record<string, unknown> = {};
      for (const [key, val] of Object.entries(v.data as Record<string, Value>)) {
        result[key] = toAny(val);
      }
      return result;
    }
    case Type.TENSOR: {
      const t = v.data as TensorData;
      // Base64 encode the data
      let binary = "";
      for (const b of t.data) binary += String.fromCharCode(b);
      return {
        _type: "tensor",
        dtype: DType[t.dtype]?.toLowerCase() ?? t.dtype,
        dims: t.shape,
        data: btoa(binary),
      };
    }
    case Type.IMAGE: {
      const img = v.data as ImageData;
      let binary = "";
      for (const b of img.data) binary += String.fromCharCode(b);
      return {
        _type: "image",
        format: ImageFormat[img.format]?.toLowerCase() ?? img.format,
        width: img.width,
        height: img.height,
        data: btoa(binary),
      };
    }
    case Type.AUDIO: {
      const aud = v.data as AudioData;
      let binary = "";
      for (const b of aud.data) binary += String.fromCharCode(b);
      return {
        _type: "audio",
        encoding: AudioEncoding[aud.encoding]?.toLowerCase() ?? aud.encoding,
        rate: aud.sampleRate,
        channels: aud.channels,
        data: btoa(binary),
      };
    }
    case Type.TENSOR_REF: {
      const ref = v.data as TensorRefData;
      let binary = "";
      for (const b of ref.key) binary += String.fromCharCode(b);
      return {
        _type: "tensor_ref",
        store: ref.storeId,
        key: btoa(binary),
      };
    }
    // Graph types
    case Type.NODE: {
      const node = v.data as NodeData;
      const propsJson: Record<string, unknown> = {};
      for (const [key, val] of Object.entries(node.props)) {
        propsJson[key] = toAny(val);
      }
      return {
        _type: "node",
        id: node.id,
        labels: node.labels,
        props: propsJson,
      };
    }
    case Type.EDGE: {
      const edge = v.data as EdgeData;
      const propsJson: Record<string, unknown> = {};
      for (const [key, val] of Object.entries(edge.props)) {
        propsJson[key] = toAny(val);
      }
      return {
        _type: "edge",
        fromId: edge.fromId,
        toId: edge.toId,
        type: edge.edgeType,
        props: propsJson,
      };
    }
    case Type.NODE_BATCH: {
      const batch = v.data as NodeBatchData;
      return {
        _type: "node_batch",
        nodes: batch.nodes.map((n) => ({
          _type: "node",
          id: n.id,
          labels: n.labels,
          props: Object.fromEntries(
            Object.entries(n.props).map(([k, val]) => [k, toAny(val)])
          ),
        })),
      };
    }
    case Type.EDGE_BATCH: {
      const batch = v.data as EdgeBatchData;
      return {
        _type: "edge_batch",
        edges: batch.edges.map((e) => ({
          _type: "edge",
          fromId: e.fromId,
          toId: e.toId,
          type: e.edgeType,
          props: Object.fromEntries(
            Object.entries(e.props).map(([k, val]) => [k, toAny(val)])
          ),
        })),
      };
    }
    case Type.BITMASK: {
      const bm = v.data as BitmaskData;
      // Base64-encode the packed bits (matches Go's base64.StdEncoding)
      let binary = "";
      for (const b of bm.bits) binary += String.fromCharCode(b);
      return {
        _type: "bitmask",
        count: bm.count,
        bits: btoa(binary),
      };
    }
    case Type.UNKNOWN_EXT: {
      const ext = v.data as UnknownExtData;
      let binary = "";
      for (const b of ext.payload) binary += String.fromCharCode(b);
      const extVal = ext.extType <= MAX_SAFE_INT ? Number(ext.extType) : ext.extType.toString();
      return {
        _type: "unknown_ext",
        ext_type: extVal,
        payload: btoa(binary),
      };
    }
  }
}

// Convenience functions
export function dumps(obj: unknown): Uint8Array {
  return encode(fromAny(obj));
}

export function loads(data: Uint8Array): unknown {
  return toAny(decode(data));
}

// ============================================================
// Content Address (SPEC-v1 §3)
// ============================================================

import { createHash } from 'node:crypto';

/**
 * Content address (§3) of already-canonical, uncompressed wire bytes: a multihash
 * SHA-256 — 0x12 0x20 (sha2-256 code, 32-byte length) followed by the 32 digest bytes.
 */
export function addressOfBytes(canonical: Uint8Array): Uint8Array {
  const digest = createHash('sha256').update(canonical).digest();
  const out = new Uint8Array(34);
  out[0] = 0x12;
  out[1] = 0x20;
  out.set(digest, 2);
  return out;
}

/**
 * Content address (§3) of a value: encode it canonically, then addressOfBytes.
 * Equal canonical bytes => equal address, across every conforming impl.
 */
export function contentAddress(value: Value): Uint8Array {
  return addressOfBytes(encode(value));
}

/** Lowercase hex (68 chars) of a content address. */
export function contentAddressHex(value: Value): string {
  return Buffer.from(contentAddress(value)).toString('hex');
}

// ============================================================
// Cowrie FILE container + Merkle file identity (SPEC-v1 §7)
//
// A Cowrie file packages an ordered list of frames (each frame is one complete,
// canonical §2.2 COWR value's wire bytes) with a sealed footer index for O(1)
// random access, and a Merkle root over the frames that is the file's content
// identity. Mirrors tools/cowrie_ref/file.py exactly.
//
// Layout (multi-byte ints little-endian; uvarint = §2.1):
//   "CWRF"        file magic
//   version  u8 (0x01)
//   reserved u8 (0x00)
//   uvarint  frame_count
//   repeat frame_count times: uvarint frame_len, frame_bytes
//   FOOTER (@ footer_offset):
//     uvarint frame_count
//     repeat: uvarint frame_offset, uvarint frame_len   (offset = file-start to frame_bytes)
//     merkle_root 34 bytes
//   u64 LE  footer_offset
//   "CWRF"        end magic
//
// Merkle (RFC 6962 domain separation, promote-odd — never duplicate; count bound in root):
//   leaf(f)    = SHA-256(0x00 || f)
//   node(a, b) = SHA-256(0x01 || a || b)
//   tree_digest = promote-odd reduction of the leaves (empty file: SHA-256(b""))
//   root_digest = SHA-256(0x02 || uvarint(frame_count) || tree_digest)
//   merkle_root = 0x12 0x20 || root_digest
// ============================================================

const FILE_MAGIC = new Uint8Array([0x43, 0x57, 0x52, 0x46]); // 'CWRF'
const FILE_VERSION = 1;
const FILE_MAX_FRAMES = 2 ** 32; // §7 frame-count bound (DoS guard); JS << wraps at 32 bits

function sha256(data: Uint8Array): Uint8Array {
  return new Uint8Array(createHash('sha256').update(data).digest());
}

function concatBytes(...parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

function merkleLeaf(frame: Uint8Array): Uint8Array {
  return sha256(concatBytes(new Uint8Array([0x00]), frame));
}

function merkleNode(a: Uint8Array, b: Uint8Array): Uint8Array {
  return sha256(concatBytes(new Uint8Array([0x01]), a, b));
}

function treeDigest(frames: Uint8Array[]): Uint8Array {
  if (frames.length === 0) {
    return sha256(new Uint8Array(0));
  }
  let level = frames.map(merkleLeaf);
  while (level.length > 1) {
    const next: Uint8Array[] = [];
    for (let i = 0; i + 1 < level.length; i += 2) {
      next.push(merkleNode(level[i], level[i + 1]));
    }
    if (level.length % 2 === 1) {
      next.push(level[level.length - 1]); // promote the odd node unchanged (never duplicate)
    }
    level = next;
  }
  return level[0];
}

/**
 * Multihash SHA-256 Merkle root over the ordered frame byte-blobs (§7), with the
 * frame count bound into the root (0x02 domain) so distinct frame lists can never
 * share a root. Returns the 34-byte multihash (0x12 0x20 || root_digest).
 */
export function merkleRoot(frames: Uint8Array[]): Uint8Array {
  const root = sha256(
    concatBytes(new Uint8Array([0x02]), encodeUvarint(frames.length), treeDigest(frames)),
  );
  return concatBytes(new Uint8Array([0x12, 0x20]), root);
}

/**
 * Build a Cowrie file from an ordered list of frames. Each frame must be one
 * complete, canonical §2.2 COWR value's wire bytes (uncompressed). Re-encoding the
 * frames returned by decodeFile reproduces a canonical file byte-for-byte.
 */
export function encodeFile(frames: Uint8Array[]): Uint8Array {
  const parts: Uint8Array[] = [];
  parts.push(FILE_MAGIC);
  parts.push(new Uint8Array([FILE_VERSION, 0x00])); // version, reserved
  parts.push(encodeUvarint(frames.length));

  // Track the running byte length to compute frame offsets (file-start to frame_bytes).
  let pos = FILE_MAGIC.length + 2 + encodeUvarint(frames.length).length;
  const offsets: number[] = [];
  for (const f of frames) {
    const lenPrefix = encodeUvarint(f.length);
    parts.push(lenPrefix);
    pos += lenPrefix.length;
    // Align the frame start to a 64-byte file offset (§7) so in-frame tensors stay
    // 64-aligned within the file. pad = (-pos) mod 64 zero bytes after frame_len.
    const framePad = ((ALIGN - (pos % ALIGN)) % ALIGN);
    if (framePad > 0) {
      parts.push(new Uint8Array(framePad));
      pos += framePad;
    }
    offsets.push(pos); // offset to frame_bytes (64-byte aligned)
    parts.push(f);
    pos += f.length;
  }
  const footerOffset = pos;

  // Footer (self-contained).
  parts.push(encodeUvarint(frames.length));
  for (let i = 0; i < frames.length; i++) {
    parts.push(encodeUvarint(offsets[i]));
    parts.push(encodeUvarint(frames[i].length));
  }
  parts.push(merkleRoot(frames));

  const footerOffsetBytes = new Uint8Array(8);
  new DataView(footerOffsetBytes.buffer).setBigUint64(0, BigInt(footerOffset), true);
  parts.push(footerOffsetBytes);
  parts.push(FILE_MAGIC);
  return concatBytes(...parts);
}

/**
 * Return the ordered frame byte-blobs, fully validated (§7). The BODY is the source
 * of truth: frames are read sequentially via their length prefixes, and the footer
 * MUST exactly mirror that layout (count, contiguous offsets/lengths) — so there is
 * exactly one canonical file byte-string per ordered frame list. With verify=true
 * (default) the stored Merkle root must also match.
 */
export function decodeFile(data: Uint8Array, verify = true): Uint8Array[] {
  if (data.length < 4 + 1 + 1 + 1 + 8 + 4) {
    throw new CowrieError('ERR_TRUNCATED', 'file too short');
  }
  const eq4 = (off: number) =>
    data[off] === FILE_MAGIC[0] && data[off + 1] === FILE_MAGIC[1] &&
    data[off + 2] === FILE_MAGIC[2] && data[off + 3] === FILE_MAGIC[3];
  if (!eq4(0)) throw new CowrieError('ERR_INVALID_MAGIC', 'bad file magic');
  if (!eq4(data.length - 4)) throw new CowrieError('ERR_INVALID_MAGIC', 'bad end magic');
  if (data[4] !== FILE_VERSION) throw new CowrieError('ERR_INVALID_MAGIC', 'bad file version');
  if (data[5] !== 0x00) throw new CowrieError(ERR_NON_CANONICAL, 'reserved byte must be 0');

  const footerOffset = Number(
    new DataView(data.buffer, data.byteOffset + data.length - 12, 8).getBigUint64(0, true),
  );
  if (!(footerOffset > 6 && footerOffset <= data.length - 12)) {
    throw new CowrieError('ERR_TRUNCATED', 'footer offset out of range');
  }

  // --- body: read frames sequentially (the source of truth) ---
  let pos = 6;
  let nBig: bigint;
  [nBig, pos] = decodeUvarint(data, pos);
  if (nBig > BigInt(FILE_MAX_FRAMES)) {
    throw new CowrieError('ERR_TRUNCATED', 'frame count exceeds limit');
  }
  const n = Number(nBig);
  const frames: Uint8Array[] = [];
  const layout: [number, number][] = [];
  for (let i = 0; i < n; i++) {
    let flenBig: bigint;
    [flenBig, pos] = decodeUvarint(data, pos);
    const flen = Number(flenBig);
    // Frame bytes begin at a 64-byte file offset (§7): after frame_len, consume the
    // (-pos) mod 64 zero-pad and verify it is all zero (canonical).
    const framePad = ((ALIGN - (pos % ALIGN)) % ALIGN);
    for (let p = 0; p < framePad; p++) {
      if (data[pos + p] !== 0) {
        throw new CowrieError(ERR_NON_CANONICAL, 'frame alignment padding not zero');
      }
    }
    pos += framePad;
    if (pos + flen > footerOffset) {
      throw new CowrieError('ERR_TRUNCATED', 'frame extends past footer');
    }
    layout.push([pos, flen]);
    frames.push(data.slice(pos, pos + flen));
    pos += flen;
  }
  if (pos !== footerOffset) {
    throw new CowrieError(ERR_NON_CANONICAL, 'body does not end exactly at the footer');
  }

  // --- footer: must mirror the body layout exactly (canonical) ---
  let fcountBig: bigint;
  [fcountBig, pos] = decodeUvarint(data, pos);
  if (Number(fcountBig) !== n) {
    throw new CowrieError(ERR_NON_CANONICAL, 'footer frame count != header');
  }
  for (const [off, flen] of layout) {
    let foffBig: bigint;
    let flen2Big: bigint;
    [foffBig, pos] = decodeUvarint(data, pos);
    [flen2Big, pos] = decodeUvarint(data, pos);
    if (Number(foffBig) !== off || Number(flen2Big) !== flen) {
      throw new CowrieError(ERR_NON_CANONICAL, 'footer index does not mirror body layout');
    }
  }
  const storedRoot = data.slice(pos, pos + 34);
  pos += 34;
  if (pos !== data.length - 12) {
    throw new CowrieError(ERR_NON_CANONICAL, 'footer length not canonical');
  }
  if (verify && compareBytes(merkleRoot(frames), storedRoot) !== 0) {
    throw new CowrieError(ERR_NON_CANONICAL, 'merkle root mismatch (tampered or non-canonical)');
  }
  return frames;
}

/** The file's content identity = its verified Merkle root (34-byte multihash). */
export function fileIdentity(data: Uint8Array): Uint8Array {
  return merkleRoot(decodeFile(data, true));
}

// ============================================================
// Deterministic Encoding
// ============================================================

export interface EncodeOptions {
  deterministic?: boolean;
  omitNull?: boolean;
}

class DeterministicEncoder extends Encoder {
  private sortedBuf: number[] = [];
  private sortedDictKeys: string[] = [];
  private sortedDictLookup = new Map<string, number>();
  private opts: EncodeOptions;

  constructor(opts: EncodeOptions = {}) {
    super();
    this.opts = opts;
  }

  private addKeySorted(key: string): number {
    const existing = this.sortedDictLookup.get(key);
    if (existing !== undefined) return existing;
    const idx = this.sortedDictKeys.length;
    this.sortedDictKeys.push(key);
    this.sortedDictLookup.set(key, idx);
    return idx;
  }

  private collectKeysSorted(v: Value): void {
    if (v.type === Type.ARRAY) {
      for (const item of v.data as Value[]) {
        this.collectKeysSorted(item);
      }
    } else if (v.type === Type.OBJECT) {
      // Use byte-order comparison for locale-independent deterministic sorting
      let entries = Object.entries(v.data as Record<string, Value>).sort((a, b) =>
        a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0
      );
      // Filter out null values if omitNull is set
      if (this.opts.omitNull) {
        entries = entries.filter(([_, val]) => val.type !== Type.NULL);
      }
      for (const [key, val] of entries) {
        this.addKeySorted(key);
        this.collectKeysSorted(val);
      }
    } else if (v.type === Type.NODE) {
      this.collectPropKeysSorted((v.data as NodeData).props);
    } else if (v.type === Type.EDGE) {
      this.collectPropKeysSorted((v.data as EdgeData).props);
    } else if (v.type === Type.NODE_BATCH) {
      for (const node of (v.data as NodeBatchData).nodes) {
        this.collectPropKeysSorted(node.props);
      }
    } else if (v.type === Type.EDGE_BATCH) {
      for (const edge of (v.data as EdgeBatchData).edges) {
        this.collectPropKeysSorted(edge.props);
      }
    }
  }

  private collectPropKeysSorted(props: Record<string, Value>): void {
    const keys = Object.keys(props).sort((a, b) => a < b ? -1 : a > b ? 1 : 0);
    for (const key of keys) {
      this.addKeySorted(key);
      this.collectKeysSorted(props[key]);
    }
  }

  private writeSorted(data: Uint8Array): void {
    for (const b of data) this.sortedBuf.push(b);
  }

  private writeByteSorted(b: number): void {
    this.sortedBuf.push(b);
  }

  private writeUvarintSorted(n: bigint | number): void {
    this.writeSorted(encodeUvarint(n));
  }

  private writeStringSorted(s: string): void {
    const bytes = sharedTextEncoder.encode(s);
    this.writeUvarintSorted(bytes.length);
    this.writeSorted(bytes);
  }

  private encodeValueSorted(v: Value): void {
    switch (v.type) {
      case Type.NULL:
        this.writeByteSorted(Tag.NULL);
        break;
      case Type.BOOL:
        this.writeByteSorted(v.data ? Tag.TRUE : Tag.FALSE);
        break;
      case Type.INT64: {
        const i64 = v.data as bigint;
        // v3 inline encoding: FIXINT for 0-127, FIXNEG for -1 to -16
        if (i64 >= 0n && i64 <= 127n) {
          this.writeByteSorted(Tag.FIXINT_BASE + Number(i64));
          break;
        }
        if (i64 >= -16n && i64 <= -1n) {
          this.writeByteSorted(Tag.FIXNEG_BASE + Number(-1n - i64));
          break;
        }
        this.writeByteSorted(Tag.INT64);
        this.writeUvarintSorted(zigzagEncode(i64));
        break;
      }
      case Type.UINT64: {
        const u64 = v.data as bigint;
        if (u64 < 0n || u64 > 0xFFFFFFFFFFFFFFFFn) {
          throw new RangeError(`UINT64 value out of range: ${u64} (must be 0..2^64-1)`);
        }
        this.writeByteSorted(Tag.UINT64);
        this.writeUvarintSorted(u64);
        break;
      }
      case Type.FLOAT64: {
        this.writeByteSorted(Tag.FLOAT64);
        this.writeSorted(encodeFloat64Bytes(v.data as number));
        break;
      }
      case Type.DECIMAL128: {
        const d = v.data as Decimal128;
        this.writeByteSorted(Tag.DECIMAL128);
        // §2.3: scale is a SVARINT (zigzag + LEB128), NOT a raw int8 byte. A raw byte
        // desyncs the stream for |scale| > 63 and diverges from the main encoder
        // (index.ts:777) and cowrie_ref encode.py:60.
        this.writeUvarintSorted(zigzagEncode(BigInt(d.scale)));
        this.writeSorted(d.coef);
        break;
      }
      case Type.STRING:
        this.writeByteSorted(Tag.STRING);
        this.writeStringSorted(v.data as string);
        break;
      case Type.BYTES: {
        const bytes = v.data as Uint8Array;
        this.writeByteSorted(Tag.BYTES);
        this.writeUvarintSorted(bytes.length);
        this.writeSorted(bytes);
        break;
      }
      case Type.DATETIME64: {
        this.writeByteSorted(Tag.DATETIME64);
        const buf = new ArrayBuffer(8);
        new DataView(buf).setBigInt64(0, v.data as bigint, true);
        this.writeSorted(new Uint8Array(buf));
        break;
      }
      case Type.UUID128:
        this.writeByteSorted(Tag.UUID128);
        this.writeSorted(v.data as Uint8Array);
        break;
      case Type.BIGINT: {
        const bytes = v.data as Uint8Array;
        this.writeByteSorted(Tag.BIGINT);
        this.writeUvarintSorted(bytes.length);
        this.writeSorted(bytes);
        break;
      }
      case Type.ARRAY: {
        const arr = v.data as Value[];
        // v3 inline encoding: FIXARRAY for length 0-15
        if (arr.length <= 15) {
          this.writeByteSorted(Tag.FIXARRAY_BASE + arr.length);
        } else {
          this.writeByteSorted(Tag.ARRAY);
          this.writeUvarintSorted(arr.length);
        }
        for (const item of arr) this.encodeValueSorted(item);
        break;
      }
      case Type.OBJECT: {
        const obj = v.data as Record<string, Value>;
        // Use byte-order comparison for locale-independent deterministic sorting
        let entries = Object.entries(obj).sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0);
        // Filter out null values if omitNull is set
        if (this.opts.omitNull) {
          entries = entries.filter(([_, val]) => val.type !== Type.NULL);
        }
        // v3 inline encoding: FIXMAP for count 0-15
        if (entries.length <= 15) {
          this.writeByteSorted(Tag.FIXMAP_BASE + entries.length);
        } else {
          this.writeByteSorted(Tag.OBJECT);
          this.writeUvarintSorted(entries.length);
        }
        for (const [key, val] of entries) {
          const idx = this.sortedDictLookup.get(key)!;
          this.writeUvarintSorted(idx);
          this.encodeValueSorted(val);
        }
        break;
      }
      case Type.TENSOR: {
        const t = v.data as TensorData;
        this.writeByteSorted(Tag.TENSOR);
        this.writeByteSorted(t.dtype);
        this.writeUvarintSorted(t.shape.length);
        for (const dim of t.shape) {
          this.writeUvarintSorted(dim);
        }
        this.writeUvarintSorted(t.data.length);
        // 64-byte tensor data alignment (§2.5): pad = (-offset) mod 64 zero bytes after
        // dataLen. sortedBuf.length is the absolute offset right after dataLen.
        const detPad = ((ALIGN - (this.sortedBuf.length % ALIGN)) % ALIGN);
        for (let i = 0; i < detPad; i++) this.writeByteSorted(0);
        this.writeSorted(t.data);
        break;
      }
      case Type.IMAGE: {
        const img = v.data as ImageData;
        this.writeByteSorted(Tag.IMAGE);
        this.writeByteSorted(img.format);
        const buf = new ArrayBuffer(4);
        const view = new DataView(buf);
        view.setUint16(0, img.width, true);
        view.setUint16(2, img.height, true);
        this.writeSorted(new Uint8Array(buf));
        this.writeUvarintSorted(img.data.length);
        this.writeSorted(img.data);
        break;
      }
      case Type.AUDIO: {
        const aud = v.data as AudioData;
        this.writeByteSorted(Tag.AUDIO);
        this.writeByteSorted(aud.encoding);
        const buf = new ArrayBuffer(4);
        new DataView(buf).setUint32(0, aud.sampleRate, true);
        this.writeSorted(new Uint8Array(buf));
        this.writeByteSorted(aud.channels);
        this.writeUvarintSorted(aud.data.length);
        this.writeSorted(aud.data);
        break;
      }
      case Type.TENSOR_REF: {
        const ref = v.data as TensorRefData;
        this.writeByteSorted(Tag.TENSOR_REF);
        this.writeByteSorted(ref.storeId);
        this.writeUvarintSorted(ref.key.length);
        this.writeSorted(ref.key);
        break;
      }
      case Type.NODE: {
        this.writeByteSorted(Tag.NODE);
        this.encodeNodeSorted(v.data as NodeData);
        break;
      }
      case Type.EDGE: {
        this.writeByteSorted(Tag.EDGE);
        this.encodeEdgeSorted(v.data as EdgeData);
        break;
      }
      case Type.NODE_BATCH: {
        const batch = v.data as NodeBatchData;
        this.writeByteSorted(Tag.NODE_BATCH);
        this.writeUvarintSorted(batch.nodes.length);
        for (const node of batch.nodes) this.encodeNodeSorted(node);
        break;
      }
      case Type.EDGE_BATCH: {
        const batch = v.data as EdgeBatchData;
        this.writeByteSorted(Tag.EDGE_BATCH);
        this.writeUvarintSorted(batch.edges.length);
        for (const edge of batch.edges) this.encodeEdgeSorted(edge);
        break;
      }
      case Type.BITMASK: {
        const bm = v.data as BitmaskData;
        this.writeByteSorted(Tag.BITMASK);
        this.writeUvarintSorted(bm.count);
        this.writeSorted(bm.bits);
        break;
      }
    }
  }

  private encodeNodeSorted(node: NodeData): void {
    this.writeStringSorted(node.id);
    this.writeUvarintSorted(node.labels.length);
    for (const label of node.labels) this.writeStringSorted(label);
    this.encodePropsSorted(node.props);
  }

  private encodeEdgeSorted(edge: EdgeData): void {
    this.writeStringSorted(edge.fromId);
    this.writeStringSorted(edge.toId);
    this.writeStringSorted(edge.edgeType);
    this.encodePropsSorted(edge.props);
  }

  private encodePropsSorted(props: Record<string, Value>): void {
    const entries = Object.entries(props).sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0);
    this.writeUvarintSorted(entries.length);
    for (const [key, val] of entries) {
      const idx = this.sortedDictLookup.get(key);
      if (idx === undefined) {
        throw new Error(`Internal: graph prop key "${key}" missing from sorted dictionary`);
      }
      this.writeUvarintSorted(idx);
      this.encodeValueSorted(val);
    }
  }

  encodeWithOpts(v: Value): Uint8Array {
    if (!this.opts.deterministic) {
      return this.encode(v);
    }

    this.collectKeysSorted(v);

    // Header
    this.writeSorted(MAGIC);
    this.writeByteSorted(VERSION);
    this.writeByteSorted(0); // flags

    // Dictionary
    this.writeUvarintSorted(this.sortedDictKeys.length);
    for (const key of this.sortedDictKeys) {
      this.writeStringSorted(key);
    }

    // Root value
    this.encodeValueSorted(v);

    return new Uint8Array(this.sortedBuf);
  }
}

export function encodeWithOpts(v: Value, opts: EncodeOptions = {}): Uint8Array {
  return new DeterministicEncoder(opts).encodeWithOpts(v);
}

// ============================================================
// Schema Fingerprinting (FNV-1a)
// ============================================================

const FNV_OFFSET_BASIS = 14695981039346656037n;
const FNV_PRIME = 1099511628211n;

// §4.1 fingerprint codes (FPC) — a namespace DISTINCT from §2.3 wire tags and from any
// host enum/iota (the B4 root bug). Mirrors tools/cowrie_ref/model.py FPC exactly.
const FPC = {
  null: 0x00, bool: 0x01, int: 0x02, uint: 0x03, bigint: 0x04, float: 0x05,
  decimal: 0x06, string: 0x07, bytes: 0x08, uuid: 0x09, datetime: 0x0a,
  array: 0x0b, object: 0x0c, tensor: 0x0d, bitmask: 0x0e, extension: 0x0f,
} as const;

/** FNV-1a-64 a single byte into the running hash (§4.3). */
function fnvHashByte(h: bigint, b: number): bigint {
  h ^= BigInt(b & 0xff);
  h = BigInt.asUintN(64, h * FNV_PRIME);
  return h;
}

/** FNV-1a-64 a minimal LEB128 uvarint into the running hash (§4.2 lengths/counts). */
function fnvHashUvarint(h: bigint, n: bigint | number): bigint {
  for (const b of encodeUvarint(n)) {
    h = fnvHashByte(h, b);
  }
  return h;
}

/**
 * §4 schema fingerprint, FPC grammar (mirrors tools/cowrie_ref/fingerprint.py `_fp`).
 * Hashes TYPE STRUCTURE ONLY using the §4.1 FPC codes — never wire tags or iota ordinals,
 * never values. Collection sizes, object keys, tensor dtype+rank+shape, bitmask count, and
 * extension type are structural; all other payload is excluded. Lengths/counts/dims are
 * MINIMAL LEB128 uvarints (not fixed 8-byte words).
 */
function hashSchema(v: Value, h: bigint): bigint {
  switch (v.type) {
    case Type.NULL:
      return fnvHashByte(h, FPC.null);
    case Type.BOOL:
      return fnvHashByte(h, FPC.bool);
    case Type.INT64:
      return fnvHashByte(h, FPC.int);
    case Type.UINT64:
      return fnvHashByte(h, FPC.uint);
    case Type.BIGINT:
      return fnvHashByte(h, FPC.bigint);
    case Type.FLOAT64:
      return fnvHashByte(h, FPC.float);
    case Type.DECIMAL128:
      return fnvHashByte(h, FPC.decimal);
    case Type.STRING:
      return fnvHashByte(h, FPC.string);
    case Type.BYTES:
      return fnvHashByte(h, FPC.bytes);
    case Type.UUID128:
      return fnvHashByte(h, FPC.uuid);
    case Type.DATETIME64:
      return fnvHashByte(h, FPC.datetime);

    case Type.BITMASK: {
      // §4.2: bitmask collection size is structural.
      const bm = v.data as BitmaskData;
      h = fnvHashByte(h, FPC.bitmask);
      return fnvHashUvarint(h, bm.count);
    }

    case Type.TENSOR: {
      // §4.2: dtype + rank + shape are type structure (dims hashed, data excluded).
      const t = v.data as TensorData;
      h = fnvHashByte(h, FPC.tensor);
      h = fnvHashByte(h, t.dtype);
      h = fnvHashByte(h, t.shape.length);
      for (const d of t.shape) {
        h = fnvHashUvarint(h, d);
      }
      return h;
    }

    case Type.UNKNOWN_EXT: {
      // §4.2: extType is structure, payload excluded.
      const ext = v.data as UnknownExtData;
      h = fnvHashByte(h, FPC.extension);
      return fnvHashUvarint(h, ext.extType);
    }

    case Type.ARRAY: {
      const arr = v.data as Value[];
      h = fnvHashByte(h, FPC.array);
      h = fnvHashUvarint(h, arr.length);
      for (const item of arr) {
        h = hashSchema(item, h);
      }
      return h;
    }

    case Type.OBJECT: {
      const obj = v.data as Record<string, Value>;
      // Canonical key order: ascending by raw UTF-8 bytes (§2.4 / sorted_keys).
      const keys = Object.keys(obj).sort((a, b) =>
        compareBytes(sharedTextEncoder.encode(a), sharedTextEncoder.encode(b)));
      h = fnvHashByte(h, FPC.object);
      h = fnvHashUvarint(h, keys.length);
      for (const key of keys) {
        const raw = sharedTextEncoder.encode(key);
        h = fnvHashUvarint(h, raw.length);
        for (const b of raw) h = fnvHashByte(h, b);
        h = hashSchema(obj[key], h);
      }
      return h;
    }

    default:
      // No core value has any other Type; TensorRef/Image/Audio/graph types are not
      // part of SPEC-v1 §4 (their decode arms reject as reserved tags).
      throw new CowrieError(ERR_RESERVED_TAG, `fingerprint: unsupported type ${v.type}`);
  }
}

/**
 * Compute a 64-bit FNV-1a fingerprint of the value's schema.
 * The fingerprint captures type structure (field names, types)
 * but not actual values.
 */
export function schemaFingerprint64(v: Value): bigint {
  return hashSchema(v, FNV_OFFSET_BASIS);
}

/**
 * Returns the low 32 bits of the 64-bit schema fingerprint.
 * Suitable for use as a type ID in stream frames.
 */
export function schemaFingerprint32(v: Value): number {
  return Number(schemaFingerprint64(v) & 0xffffffffn);
}

/**
 * Check if two values have structurally identical schemas.
 * Recursively compares types, object field names, array element schemas,
 * tensor dtype/rank, image format, audio encoding/channels, tensor_ref storeId,
 * and unknown-ext type — mirroring exactly what schemaFingerprint64 hashes.
 * Actual values (ints, strings, tensor data, etc.) are not compared.
 *
 * Use schemaFingerprintEqual for a faster probabilistic check.
 */
export function schemaEquals(a: Value, b: Value): boolean {
  if (a.type !== b.type) return false;

  switch (a.type) {
    case Type.NULL:
    case Type.BOOL:
    case Type.INT64:
    case Type.UINT64:
    case Type.FLOAT64:
    case Type.DECIMAL128:
    case Type.STRING:
    case Type.BYTES:
    case Type.DATETIME64:
    case Type.UUID128:
    case Type.BIGINT:
      // Scalar types: type tag is sufficient
      return true;

    case Type.ARRAY: {
      const aArr = a.data as Value[];
      const bArr = b.data as Value[];
      if (aArr.length !== bArr.length) return false;
      for (let i = 0; i < aArr.length; i++) {
        if (!schemaEquals(aArr[i], bArr[i])) return false;
      }
      return true;
    }

    case Type.OBJECT: {
      const aObj = a.data as Record<string, Value>;
      const bObj = b.data as Record<string, Value>;
      const aEntries = Object.entries(aObj).sort((x, y) => x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : 0);
      const bEntries = Object.entries(bObj).sort((x, y) => x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : 0);
      if (aEntries.length !== bEntries.length) return false;
      for (let i = 0; i < aEntries.length; i++) {
        if (aEntries[i][0] !== bEntries[i][0]) return false;
        if (!schemaEquals(aEntries[i][1], bEntries[i][1])) return false;
      }
      return true;
    }

    case Type.TENSOR: {
      const aT = a.data as TensorData;
      const bT = b.data as TensorData;
      return aT.dtype === bT.dtype && aT.shape.length === bT.shape.length;
    }

    case Type.TENSOR_REF: {
      const aR = a.data as TensorRefData;
      const bR = b.data as TensorRefData;
      return aR.storeId === bR.storeId;
    }

    case Type.IMAGE: {
      const aI = a.data as ImageData;
      const bI = b.data as ImageData;
      return aI.format === bI.format;
    }

    case Type.AUDIO: {
      const aA = a.data as AudioData;
      const bA = b.data as AudioData;
      return aA.encoding === bA.encoding && aA.channels === bA.channels;
    }

    case Type.UNKNOWN_EXT: {
      const aE = a.data as UnknownExtData;
      const bE = b.data as UnknownExtData;
      return aE.extType === bE.extType;
    }

    default:
      // Graph types, bitmask, etc.: type tag equality (checked above) is sufficient.
      return true;
  }
}

/**
 * Returns true if the two values share the same 64-bit FNV-1a schema fingerprint.
 * This is the original (probabilistic) implementation: fast for large schemas,
 * but carries a 1-in-2^64 collision risk.
 * Prefer schemaEquals for correctness-critical comparisons.
 */
export function schemaFingerprintEqual(a: Value, b: Value): boolean {
  return schemaFingerprint64(a) === schemaFingerprint64(b);
}

// ============================================================
// CRC32-IEEE
// ============================================================

const CRC32_POLYNOMIAL = 0xedb88320;

function buildCrc32Table(): Uint32Array {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) {
      if (c & 1) {
        c = CRC32_POLYNOMIAL ^ (c >>> 1);
      } else {
        c >>>= 1;
      }
    }
    table[i] = c;
  }
  return table;
}

const CRC32_TABLE = buildCrc32Table();

export function crc32(data: Uint8Array): number {
  let crc = 0xffffffff;
  for (const b of data) {
    crc = CRC32_TABLE[(crc ^ b) & 0xff] ^ (crc >>> 8);
  }
  return ~crc >>> 0;
}

// ============================================================
// Master Stream Implementation
// ============================================================

const MASTER_MAGIC = new Uint8Array([0x53, 0x4a, 0x53, 0x54]); // "SJST"
const MASTER_VERSION = 0x02;

// Master stream frame flags
const MFLAG_COMPRESSED = 0x01;
const MFLAG_CRC = 0x02;
const MFLAG_DETERMINISTIC = 0x04;
const MFLAG_META = 0x08;
const MFLAG_COMP_GZIP = 0x10;
const MFLAG_COMP_ZSTD = 0x20;

export interface MasterFrameHeader {
  version: number;
  flags: number;
  headerLen: number;
  typeId: number;
  payloadLen: number;
  rawLen: number;
  metaLen: number;
}

export interface MasterFrame {
  header: MasterFrameHeader;
  meta: Value | null;
  payload: Value;
  typeId: number;
}

export interface MasterWriterOptions {
  deterministic?: boolean;
  enableCrc?: boolean;
  compress?: number; // 0=none, 1=gzip, 2=zstd
}

/**
 * Check if data starts with master stream magic "SJST"
 */
export function isMasterStream(data: Uint8Array): boolean {
  if (data.length < 4) return false;
  return (
    data[0] === MASTER_MAGIC[0] &&
    data[1] === MASTER_MAGIC[1] &&
    data[2] === MASTER_MAGIC[2] &&
    data[3] === MASTER_MAGIC[3]
  );
}

/**
 * Check if data starts with Cowrie document magic but not master stream
 */
export function isCowrieDocument(data: Uint8Array): boolean {
  if (data.length < 4) return false;
  if (data[0] !== MAGIC[0] || data[1] !== MAGIC[1]) return false;
  // Exclude master stream format
  if (data[2] === 0x53 && data[3] === 0x54) return false; // "ST"
  return true;
}

/**
 * Write a master stream frame
 */
export function writeMasterFrame(
  value: Value,
  meta: Value | null,
  opts: MasterWriterOptions = {}
): Uint8Array {
  const deterministic = opts.deterministic ?? true;
  const enableCrc = opts.enableCrc ?? true;

  // Encode payload
  const encOpts: EncodeOptions = { deterministic };
  const payloadBytes = encodeWithOpts(value, encOpts);

  // Encode metadata if present
  let metaBytes: Uint8Array = new Uint8Array(0);
  if (meta) {
    metaBytes = encodeWithOpts(meta, encOpts) as Uint8Array;
  }

  // Compute type ID from schema
  const typeId = schemaFingerprint32(value);

  // Build flags
  let frameFlags = 0;
  if (deterministic) frameFlags |= MFLAG_DETERMINISTIC;
  if (enableCrc) frameFlags |= MFLAG_CRC;
  if (metaBytes.length > 0) frameFlags |= MFLAG_META;

  // Header length (fixed at 24 bytes for v2)
  const headerLen = 24;

  // Build frame
  const buf: number[] = [];
  const write = (data: Uint8Array) => {
    for (const b of data) buf.push(b);
  };
  const writeByte = (b: number) => buf.push(b);
  const writeU16LE = (n: number) => {
    buf.push(n & 0xff);
    buf.push((n >> 8) & 0xff);
  };
  const writeU32LE = (n: number) => {
    buf.push(n & 0xff);
    buf.push((n >> 8) & 0xff);
    buf.push((n >> 16) & 0xff);
    buf.push((n >> 24) & 0xff);
  };

  // Magic
  write(MASTER_MAGIC);

  // Version
  writeByte(MASTER_VERSION);

  // Flags
  writeByte(frameFlags);

  // Header length
  writeU16LE(headerLen);

  // Type ID
  writeU32LE(typeId);

  // Payload length
  writeU32LE(payloadBytes.length);

  // Raw length (0 = not compressed)
  writeU32LE(0);

  // Meta length
  writeU32LE(metaBytes.length);

  // Metadata
  write(metaBytes);

  // Payload
  write(payloadBytes);

  // CRC32 if enabled
  if (enableCrc) {
    const frameSoFar = new Uint8Array(buf);
    const crcValue = crc32(frameSoFar);
    writeU32LE(crcValue);
  }

  return new Uint8Array(buf);
}

/**
 * Read a master stream frame
 * Returns [frame, bytesConsumed]
 */
export function readMasterFrame(data: Uint8Array): [MasterFrame, number] {
  if (data.length < 24) {
    throw new Error("Truncated master frame");
  }

  if (!isMasterStream(data)) {
    throw new Error("Invalid master stream magic");
  }

  const version = data[4];
  if (version !== MASTER_VERSION) {
    throw new Error(`Invalid master stream version: ${version}`);
  }

  const frameFlags = data[5];
  const headerLen = data[6] | (data[7] << 8);
  const typeId = data[8] | (data[9] << 8) | (data[10] << 16) | (data[11] << 24);
  const payloadLen = data[12] | (data[13] << 8) | (data[14] << 16) | (data[15] << 24);
  const rawLen = data[16] | (data[17] << 8) | (data[18] << 16) | (data[19] << 24);
  const metaLen = data[20] | (data[21] << 8) | (data[22] << 16) | (data[23] << 24);

  let pos = Math.max(headerLen, 24);

  // Read metadata
  let meta: Value | null = null;
  if ((frameFlags & MFLAG_META) !== 0 && metaLen > 0) {
    if (pos + metaLen > data.length) {
      throw new Error("Truncated master frame (metadata)");
    }
    meta = decode(data.slice(pos, pos + metaLen));
    pos += metaLen;
  } else {
    pos += metaLen;
  }

  // Read payload
  if (pos + payloadLen > data.length) {
    throw new Error("Truncated master frame (payload)");
  }
  const payload = decode(data.slice(pos, pos + payloadLen));
  pos += payloadLen;

  // Verify CRC
  if ((frameFlags & MFLAG_CRC) !== 0) {
    if (pos + 4 > data.length) {
      throw new Error("Truncated master frame (CRC)");
    }
    // Use >>> 0 to ensure unsigned 32-bit comparison
    const expectedCrc =
      ((data[pos] | (data[pos + 1] << 8) | (data[pos + 2] << 16) | (data[pos + 3] << 24)) >>> 0);
    const actualCrc = crc32(data.slice(0, pos)) >>> 0;
    if (actualCrc !== expectedCrc) {
      throw new Error("CRC mismatch");
    }
    pos += 4;
  }

  const header: MasterFrameHeader = {
    version,
    flags: frameFlags,
    headerLen,
    typeId,
    payloadLen,
    rawLen,
    metaLen,
  };

  return [{ header, meta, payload, typeId }, pos];
}

// ============================================================
// Compression Framing (using pako for gzip)
// ============================================================

/**
 * Encode a value with compression framing.
 * Note: Requires pako library for gzip compression.
 * Install with: npm install pako
 */
export function encodeFramed(v: Value, compression: Compression = Compression.NONE): Uint8Array {
  const raw = encode(v);

  if (compression === Compression.NONE) {
    return raw;
  }

  // Build flags
  const compBits = compression;
  const flags = FLAG_COMPRESSED | ((compBits << 1) & 0x06);

  // Compress the payload (after 4-byte header)
  const payload = raw.slice(4);
  let compressed: Uint8Array;

  if (compression === Compression.GZIP) {
    // Use pako if available, otherwise throw
    try {
      // Dynamic import for pako
      const pako = require("pako");
      compressed = pako.gzip(payload);
    } catch {
      throw new Error("pako library required for gzip compression. Install with: npm install pako");
    }
  } else if (compression === Compression.ZSTD) {
    // Try Node.js built-in zlib (available since Node 21.7+)
    try {
      const zlib = require("zlib");
      if (typeof zlib.zstdCompressSync !== "function") {
        throw new Error("zstdCompressSync not available");
      }
      compressed = new Uint8Array(zlib.zstdCompressSync(Buffer.from(payload)));
    } catch (e: any) {
      if (e.message === "zstdCompressSync not available") {
        throw new Error(
          "zstd compression requires Node.js >= 21.7 (for built-in zlib.zstdCompressSync)"
        );
      }
      throw e;
    }
  } else {
    throw new Error(`Unknown compression type: ${compression}`);
  }

  // Build output: header + uncompressed length (uvarint) + compressed data
  const uncompressedLen = payload.length;
  const lenBytes: number[] = [];
  let len = uncompressedLen;
  while (len >= 0x80) {
    lenBytes.push((len & 0x7f) | 0x80);
    len >>>= 7;
  }
  lenBytes.push(len);

  const result = new Uint8Array(4 + lenBytes.length + compressed.length);
  result[0] = raw[0]; // 'S'
  result[1] = raw[1]; // 'J'
  result[2] = raw[2]; // version
  result[3] = flags;
  result.set(lenBytes, 4);
  result.set(compressed, 4 + lenBytes.length);

  return result;
}

function decompressZstd(compressed: Uint8Array): Uint8Array {
  // Prefer Node built-in zlib (Node 21.7+), fall back to fzstd
  try {
    const zlib = require("zlib");
    if (typeof zlib.zstdDecompressSync === "function") {
      return new Uint8Array(zlib.zstdDecompressSync(Buffer.from(compressed)));
    }
  } catch {
    // Node zlib failed, fall through to fzstd
  }
  try {
    const fzstd = require('fzstd');
    return fzstd.decompress(compressed);
  } catch (e: any) {
    if (e.code === 'MODULE_NOT_FOUND') {
      throw new Error(
        'zstd decompression requires Node.js >= 21.7 or the fzstd package: npm install fzstd'
      );
    }
    throw e;
  }
}

/**
 * Decode a framed Cowrie value, automatically decompressing if needed.
 */
export function decodeFramed(
  data: Uint8Array,
  maxSizeOrOpts?: number | DecodeOptions,
): Value {
  // Backwards-compatible: accept number (old API) or DecodeOptions (new API)
  let opts: DecodeOptions = {};
  let maxSize: number;
  if (typeof maxSizeOrOpts === 'number') {
    maxSize = maxSizeOrOpts;
  } else {
    opts = maxSizeOrOpts ?? {};
    maxSize = opts.maxDecompressedSize ?? Limits.MAX_DECOMPRESSED_SIZE;
  }

  if (data.length < 4) {
    throw new Error("truncated data");
  }

  const flags = data[3];

  if ((flags & FLAG_COMPRESSED) === 0) {
    // Not compressed, decode directly
    return decode(data, opts);
  }

  // Get compression type
  const compType = (flags & 0x06) >> 1;

  // Read uncompressed length
  let pos = 4;
  let uncompressedLen = 0;
  let shift = 0;
  while (pos < data.length) {
    const b = data[pos++];
    uncompressedLen |= (b & 0x7f) << shift;
    if ((b & 0x80) === 0) break;
    shift += 7;
    if (shift > 28) throw new Error("varint overflow");
  }
  if (maxSize > 0 && uncompressedLen > maxSize) {
    throw new SecurityLimitExceeded(`Decompressed size too large: ${uncompressedLen} > ${maxSize}`);
  }

  // Decompress
  const compressed = data.slice(pos);
  let decompressed: Uint8Array;

  if (compType === 1) {
    // GZIP
    try {
      const pako = require("pako");
      decompressed = pako.ungzip(compressed);
    } catch {
      throw new Error("pako library required for gzip decompression");
    }
  } else if (compType === 2) {
    // ZSTD - prefer Node built-in zlib (Node 21.7+), fall back to fzstd
    decompressed = decompressZstd(compressed);
  } else {
    throw new Error(`Unknown compression type: ${compType}`);
  }

  if (maxSize > 0 && decompressed.length > maxSize) {
    throw new SecurityLimitExceeded(`Decompressed size too large: ${decompressed.length} > ${maxSize}`);
  }
  if (decompressed.length !== uncompressedLen) {
    throw new Error("Decompressed length mismatch");
  }

  // Reconstruct full Cowrie data with header
  const full = new Uint8Array(4 + decompressed.length);
  full[0] = data[0]; // 'S'
  full[1] = data[1]; // 'J'
  full[2] = data[2]; // version
  full[3] = 0; // flags = 0 (no compression)
  full.set(decompressed, 4);

  return decode(full, opts);
}

// ============================================================
// Zero-Copy Tensor Views
// ============================================================

/** DType codes - aligned with Go reference implementation */
export enum DType {
  FLOAT32 = 0x01,
  FLOAT16 = 0x02,
  BFLOAT16 = 0x03,
  INT8 = 0x04,
  INT16 = 0x05,
  INT32 = 0x06,
  INT64 = 0x07,
  UINT8 = 0x08,
  UINT16 = 0x09,
  UINT32 = 0x0a,
  UINT64 = 0x0b,
  FLOAT64 = 0x0c,
  BOOL = 0x0d,
  // Quantized types
  QINT4 = 0x10,    // 4-bit quantized integer
  QINT2 = 0x11,    // 2-bit quantized integer
  QINT3 = 0x12,    // 3-bit quantized integer
  TERNARY = 0x13,  // Ternary (-1, 0, 1)
  BINARY = 0x14,   // Binary (0, 1)
}

/** Tensor data structure */
export interface TensorData {
  dtype: DType;
  shape: number[];
  data: Uint8Array;
}

/**
 * Get a Float32Array view of tensor data.
 * Returns null if dtype is not FLOAT32 or alignment is wrong.
 */
export function tensorViewFloat32(tensor: TensorData): Float32Array | null {
  if (tensor.dtype !== DType.FLOAT32) return null;
  if (tensor.data.length % 4 !== 0) return null;

  // Check alignment
  if (tensor.data.byteOffset % 4 !== 0) {
    // Alignment issue - need to copy
    return tensorCopyFloat32(tensor);
  }

  return new Float32Array(tensor.data.buffer, tensor.data.byteOffset, tensor.data.length / 4);
}

/**
 * Get a Float64Array view of tensor data.
 */
export function tensorViewFloat64(tensor: TensorData): Float64Array | null {
  if (tensor.dtype !== DType.FLOAT64) return null;
  if (tensor.data.length % 8 !== 0) return null;

  if (tensor.data.byteOffset % 8 !== 0) {
    return tensorCopyFloat64(tensor);
  }

  return new Float64Array(tensor.data.buffer, tensor.data.byteOffset, tensor.data.length / 8);
}

/**
 * Get an Int32Array view of tensor data.
 */
export function tensorViewInt32(tensor: TensorData): Int32Array | null {
  if (tensor.dtype !== DType.INT32) return null;
  if (tensor.data.length % 4 !== 0) return null;

  if (tensor.data.byteOffset % 4 !== 0) {
    return tensorCopyInt32(tensor);
  }

  return new Int32Array(tensor.data.buffer, tensor.data.byteOffset, tensor.data.length / 4);
}

/**
 * Copy tensor data to a new Float32Array.
 * Use this when zero-copy view is not possible.
 */
export function tensorCopyFloat32(tensor: TensorData): Float32Array | null {
  if (tensor.dtype !== DType.FLOAT32) return null;
  if (tensor.data.length % 4 !== 0) return null;

  const count = tensor.data.length / 4;
  const result = new Float32Array(count);
  const view = new DataView(tensor.data.buffer, tensor.data.byteOffset, tensor.data.length);

  for (let i = 0; i < count; i++) {
    result[i] = view.getFloat32(i * 4, true); // little-endian
  }

  return result;
}

/**
 * Copy tensor data to a new Float64Array.
 */
export function tensorCopyFloat64(tensor: TensorData): Float64Array | null {
  if (tensor.dtype !== DType.FLOAT64) return null;
  if (tensor.data.length % 8 !== 0) return null;

  const count = tensor.data.length / 8;
  const result = new Float64Array(count);
  const view = new DataView(tensor.data.buffer, tensor.data.byteOffset, tensor.data.length);

  for (let i = 0; i < count; i++) {
    result[i] = view.getFloat64(i * 8, true);
  }

  return result;
}

/**
 * Copy tensor data to a new Int32Array.
 */
export function tensorCopyInt32(tensor: TensorData): Int32Array | null {
  if (tensor.dtype !== DType.INT32) return null;
  if (tensor.data.length % 4 !== 0) return null;

  const count = tensor.data.length / 4;
  const result = new Int32Array(count);
  const view = new DataView(tensor.data.buffer, tensor.data.byteOffset, tensor.data.length);

  for (let i = 0; i < count; i++) {
    result[i] = view.getInt32(i * 4, true);
  }

  return result;
}

/**
 * Create a tensor value from a Float32Array.
 */
export function tensorFromFloat32(shape: number[], data: Float32Array): Value {
  const bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  return {
    type: Type.BYTES, // Use BYTES as container for tensor
    data: {
      _type: "tensor",
      dtype: DType.FLOAT32,
      shape,
      data: new Uint8Array(bytes),
    } as TensorData,
  };
}

// Example usage
if (typeof require !== "undefined" && require.main === module) {
  const obj = {
    user_id: 12345,
    user_name: "alice",
    created_at: "2025-11-28T12:34:56Z",
    id: "550e8400-e29b-41d4-a716-446655440000",
    active: true,
    tags: ["admin", "user"],
  };

  const data = dumps(obj);
  const jsonData = JSON.stringify(obj);

  console.log(`Cowrie size: ${data.length} bytes`);
  console.log(`JSON size:  ${jsonData.length} bytes`);
  console.log(`Savings:    ${((1 - data.length / jsonData.length) * 100).toFixed(1)}%`);

  const result = loads(data);
  console.log("\nDecoded:", result);

  // Test deterministic encoding
  const value = fromAny(obj);
  const deterministicData = encodeWithOpts(value, { deterministic: true });
  console.log(`\nDeterministic encode: ${deterministicData.length} bytes`);

  // Test schema fingerprinting
  const fp = schemaFingerprint32(value);
  console.log(`Schema fingerprint: 0x${fp.toString(16)}`);

  // Test master stream
  const masterData = writeMasterFrame(value, null, { deterministic: true, enableCrc: true });
  console.log(`Master stream size: ${masterData.length} bytes`);

  const [frame, consumed] = readMasterFrame(masterData);
  console.log(`Master frame typeId: 0x${frame.typeId.toString(16)}`);
  console.log(`Master frame consumed: ${consumed} bytes`);
}
