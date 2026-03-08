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
const MAGIC = new Uint8Array([0x53, 0x4a]); // 'SJ'
const VERSION = 2;

// Compression
export enum Compression {
  NONE = 0,
  GZIP = 1,
  ZSTD = 2,
}

const FLAG_COMPRESSED = 0x01;
const FLAG_HAS_COLUMN_HINTS = 0x08;
const COMPRESS_THRESHOLD = 256;

// Security limits - aligned with Go reference implementation
const Limits = {
  MAX_DEPTH: 1000,               // Maximum nesting depth
  MAX_ARRAY_LEN: 1_000_000,     // 1M elements (tightened: was 100M)
  MAX_OBJECT_LEN: 1_000_000,    // 1M fields (tightened: was 10M)
  MAX_STRING_LEN: 10_000_000,   // 10MB (tightened: was 500MB)
  MAX_BYTES_LEN: 50_000_000,    // 50MB (tightened: was 1GB)
  MAX_EXT_LEN: 1_000_000,       // 1MB extension payload (tightened: was 100MB)
  MAX_RANK: 32,                  // Maximum tensor rank (dimensions)
  MAX_HINT_COUNT: 10_000,        // Maximum column hints
  MAX_DICT_LEN: 1_000_000,      // 1M dictionary entries (tightened: was 10M)
  MAX_DECOMPRESSED_SIZE: 256 * 1024 * 1024, // 256MB decompressed limit
};

export class SecurityLimitExceeded extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SecurityLimitExceeded';
  }
}

export enum UnknownExtBehavior {
  KEEP = 0,
  SKIP_AS_NULL = 1,
  ERROR = 2,
}

export interface DecodeOptions {
  onUnknownExt?: UnknownExtBehavior;
  maxDepth?: number;
  maxArrayLen?: number;
  maxObjectLen?: number;
  maxStringLen?: number;
  maxBytesLen?: number;
  maxExtLen?: number;
  maxDictLen?: number;
  maxRank?: number;
  maxHintCount?: number;
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
  maxHintCount: number;
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
    maxHintCount: opts?.maxHintCount ?? Limits.MAX_HINT_COUNT,
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
  FLOAT32 = 0x0f, // compact float32 -> decoded as number (float64)
  // ML/Multimodal extensions (0x20-0x2F)
  TENSOR = 0x20,
  TENSOR_REF = 0x21,
  IMAGE = 0x22,
  AUDIO = 0x23,
  // Graph/Delta extensions (0x30-0x3F)
  ADJLIST = 0x30,
  RICHTEXT = 0x31,
  DELTA = 0x32,
  // Graph types (v2.1)
  NODE = 0x35,
  EDGE = 0x36,
  NODE_BATCH = 0x37,
  EDGE_BATCH = 0x38,
  GRAPH_SHARD = 0x39,
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
  ADJLIST,
  RICHTEXT,
  DELTA,
  // Graph types (v2.1)
  NODE,
  EDGE,
  NODE_BATCH,
  EDGE_BATCH,
  GRAPH_SHARD,
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

// ID width for adjacency lists - aligned with Go reference implementation
export enum IDWidth {
  INT32 = 0x01,
  INT64 = 0x02,
}

// Delta operation codes - aligned with Go reference implementation
export enum DeltaOpCode {
  SET_FIELD = 0x01,
  DELETE_FIELD = 0x02,
  APPEND_ARRAY = 0x03,
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

// Adjacency list data type (CSR format for graphs)
export interface AdjlistData {
  idWidth: IDWidth;
  nodeCount: number;
  edgeCount: number;
  rowOffsets: number[]; // [nodeCount + 1] offsets into colIndices
  colIndices: Uint8Array; // Edge destinations (int32/int64 LE based on idWidth)
}

// Rich text span for annotation
export interface RichTextSpan {
  start: number; // Byte offset start
  end: number; // Byte offset end
  kindId: number; // Application-defined kind
}

// Rich text data type
export interface RichTextData {
  text: string;
  tokens?: number[]; // Token IDs (e.g., BPE tokens)
  spans?: RichTextSpan[]; // Annotated spans
}

// Delta operation
export interface DeltaOp {
  opCode: DeltaOpCode;
  fieldId: number; // Dictionary-coded field ID
  value?: Value; // For SET_FIELD and APPEND_ARRAY
}

// Delta data type (semantic diff/patch)
export interface DeltaData {
  baseId: number; // Reference to base object
  ops: DeltaOp[]; // Operations
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

// Self-contained subgraph with nodes, edges, and metadata
export interface GraphShardData {
  nodes: NodeData[];
  edges: EdgeData[];
  metadata: Record<string, Value>;
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
    return { type: Type.AUDIO, data: { encoding, sampleRate, channels, data } as AudioData };
  },
  tensorRef(storeId: number, key: Uint8Array): Value {
    return { type: Type.TENSOR_REF, data: { storeId, key } as TensorRefData };
  },
  adjlist(idWidth: IDWidth, nodeCount: number, edgeCount: number, rowOffsets: number[], colIndices: Uint8Array): Value {
    return { type: Type.ADJLIST, data: { idWidth, nodeCount, edgeCount, rowOffsets, colIndices } as AdjlistData };
  },
  richtext(text: string, tokens?: number[], spans?: RichTextSpan[]): Value {
    return { type: Type.RICHTEXT, data: { text, tokens, spans } as RichTextData };
  },
  delta(baseId: number, ops: DeltaOp[]): Value {
    return { type: Type.DELTA, data: { baseId, ops } as DeltaData };
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
  graphShard(nodes: NodeData[], edges: EdgeData[], metadata: Record<string, Value>): Value {
    return { type: Type.GRAPH_SHARD, data: { nodes, edges, metadata } as GraphShardData };
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

function decodeUvarint(data: Uint8Array, pos: number): [bigint, number] {
  let result = 0n;
  let shift = 0n;
  while (pos < data.length) {
    const b = data[pos];
    result |= BigInt(b & 0x7f) << shift;
    pos++;
    if ((b & 0x80) === 0) {
      return [result, pos];
    }
    shift += 7n;
  }
  throw new Error("Incomplete varint");
}

function zigzagEncode(n: bigint): bigint {
  return (n << 1n) ^ (n >> 63n);
}

function zigzagDecode(n: bigint): bigint {
  return (n >> 1n) ^ -(n & 1n);
}

// Reusable TextEncoder instance (created once, shared by all encoders)
const sharedTextEncoder = new TextEncoder();

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
    } else if (v.type === Type.DELTA) {
      // Delta ops may contain nested values
      const delta = v.data as DeltaData;
      for (const op of delta.ops) {
        if (op.value) {
          this.collectKeys(op.value);
        }
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
    } else if (v.type === Type.GRAPH_SHARD) {
      const shard = v.data as GraphShardData;
      for (const node of shard.nodes) {
        this.collectPropsKeys(node.props);
      }
      for (const edge of shard.edges) {
        this.collectPropsKeys(edge.props);
      }
      this.collectPropsKeys(shard.metadata);
    }
  }

  private collectPropsKeys(props: Record<string, Value>): void {
    for (const [key, val] of Object.entries(props)) {
      this.addKey(key);
      this.collectKeys(val);
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
      case Type.INT64:
        this.writeByte(Tag.INT64);
        this.writeUvarint(zigzagEncode(v.data as bigint));
        break;
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
        const buf = new ArrayBuffer(8);
        new DataView(buf).setFloat64(0, v.data as number, true);
        this.write(new Uint8Array(buf));
        break;
      }
      case Type.DECIMAL128: {
        const d = v.data as Decimal128;
        this.writeByte(Tag.DECIMAL128);
        this.writeByte(d.scale & 0xff);
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
        this.writeByte(Tag.ARRAY);
        this.writeUvarint(arr.length);
        for (const item of arr) this.encodeValue(item);
        break;
      }
      case Type.OBJECT: {
        const obj = v.data as Record<string, Value>;
        const entries = Object.entries(obj);
        this.writeByte(Tag.OBJECT);
        this.writeUvarint(entries.length);
        for (const [key, val] of entries) {
          const idx = this.dictLookup.get(key)!;
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
        this.writeByte(Tag.TENSOR);
        this.writeByte(t.dtype);
        this.writeByte(rank);
        for (const dim of t.shape) {
          this.writeUvarint(dim);
        }
        this.writeUvarint(t.data.length);
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
      case Type.ADJLIST: {
        const adj = v.data as AdjlistData;
        this.writeByte(Tag.ADJLIST);
        this.writeByte(adj.idWidth);
        this.writeUvarint(adj.nodeCount);
        this.writeUvarint(adj.edgeCount);
        // Write row_offsets as varints
        for (const offset of adj.rowOffsets) {
          this.writeUvarint(offset);
        }
        // Write col_indices as raw bytes
        this.write(adj.colIndices);
        break;
      }
      case Type.RICHTEXT: {
        const rt = v.data as RichTextData;
        this.writeByte(Tag.RICHTEXT);
        // Text (writeString format: len:varint + bytes)
        const textBytes = sharedTextEncoder.encode(rt.text);
        this.writeUvarint(textBytes.length);
        this.write(textBytes);
        // Calculate and write flags byte
        const tokens = rt.tokens || [];
        const spans = rt.spans || [];
        let flags = 0;
        if (tokens.length > 0) flags |= 0x01;
        if (spans.length > 0) flags |= 0x02;
        this.writeByte(flags);
        // Write tokens if present
        if (flags & 0x01) {
          this.writeUvarint(tokens.length);
          for (const tok of tokens) {
            const buf = new ArrayBuffer(4);
            new DataView(buf).setInt32(0, tok, true);
            this.write(new Uint8Array(buf));
          }
        }
        // Write spans if present
        if (flags & 0x02) {
          this.writeUvarint(spans.length);
          for (const span of spans) {
            this.writeUvarint(span.start);
            this.writeUvarint(span.end);
            this.writeUvarint(span.kindId);
          }
        }
        break;
      }
      case Type.DELTA: {
        const delta = v.data as DeltaData;
        this.writeByte(Tag.DELTA);
        this.writeUvarint(delta.baseId);
        this.writeUvarint(delta.ops.length);
        for (const op of delta.ops) {
          this.writeByte(op.opCode);
          this.writeUvarint(op.fieldId);
          if (op.opCode === DeltaOpCode.SET_FIELD || op.opCode === DeltaOpCode.APPEND_ARRAY) {
            if (op.value) {
              this.encodeValue(op.value);
            }
          }
        }
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
      case Type.GRAPH_SHARD: {
        const shard = v.data as GraphShardData;
        this.writeByte(Tag.GRAPH_SHARD);
        // Encode nodes
        this.writeUvarint(shard.nodes.length);
        for (const node of shard.nodes) {
          this.encodeNode(node);
        }
        // Encode edges
        this.writeUvarint(shard.edges.length);
        for (const edge of shard.edges) {
          this.encodeEdge(edge);
        }
        // Encode metadata
        this.encodeProps(shard.metadata);
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
    const entries = Object.entries(props);
    this.writeUvarint(entries.length);
    for (const [key, val] of entries) {
      const idx = this.dictLookup.get(key)!;
      this.writeUvarint(idx);
      this.encodeValue(val);
    }
  }

  encode(v: Value): Uint8Array {
    this.collectKeys(v);

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
class Decoder {
  private pos = 0;
  private dict: string[] = [];
  private depth = 0;
  private textDecoder = new TextDecoder();
  private limits: ResolvedLimits;

  constructor(
    private data: Uint8Array,
    private onUnknownExt: UnknownExtBehavior = UnknownExtBehavior.KEEP,
    limits?: ResolvedLimits,
  ) {
    this.limits = limits ?? resolveLimits();
  }

  private read(n: number): Uint8Array {
    if (this.pos + n > this.data.length) {
      throw new Error("Unexpected end of data");
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
    if (this.depth > this.limits.maxDepth) {
      throw new SecurityLimitExceeded(`Maximum nesting depth exceeded: ${this.limits.maxDepth}`);
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
      case Tag.INT64:
        return SJ.int64(zigzagDecode(this.readUvarint()));
      case Tag.UINT64:
        return SJ.uint64(this.readUvarint());
      case Tag.FLOAT64: {
        const buf = this.read(8);
        const f = new DataView(buf.buffer, buf.byteOffset).getFloat64(0, true);
        return SJ.float64(f);
      }
      case Tag.FLOAT32: {
        const buf = this.read(4);
        const f = new DataView(buf.buffer, buf.byteOffset).getFloat32(0, true);
        return SJ.float64(f);
      }
      case Tag.DECIMAL128: {
        const scale = this.readByte();
        const coef = this.read(16);
        return SJ.decimal128(scale > 127 ? scale - 256 : scale, coef);
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
        return SJ.bigint(this.read(len));
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
        if (count > this.limits.maxObjectLen) {
          throw new SecurityLimitExceeded(`Object too large: ${count} > ${this.limits.maxObjectLen}`);
        }
        this.enterNested();
        const members: Record<string, Value> = {};
        for (let i = 0; i < count; i++) {
          const fieldId = this.readUvarintAsNumber();
          if (fieldId >= this.dict.length) {
            throw new Error(`Invalid dictionary index: ${fieldId} >= ${this.dict.length}`);
          }
          const key = this.dict[fieldId];
          members[key] = this.decodeValue();
        }
        this.exitNested();
        return SJ.object(members);
      }
      case Tag.TENSOR: {
        const dtype = this.readByte() as DType;
        const rank = this.readByte();
        if (rank > this.limits.maxRank) {
          throw new SecurityLimitExceeded(`Tensor rank too large: ${rank} > ${this.limits.maxRank}`);
        }
        const shape: number[] = [];
        for (let i = 0; i < rank; i++) {
          shape.push(this.readUvarintAsNumber());
        }
        const dataLen = this.readUvarintAsNumber();
        if (dataLen > this.limits.maxBytesLen) {
          throw new SecurityLimitExceeded(`Tensor data too large: ${dataLen} > ${this.limits.maxBytesLen}`);
        }
        const data = this.read(dataLen);
        return SJ.tensor(dtype, shape, data);
      }
      case Tag.IMAGE: {
        const format = this.readByte() as ImageFormat;
        const dimBuf = this.read(4);
        const dimView = new DataView(dimBuf.buffer, dimBuf.byteOffset, 4);
        const width = dimView.getUint16(0, true);
        const height = dimView.getUint16(2, true);
        const dataLen = this.readUvarintAsNumber();
        if (dataLen > this.limits.maxBytesLen) {
          throw new SecurityLimitExceeded(`Image data too large: ${dataLen} > ${this.limits.maxBytesLen}`);
        }
        const data = this.read(dataLen);
        return SJ.image(format, width, height, data);
      }
      case Tag.AUDIO: {
        const encoding = this.readByte() as AudioEncoding;
        const rateBuf = this.read(4);
        const sampleRate = new DataView(rateBuf.buffer, rateBuf.byteOffset, 4).getUint32(0, true);
        const channels = this.readByte();
        const dataLen = this.readUvarintAsNumber();
        if (dataLen > this.limits.maxBytesLen) {
          throw new SecurityLimitExceeded(`Audio data too large: ${dataLen} > ${this.limits.maxBytesLen}`);
        }
        const data = this.read(dataLen);
        return SJ.audio(encoding, sampleRate, channels, data);
      }
      case Tag.TENSOR_REF: {
        const storeId = this.readByte();
        const keyLen = this.readUvarintAsNumber();
        if (keyLen > this.limits.maxStringLen) {
          throw new SecurityLimitExceeded(`TensorRef key too long: ${keyLen} > ${this.limits.maxStringLen}`);
        }
        const key = this.read(keyLen);
        return SJ.tensorRef(storeId, key);
      }
      case Tag.ADJLIST: {
        const idWidth = this.readByte() as IDWidth;
        const nodeCount = this.readUvarintAsNumber();
        if (nodeCount > this.limits.maxArrayLen) {
          throw new SecurityLimitExceeded(`Adjlist node count too large: ${nodeCount} > ${this.limits.maxArrayLen}`);
        }
        const edgeCount = this.readUvarintAsNumber();
        if (edgeCount > this.limits.maxArrayLen) {
          throw new SecurityLimitExceeded(`Adjlist edge count too large: ${edgeCount} > ${this.limits.maxArrayLen}`);
        }
        // Read row_offsets (node_count + 1 varints)
        const rowOffsets: number[] = [];
        for (let i = 0; i < nodeCount + 1; i++) {
          rowOffsets.push(this.readUvarintAsNumber());
        }
        // Read col_indices (edge_count * id_size bytes)
        const idSize = idWidth === IDWidth.INT32 ? 4 : 8;
        const colIndices = this.read(edgeCount * idSize);
        return SJ.adjlist(idWidth, nodeCount, edgeCount, rowOffsets, colIndices);
      }
      case Tag.RICHTEXT: {
        // Text (readString format: len:varint + bytes)
        const textLen = this.readUvarintAsNumber();
        if (textLen > this.limits.maxStringLen) {
          throw new SecurityLimitExceeded(`RichText text too long: ${textLen} > ${this.limits.maxStringLen}`);
        }
        const text = this.textDecoder.decode(this.read(textLen));
        // Read flags byte
        const flags = this.readByte();
        // Read tokens if present (flags & 0x01)
        let tokens: number[] | undefined;
        if (flags & 0x01) {
          const tokenCount = this.readUvarintAsNumber();
          if (tokenCount > this.limits.maxArrayLen) {
            throw new SecurityLimitExceeded(`RichText token count too large: ${tokenCount} > ${this.limits.maxArrayLen}`);
          }
          tokens = [];
          for (let i = 0; i < tokenCount; i++) {
            const buf = this.read(4);
            tokens.push(new DataView(buf.buffer, buf.byteOffset, 4).getInt32(0, true));
          }
        }
        // Read spans if present (flags & 0x02)
        let spans: RichTextSpan[] | undefined;
        if (flags & 0x02) {
          const spanCount = this.readUvarintAsNumber();
          if (spanCount > this.limits.maxArrayLen) {
            throw new SecurityLimitExceeded(`RichText span count too large: ${spanCount} > ${this.limits.maxArrayLen}`);
          }
          spans = [];
          for (let i = 0; i < spanCount; i++) {
            const start = this.readUvarintAsNumber();
            const end = this.readUvarintAsNumber();
            const kindId = this.readUvarintAsNumber();
            spans.push({ start, end, kindId });
          }
        }
        return SJ.richtext(text, tokens, spans);
      }
      case Tag.DELTA: {
        const baseId = this.readUvarintAsNumber();
        const opCount = this.readUvarintAsNumber();
        if (opCount > this.limits.maxArrayLen) {
          throw new SecurityLimitExceeded(`Delta op count too large: ${opCount} > ${this.limits.maxArrayLen}`);
        }
        const ops: DeltaOp[] = [];
        for (let i = 0; i < opCount; i++) {
          const opCode = this.readByte() as DeltaOpCode;
          const fieldId = this.readUvarintAsNumber();
          let value: Value | undefined;
          if (opCode === DeltaOpCode.SET_FIELD || opCode === DeltaOpCode.APPEND_ARRAY) {
            value = this.decodeValue();
          }
          ops.push({ opCode, fieldId, value });
        }
        return SJ.delta(baseId, ops);
      }
      // Graph types
      case Tag.NODE: {
        const node = this.decodeNode();
        return SJ.node(node.id, node.labels, node.props);
      }
      case Tag.EDGE: {
        const edge = this.decodeEdge();
        return SJ.edge(edge.fromId, edge.toId, edge.edgeType, edge.props);
      }
      case Tag.NODE_BATCH: {
        const count = this.readUvarintAsNumber();
        if (count > this.limits.maxArrayLen) {
          throw new SecurityLimitExceeded(`Node batch count too large: ${count} > ${this.limits.maxArrayLen}`);
        }
        const nodes: NodeData[] = [];
        for (let i = 0; i < count; i++) {
          nodes.push(this.decodeNode());
        }
        return SJ.nodeBatch(nodes);
      }
      case Tag.EDGE_BATCH: {
        const count = this.readUvarintAsNumber();
        if (count > this.limits.maxArrayLen) {
          throw new SecurityLimitExceeded(`Edge batch count too large: ${count} > ${this.limits.maxArrayLen}`);
        }
        const edges: EdgeData[] = [];
        for (let i = 0; i < count; i++) {
          edges.push(this.decodeEdge());
        }
        return SJ.edgeBatch(edges);
      }
      case Tag.GRAPH_SHARD: {
        // Decode nodes
        const nodeCount = this.readUvarintAsNumber();
        if (nodeCount > this.limits.maxArrayLen) {
          throw new SecurityLimitExceeded(`Graph shard node count too large: ${nodeCount} > ${this.limits.maxArrayLen}`);
        }
        const nodes: NodeData[] = [];
        for (let i = 0; i < nodeCount; i++) {
          nodes.push(this.decodeNode());
        }
        // Decode edges
        const edgeCount = this.readUvarintAsNumber();
        if (edgeCount > this.limits.maxArrayLen) {
          throw new SecurityLimitExceeded(`Graph shard edge count too large: ${edgeCount} > ${this.limits.maxArrayLen}`);
        }
        const edges: EdgeData[] = [];
        for (let i = 0; i < edgeCount; i++) {
          edges.push(this.decodeEdge());
        }
        // Decode metadata
        const metadata = this.decodeProps();
        return SJ.graphShard(nodes, edges, metadata);
      }
      default:
        throw new Error(`Invalid tag: ${tag}`);
    }
  }

  private skipHints(): void {
    const count = this.readUvarintAsNumber();
    if (count > this.limits.maxHintCount) {
      throw new SecurityLimitExceeded(`Too many hints: ${count} > ${this.limits.maxHintCount}`);
    }
    for (let i = 0; i < count; i++) {
      this.readString(); // field name
      this.readByte();   // type
      const shapeLen = this.readUvarintAsNumber();
      if (shapeLen > this.limits.maxRank) {
        throw new SecurityLimitExceeded(`Hint shape too large: ${shapeLen} > ${this.limits.maxRank}`);
      }
      for (let j = 0; j < shapeLen; j++) {
        this.readUvarint();
      }
      this.readByte();   // flags
    }
  }

  decode(): Value {
    // Header
    const magic = this.read(2);
    if (magic[0] !== MAGIC[0] || magic[1] !== MAGIC[1]) {
      throw new Error("Invalid magic bytes");
    }
    const version = this.readByte();
    if (version !== VERSION) {
      throw new Error(`Unsupported version: ${version}`);
    }
    const flags = this.readByte();
    if (flags & FLAG_HAS_COLUMN_HINTS) {
      this.skipHints();
    }

    // Dictionary
    const dictLen = this.readUvarintAsNumber();
    if (dictLen > this.limits.maxDictLen) {
      throw new SecurityLimitExceeded('cowrie: dictionary too large');
    }
    for (let i = 0; i < dictLen; i++) {
      this.dict.push(this.readString());
    }

    const result = this.decodeValue();

    // Verify all input consumed — trailing bytes indicate corruption or concatenated data
    if (this.pos < this.data.length) {
      const remaining = this.data.length - this.pos;
      throw new Error(
        `cowrie: trailing data after root value: ${remaining} unconsumed bytes at position ${this.pos}`
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
  ).decode();
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
    if ((obj._type === "unknown_ext" || obj._type === "ext") && obj.ext_type !== undefined && obj.payload !== undefined) {
      const extType = typeof obj.ext_type === "string" ? BigInt(obj.ext_type) : BigInt(obj.ext_type as number);
      const payloadStr = String(obj.payload);
      let payload: Uint8Array;
      if (typeof Buffer !== "undefined") {
        payload = new Uint8Array(Buffer.from(payloadStr, "base64"));
      } else {
        const bin = atob(payloadStr);
        const arr = new Uint8Array(bin.length);
        for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
        payload = arr;
      }
      return SJ.unknownExt(extType, payload);
    }
    const members: Record<string, Value> = {};
    for (const [key, val] of Object.entries(v)) {
      members[key] = fromAny(val, key);
    }
    return SJ.object(members);
  }
  return SJ.string(String(v));
}

function inferStringType(s: string, fieldName: string): Value {
  const lowerField = fieldName.toLowerCase();

  // Date inference
  if (
    lowerField.includes("time") ||
    lowerField.includes("date") ||
    lowerField.endsWith("_at") ||
    ["created", "updated"].includes(lowerField)
  ) {
    if (ISO8601_PATTERN.test(s)) {
      try {
        const d = new Date(s);
        if (!isNaN(d.getTime())) return SJ.datetime(d);
      } catch {
        /* ignore */
      }
    }
  }

  // Pattern inference
  if (ISO8601_PATTERN.test(s)) {
    try {
      const d = new Date(s);
      if (!isNaN(d.getTime())) return SJ.datetime(d);
    } catch {
      /* ignore */
    }
  }

  if (UUID_PATTERN.test(s)) {
    try {
      return SJ.uuid128(s);
    } catch {
      /* ignore */
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
        shape: t.shape,
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
        sampleRate: aud.sampleRate,
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
        storeId: ref.storeId,
        key: btoa(binary),
      };
    }
    case Type.ADJLIST: {
      const adj = v.data as AdjlistData;
      let binary = "";
      for (const b of adj.colIndices) binary += String.fromCharCode(b);
      return {
        _type: "adjlist",
        idWidth: adj.idWidth === IDWidth.INT32 ? "int32" : "int64",
        nodeCount: adj.nodeCount,
        edgeCount: adj.edgeCount,
        rowOffsets: adj.rowOffsets,
        colIndices: btoa(binary),
      };
    }
    case Type.RICHTEXT: {
      const rt = v.data as RichTextData;
      const result: Record<string, unknown> = {
        _type: "richtext",
        text: rt.text,
      };
      if (rt.tokens && rt.tokens.length > 0) {
        result.tokens = rt.tokens;
      }
      if (rt.spans && rt.spans.length > 0) {
        result.spans = rt.spans.map((s) => ({
          start: s.start,
          end: s.end,
          kindId: s.kindId,
        }));
      }
      return result;
    }
    case Type.DELTA: {
      const delta = v.data as DeltaData;
      const opsJson = delta.ops.map((op) => {
        const opDict: Record<string, unknown> = {
          opCode: DeltaOpCode[op.opCode]?.toLowerCase() ?? op.opCode,
          fieldId: op.fieldId,
        };
        if (op.value) {
          opDict.value = toAny(op.value);
        }
        return opDict;
      });
      return {
        _type: "delta",
        baseId: delta.baseId,
        ops: opsJson,
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
        from: edge.fromId,
        to: edge.toId,
        type: edge.edgeType,
        props: propsJson,
      };
    }
    case Type.NODE_BATCH: {
      const batch = v.data as NodeBatchData;
      return {
        _type: "node_batch",
        nodes: batch.nodes.map((n) => ({
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
          from: e.fromId,
          to: e.toId,
          type: e.edgeType,
          props: Object.fromEntries(
            Object.entries(e.props).map(([k, val]) => [k, toAny(val)])
          ),
        })),
      };
    }
    case Type.GRAPH_SHARD: {
      const shard = v.data as GraphShardData;
      return {
        _type: "graph_shard",
        nodes: shard.nodes.map((n) => ({
          id: n.id,
          labels: n.labels,
          props: Object.fromEntries(
            Object.entries(n.props).map(([k, val]) => [k, toAny(val)])
          ),
        })),
        edges: shard.edges.map((e) => ({
          from: e.fromId,
          to: e.toId,
          type: e.edgeType,
          props: Object.fromEntries(
            Object.entries(e.props).map(([k, val]) => [k, toAny(val)])
          ),
        })),
        metadata: Object.fromEntries(
          Object.entries(shard.metadata).map(([k, val]) => [k, toAny(val)])
        ),
      };
    }
    case Type.UNKNOWN_EXT: {
      const ext = v.data as UnknownExtData;
      let binary = "";
      for (const b of ext.payload) binary += String.fromCharCode(b);
      const extVal = ext.extType <= MAX_SAFE_INT ? Number(ext.extType) : ext.extType.toString();
      return {
        _type: "ext",
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
    } else if (v.type === Type.DELTA) {
      // Delta ops may contain nested values
      const delta = v.data as DeltaData;
      for (const op of delta.ops) {
        if (op.value) {
          this.collectKeysSorted(op.value);
        }
      }
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
      case Type.INT64:
        this.writeByteSorted(Tag.INT64);
        this.writeUvarintSorted(zigzagEncode(v.data as bigint));
        break;
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
        const buf = new ArrayBuffer(8);
        new DataView(buf).setFloat64(0, v.data as number, true);
        this.writeSorted(new Uint8Array(buf));
        break;
      }
      case Type.DECIMAL128: {
        const d = v.data as Decimal128;
        this.writeByteSorted(Tag.DECIMAL128);
        this.writeByteSorted(d.scale & 0xff);
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
        this.writeByteSorted(Tag.ARRAY);
        this.writeUvarintSorted(arr.length);
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
        this.writeByteSorted(Tag.OBJECT);
        this.writeUvarintSorted(entries.length);
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
      case Type.ADJLIST: {
        const adj = v.data as AdjlistData;
        this.writeByteSorted(Tag.ADJLIST);
        this.writeByteSorted(adj.idWidth);
        this.writeUvarintSorted(adj.nodeCount);
        this.writeUvarintSorted(adj.edgeCount);
        for (const offset of adj.rowOffsets) {
          this.writeUvarintSorted(offset);
        }
        this.writeSorted(adj.colIndices);
        break;
      }
      case Type.RICHTEXT: {
        const rt = v.data as RichTextData;
        this.writeByteSorted(Tag.RICHTEXT);
        const textBytes = sharedTextEncoder.encode(rt.text);
        this.writeUvarintSorted(textBytes.length);
        this.writeSorted(textBytes);
        // Calculate and write flags byte
        const tokens = rt.tokens || [];
        const spans = rt.spans || [];
        let flags = 0;
        if (tokens.length > 0) flags |= 0x01;
        if (spans.length > 0) flags |= 0x02;
        this.writeByteSorted(flags);
        // Write tokens if present
        if (flags & 0x01) {
          this.writeUvarintSorted(tokens.length);
          for (const tok of tokens) {
            const buf = new ArrayBuffer(4);
            new DataView(buf).setInt32(0, tok, true);
            this.writeSorted(new Uint8Array(buf));
          }
        }
        // Write spans if present
        if (flags & 0x02) {
          this.writeUvarintSorted(spans.length);
          for (const span of spans) {
            this.writeUvarintSorted(span.start);
            this.writeUvarintSorted(span.end);
            this.writeUvarintSorted(span.kindId);
          }
        }
        break;
      }
      case Type.DELTA: {
        const delta = v.data as DeltaData;
        this.writeByteSorted(Tag.DELTA);
        this.writeUvarintSorted(delta.baseId);
        this.writeUvarintSorted(delta.ops.length);
        for (const op of delta.ops) {
          this.writeByteSorted(op.opCode);
          this.writeUvarintSorted(op.fieldId);
          if (op.opCode === DeltaOpCode.SET_FIELD || op.opCode === DeltaOpCode.APPEND_ARRAY) {
            if (op.value) {
              this.encodeValueSorted(op.value);
            }
          }
        }
        break;
      }
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

function fnvHashByte(h: bigint, b: number): bigint {
  h ^= BigInt(b);
  h = BigInt.asUintN(64, h * FNV_PRIME);
  return h;
}

function fnvHashU64(h: bigint, v: bigint): bigint {
  for (let i = 0; i < 8; i++) {
    h ^= (v >> BigInt(i * 8)) & 0xffn;
    h = BigInt.asUintN(64, h * FNV_PRIME);
  }
  return h;
}

function fnvHashString(h: bigint, s: string): bigint {
  h = fnvHashU64(h, BigInt(s.length));
  const bytes = sharedTextEncoder.encode(s);
  for (const b of bytes) {
    h ^= BigInt(b);
    h = BigInt.asUintN(64, h * FNV_PRIME);
  }
  return h;
}

/**
 * Map Type enum to ordinal for cross-language fingerprint compatibility.
 * Matches Go's Type enum: Null=0, Bool=1, Int64=2, Uint64=3, Float64=4, Decimal128=5,
 * String=6, Bytes=7, Datetime64=8, UUID128=9, BigInt=10, Array=11, Object=12,
 * Tensor=13, Image=14, Audio=15, TensorRef=16, Adjlist=17, RichText=18, Delta=19
 */
function typeToOrd(type: Type): number {
  switch (type) {
    case Type.NULL:
      return 0;
    case Type.BOOL:
      return 1;
    case Type.INT64:
      return 2;
    case Type.UINT64:
      return 3;
    case Type.FLOAT64:
      return 4;
    case Type.DECIMAL128:
      return 5;
    case Type.STRING:
      return 6;
    case Type.BYTES:
      return 7;
    case Type.DATETIME64:
      return 8;
    case Type.UUID128:
      return 9;
    case Type.BIGINT:
      return 10;
    case Type.ARRAY:
      return 11;
    case Type.OBJECT:
      return 12;
    case Type.TENSOR:
      return 13;
    case Type.IMAGE:
      return 14;
    case Type.AUDIO:
      return 15;
    case Type.TENSOR_REF:
      return 16;
    case Type.ADJLIST:
      return 17;
    case Type.RICHTEXT:
      return 18;
    case Type.DELTA:
      return 19;
    default:
      return 0xff;
  }
}

function hashSchema(v: Value, h: bigint): bigint {
  h = fnvHashByte(h, typeToOrd(v.type));

  switch (v.type) {
    case Type.NULL:
    case Type.BOOL:
    case Type.INT64:
    case Type.UINT64:
    case Type.FLOAT64:
    case Type.STRING:
    case Type.BYTES:
    case Type.DECIMAL128:
    case Type.DATETIME64:
    case Type.UUID128:
    case Type.BIGINT:
      // Scalar types: type tag is sufficient
      break;

    case Type.ARRAY: {
      const arr = v.data as Value[];
      h = fnvHashU64(h, BigInt(arr.length));
      for (const item of arr) {
        h = hashSchema(item, h);
      }
      break;
    }

    case Type.OBJECT: {
      const obj = v.data as Record<string, Value>;
      // Use byte-order comparison for locale-independent deterministic sorting
      const entries = Object.entries(obj).sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0);
      h = fnvHashU64(h, BigInt(entries.length));
      for (const [key, val] of entries) {
        h = fnvHashString(h, key);
        h = hashSchema(val, h);
      }
      break;
    }

    case Type.TENSOR: {
      // Include dtype and rank (dims are data, not schema)
      const t = v.data as TensorData;
      h = fnvHashByte(h, t.dtype);
      h = fnvHashU64(h, BigInt(t.shape.length));
      break;
    }

    case Type.IMAGE: {
      // Include format in schema (dimensions are data, not schema)
      const img = v.data as ImageData;
      h = fnvHashByte(h, img.format);
      break;
    }

    case Type.AUDIO: {
      // Include encoding in schema (sample_rate, channels are data)
      const aud = v.data as AudioData;
      h = fnvHashByte(h, aud.encoding);
      break;
    }

    case Type.TENSOR_REF: {
      // Include store_id in schema (key is data)
      const ref = v.data as TensorRefData;
      h = fnvHashByte(h, ref.storeId);
      break;
    }

    case Type.ADJLIST: {
      // Include id_width in schema (counts are data)
      const adj = v.data as AdjlistData;
      h = fnvHashByte(h, adj.idWidth);
      break;
    }

    case Type.RICHTEXT:
      // Type tag is sufficient for schema (text/tokens/spans are data)
      break;

    case Type.DELTA: {
      // Include op codes in schema
      const delta = v.data as DeltaData;
      h = fnvHashU64(h, BigInt(delta.ops.length));
      for (const op of delta.ops) {
        h = fnvHashByte(h, op.opCode);
        if (op.value) {
          h = hashSchema(op.value, h);
        }
      }
      break;
    }
  }

  return h;
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
 * Check if two values have the same schema.
 */
export function schemaEquals(a: Value, b: Value): boolean {
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

  // Check magic
  if (!isMasterStream(data)) {
    // Check for legacy Cowrie document
    if (isCowrieDocument(data)) {
      return readLegacyDocument(data);
    }
    // Try legacy stream format
    return readLegacyStream(data);
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

function readLegacyDocument(data: Uint8Array): [MasterFrame, number] {
  const payload = decode(data);
  const typeId = schemaFingerprint32(payload);

  // Re-encode to find length (we need actual consumed bytes)
  const reencoded = encode(payload);

  const header: MasterFrameHeader = {
    version: MASTER_VERSION,
    flags: 0,
    headerLen: 0,
    typeId,
    payloadLen: reencoded.length,
    rawLen: 0,
    metaLen: 0,
  };

  return [{ header, meta: null, payload, typeId }, reencoded.length];
}

function readLegacyStream(data: Uint8Array): [MasterFrame, number] {
  if (data.length < 4) {
    throw new Error("Truncated legacy stream");
  }

  const frameLen = data[0] | (data[1] << 8) | (data[2] << 16) | (data[3] << 24);
  if (frameLen === 0) {
    throw new Error("Invalid legacy stream frame length");
  }

  if (4 + frameLen > data.length) {
    throw new Error("Truncated legacy stream (payload)");
  }

  const payload = decode(data.slice(4, 4 + frameLen));
  const typeId = schemaFingerprint32(payload);

  const header: MasterFrameHeader = {
    version: MASTER_VERSION,
    flags: 0,
    headerLen: 0,
    typeId,
    payloadLen: frameLen,
    rawLen: 0,
    metaLen: 0,
  };

  return [{ header, meta: null, payload, typeId }, 4 + frameLen];
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
