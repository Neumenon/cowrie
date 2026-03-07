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
 * import { encode, decode } from 'cowrie/gen1';
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

export interface Gen1Node {
  id: number | bigint;
  label: string;
  properties: Record<string, JsonValue>;
}

export interface Gen1Edge {
  src: number | bigint;
  dst: number | bigint;
  label: string;
  properties: Record<string, JsonValue>;
}

export interface Gen1AdjList {
  id_width: number;
  node_count: number;
  edge_count: number;
  row_offsets: Array<number | bigint>;
  col_indices: number[];
}

export interface Gen1NodeBatch {
  nodes: Gen1Node[];
}

export interface Gen1EdgeBatch {
  edges: Gen1Edge[];
}

export interface Gen1GraphShard {
  nodes: Gen1Node[];
  edges: Gen1Edge[];
  meta: Record<string, JsonValue>;
}

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

function isIntegerLike(v: unknown): v is number | bigint {
  return (typeof v === 'number' && Number.isInteger(v)) || typeof v === 'bigint';
}

function isObjectRecord(v: unknown): v is Record<string, JsonValue> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function hasExactKeys(obj: Record<string, unknown>, keys: string[]): boolean {
  const objKeys = Object.keys(obj);
  if (objKeys.length !== keys.length) return false;
  return keys.every((k) => Object.prototype.hasOwnProperty.call(obj, k));
}

function isByteArray(v: unknown): v is number[] {
  return Array.isArray(v) && v.every((x) => typeof x === 'number' && Number.isInteger(x) && x >= 0 && x <= 255);
}

function isNodeObject(v: Record<string, JsonValue>): v is Gen1Node {
  if (!hasExactKeys(v, ['id', 'label', 'properties'])) return false;
  return isIntegerLike(v.id) && typeof v.label === 'string' && isObjectRecord(v.properties);
}

function isEdgeObject(v: Record<string, JsonValue>): v is Gen1Edge {
  if (!hasExactKeys(v, ['src', 'dst', 'label', 'properties'])) return false;
  return isIntegerLike(v.src) && isIntegerLike(v.dst) && typeof v.label === 'string' && isObjectRecord(v.properties);
}

function isAdjListObject(v: Record<string, JsonValue>): v is Gen1AdjList {
  if (!hasExactKeys(v, ['id_width', 'node_count', 'edge_count', 'row_offsets', 'col_indices'])) return false;
  if (!isIntegerLike(v.id_width) || !isIntegerLike(v.node_count) || !isIntegerLike(v.edge_count)) return false;
  if (!Array.isArray(v.row_offsets) || !v.row_offsets.every(isIntegerLike)) return false;
  return isByteArray(v.col_indices);
}

function isNodeBatchObject(v: Record<string, JsonValue>): v is Gen1NodeBatch {
  if (!hasExactKeys(v, ['nodes']) || !Array.isArray(v.nodes)) return false;
  return v.nodes.every((n) => isObjectRecord(n) && isNodeObject(n));
}

function isEdgeBatchObject(v: Record<string, JsonValue>): v is Gen1EdgeBatch {
  if (!hasExactKeys(v, ['edges']) || !Array.isArray(v.edges)) return false;
  return v.edges.every((e) => isObjectRecord(e) && isEdgeObject(e));
}

function isGraphShardObject(v: Record<string, JsonValue>): v is Gen1GraphShard {
  if (!hasExactKeys(v, ['nodes', 'edges', 'meta'])) return false;
  if (!Array.isArray(v.nodes) || !v.nodes.every((n) => isObjectRecord(n) && isNodeObject(n))) return false;
  if (!Array.isArray(v.edges) || !v.edges.every((e) => isObjectRecord(e) && isEdgeObject(e))) return false;
  return isObjectRecord(v.meta);
}

function writeSignedInt(buf: number[], n: number | bigint): void {
  if (typeof n === 'bigint') {
    writeUvarintBigInt(buf, zigzagEncodeBigInt(n));
    return;
  }
  if (n >= -2147483648 && n <= 2147483647) {
    writeUvarint(buf, zigzagEncode(n));
  } else {
    writeUvarintBigInt(buf, zigzagEncodeBigInt(BigInt(n)));
  }
}

function writeString(buf: number[], s: string): void {
  const encoded = new TextEncoder().encode(s);
  writeUvarint(buf, encoded.length);
  for (const byte of encoded) buf.push(byte);
}

function writeObjectEntries(buf: number[], obj: Record<string, JsonValue>): void {
  const keys = Object.keys(obj).sort();
  writeUvarint(buf, keys.length);
  for (const key of keys) {
    writeString(buf, key);
    encodeValue(buf, obj[key]);
  }
}

function encodeNode(buf: number[], node: Gen1Node): void {
  buf.push(Tags.NODE);
  writeSignedInt(buf, node.id);
  writeString(buf, node.label);
  writeObjectEntries(buf, node.properties);
}

function encodeEdge(buf: number[], edge: Gen1Edge): void {
  buf.push(Tags.EDGE);
  writeSignedInt(buf, edge.src);
  writeSignedInt(buf, edge.dst);
  writeString(buf, edge.label);
  writeObjectEntries(buf, edge.properties);
}

function encodeAdjList(buf: number[], adj: Gen1AdjList): void {
  const idWidth = Number(adj.id_width);
  const nodeCount = Number(adj.node_count);
  const edgeCount = Number(adj.edge_count);
  if (!Number.isInteger(idWidth) || idWidth < 0 || idWidth > 255) {
    throw new Error('Invalid adjlist id_width');
  }
  if (!Number.isInteger(nodeCount) || nodeCount < 0) {
    throw new Error('Invalid adjlist node_count');
  }
  if (!Number.isInteger(edgeCount) || edgeCount < 0) {
    throw new Error('Invalid adjlist edge_count');
  }
  if (adj.row_offsets.length !== nodeCount + 1) {
    throw new Error('Invalid adjlist row_offsets length');
  }
  const expectedBytes = edgeCount * (idWidth === 1 ? 4 : 8);
  if (adj.col_indices.length !== expectedBytes) {
    throw new Error('Invalid adjlist col_indices length');
  }

  buf.push(Tags.ADJLIST);
  buf.push(idWidth);
  writeUvarint(buf, nodeCount);
  writeUvarint(buf, edgeCount);
  for (const off of adj.row_offsets) {
    if (!isIntegerLike(off)) throw new Error('Invalid row offset type');
    if (typeof off === 'number' && off < 0) throw new Error('Negative row offset');
    if (typeof off === 'bigint' && off < 0n) throw new Error('Negative row offset');
    if (typeof off === 'bigint') {
      writeUvarintBigInt(buf, off);
    } else {
      writeUvarint(buf, off);
    }
  }
  for (const b of adj.col_indices) buf.push(b);
}

function encodeNodeBatch(buf: number[], nb: Gen1NodeBatch): void {
  buf.push(Tags.NODE_BATCH);
  writeUvarint(buf, nb.nodes.length);
  for (const n of nb.nodes) encodeNode(buf, n);
}

function encodeEdgeBatch(buf: number[], eb: Gen1EdgeBatch): void {
  buf.push(Tags.EDGE_BATCH);
  writeUvarint(buf, eb.edges.length);
  for (const e of eb.edges) encodeEdge(buf, e);
}

function encodeGraphShard(buf: number[], gs: Gen1GraphShard): void {
  buf.push(Tags.GRAPH_SHARD);
  writeUvarint(buf, gs.nodes.length);
  for (const n of gs.nodes) encodeNode(buf, n);
  writeUvarint(buf, gs.edges.length);
  for (const e of gs.edges) encodeEdge(buf, e);
  writeObjectEntries(buf, gs.meta);
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
    writeString(buf, value);
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
    const obj = value as Record<string, JsonValue>;
    if (isGraphShardObject(obj)) {
      encodeGraphShard(buf, obj);
    } else if (isNodeBatchObject(obj)) {
      encodeNodeBatch(buf, obj);
    } else if (isEdgeBatchObject(obj)) {
      encodeEdgeBatch(buf, obj);
    } else if (isNodeObject(obj)) {
      encodeNode(buf, obj);
    } else if (isEdgeObject(obj)) {
      encodeEdge(buf, obj);
    } else if (isAdjListObject(obj)) {
      encodeAdjList(buf, obj);
    } else {
      buf.push(Tags.OBJECT);
      writeObjectEntries(buf, obj);
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

function readString(r: Reader): string {
  const len = readUvarint(r);
  if (len > Limits.MAX_STRING_LEN) {
    throw new SecurityLimitExceeded(`String too long: ${len} > ${Limits.MAX_STRING_LEN}`);
  }
  return new TextDecoder().decode(readBytes(r, len));
}

function readSignedInt(r: Reader): number | bigint {
  const decoded = zigzagDecodeBigInt(readUvarintBigInt(r));
  if (decoded >= -9007199254740991n && decoded <= 9007199254740991n) {
    return Number(decoded);
  }
  return decoded;
}

function readObjectEntries(r: Reader, depth: number): Record<string, JsonValue> {
  const count = readUvarint(r);
  if (count > Limits.MAX_OBJECT_LEN) {
    throw new SecurityLimitExceeded(`Object too large: ${count} > ${Limits.MAX_OBJECT_LEN}`);
  }
  const obj: Record<string, JsonValue> = {};
  for (let i = 0; i < count; i++) {
    const key = readString(r);
    obj[key] = decodeValue(r, depth + 1);
  }
  return obj;
}

function decodeNode(r: Reader, depth: number): Gen1Node {
  return {
    id: readSignedInt(r),
    label: readString(r),
    properties: readObjectEntries(r, depth),
  };
}

function decodeEdge(r: Reader, depth: number): Gen1Edge {
  return {
    src: readSignedInt(r),
    dst: readSignedInt(r),
    label: readString(r),
    properties: readObjectEntries(r, depth),
  };
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
      return readString(r);
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
      return readObjectEntries(r, depth);
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
        arr.push(readString(r));
      }
      return arr;
    }
    case Tags.NODE:
      return decodeNode(r, depth);
    case Tags.EDGE:
      return decodeEdge(r, depth);
    case Tags.ADJLIST: {
      const idWidth = readByte(r);
      const nodeCount = readUvarint(r);
      const edgeCount = readUvarint(r);
      if (nodeCount > Limits.MAX_ARRAY_LEN) {
        throw new SecurityLimitExceeded(`Adjlist node_count too large: ${nodeCount}`);
      }
      if (edgeCount > Limits.MAX_ARRAY_LEN) {
        throw new SecurityLimitExceeded(`Adjlist edge_count too large: ${edgeCount}`);
      }
      const rowOffsets: Array<number | bigint> = [];
      for (let i = 0; i < nodeCount + 1; i++) {
        rowOffsets.push(readUvarintBigInt(r));
      }
      const colBytes = edgeCount * (idWidth === 1 ? 4 : 8);
      const colIndices = Array.from(readBytes(r, colBytes));
      return {
        id_width: idWidth,
        node_count: nodeCount,
        edge_count: edgeCount,
        row_offsets: rowOffsets,
        col_indices: colIndices,
      };
    }
    case Tags.NODE_BATCH: {
      const count = readUvarint(r);
      if (count > Limits.MAX_ARRAY_LEN) {
        throw new SecurityLimitExceeded(`Node batch too large: ${count}`);
      }
      const nodes: Gen1Node[] = [];
      for (let i = 0; i < count; i++) {
        const n = decodeValue(r, depth + 1);
        if (!isObjectRecord(n) || !isNodeObject(n)) {
          throw new Error('Invalid node in node batch');
        }
        nodes.push(n);
      }
      return { nodes };
    }
    case Tags.EDGE_BATCH: {
      const count = readUvarint(r);
      if (count > Limits.MAX_ARRAY_LEN) {
        throw new SecurityLimitExceeded(`Edge batch too large: ${count}`);
      }
      const edges: Gen1Edge[] = [];
      for (let i = 0; i < count; i++) {
        const e = decodeValue(r, depth + 1);
        if (!isObjectRecord(e) || !isEdgeObject(e)) {
          throw new Error('Invalid edge in edge batch');
        }
        edges.push(e);
      }
      return { edges };
    }
    case Tags.GRAPH_SHARD: {
      const nodeCount = readUvarint(r);
      if (nodeCount > Limits.MAX_ARRAY_LEN) {
        throw new SecurityLimitExceeded(`GraphShard node_count too large: ${nodeCount}`);
      }
      const nodes: Gen1Node[] = [];
      for (let i = 0; i < nodeCount; i++) {
        const n = decodeValue(r, depth + 1);
        if (!isObjectRecord(n) || !isNodeObject(n)) {
          throw new Error('Invalid node in graph shard');
        }
        nodes.push(n);
      }
      const edgeCount = readUvarint(r);
      if (edgeCount > Limits.MAX_ARRAY_LEN) {
        throw new SecurityLimitExceeded(`GraphShard edge_count too large: ${edgeCount}`);
      }
      const edges: Gen1Edge[] = [];
      for (let i = 0; i < edgeCount; i++) {
        const e = decodeValue(r, depth + 1);
        if (!isObjectRecord(e) || !isEdgeObject(e)) {
          throw new Error('Invalid edge in graph shard');
        }
        edges.push(e);
      }
      const meta = readObjectEntries(r, depth);
      return { nodes, edges, meta };
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
