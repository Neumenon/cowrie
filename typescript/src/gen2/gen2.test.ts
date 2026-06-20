/**
 * Cowrie Cross-Language Parity Tests for TypeScript
 *
 * Tests deterministic encoding, schema fingerprinting, and master stream
 * against Go-generated golden files.
 *
 * Run: node --import tsx --test src/gen2/gen2.test.ts
 */

import { test } from "node:test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";
import {
  SJ,
  Value,
  Type,
  DType,
  AudioEncoding,
  ImageFormat,
  NodeData,
  EdgeData,
  TensorData,
  ImageData,
  AudioData,
  TensorRefData,
  BitmaskData,
  UnknownExtData,
  encode,
  decode,
  encodeWithOpts,
  toJSON,
  fromJSON,
  schemaFingerprint32,
  schemaFingerprint64,
  schemaEquals,
  schemaFingerprintEqual,
  writeMasterFrame,
  readMasterFrame,
  isMasterStream,
  crc32,
} from "./index.ts";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const TESTDATA_DIR = path.join(__dirname, "..", "..", "..", "testdata", "gen2");

function assertEqual<T>(actual: T, expected: T, msg: string): void {
  if (actual !== expected) {
    throw new Error(`${msg}: expected ${expected}, got ${actual}`);
  }
}

function assertClose(actual: number, expected: number, epsilon: number, msg: string): void {
  if (Math.abs(actual - expected) > epsilon) {
    throw new Error(`${msg}: expected ~${expected}, got ${actual}`);
  }
}

function readGolden(name: string): Uint8Array {
  const filePath = path.join(TESTDATA_DIR, `${name}.cowrie`);
  return new Uint8Array(fs.readFileSync(filePath));
}

function readGoldenFingerprint(name: string): number {
  const filePath = path.join(TESTDATA_DIR, `${name}.cowrie.fingerprint`);
  const hex = fs.readFileSync(filePath, "utf-8").trim();
  return parseInt(hex, 16);
}

// ============================================================
// Test Suite: Primitives Parity
// ============================================================

console.log("\n--- Primitives Parity Tests ---");

test("decode primitives.cowrie", () => {
  const data = readGolden("primitives");
  const val = decode(data);
  assertEqual(val.type, Type.OBJECT, "should be object");

  const obj = val.data as Record<string, Value>;

  // null_val
  assertEqual(obj["null_val"]?.type, Type.NULL, "null_val type");

  // bool_true
  assertEqual(obj["bool_true"]?.data, true, "bool_true value");

  // bool_false
  assertEqual(obj["bool_false"]?.data, false, "bool_false value");

  // int_positive
  assertEqual(obj["int_positive"]?.data, 42n, "int_positive value");

  // int_negative
  assertEqual(obj["int_negative"]?.data, -42n, "int_negative value");

  // string_val
  assertEqual(obj["string_val"]?.data, "hello, world!", "string_val value");

  // string_unicode
  assertEqual(obj["string_unicode"]?.data, "你好世界 🌍", "string_unicode value");
});

test("decode nested.cowrie", () => {
  const data = readGolden("nested");
  const val = decode(data);
  const obj = val.data as Record<string, Value>;

  const user = obj["user"]?.data as Record<string, Value>;
  assertEqual(user["name"]?.data, "Alice", "user.name");
  assertEqual(user["age"]?.data, 30n, "user.age");

  const emails = user["emails"]?.data as Value[];
  assertEqual(emails.length, 2, "emails length");
  assertEqual(emails[0].data, "alice@example.com", "emails[0]");
});

test("decode empty.cowrie", () => {
  const data = readGolden("empty");
  const val = decode(data);
  const obj = val.data as Record<string, Value>;

  const emptyArr = obj["empty_array"]?.data as Value[];
  assertEqual(emptyArr.length, 0, "empty_array length");

  const emptyObj = obj["empty_object"]?.data as Record<string, Value>;
  assertEqual(Object.keys(emptyObj).length, 0, "empty_object keys");

  assertEqual(obj["empty_string"]?.data, "", "empty_string");
});

test("decode integers.cowrie", () => {
  const data = readGolden("integers");
  const val = decode(data);
  const obj = val.data as Record<string, Value>;

  assertEqual(obj["zero"]?.data, 0n, "zero");
  assertEqual(obj["one"]?.data, 1n, "one");
  assertEqual(obj["minus_one"]?.data, -1n, "minus_one");
  assertEqual(obj["int_min"]?.data, -9223372036854775808n, "int_min");
  assertEqual(obj["int_max"]?.data, 9223372036854775807n, "int_max");
  assertEqual(obj["uint_max"]?.data, 18446744073709551615n, "uint_max");
});

test("decode floats.cowrie", () => {
  const data = readGolden("floats");
  const val = decode(data);
  const obj = val.data as Record<string, Value>;

  assertEqual(obj["zero"]?.data, 0.0, "zero");
  assertClose(obj["pi"]?.data as number, Math.PI, 1e-10, "pi");
  assertClose(obj["e"]?.data as number, Math.E, 1e-10, "e");
});

test("decode mixed_array.cowrie", () => {
  const data = readGolden("mixed_array");
  const val = decode(data);
  const arr = val.data as Value[];

  assertEqual(arr.length, 7, "array length");
  assertEqual(arr[0].type, Type.NULL, "arr[0] null");
  assertEqual(arr[1].data, true, "arr[1] true");
  assertEqual(arr[2].data, 42n, "arr[2] int");
  assertEqual(arr[4].data, "hello", "arr[4] string");
});

// ============================================================
// Test Suite: Deterministic Encoding
// ============================================================

console.log("\n--- Deterministic Encoding Tests ---");

test("decode deterministic.cowrie", () => {
  const data = readGolden("deterministic");
  const val = decode(data);
  const obj = val.data as Record<string, Value>;

  // Keys should exist (sorting verified by Go generator)
  assertEqual(obj["apple"]?.data, 2n, "apple");
  assertEqual(obj["mango"]?.data, 3n, "mango");
  assertEqual(obj["zebra"]?.data, 1n, "zebra");

  const banana = obj["banana"]?.data as Record<string, Value>;
  assertEqual(banana["a_inner"]?.data, "a", "banana.a_inner");
  assertEqual(banana["z_inner"]?.data, "z", "banana.z_inner");
});

test("deterministic encode produces identical output", () => {
  const obj1 = SJ.object({
    zebra: SJ.int64(1),
    apple: SJ.int64(2),
    mango: SJ.int64(3),
  });

  const obj2 = SJ.object({
    apple: SJ.int64(2),
    mango: SJ.int64(3),
    zebra: SJ.int64(1),
  });

  const enc1 = encodeWithOpts(obj1, { deterministic: true });
  const enc2 = encodeWithOpts(obj2, { deterministic: true });

  assertEqual(enc1.length, enc2.length, "encoded lengths should match");

  let match = true;
  for (let i = 0; i < enc1.length; i++) {
    if (enc1[i] !== enc2[i]) {
      match = false;
      break;
    }
  }
  if (!match) {
    throw new Error("deterministic encoding should produce identical bytes");
  }
});

test("deterministic encoding is stable across calls", () => {
  const obj = SJ.object({
    z: SJ.int64(3),
    a: SJ.int64(1),
    m: SJ.int64(2),
  });

  const enc1 = encodeWithOpts(obj, { deterministic: true });
  const enc2 = encodeWithOpts(obj, { deterministic: true });
  const enc3 = encodeWithOpts(obj, { deterministic: true });

  let allMatch = true;
  for (let i = 0; i < enc1.length; i++) {
    if (enc1[i] !== enc2[i] || enc2[i] !== enc3[i]) {
      allMatch = false;
      break;
    }
  }
  if (!allMatch) {
    throw new Error("deterministic encoding should be stable");
  }
});

// ============================================================
// Test Suite: Schema Fingerprinting
// ============================================================

console.log("\n--- Schema Fingerprinting Tests ---");

test("schema fingerprint matches Go (schema1)", () => {
  const data = readGolden("schema1");
  const val = decode(data);
  const expected = readGoldenFingerprint("schema1");
  const actual = schemaFingerprint32(val);

  assertEqual(actual, expected, `fingerprint mismatch: 0x${actual.toString(16)} vs 0x${expected.toString(16)}`);
});

test("schema fingerprint matches Go (schema2)", () => {
  const data = readGolden("schema2");
  const val = decode(data);
  const expected = readGoldenFingerprint("schema2");
  const actual = schemaFingerprint32(val);

  assertEqual(actual, expected, `fingerprint mismatch: 0x${actual.toString(16)} vs 0x${expected.toString(16)}`);
});

test("bitmask schema fingerprint matches Go (regression: typeToOrd must not return 0xff)", () => {
  // Ground truth computed from the Go reference: SchemaFingerprint{32,64}(Bitmask(8, [0xFF])).
  // Before the typeToOrd fix, TS returned 0xff for Type.BITMASK, producing a fingerprint
  // that silently disagreed with every other implementation. Bitmask (wire 0x24) is active.
  const val = SJ.bitmask(8, new Uint8Array([0xff]));
  assertEqual(schemaFingerprint32(val), 2248264336, "bitmask fp32 must match Go");
  assertEqual(
    schemaFingerprint64(val).toString(),
    "12638165210323077776",
    "bitmask fp64 must match Go",
  );
});

test("audio + unknown_ext schema fingerprints match Go (regression: channels + extType)", () => {
  // Ground truth from the Go reference. Before this fix, TS omitted Audio channels
  // (so ch1 == ch2, disagreeing with Go) and returned 0xff for UNKNOWN_EXT.
  const d = new Uint8Array([1, 2, 3]);
  const a2 = SJ.audio(AudioEncoding.PCM_INT16, 44100, 2, d);
  const a1 = SJ.audio(AudioEncoding.PCM_INT16, 44100, 1, d);
  assertEqual(schemaFingerprint32(a2), 3129750488, "audio ch=2 fp must match Go");
  assertEqual(schemaFingerprint32(a1), 3129751793, "audio ch=1 fp must match Go");
  if (schemaFingerprint32(a1) === schemaFingerprint32(a2)) {
    throw new Error("audio channels must affect the schema fingerprint");
  }
  assertEqual(schemaFingerprint32(SJ.unknownExt(99, d)), 2917281034, "unknown_ext fp must match Go");
});

test("schema1 and schema2 have same fingerprint", () => {
  const val1 = decode(readGolden("schema1"));
  const val2 = decode(readGolden("schema2"));

  const fp1 = schemaFingerprint32(val1);
  const fp2 = schemaFingerprint32(val2);

  assertEqual(fp1, fp2, "same schema should produce same fingerprint");
});

test("schemaEquals returns true for same schema", () => {
  const val1 = decode(readGolden("schema1"));
  const val2 = decode(readGolden("schema2"));

  if (!schemaEquals(val1, val2)) {
    throw new Error("schemaEquals should return true for same schema");
  }
});

test("different schemas have different fingerprints", () => {
  const obj1 = SJ.object({ name: SJ.string("test") });
  const obj2 = SJ.object({ id: SJ.string("test") });

  const fp1 = schemaFingerprint32(obj1);
  const fp2 = schemaFingerprint32(obj2);

  if (fp1 === fp2) {
    throw new Error("different field names should produce different fingerprints");
  }
});

test("same schema different values have same fingerprint", () => {
  const obj1 = SJ.object({ x: SJ.int64(1), y: SJ.int64(2) });
  const obj2 = SJ.object({ x: SJ.int64(100), y: SJ.int64(200) });

  const fp1 = schemaFingerprint32(obj1);
  const fp2 = schemaFingerprint32(obj2);

  assertEqual(fp1, fp2, "same schema should have same fingerprint regardless of values");
});

test("deterministic fingerprint matches Go", () => {
  const data = readGolden("deterministic");
  const val = decode(data);
  const expected = readGoldenFingerprint("deterministic");
  const actual = schemaFingerprint32(val);

  assertEqual(actual, expected, `deterministic fingerprint mismatch: 0x${actual.toString(16)} vs 0x${expected.toString(16)}`);
});

// ============================================================
// Test Suite: Master Stream
// ============================================================

console.log("\n--- Master Stream Tests ---");

test("write and read master frame roundtrip", () => {
  const value = SJ.object({
    name: SJ.string("test"),
    count: SJ.int64(42),
  });

  const frame = writeMasterFrame(value, null);

  if (!isMasterStream(frame)) {
    throw new Error("writeMasterFrame output should be recognized as master stream");
  }

  const [parsed, consumed] = readMasterFrame(frame);

  assertEqual(consumed, frame.length, "should consume entire buffer");

  const obj = parsed.payload.data as Record<string, Value>;
  assertEqual(obj["name"]?.data, "test", "payload.name");
  assertEqual(obj["count"]?.data, 42n, "payload.count");
});

test("master frame with metadata", () => {
  const value = SJ.object({ data: SJ.int64(123) });
  const meta = SJ.object({ trace_id: SJ.string("abc-123") });

  const frame = writeMasterFrame(value, meta);
  const [parsed, _] = readMasterFrame(frame);

  if (!parsed.meta) {
    throw new Error("metadata should be present");
  }

  const metaObj = parsed.meta.data as Record<string, Value>;
  assertEqual(metaObj["trace_id"]?.data, "abc-123", "meta.trace_id");
});

test("master frame with CRC", () => {
  const value = SJ.object({ test: SJ.bool(true) });

  const frame = writeMasterFrame(value, null, { enableCrc: true });
  const [parsed, _] = readMasterFrame(frame);

  const obj = parsed.payload.data as Record<string, Value>;
  assertEqual(obj["test"]?.data, true, "payload.test");
});

test("master frame with deterministic encoding", () => {
  const value = SJ.object({
    z: SJ.int64(3),
    a: SJ.int64(1),
    m: SJ.int64(2),
  });

  const frame1 = writeMasterFrame(value, null, { deterministic: true });
  const frame2 = writeMasterFrame(value, null, { deterministic: true });

  assertEqual(frame1.length, frame2.length, "deterministic frames should have same length");

  let match = true;
  for (let i = 0; i < frame1.length; i++) {
    if (frame1[i] !== frame2[i]) {
      match = false;
      break;
    }
  }
  if (!match) {
    throw new Error("deterministic frames should be identical");
  }
});

test("master stream magic is SJST", () => {
  const frame = writeMasterFrame(SJ.null(), null);

  assertEqual(frame[0], 0x53, "magic[0] = 'S'");
  assertEqual(frame[1], 0x4a, "magic[1] = 'J'");
  assertEqual(frame[2], 0x53, "magic[2] = 'S'");
  assertEqual(frame[3], 0x54, "magic[3] = 'T'");
});

// ============================================================
// Test Suite: CRC32
// ============================================================

console.log("\n--- CRC32 Tests ---");

test("crc32 empty data", () => {
  const result = crc32(new Uint8Array(0));
  assertEqual(result, 0, "crc32 of empty should be 0");
});

test("crc32 known value", () => {
  const data = new TextEncoder().encode("123456789");
  const result = crc32(data);
  // IEEE 802.3 CRC32 of "123456789" is 0xCBF43926
  assertEqual(result, 0xcbf43926, "crc32 of '123456789'");
});

test("crc32 consistency", () => {
  const data = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd]);
  const crc1 = crc32(data);
  const crc2 = crc32(data);
  assertEqual(crc1, crc2, "crc32 should be consistent");
});

// ============================================================
// Overflow Protection Tests
// ============================================================

console.log("# --- Overflow Protection Tests ---");

function encodeUvarintBytes(n: bigint): Uint8Array {
  const bytes: number[] = [];
  while (n >= 0x80n) {
    bytes.push(Number(n & 0x7fn) | 0x80);
    n >>= 7n;
  }
  bytes.push(Number(n));
  return new Uint8Array(bytes);
}

test("varint > 2^53 throws SecurityLimitExceeded", () => {
  // Craft a payload: header(SJ, v2, flags=0) + dict(count=0) + SJT_ARRAY + count=2^53+1
  const hugeCount = (1n << 53n) + 1n;
  const countBytes = encodeUvarintBytes(hugeCount);

  const header = new Uint8Array([
    0x53, 0x4a,  // magic "SJ"
    0x02,        // version 2
    0x00,        // flags
    0x00,        // dict count = 0
    0x06,        // SJT_ARRAY tag
  ]);

  const payload = new Uint8Array(header.length + countBytes.length);
  payload.set(header);
  payload.set(countBytes, header.length);

  let threw = false;
  try {
    decode(payload);
  } catch (e: any) {
    threw = true;
    if (!e.message.includes("safe integer")) {
      throw new Error(`Expected 'safe integer' error, got: ${e.message}`);
    }
  }
  if (!threw) {
    throw new Error("Expected decode to throw for varint > 2^53");
  }
});

test("varint at exactly 2^53-1 does not throw for safe integer check", () => {
  // This should pass the safe integer check (but fail later due to no data)
  const maxSafe = (1n << 53n) - 1n;
  const countBytes = encodeUvarintBytes(maxSafe);

  const header = new Uint8Array([
    0x53, 0x4a,  // magic "SJ"
    0x02,        // version 2
    0x00,        // flags
    0x00,        // dict count = 0
    0x06,        // SJT_ARRAY tag
  ]);

  const payload = new Uint8Array(header.length + countBytes.length);
  payload.set(header);
  payload.set(countBytes, header.length);

  let threwSafeInt = false;
  try {
    decode(payload);
  } catch (e: any) {
    if (e.message.includes("safe integer")) {
      threwSafeInt = true;
    }
    // Other errors (like "array too long") are expected and OK
  }
  if (threwSafeInt) {
    throw new Error("Should not throw safe integer error for 2^53-1");
  }
});

// ============================================================
// Test Suite: Graph Determinism (Go byte-parity regression)
// ============================================================

console.log("\n--- Graph Determinism Tests (Go parity) ---");

// Ground truth from Go canonical deterministic encoding.
// Props sorted by UTF-8 byte order: "age" < "name", "since" < "weight".
const GO_NODE_HEX       = "534a02000203616765046e616d6535026e310106506572736f6e02005e010505416c696365";
const GO_EDGE_HEX       = "534a0200020573696e6365067765696768743601610162054b4e4f5753020003c81f0145";
const GO_NODE_BATCH_HEX = "534a02000203616765046e616d653701026e310106506572736f6e02005e010505416c696365";
const GO_EDGE_BATCH_HEX = "534a0200020573696e636506776569676874380101610162054b4e4f5753020003c81f0145";

const testNode = SJ.node("n1", ["Person"], {
  name: SJ.string("Alice"),
  age: SJ.int64(30n),
});

const testEdge = SJ.edge("a", "b", "KNOWS", {
  weight: SJ.int64(5n),
  since: SJ.int64(2020n),
});

const testNodeBatch = SJ.nodeBatch([testNode.data as NodeData]);
const testEdgeBatch = SJ.edgeBatch([testEdge.data as EdgeData]);

test("graph NODE deterministic encoding matches Go hex", () => {
  const encoded = encodeWithOpts(testNode, { deterministic: true });
  const got = Buffer.from(encoded).toString("hex");
  if (got !== GO_NODE_HEX) {
    throw new Error(`NODE hex mismatch:\n  got: ${got}\n want: ${GO_NODE_HEX}`);
  }
});

test("graph EDGE deterministic encoding matches Go hex", () => {
  const encoded = encodeWithOpts(testEdge, { deterministic: true });
  const got = Buffer.from(encoded).toString("hex");
  if (got !== GO_EDGE_HEX) {
    throw new Error(`EDGE hex mismatch:\n  got: ${got}\n want: ${GO_EDGE_HEX}`);
  }
});

test("graph NODE_BATCH deterministic encoding matches Go hex", () => {
  const encoded = encodeWithOpts(testNodeBatch, { deterministic: true });
  const got = Buffer.from(encoded).toString("hex");
  if (got !== GO_NODE_BATCH_HEX) {
    throw new Error(`NODE_BATCH hex mismatch:\n  got: ${got}\n want: ${GO_NODE_BATCH_HEX}`);
  }
});

test("graph EDGE_BATCH deterministic encoding matches Go hex", () => {
  const encoded = encodeWithOpts(testEdgeBatch, { deterministic: true });
  const got = Buffer.from(encoded).toString("hex");
  if (got !== GO_EDGE_BATCH_HEX) {
    throw new Error(`EDGE_BATCH hex mismatch:\n  got: ${got}\n want: ${GO_EDGE_BATCH_HEX}`);
  }
});

// ============================================================
// FIX 1 — BigInt two's-complement (already correct, regression tests)
// ============================================================

console.log("\n--- BigInt Two's-Complement Tests ---");

test("bigint zero encodes to single 0x00 byte", () => {
  const v = SJ.bigint(0n);
  const b = v.data as Uint8Array;
  if (b.length !== 1 || b[0] !== 0x00) {
    throw new Error(`Expected [0x00], got [${Array.from(b).map(x => x.toString(16)).join(',')}]`);
  }
});

test("bigint -1 encodes as 0xff (two's complement)", () => {
  const v = SJ.bigint(-1n);
  const b = v.data as Uint8Array;
  if (b.length !== 1 || b[0] !== 0xff) {
    throw new Error(`Expected [0xff], got [${Array.from(b).map(x => x.toString(16)).join(',')}]`);
  }
});

test("bigint -128 encodes as 0x80 (minimal, no extra 0xff prefix)", () => {
  const v = SJ.bigint(-128n);
  const b = v.data as Uint8Array;
  if (b.length !== 1 || b[0] !== 0x80) {
    throw new Error(`Expected [0x80], got [${Array.from(b).map(x => x.toString(16)).join(',')}]`);
  }
});

test("bigint -256 encodes as [0xff, 0x00]", () => {
  const v = SJ.bigint(-256n);
  const b = v.data as Uint8Array;
  if (b.length !== 2 || b[0] !== 0xff || b[1] !== 0x00) {
    throw new Error(`Expected [0xff, 0x00], got [${Array.from(b).map(x => x.toString(16)).join(',')}]`);
  }
});

test("bigint 255 encodes as [0x00, 0xff] (top bit guard)", () => {
  const v = SJ.bigint(255n);
  const b = v.data as Uint8Array;
  if (b.length !== 2 || b[0] !== 0x00 || b[1] !== 0xff) {
    throw new Error(`Expected [0x00, 0xff], got [${Array.from(b).map(x => x.toString(16)).join(',')}]`);
  }
});

test("bigint large negative roundtrip", () => {
  const n = -123456789012345678901234567890n;
  const encoded = encode(SJ.bigint(n));
  const decoded = decode(encoded);
  // Re-derive bigint from stored bytes using bytesToBigint logic
  const bytes = decoded.data as Uint8Array;
  const neg = (bytes[0] & 0x80) !== 0;
  let result: bigint;
  if (neg) {
    const inv = new Uint8Array(bytes.length);
    let carry = 1;
    for (let i = bytes.length - 1; i >= 0; i--) {
      inv[i] = (~bytes[i]) & 0xff;
      inv[i] += carry;
      carry = inv[i] > 0xff ? 1 : 0;
      inv[i] &= 0xff;
    }
    let mag = 0n;
    for (const b of inv) mag = (mag << 8n) | BigInt(b);
    result = -mag;
  } else {
    result = 0n;
    for (const b of bytes) result = (result << 8n) | BigInt(b);
  }
  if (result !== n) {
    throw new Error(`Large negative bigint roundtrip failed: expected ${n}, got ${result}`);
  }
});

// ============================================================
// FIX 3 — Tensor decode validation (dataLen must match shape/dtype)
// ============================================================

console.log("\n--- Tensor Decode Validation Tests ---");

/** Build a raw Cowrie tensor payload with the given bytes at the end (bypassing SJ.tensor constructor) */
function buildTensorPayload(dtype: number, shape: number[], dataBytes: Uint8Array): Uint8Array {
  // Header: SJ magic + version + flags + dict(0)
  const header = new Uint8Array([0x53, 0x4a, 0x02, 0x00, 0x00]);
  // TENSOR tag
  const tag = new Uint8Array([0x20]);
  // dtype byte
  const dtypeByte = new Uint8Array([dtype]);
  // rank byte
  const rankByte = new Uint8Array([shape.length]);
  // dims as uvarints
  const encodeUv = (n: number): number[] => {
    const out: number[] = [];
    while (n >= 0x80) { out.push((n & 0x7f) | 0x80); n >>>= 7; }
    out.push(n);
    return out;
  };
  const dims: number[] = [];
  for (const d of shape) dims.push(...encodeUv(d));
  // data length + data
  const dataLen = encodeUv(dataBytes.length);
  const total = header.length + tag.length + dtypeByte.length + rankByte.length + dims.length + dataLen.length + dataBytes.length;
  const buf = new Uint8Array(total);
  let pos = 0;
  const write = (arr: Uint8Array | number[]) => { for (const b of arr) buf[pos++] = b; };
  write(header); write(tag); write(dtypeByte); write(rankByte);
  write(dims); write(dataLen); write(dataBytes);
  return buf;
}

test("tensor decode: valid float32 [2,3] = 24 bytes passes", () => {
  // 2 * 3 * 4 = 24 bytes
  const data = new Uint8Array(24);
  const payload = buildTensorPayload(0x01 /* FLOAT32 */, [2, 3], data);
  const v = decode(payload);
  if (v.type !== Type.TENSOR) throw new Error("expected TENSOR");
});

test("tensor decode: malformed float32 [2,3] with wrong byte count throws", () => {
  // shape says 24 bytes but we give 16
  const data = new Uint8Array(16);
  const payload = buildTensorPayload(0x01 /* FLOAT32 */, [2, 3], data);
  let threw = false;
  try {
    decode(payload);
  } catch (e: any) {
    threw = true;
    if (!e.message.includes("does not match shape/dtype")) {
      throw new Error(`Expected shape/dtype error, got: ${e.message}`);
    }
  }
  if (!threw) throw new Error("Expected tensor validation to throw");
});

test("tensor decode: malformed float64 [3] with 16 bytes (expected 24) throws", () => {
  const data = new Uint8Array(16);
  const payload = buildTensorPayload(0x0c /* FLOAT64 */, [3], data);
  let threw = false;
  try {
    decode(payload);
  } catch (e: any) {
    threw = true;
    if (!e.message.includes("does not match shape/dtype")) {
      throw new Error(`Expected shape/dtype error, got: ${e.message}`);
    }
  }
  if (!threw) throw new Error("Expected tensor validation to throw for float64 mismatch");
});

test("tensor decode: sub-byte dtype (BOOL=0x0d) skips validation", () => {
  // BOOL is sub-byte packed; we accept any length without checking
  const data = new Uint8Array(3); // arbitrary size
  const payload = buildTensorPayload(0x0d /* BOOL */, [10], data);
  // Should not throw (validation skipped for sub-byte types)
  decode(payload);
});

test("tensor decode: rank-0 tensor with 0 bytes passes", () => {
  const data = new Uint8Array(0);
  const payload = buildTensorPayload(0x01 /* FLOAT32 */, [], data);
  const v = decode(payload);
  if (v.type !== Type.TENSOR) throw new Error("expected TENSOR");
});

// ============================================================
// FIX 4 — schemaEquals structural (not fingerprint-based)
// ============================================================

console.log("\n--- schemaEquals Structural Tests ---");

test("schemaEquals: same object schema returns true", () => {
  const a = SJ.object({ name: SJ.string("alice"), age: SJ.int64(30n) });
  const b = SJ.object({ name: SJ.string("bob"), age: SJ.int64(99n) });
  if (!schemaEquals(a, b)) throw new Error("same schema should return true");
});

test("schemaEquals: different field names returns false", () => {
  const a = SJ.object({ name: SJ.string("alice") });
  const b = SJ.object({ id: SJ.string("alice") });
  if (schemaEquals(a, b)) throw new Error("different field names should return false");
});

test("schemaEquals: different field types returns false", () => {
  const a = SJ.object({ x: SJ.string("hi") });
  const b = SJ.object({ x: SJ.int64(1n) });
  if (schemaEquals(a, b)) throw new Error("different field types should return false");
});

test("schemaEquals: structurally different nested objects", () => {
  const a = SJ.object({ inner: SJ.object({ x: SJ.int64(1n) }) });
  const b = SJ.object({ inner: SJ.object({ y: SJ.int64(1n) }) });
  if (schemaEquals(a, b)) throw new Error("structurally different nested objects should return false");
});

test("schemaEquals: different array lengths returns false", () => {
  const a = SJ.array([SJ.int64(1n), SJ.int64(2n)]);
  const b = SJ.array([SJ.int64(1n)]);
  if (schemaEquals(a, b)) throw new Error("arrays with different lengths should return false");
});

test("schemaEquals: different tensor dtypes returns false", () => {
  const data = new Uint8Array(4);
  const a = SJ.tensor(DType.FLOAT32, [1], data);
  const b = SJ.tensor(DType.FLOAT64, [1], new Uint8Array(8));
  if (schemaEquals(a, b)) throw new Error("different tensor dtypes should return false");
});

test("schemaEquals: different tensor ranks returns false", () => {
  const a = SJ.tensor(DType.FLOAT32, [2, 3], new Uint8Array(24));
  const b = SJ.tensor(DType.FLOAT32, [6], new Uint8Array(24));
  if (schemaEquals(a, b)) throw new Error("different tensor ranks should return false");
});

test("schemaEquals: same tensor dtype+rank with different values is true", () => {
  const a = SJ.tensor(DType.FLOAT32, [2, 3], new Uint8Array(24));
  const b = SJ.tensor(DType.FLOAT32, [4, 5], new Uint8Array(80));
  // Same dtype + same rank (2) → same schema
  if (!schemaEquals(a, b)) throw new Error("same dtype+rank should return true regardless of dim values");
});

test("schemaEquals: image format difference returns false", () => {
  const a = SJ.image(ImageFormat.JPEG, 100, 100, new Uint8Array(100));
  const b = SJ.image(ImageFormat.PNG, 100, 100, new Uint8Array(100));
  if (schemaEquals(a, b)) throw new Error("different image formats should return false");
});

test("schemaFingerprintEqual is exported and agrees with schemaEquals for same schema", () => {
  const a = SJ.object({ x: SJ.int64(1n) });
  const b = SJ.object({ x: SJ.int64(999n) });
  if (!schemaEquals(a, b)) throw new Error("schemaEquals should be true");
  if (!schemaFingerprintEqual(a, b)) throw new Error("schemaFingerprintEqual should also be true");
});

test("schemaEquals: scalars of same type match", () => {
  if (!schemaEquals(SJ.int64(1n), SJ.int64(99n))) throw new Error("int64 schema should match");
  if (!schemaEquals(SJ.string("a"), SJ.string("b"))) throw new Error("string schema should match");
  if (schemaEquals(SJ.int64(1n), SJ.string("a"))) throw new Error("int64 vs string should not match");
});

// ============================================================
// JSON Bridge Round-Trip Tests
// ============================================================

console.log("\n--- JSON Bridge Round-Trip Tests ---");

function assertBytesEqual(a: Uint8Array, b: Uint8Array, msg: string): void {
  if (a.length !== b.length) {
    throw new Error(`${msg}: length mismatch ${a.length} vs ${b.length}`);
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      throw new Error(`${msg}: byte mismatch at index ${i}: ${a[i]} vs ${b[i]}`);
    }
  }
}

test("JSON bridge round-trip: tensor", () => {
  // float32 [2,3] = 24 bytes
  const data = new Uint8Array(24);
  for (let i = 0; i < 24; i++) data[i] = i;
  const orig = SJ.tensor(DType.FLOAT32, [2, 3], data);

  const json = toJSON(orig);
  // Verify canonical field names in JSON output
  const parsed = JSON.parse(json);
  if (parsed._type !== "tensor") throw new Error(`expected _type "tensor", got "${parsed._type}"`);
  if (!Array.isArray(parsed.dims)) throw new Error("expected 'dims' field (not 'shape')");
  if (parsed.dims[0] !== 2 || parsed.dims[1] !== 3) throw new Error(`dims mismatch: ${JSON.stringify(parsed.dims)}`);
  if (typeof parsed.data !== "string") throw new Error("expected base64 data string");
  if (parsed.dtype !== "float32") throw new Error(`dtype mismatch: ${parsed.dtype}`);

  const roundTripped = fromJSON(json);
  if (roundTripped.type !== Type.TENSOR) throw new Error("round-trip: expected TENSOR type");
  const t = roundTripped.data as TensorData;
  if (t.dtype !== DType.FLOAT32) throw new Error(`round-trip dtype mismatch: ${t.dtype}`);
  if (t.shape[0] !== 2 || t.shape[1] !== 3) throw new Error(`round-trip shape mismatch: ${t.shape}`);
  assertBytesEqual(t.data, data, "round-trip tensor data");
});

test("JSON bridge round-trip: image", () => {
  const data = new Uint8Array([0xff, 0xd8, 0xff, 0xe0, 0x01]);
  const orig = SJ.image(ImageFormat.JPEG, 640, 480, data);

  const json = toJSON(orig);
  const parsed = JSON.parse(json);
  if (parsed._type !== "image") throw new Error(`expected _type "image"`);
  if (parsed.format !== "jpeg") throw new Error(`format mismatch: ${parsed.format}`);
  if (parsed.width !== 640) throw new Error(`width mismatch: ${parsed.width}`);
  if (parsed.height !== 480) throw new Error(`height mismatch: ${parsed.height}`);

  const roundTripped = fromJSON(json);
  if (roundTripped.type !== Type.IMAGE) throw new Error("round-trip: expected IMAGE type");
  const img = roundTripped.data as ImageData;
  if (img.format !== ImageFormat.JPEG) throw new Error(`round-trip format mismatch: ${img.format}`);
  if (img.width !== 640) throw new Error(`round-trip width mismatch: ${img.width}`);
  if (img.height !== 480) throw new Error(`round-trip height mismatch: ${img.height}`);
  assertBytesEqual(img.data, data, "round-trip image data");
});

test("JSON bridge round-trip: audio", () => {
  const data = new Uint8Array([0x01, 0x02, 0x03, 0x04]);
  const orig = SJ.audio(AudioEncoding.PCM_INT16, 44100, 2, data);

  const json = toJSON(orig);
  const parsed = JSON.parse(json);
  if (parsed._type !== "audio") throw new Error(`expected _type "audio"`);
  if (parsed.encoding !== "pcm_int16") throw new Error(`encoding mismatch: ${parsed.encoding}`);
  if (parsed.rate !== 44100) throw new Error(`rate mismatch (expected 'rate' not 'sampleRate'): ${JSON.stringify(parsed)}`);
  if (parsed.channels !== 2) throw new Error(`channels mismatch: ${parsed.channels}`);
  if (parsed.sampleRate !== undefined) throw new Error("found 'sampleRate' key — should be 'rate'");

  const roundTripped = fromJSON(json);
  if (roundTripped.type !== Type.AUDIO) throw new Error("round-trip: expected AUDIO type");
  const aud = roundTripped.data as AudioData;
  if (aud.encoding !== AudioEncoding.PCM_INT16) throw new Error(`round-trip encoding mismatch`);
  if (aud.sampleRate !== 44100) throw new Error(`round-trip sampleRate mismatch: ${aud.sampleRate}`);
  if (aud.channels !== 2) throw new Error(`round-trip channels mismatch: ${aud.channels}`);
  assertBytesEqual(aud.data, data, "round-trip audio data");
});

test("JSON bridge round-trip: tensor_ref", () => {
  const key = new Uint8Array([0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02, 0x03,
                               0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b]);
  const orig = SJ.tensorRef(7, key);

  const json = toJSON(orig);
  const parsed = JSON.parse(json);
  if (parsed._type !== "tensor_ref") throw new Error(`expected _type "tensor_ref"`);
  if (parsed.store !== 7) throw new Error(`store mismatch (expected 'store' not 'storeId'): ${JSON.stringify(parsed)}`);
  if (parsed.storeId !== undefined) throw new Error("found 'storeId' key — should be 'store'");
  if (typeof parsed.key !== "string") throw new Error("expected base64 key string");

  const roundTripped = fromJSON(json);
  if (roundTripped.type !== Type.TENSOR_REF) throw new Error("round-trip: expected TENSOR_REF type");
  const ref = roundTripped.data as TensorRefData;
  if (ref.storeId !== 7) throw new Error(`round-trip storeId mismatch: ${ref.storeId}`);
  assertBytesEqual(ref.key, key, "round-trip tensor_ref key");
});

test("JSON bridge round-trip: node", () => {
  const orig = SJ.node("n42", ["Person", "Employee"], {
    name: SJ.string("Alice"),
    score: SJ.int64(99n),
  });

  const json = toJSON(orig);
  const parsed = JSON.parse(json);
  if (parsed._type !== "node") throw new Error(`expected _type "node"`);
  if (parsed.id !== "n42") throw new Error(`id mismatch`);
  if (!Array.isArray(parsed.labels)) throw new Error("expected labels array");
  if (parsed.labels[0] !== "Person") throw new Error("labels[0] mismatch");
  if (typeof parsed.props !== "object") throw new Error("expected props object");

  const roundTripped = fromJSON(json);
  if (roundTripped.type !== Type.NODE) throw new Error("round-trip: expected NODE type");
  const node = roundTripped.data as NodeData;
  if (node.id !== "n42") throw new Error(`round-trip id mismatch: ${node.id}`);
  if (node.labels[0] !== "Person" || node.labels[1] !== "Employee") throw new Error("round-trip labels mismatch");
  if (node.props["name"]?.data !== "Alice") throw new Error("round-trip props.name mismatch");
  if (node.props["score"]?.data !== 99n) throw new Error("round-trip props.score mismatch");
});

test("JSON bridge round-trip: edge (canonical field names fromId/toId)", () => {
  const orig = SJ.edge("n1", "n2", "KNOWS", { weight: SJ.float64(0.9) });

  const json = toJSON(orig);
  const parsed = JSON.parse(json);
  if (parsed._type !== "edge") throw new Error(`expected _type "edge"`);
  if (parsed.fromId !== "n1") throw new Error(`expected 'fromId', got: ${JSON.stringify(parsed)}`);
  if (parsed.toId !== "n2") throw new Error(`expected 'toId', got: ${JSON.stringify(parsed)}`);
  if (parsed.type !== "KNOWS") throw new Error(`type mismatch`);
  if (parsed.from !== undefined) throw new Error("found 'from' key — should be 'fromId'");
  if (parsed.to !== undefined) throw new Error("found 'to' key — should be 'toId'");

  const roundTripped = fromJSON(json);
  if (roundTripped.type !== Type.EDGE) throw new Error("round-trip: expected EDGE type");
  const edge = roundTripped.data as EdgeData;
  if (edge.fromId !== "n1") throw new Error(`round-trip fromId mismatch: ${edge.fromId}`);
  if (edge.toId !== "n2") throw new Error(`round-trip toId mismatch: ${edge.toId}`);
  if (edge.edgeType !== "KNOWS") throw new Error(`round-trip edgeType mismatch: ${edge.edgeType}`);
});

test("JSON bridge round-trip: node_batch", () => {
  const n1: NodeData = { id: "a", labels: ["X"], props: { val: SJ.int64(1n) } };
  const n2: NodeData = { id: "b", labels: ["Y"], props: {} };
  const orig = SJ.nodeBatch([n1, n2]);

  const json = toJSON(orig);
  const parsed = JSON.parse(json);
  if (parsed._type !== "node_batch") throw new Error(`expected _type "node_batch"`);
  if (!Array.isArray(parsed.nodes)) throw new Error("expected nodes array");
  if (parsed.nodes[0]._type !== "node") throw new Error("inner node should have _type 'node'");
  if (parsed.nodes[0].id !== "a") throw new Error(`nodes[0].id mismatch`);

  const roundTripped = fromJSON(json);
  if (roundTripped.type !== Type.NODE_BATCH) throw new Error("round-trip: expected NODE_BATCH type");
  const batch = roundTripped.data as { nodes: NodeData[] };
  if (batch.nodes.length !== 2) throw new Error(`round-trip nodes.length mismatch: ${batch.nodes.length}`);
  if (batch.nodes[0].id !== "a") throw new Error(`round-trip nodes[0].id mismatch`);
  if (batch.nodes[1].id !== "b") throw new Error(`round-trip nodes[1].id mismatch`);
});

test("JSON bridge round-trip: edge_batch", () => {
  const e1: EdgeData = { fromId: "a", toId: "b", edgeType: "REL", props: {} };
  const orig = SJ.edgeBatch([e1]);

  const json = toJSON(orig);
  const parsed = JSON.parse(json);
  if (parsed._type !== "edge_batch") throw new Error(`expected _type "edge_batch"`);
  if (!Array.isArray(parsed.edges)) throw new Error("expected edges array");
  if (parsed.edges[0]._type !== "edge") throw new Error("inner edge should have _type 'edge'");
  if (parsed.edges[0].fromId !== "a") throw new Error(`edges[0].fromId mismatch`);
  if (parsed.edges[0].toId !== "b") throw new Error(`edges[0].toId mismatch`);

  const roundTripped = fromJSON(json);
  if (roundTripped.type !== Type.EDGE_BATCH) throw new Error("round-trip: expected EDGE_BATCH type");
  const batch = roundTripped.data as { edges: EdgeData[] };
  if (batch.edges[0].fromId !== "a") throw new Error(`round-trip edges[0].fromId mismatch`);
  if (batch.edges[0].toId !== "b") throw new Error(`round-trip edges[0].toId mismatch`);
  if (batch.edges[0].edgeType !== "REL") throw new Error(`round-trip edges[0].edgeType mismatch`);
});

test("JSON bridge round-trip: bitmask (bits as base64, not boolean array)", () => {
  // 10 bits: [true, false, true, false, true, true, false, false, true, false]
  // byte 0: 0b00110101 = 0x35, byte 1: 0b00000001 = 0x01
  const bits = new Uint8Array([0x35, 0x01]);
  const orig = SJ.bitmask(10, bits);

  const json = toJSON(orig);
  const parsed = JSON.parse(json);
  if (parsed._type !== "bitmask") throw new Error(`expected _type "bitmask"`);
  if (parsed.count !== 10) throw new Error(`count mismatch: ${parsed.count}`);
  if (typeof parsed.bits !== "string") throw new Error(`'bits' must be a base64 string, got ${typeof parsed.bits}`);
  if (Array.isArray(parsed.bits)) throw new Error("'bits' must NOT be a boolean array — must be base64 string");

  const roundTripped = fromJSON(json);
  if (roundTripped.type !== Type.BITMASK) throw new Error("round-trip: expected BITMASK type");
  const bm = roundTripped.data as BitmaskData;
  if (bm.count !== 10) throw new Error(`round-trip count mismatch: ${bm.count}`);
  assertBytesEqual(bm.bits, bits, "round-trip bitmask bits");
});

test("JSON bridge round-trip: unknown_ext (_type is 'unknown_ext' not 'ext')", () => {
  const payload = new Uint8Array([0xca, 0xfe, 0xba, 0xbe]);
  const orig = SJ.unknownExt(99, payload);

  const json = toJSON(orig);
  const parsed = JSON.parse(json);
  if (parsed._type !== "unknown_ext") throw new Error(`expected _type "unknown_ext", got "${parsed._type}"`);
  if (parsed.ext_type !== 99) throw new Error(`ext_type mismatch: ${parsed.ext_type}`);
  if (typeof parsed.payload !== "string") throw new Error("expected base64 payload string");

  const roundTripped = fromJSON(json);
  if (roundTripped.type !== Type.UNKNOWN_EXT) throw new Error("round-trip: expected UNKNOWN_EXT type");
  const ext = roundTripped.data as UnknownExtData;
  if (ext.extType !== 99n) throw new Error(`round-trip extType mismatch: ${ext.extType}`);
  assertBytesEqual(ext.payload, payload, "round-trip unknown_ext payload");
});

