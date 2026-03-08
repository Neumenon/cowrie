/**
 * Cowrie Cross-Language Parity Tests for TypeScript
 *
 * Tests deterministic encoding, schema fingerprinting, and master stream
 * against Go-generated golden files.
 *
 * Run: npx ts-node cowrie.test.ts
 */

import * as fs from "fs";
import * as path from "path";
import {
  SJ,
  Value,
  Type,
  encode,
  decode,
  encodeWithOpts,
  schemaFingerprint32,
  schemaFingerprint64,
  schemaEquals,
  writeMasterFrame,
  readMasterFrame,
  isMasterStream,
  crc32,
} from "./index.ts";

const TESTDATA_DIR = path.join(__dirname, "..", "..", "..", "testdata", "gen2");

let passed = 0;
let failed = 0;

function test(name: string, fn: () => void): void {
  try {
    fn();
    console.log(`  PASS: ${name}`);
    passed++;
  } catch (e: any) {
    console.log(`  FAIL: ${name}`);
    console.log(`        ${e.message}`);
    failed++;
  }
}

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
// Summary
// ============================================================

console.log("\n========================================");
console.log(`PASSED: ${passed}`);
console.log(`FAILED: ${failed}`);
console.log("========================================\n");

if (failed > 0) {
  process.exit(1);
}
