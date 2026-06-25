/**
 * Regression gate for the DETERMINISTIC (sorted) encoder's Decimal128 path.
 *
 * The cross-language conformance gate (tools/conformance_gate.py) drives recode.ts, which
 * uses the MAIN encoder (`encode`). It never exercises the DeterministicEncoder/sorted path
 * (`encodeWithOpts({ deterministic: true })`). That path had a bug: it wrote `scale & 0xff`
 * as a raw int8 byte instead of a SVARINT (zigzag + LEB128), so it desynced for |scale| > 63
 * (e.g. decimal_scale_200, scale=200 -> SVARINT 0x90 0x03) and diverged from both the main
 * encoder and the Python oracle (cowrie_ref encode.py:60).
 *
 * This test pins the fix: for EVERY golden Decimal128 vector, the sorted encoder must produce
 * bytes BYTE-IDENTICAL to (a) the main encoder and (b) the golden canonical bytes.
 *
 * Run: node --import tsx --test src/gen2/decimal_sorted_parity.test.ts
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";
import { encode, encodeWithOpts, decode, fromAny, CowrieError, ERR_NON_CANONICAL } from "./index.ts";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const GOLDEN_PATH = path.join(__dirname, "..", "..", "..", "testdata", "v1_golden.json");
const NEGATIVE_PATH = path.join(__dirname, "..", "..", "..", "testdata", "v1_negative.json");

// v1_golden.json contains bare `NaN`/`Infinity` literals in some float vectors (display only),
// which JSON.parse cannot handle. We never read those fields here, so neutralize them to null.
function loadVectors(p: string): Record<string, any> {
  const txt = fs
    .readFileSync(p, "utf8")
    .replace(/:\s*-?Infinity/g, ": null")
    .replace(/:\s*NaN/g, ": null");
  return JSON.parse(txt);
}

const GOLDEN = loadVectors(GOLDEN_PATH);
const NEGATIVE = loadVectors(NEGATIVE_PATH);

const DECIMAL_VECTORS = Object.entries<any>(GOLDEN).filter(([name]) => name.startsWith("decimal"));

function hex(b: Uint8Array): string {
  return Buffer.from(b).toString("hex");
}

test("there are golden Decimal128 vectors to gate", () => {
  // Guards against a silently-empty filter (e.g. a rename) making this whole suite a no-op.
  assert.ok(DECIMAL_VECTORS.length >= 10, `expected >= 10 decimal vectors, got ${DECIMAL_VECTORS.length}`);
});

for (const [name, rec] of DECIMAL_VECTORS) {
  test(`deterministic (sorted) encoder == main encoder == golden: ${name}`, () => {
    const golden: string = rec.canonical_hex;
    const value = decode(Buffer.from(golden, "hex"));

    const main = hex(encode(value));
    const sorted = hex(encodeWithOpts(value, { deterministic: true }));

    // The main encoder must reproduce the golden bytes (sanity for the round-trip).
    assert.equal(main, golden, `main encoder != golden for ${name}`);
    // The bug this gates: the sorted encoder must match the main encoder byte-for-byte...
    assert.equal(sorted, main, `sorted encoder != main encoder for ${name}`);
    // ...and therefore the golden bytes as well.
    assert.equal(sorted, golden, `sorted encoder != golden for ${name}`);
  });
}

// ---------------------------------------------------------------------------
// Object-ordering parity: the deterministic (sorted) encoder MUST produce
// byte-identical output to the main encoder for objects, including nested
// objects and non-BMP/astral keys. The old DeterministicEncoder built its dict
// in DFS-discovery order (never globally re-sorted) and sorted sibling/object
// keys with JS UTF-16 `<` instead of UTF-8 bytes, so it diverged from encode()
// for these cases.
// ---------------------------------------------------------------------------
const OBJECT_PARITY_CASES: Record<string, unknown> = {
  // Nested object: dict discovery order is z, a (DFS) but canonical UTF-8 dict
  // order is a, z — exercises the global re-sort.
  nested_za: { z: { a: 1 } },
  // Sibling key b before nested a/c — exercises dict order across nesting levels.
  b_then_nested: { b: 1, a: { c: 2 } },
  // Astral/emoji key (U+1F600, non-BMP) vs fullwidth exclamation (U+FF01).
  // JS UTF-16 `<` orders the surrogate-pair emoji BEFORE U+FF01, but UTF-8 bytes
  // order U+FF01 (EF BC 81) BEFORE U+1F600 (F0 9F 98 80). Exercises UTF-8 sort.
  astral_keys: { "\u{1F600}": 1, "！": 2 },
};

for (const [name, plain] of Object.entries(OBJECT_PARITY_CASES)) {
  test(`deterministic (sorted) encoder == main encoder for object case: ${name}`, () => {
    const value = fromAny(plain);
    const main = hex(encode(value));
    const sorted = hex(encodeWithOpts(value, { deterministic: true }));
    assert.equal(sorted, main, `sorted encoder != main encoder for ${name}`);
    // Default opts (deterministic falsy) must also match: it falls through to encode().
    const sortedDefault = hex(encodeWithOpts(value, {}));
    assert.equal(sortedDefault, main, `encodeWithOpts(default) != main encoder for ${name}`);
    // Round-trips back to the same logical value.
    assert.deepEqual(decode(Buffer.from(sorted, "hex")), value);
  });
}

test("strict decode rejects decimal_zero_scale (coeff==0, scale!=0) with ERR_NON_CANONICAL", () => {
  const fx = NEGATIVE["decimal_zero_scale"];
  assert.ok(fx, "decimal_zero_scale fixture present in v1_negative.json");
  const raw = Buffer.from(fx.hex, "hex");

  // Lenient decode normalizes it (no throw).
  assert.doesNotThrow(() => decode(raw, { strict: false }));

  // Strict decode must reject it with the exact canonical code.
  assert.throws(
    () => decode(raw, { strict: true }),
    (err: unknown) => {
      assert.ok(err instanceof CowrieError, "expected a CowrieError");
      assert.equal((err as CowrieError).code, ERR_NON_CANONICAL, "expected ERR_NON_CANONICAL");
      return true;
    }
  );
});
