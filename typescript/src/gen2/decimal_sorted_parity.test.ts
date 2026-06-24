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
import { encode, encodeWithOpts, decode, CowrieError, ERR_NON_CANONICAL } from "./index.ts";

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
