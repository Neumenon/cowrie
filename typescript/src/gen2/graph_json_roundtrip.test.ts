/**
 * Graph JSON encode-after-roundtrip parity.
 *
 * Mirrors go/graph_json_roundtrip_test.go (review gap #2). The JSON bridge
 * reconstructs graph values whose numeric props must be codec-safe; this asserts
 * the reconstructed value also ENCODES (the Go bug was encode-after-JSON, which a
 * pure toAny/fromAny round-trip would not catch). Confirms the TS bridge has no
 * analogous bug.
 *
 * Run: node --import tsx --test src/gen2/graph_json_roundtrip.test.ts
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { SJ, toAny, fromAny, encode, decode, Type } from "./index.ts";
import type { Value } from "./index.ts";

function jsonThenEncode(v: Value): Value {
  // toAny -> JSON string -> JSON parse -> fromAny -> encode -> decode.
  const back = fromAny(JSON.parse(JSON.stringify(toAny(v))));
  return decode(encode(back));
}

test("graph parity: node encodes after JSON round-trip", () => {
  const n = SJ.node("n1", ["Person"], {
    age: SJ.int64(42n),
    score: SJ.float64(3.5),
    name: SJ.string("Alice"),
  });
  assert.equal(jsonThenEncode(n).type, Type.NODE);
});

test("graph parity: edge encodes after JSON round-trip", () => {
  const e = SJ.edge("a", "b", "KNOWS", { weight: SJ.int64(7n), w2: SJ.float64(1.5) });
  assert.equal(jsonThenEncode(e).type, Type.EDGE);
});

test("graph parity: node_batch encodes after JSON round-trip", () => {
  const nb = SJ.nodeBatch([{ id: "n1", labels: ["P"], props: { age: SJ.int64(1n), f: SJ.float64(2.5) } }]);
  assert.equal(jsonThenEncode(nb).type, Type.NODE_BATCH);
});

test("graph parity: edge_batch encodes after JSON round-trip", () => {
  const eb = SJ.edgeBatch([{ fromId: "a", toId: "b", edgeType: "R", props: { w: SJ.int64(2n) } }]);
  assert.equal(jsonThenEncode(eb).type, Type.EDGE_BATCH);
});
