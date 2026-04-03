/**
 * vLLM Payload Benchmark: Cowrie TS vs @msgpack/msgpack vs JSON
 *
 * Run: npx tsx src/gen2/bench_vllm.ts
 */

import { encode, decode, fromAny, toAny, dumps, loads, Type, DType, SJ } from "./index.js";
import type { Value, TensorData } from "./index.js";
import { encode as mpEncode, decode as mpDecode, ExtensionCodec } from "@msgpack/msgpack";
import { encode as wasmEncode, decode as wasmDecode } from "../../../rust/cowrie-wasm/pkg-node/cowrie_wasm.js";

// ── Helpers ──

function randFloat32(dims: number, seed = 42): Uint8Array {
  // Deterministic pseudo-random float32 bytes
  let s = seed;
  const buf = new Float32Array(dims);
  for (let i = 0; i < dims; i++) {
    s = (s * 1103515245 + 12345) & 0x7fffffff;
    buf[i] = (s / 0x7fffffff) * 2 - 1;
  }
  return new Uint8Array(buf.buffer);
}

function fmt(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

function benchEncode(name: string, fn: () => Uint8Array, iters: number): { data: Uint8Array; us: number } {
  // warmup
  for (let i = 0; i < 20; i++) fn();
  const t0 = performance.now();
  let data!: Uint8Array;
  for (let i = 0; i < iters; i++) data = fn();
  const t1 = performance.now();
  return { data, us: ((t1 - t0) * 1000) / iters };
}

function benchDecode(name: string, data: Uint8Array, fn: (d: Uint8Array) => unknown, iters: number): number {
  for (let i = 0; i < 20; i++) fn(data);
  const t0 = performance.now();
  for (let i = 0; i < iters; i++) fn(data);
  const t1 = performance.now();
  return ((t1 - t0) * 1000) / iters;
}

// ── MsgPack with tensor ext (like vLLM PR #12918 approach) ──

const tensorExtCodec = new ExtensionCodec();
tensorExtCodec.register({
  type: 0x10,
  encode: (object: unknown): Uint8Array | null => {
    if (object instanceof Float32Array) {
      return new Uint8Array(object.buffer, object.byteOffset, object.byteLength);
    }
    if (object instanceof Uint8Array && (object as any).__tensor) {
      return object;
    }
    return null;
  },
  decode: (data: Uint8Array): Float32Array => {
    // Must copy — msgpack buffer may not be 4-byte aligned
    const copy = new Uint8Array(data.byteLength);
    copy.set(data);
    return new Float32Array(copy.buffer);
  },
});

// ── Payloads ──

function makeSamplingParams() {
  return {
    n: 1, best_of: 1,
    presence_penalty: 0.0, frequency_penalty: 0.0,
    repetition_penalty: 1.0, temperature: 0.7,
    top_p: 0.95, top_k: 50, min_p: 0.0,
    use_beam_search: false, length_penalty: 1.0,
    early_stopping: false, stop: [], stop_token_ids: [],
    max_tokens: 2048, min_tokens: 0,
    skip_special_tokens: true, spaces_between_special_tokens: true,
  };
}

function makeEmbedding(dims: number) {
  const tensorBytes = randFloat32(dims);
  return {
    json: {
      object: "embedding",
      index: 0,
      embedding: Array.from(new Float32Array(tensorBytes.buffer)),
    },
    msgpack: {
      object: "embedding",
      index: 0,
      embedding: new Float32Array(tensorBytes.buffer), // ExtType encodes this
    },
    cowrie: (): Value => {
      return fromAny({
        object: "embedding",
        index: 0,
      });
    },
    cowrieWithTensor: (): Uint8Array => {
      const v = SJ.object({
        object: SJ.string("embedding"),
        index: SJ.int64(0),
        embedding: SJ.tensor(DType.FLOAT32, [dims], tensorBytes),
      });
      return encode(v);
    },
    tensorBytes,
  };
}

// ── Main ──

console.log("=".repeat(78));
console.log("  Cowrie TS vs MsgPack vs JSON — vLLM Payload Shapes");
console.log("=".repeat(78));
console.log();

const payloads: [string, number][] = [
  ["SamplingParams (metadata)", 0],
  ["Embedding 1536-dim (OpenAI small)", 1536],
  ["Embedding 3072-dim (OpenAI large)", 3072],
];

interface Result {
  name: string;
  jsonSize: number;
  jsonEnc: number;
  jsonDec: number;
  mpSize: number;
  mpEnc: number;
  mpDec: number;
  cowSize: number;
  cowEnc: number;
  cowDec: number;
}

const results: Result[] = [];

for (const [title, dims] of payloads) {
  console.log("-".repeat(78));
  console.log(`  ${title}`);
  console.log("-".repeat(78));

  const iters = dims > 0 ? 2000 : 5000;

  if (dims === 0) {
    // SamplingParams — metadata only
    const sp = makeSamplingParams();

    const jRes = benchEncode("json", () => new TextEncoder().encode(JSON.stringify(sp)), iters);
    const jDec = benchDecode("json", jRes.data, (d) => JSON.parse(new TextDecoder().decode(d)), iters);

    const mRes = benchEncode("msgpack", () => mpEncode(sp), iters);
    const mDec = benchDecode("msgpack", mRes.data, (d) => mpDecode(d), iters);

    const cRes = benchEncode("cowrie", () => dumps(sp), iters);
    const cDec = benchDecode("cowrie", cRes.data, (d) => loads(d), iters);

    console.log(`  JSON          ${fmt(jRes.data.length).padStart(10)}  enc ${jRes.us.toFixed(1).padStart(8)} us  dec ${jDec.toFixed(1).padStart(8)} us`);
    console.log(`  MsgPack       ${fmt(mRes.data.length).padStart(10)}  enc ${mRes.us.toFixed(1).padStart(8)} us  dec ${mDec.toFixed(1).padStart(8)} us`);
    console.log(`  Cowrie        ${fmt(cRes.data.length).padStart(10)}  enc ${cRes.us.toFixed(1).padStart(8)} us  dec ${cDec.toFixed(1).padStart(8)} us`);

    results.push({
      name: "SamplingParams",
      jsonSize: jRes.data.length, jsonEnc: jRes.us, jsonDec: jDec,
      mpSize: mRes.data.length, mpEnc: mRes.us, mpDec: mDec,
      cowSize: cRes.data.length, cowEnc: cRes.us, cowDec: cDec,
    });
  } else {
    // Embedding with tensor
    const emb = makeEmbedding(dims);

    // JSON (float array)
    const jRes = benchEncode("json", () => new TextEncoder().encode(JSON.stringify(emb.json)), iters);
    const jDec = benchDecode("json", jRes.data, (d) => JSON.parse(new TextDecoder().decode(d)), iters);

    // MsgPack with tensor ext
    const mRes = benchEncode("msgpack+ext", () => mpEncode(emb.msgpack, { extensionCodec: tensorExtCodec }), iters);
    const mDec = benchDecode("msgpack", mRes.data, (d) => mpDecode(d, { extensionCodec: tensorExtCodec }), iters);

    // Cowrie with native tensor
    const cRes = benchEncode("cowrie", () => emb.cowrieWithTensor(), iters);
    const cDec = benchDecode("cowrie", cRes.data, (d) => toAny(decode(d)), iters);

    // Cowrie WASM (Rust compiled to WASM)
    const wasmPayload = {
      object: "embedding",
      index: 0,
      embedding: { _cowrie_type: "tensor", dtype: "float32", shape: [dims], data: emb.tensorBytes },
    };
    const wRes = benchEncode("cowrie-wasm", () => wasmEncode(wasmPayload), iters);
    const wDec = benchDecode("cowrie-wasm", wRes.data, (d) => wasmDecode(d), iters);

    const ratio = (cRes.data.length / jRes.data.length).toFixed(2);
    console.log(`  JSON          ${fmt(jRes.data.length).padStart(10)}  enc ${jRes.us.toFixed(1).padStart(8)} us  dec ${jDec.toFixed(1).padStart(8)} us`);
    console.log(`  MsgPack+ext   ${fmt(mRes.data.length).padStart(10)}  enc ${mRes.us.toFixed(1).padStart(8)} us  dec ${mDec.toFixed(1).padStart(8)} us`);
    console.log(`  Cowrie (TS)   ${fmt(cRes.data.length).padStart(10)}  enc ${cRes.us.toFixed(1).padStart(8)} us  dec ${cDec.toFixed(1).padStart(8)} us  (${ratio}x vs JSON)`);
    console.log(`  Cowrie (WASM) ${fmt(wRes.data.length).padStart(10)}  enc ${wRes.us.toFixed(1).padStart(8)} us  dec ${wDec.toFixed(1).padStart(8)} us`);

    results.push({
      name: `Embed ${dims}`,
      jsonSize: jRes.data.length, jsonEnc: jRes.us, jsonDec: jDec,
      mpSize: mRes.data.length, mpEnc: mRes.us, mpDec: mDec,
      cowSize: cRes.data.length, cowEnc: cRes.us, cowDec: cDec,
    });
  }
}

// Summary
console.log();
console.log("=".repeat(78));
console.log("  SUMMARY");
console.log("=".repeat(78));
console.log(`  ${"Payload".padEnd(20)} ${"JSON".padStart(10)} ${"MsgPack".padStart(10)} ${"Cowrie".padStart(10)} ${"vs JSON".padStart(8)} ${"vs MP".padStart(8)}`);
console.log(`  ${"-".repeat(20)} ${"-".repeat(10)} ${"-".repeat(10)} ${"-".repeat(10)} ${"-".repeat(8)} ${"-".repeat(8)}`);
for (const r of results) {
  const vsJ = (r.cowSize / r.jsonSize).toFixed(2) + "x";
  const vsM = r.mpSize > 0 ? (r.cowSize / r.mpSize).toFixed(2) + "x" : "—";
  console.log(`  ${r.name.padEnd(20)} ${String(r.jsonSize).padStart(10)} ${String(r.mpSize).padStart(10)} ${String(r.cowSize).padStart(10)} ${vsJ.padStart(8)} ${vsM.padStart(8)}`);
}
console.log();
console.log("  Encode speed (us):");
console.log(`  ${"Payload".padEnd(20)} ${"JSON".padStart(10)} ${"MsgPack".padStart(10)} ${"Cowrie".padStart(10)} ${"JSON/Cow".padStart(10)}`);
for (const r of results) {
  const speedup = (r.jsonEnc / r.cowEnc).toFixed(1) + "x";
  console.log(`  ${r.name.padEnd(20)} ${r.jsonEnc.toFixed(1).padStart(10)} ${r.mpEnc.toFixed(1).padStart(10)} ${r.cowEnc.toFixed(1).padStart(10)} ${speedup.padStart(10)}`);
}
console.log();
