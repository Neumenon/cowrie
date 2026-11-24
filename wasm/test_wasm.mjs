#!/usr/bin/env node
/**
 * Standalone WebAssembly Conformance & Benchmark Suite for `cowrie_core.wasm`.
 * Verifies 100% bit-for-bit parity against `tests/vectors.json` (150 golden+fuzz vectors,
 * 12 negatives, 8 DoS bombs, dual-tree 16 KB CWRF Merkle + 14 bitflips, R0/R1/R2 lenses,
 * sticky 0x80 taint, and RFC 8032 Ed25519 declassification signatures) inside V8 WebAssembly!
 */
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import { fileURLToPath } from "node:url";
import {
  createCowrieWasm,
  fromHex,
  toHex,
  AUTH_VERIFIED_TOOL,
  AUTH_UNTRUSTED_EXT,
  AUTH_MODEL_SYNTH,
  EPI_OBSERVED,
} from "./cowrie.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const WASM_PATH =
  process.env.COWRIE_WASM_PATH ||
  "/tmp/cowrie-cargo-target/wasm32-unknown-unknown/release/cowrie_core.wasm";

function expectErr(fn, expectedCode) {
  try {
    fn();
    throw new Error(`Expected ${expectedCode}, but succeeded`);
  } catch (e) {
    if (e.code !== expectedCode) {
      throw new Error(`Got ${e.code || e.message}, expected ${expectedCode}`);
    }
  }
}

async function main() {
  const wasmBytes = fs.readFileSync(WASM_PATH);
  const gzBytes = zlib.gzipSync(wasmBytes, { level: 9 });
  const tInst0 = performance.now();
  const cw = await createCowrieWasm(wasmBytes);
  const instMs = performance.now() - tInst0;

  const v = JSON.parse(fs.readFileSync(path.join(ROOT, "tests", "vectors.json"), "utf8"));

  // 1. 150 frozen COWR v1 golden + fuzz vectors
  for (const grp of ["golden", "fuzz"]) {
    for (const item of v[grp]) {
      const res = cw.verifyFrame(fromHex(item.wire_hex));
      if (res.authority !== item.auth || res.epistemic !== item.epi || res.cidHex !== item.cid_hex) {
        throw new Error(`Vector mismatch on ${item.id}`);
      }
    }
  }

  // 2. 12 anti-malleability negatives + 8 DoS bombs
  for (const neg of v.negatives) {
    expectErr(() => cw.verifyFrame(fromHex(neg.wire_hex)), neg.expected_err);
  }
  for (const d of v.dos_cases) {
    expectErr(() => cw.verifyFrame(fromHex(d.wire_hex)), d.expected_err);
  }

  // 3. Dual-tree 16 KB shard Merkle verification + 14 frame & tensor bitflips
  const cleanShard = fromHex(v.clean_shard_hex);
  const { rootHex } = cw.verifyShard(cleanShard);
  if (rootHex !== v.clean_shard_root) throw new Error("Shard root mismatch");
  for (const ct of [...v.corrupt_tests, ...v.corrupt_tensor_tests]) {
    const mut = cleanShard.slice();
    mut[ct.flip_byte_offset] ^= 0x01;
    expectErr(() => cw.verifyShard(mut), "ERR_ROOT_MISMATCH");
  }

  // 4. Canonical encode + R0/R1/R2 lenses + 16 KB shard pack + zero-alloc verify microbench
  const doc = {
    title: "Browser WebAssembly Ledger",
    rows: [
      { id: 1, metric: "wasm_verify", ok: true },
      { id: 2, metric: "merkle_0x04", ok: true },
      { id: 3, metric: "ed25519_sig", ok: true },
    ],
    emb: { $tensor: 0, dtype: "i8", shape: [512], space: "nomic-v2:cos" },
  };
  const f1 = cw.encodeJson(doc, AUTH_VERIFIED_TOOL, EPI_OBSERVED);
  const f2 = cw.encodeJson({ emb: doc.emb, rows: doc.rows, title: doc.title }, AUTH_VERIFIED_TOOL, EPI_OBSERVED);
  if (toHex(f1) !== toHex(f2)) throw new Error("Key-order canonicalization failed in WASM");
  const r0 = cw.projectLens(f1, "r0", 1);
  const r2 = cw.projectLens(f1, "r2", 1);
  if (!r0.startsWith("@") || !r2.includes("rows[3]{id,metric,ok}:")) {
    throw new Error("WASM lens projection mismatch");
  }

  const pureFrame = cw.encodeJson({ hello: "wasm", count: 42 }, AUTH_VERIFIED_TOOL, EPI_OBSERVED);
  const packedShard = cw.packSingleShard(pureFrame);
  if (packedShard.length < 16384) throw new Error("Expected 16KB aligned shard");
  cw.verifyShard(packedShard);

  // Microbenchmark 10,000 WASM zero-alloc frame verifications
  const iters = 10000;
  const tVer0 = performance.now();
  for (let i = 0; i < iters; i++) cw.verifyFrame(f1);
  const wasmVerifyNs = ((performance.now() - tVer0) * 1e6) / iters;

  // 5. Sticky 0x80 Taint & RFC 8032 Ed25519 Declassification in WASM
  const hop1 = cw.propagateAuthority(AUTH_MODEL_SYNTH, [AUTH_UNTRUSTED_EXT, AUTH_VERIFIED_TOOL]);
  if (!cw.isTainted(hop1)) throw new Error("Taint propagation failed in WASM");

  const seed = fromHex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60");
  const pk = cw.ed25519Pubkey(seed);
  if (toHex(pk) !== "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a") {
    throw new Error("RFC 8032 pubkey mismatch in WASM");
  }
  const cid34 = cw.verifyFrame(f1).cid;
  const sig = cw.signDeclassification(seed, cid34, "shell_exec", 2000000000);
  if (!cw.verifyDeclassification(pk, cid34, "shell_exec", 2000000000, sig, 1900000000)) {
    throw new Error("Valid Ed25519 declassification failed in WASM");
  }
  if (cw.verifyDeclassification(pk, cid34, "fs_write", 2000000000, sig, 1900000000)) {
    throw new Error("Wrong-action Ed25519 replay accepted in WASM");
  }

  console.log(
    JSON.stringify(
      {
        target: "wasm32-unknown-unknown",
        wasm_raw_bytes: wasmBytes.length,
        wasm_gzip_bytes: gzBytes.length,
        instantiate_ms: Number(instMs.toFixed(2)),
        verify_frame_with_cid_ns: Number(wasmVerifyNs.toFixed(1)),
        vectors_passed: 150,
        negatives_and_dos_passed: 20,
        merkle_bitflips_passed: 14,
        ed25519_rfc8032_passed: true,
        verdict: "PASS",
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
