// differential_3way.mjs — 3-Way Differential Lockstep Oracle
//   (Lean 4.34 Native `cowrie-lean-oracle` <-> Bend 2.0.35 `.mjs` <-> Rust/WASM `cowrie_core.wasm`)
//
// Honestly tracks true 3-way (Lean <-> Bend <-> WASM) vs 2-way (Lean <-> WASM / Lean <-> Bend)
// vector counts across all 12 subsystems including real HMAC-SHA256 `StatePage` precedence!

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import { pathToFileURL } from "node:url";
import { createCowrieWasm } from "../../wasm/cowrie.mjs";

const leanBin = process.argv[2] || "/home/omen/cowrie/formal/lean/.lake/build/bin/cowrie-lean-oracle";
const bendMjs = process.argv[3] || "/tmp/cowrie-cargo-target/cowrie_kernel_verified.mjs";
const wasmPath = process.argv[4] || "/tmp/cowrie-cargo-target/wasm32-unknown-unknown/release/cowrie_core.wasm";

const bendMod = await import(pathToFileURL(bendMjs).href);
const B = bendMod.default || bendMod;
const W = await createCowrieWasm(fs.readFileSync(wasmPath));

let useTaggedBool = false;
try {
  useTaggedBool = typeof B["SPEC.bnot"](true) !== "boolean";
} catch {
  useTaggedBool = true;
}
const toB = (b) => (useTaggedBool ? { $: b ? "True" : "False" } : Boolean(b));
const fromB = (v) => (typeof v === "boolean" ? v : v.$ === "True");
const ST = (tag, fields = {}) => ({ $: `SPEC.${tag}`, ...fields });
const tagOf = (v) => (v && typeof v.$ === "string" && v.$.startsWith("SPEC.") ? v.$.slice(5) : v.$);
const toList = (arr) => {
  let out = { $: "Nil" };
  for (let i = arr.length - 1; i >= 0; i--) out = { $: "Con", head: arr[i], tail: out };
  return out;
};
const fromList = (lst) => {
  const out = [];
  let cur = lst;
  while (cur && cur.$ === "Con") { out.push(cur.head); cur = cur.tail; }
  return out;
};

let rngState = 0x5EEDBEEF;
function randU32() {
  let x = rngState | 0;
  x ^= x << 13;
  x ^= x >>> 17;
  x ^= x << 5;
  rngState = x;
  return x >>> 0;
}

function byteToBendAuth(b) {
  return ST("Auth", { clean_bits: BigInt(b & 0x0B), ext: toB((b & 0x04) !== 0), tainted: toB((b & 0x80) !== 0) });
}
function bendAuthToByte(a) {
  return Number(a.clean_bits) | (fromB(a.ext) ? 0x04 : 0) | (fromB(a.tainted) ? 0x80 : 0);
}
function encodeUvarint(nBig) {
  let n = BigInt(nBig);
  const out = [];
  while (true) {
    const b = Number(n & 0x7Fn);
    n >>= 7n;
    if (n !== 0n) out.push(b | 0x80);
    else { out.push(b); break; }
  }
  return out;
}

// Build a real 32,862-byte CWSP StatePage with optional real HMAC-SHA256 & controlled header validity
function makeStatePage(ctxTokens, fillByte, hmacFlag, macValid, hdrValid, hmacKeyStr) {
  const PAGE = 16384;
  const payload = new Uint8Array(64).fill(fillByte & 0xFF);

  const leafIn = new Uint8Array(1 + payload.length);
  leafIn[0] = 0x00;
  leafIn.set(payload, 1);
  const leafHex = W.sha256Hex(leafIn);
  const rootIn = new Uint8Array(2 + 32);
  rootIn[0] = 0x02;
  rootIn[1] = 0x01;
  for (let i = 0; i < 32; i++) rootIn[2 + i] = parseInt(leafHex.slice(2 * i, 2 * i + 2), 16);
  const stateShaHex = W.sha256Hex(rootIn);
  const stateShaBytes = new Uint8Array(32);
  for (let i = 0; i < 32; i++) stateShaBytes[i] = parseInt(stateShaHex.slice(2 * i, 2 * i + 2), 16);

  const tlIn = new Uint8Array(2 + 32);
  tlIn[0] = 0x03;
  tlIn[1] = 64;
  tlIn.set(stateShaBytes, 2);
  const tlHex = W.sha256Hex(tlIn);
  const emptyTreeHex = W.sha256Hex(new Uint8Array(0));

  const mrIn = new Uint8Array(1 + 1 + 32 + 1 + 32);
  mrIn[0] = 0x04;
  mrIn[1] = 0x00;
  for (let i = 0; i < 32; i++) mrIn[2 + i] = parseInt(emptyTreeHex.slice(2 * i, 2 * i + 2), 16);
  mrIn[34] = 0x01;
  for (let i = 0; i < 32; i++) mrIn[35 + i] = parseInt(tlHex.slice(2 * i, 2 * i + 2), 16);
  const mrHex = W.sha256Hex(mrIn);

  const total = PAGE + PAGE + 94;
  const buf = new Uint8Array(total);
  const dv = new DataView(buf.buffer);
  const encUtf8 = new TextEncoder();
  buf.set(encUtf8.encode("CWRF"), 0);
  buf[4] = 1;
  buf[5] = 14;
  // When hdrValid == false, set invalid shard flags = 3 (FLAG_STATEPAGE | FLAG_IVF_COLOCATED) -> ERR_INVALID_CONTRACT
  dv.setUint16(6, hdrValid ? 1 : 3, true);
  dv.setUint32(8, 0, true);
  dv.setUint32(12, 1, true);
  dv.setBigUint64(16, 64n, true);
  dv.setBigUint64(24, 276n, true);
  dv.setBigUint64(32, BigInt(PAGE), true);
  dv.setBigUint64(40, BigInt(PAGE), true);
  dv.setBigUint64(48, BigInt(2 * PAGE), true);

  buf.set(encUtf8.encode("CWSP"), 64);
  dv.setUint32(68, ctxTokens, true);
  buf[72] = 1;
  buf[73] = 2;
  buf[74] = 1;
  buf[75] = hmacFlag;
  dv.setBigUint64(80, BigInt(PAGE), true);
  dv.setBigUint64(88, BigInt(payload.length), true);
  buf.set(encUtf8.encode("a".repeat(64)), 96);
  buf.set(encUtf8.encode("b".repeat(64)), 160);
  buf.set(encUtf8.encode(stateShaHex), 224);
  buf.set(encUtf8.encode("wasm-v1"), 288);
  dv.setUint16(304, 1, true);

  if (hmacFlag === 1) {
    const mac = crypto.createHmac("sha256", hmacKeyStr).update(buf.subarray(0, 308)).digest();
    if (!macValid) mac[0] ^= 0xFF;
    buf.set(mac, 308);
  }

  buf.set(payload, PAGE);

  const foff = 2 * PAGE;
  dv.setBigUint64(foff, BigInt(PAGE), true);
  dv.setBigUint64(foff + 8, BigInt(payload.length), true);
  buf.set(stateShaBytes, foff + 16);
  buf[foff + 48] = 0x12;
  buf[foff + 49] = 0x20;
  for (let i = 0; i < 32; i++) buf[foff + 50 + i] = parseInt(mrHex.slice(2 * i, 2 * i + 2), 16);
  dv.setBigUint64(foff + 82, BigInt(foff), true);
  buf.set(encUtf8.encode("CWRF"), foff + 90);
  return buf;
}

const cmds = [];
const checks = [];
const counts3Way = {
  propagate: 0,
  header_meta_byte: 0,
  sanitize_6sym: 0,
  r2_compact: 0,
  wire_int: 0,
  f64_bits: 0,
  map_keys: 0,
  sp_precedence_all16: 0,
};
const counts2Way = {
  zigzag64_lean_wasm: 0,
  cwzm_disjoint_lean_bend: 0,
  ivf_watermark_lean_bend: 0,
  mcp_classify_lean_bend: 0,
};

// Mixed authority bases covering clean, untrusted, synth, mixed low bits, and tainted
const AUTH_BASES = [0x01, 0x02, 0x03, 0x04, 0x05, 0x08, 0x09, 0x0F, 0x81, 0x82, 0x83, 0x84, 0x88, 0x8F];

// 1. 300 3-way PROPAGATE vectors (Lean UInt8 <-> Bend Auth <-> WASM u8)
for (let t = 0; t < 300; t++) {
  const base = AUTH_BASES[randU32() % AUTH_BASES.length];
  const n = randU32() % 6;
  const ps = [];
  for (let i = 0; i < n; i++) ps.push(AUTH_BASES[randU32() % AUTH_BASES.length]);
  const bOut = B.propagate(byteToBendAuth(base), toList(ps.map(byteToBendAuth)));
  const bByte = bendAuthToByte(bOut);
  const bT = fromB(B.is_tainted(bOut));
  const wByte = W.propagateAuthority(base, ps);
  const wT = W.isTainted(wByte);
  assert.equal(bByte, wByte);
  assert.equal(bT, wT);
  cmds.push(["PROPAGATE", String(base), ...ps.map(String)].join(" "));
  checks.push({ kind: "propagate", expected: `${wByte} ${wT ? 1 : 0}` });
  counts3Way.propagate++;
}

// 2. 100 3-way HEADER_META_BYTE vectors (Lean UInt8 `isValidHeaderMetaByte` <-> Bend <-> WASM)
for (let t = 0; t < 100; t++) {
  const low4 = t % 17;
  const res3 = (t >>> 2) % 3;
  const epi = t % 5;
  const flags = (t % 7 === 0) ? 1 : 0;
  const authByte = ((t % 2 === 0) ? 0x80 : 0) | ((res3 & 7) << 4) | (low4 & 0xF);
  const aLow4 = authByte & 0xF;
  const aRes3 = (authByte >>> 4) & 7;
  const bOk = fromB(B.is_valid_header_meta(BigInt(aLow4), BigInt(aRes3), BigInt(epi), BigInt(flags)));
  const fr = new Uint8Array([0x43, 0x4F, 0x57, 0x52, 0x01, authByte, epi, flags, 0x00, 0x40]);
  let wOk = false;
  try { W.verifyFrame(fr); wOk = true; } catch { wOk = false; }
  assert.equal(bOk, wOk);
  cmds.push(`HEADER_META_BYTE ${authByte} ${epi} ${flags}`);
  checks.push({ kind: "header_meta_byte", expected: wOk ? "1" : "0" });
  counts3Way.header_meta_byte++;
}

// 3. 300 3-way 6-symbol MEALY SANITIZE vectors (Lean <-> Bend <-> WASM)
const SYM_MAP = [
  ["<", "L", ST("Lt")],
  [">", "G", ST("Gt")],
  ["|", "P", ST("Pipe")],
  ["\\", "E", ST("Esc")],
  ["\u200B", "H", ST("Hidden")],
  ["\u202E", "H", ST("Hidden")],
  ["\u061C", "H", ST("Hidden")],
  ["A", "O", ST("Other")],
  ["E", "O", ST("Other")],
];
function symListToCodes(lst) {
  const arr = fromList(lst);
  if (arr.length === 0) return "-";
  return arr.map((x) => {
    const k = tagOf(x);
    if (k === "Lt") return "L";
    if (k === "Gt") return "G";
    if (k === "Pipe") return "P";
    if (k === "Esc") return "E";
    if (k === "Hidden") return "H";
    return "O";
  }).join("");
}
function strToCodes(s) {
  if (s.length === 0) return "-";
  let out = "";
  for (const ch of s) {
    if (ch === "<") out += "L";
    else if (ch === ">") out += "G";
    else if (ch === "|") out += "P";
    else if (ch === "\\") out += "E";
    else if (ch === "\u200B" || ch === "\u202E" || ch === "\u061C") out += "H";
    else out += "O";
  }
  return out;
}
for (let t = 0; t < 300; t++) {
  const len = 1 + (randU32() % 24);
  let s = "";
  let codeIn = "";
  const bSyms = [];
  for (let i = 0; i < len; i++) {
    const [ch, cd, st] = SYM_MAP[randU32() % SYM_MAP.length];
    s += ch;
    codeIn += cd;
    bSyms.push(st);
  }
  const bOut = symListToCodes(B.sanitize_fences(toList(bSyms)));
  const frame = W.encodeJson({ m: s }, 0x04, 0x01);
  const r2 = W.projectLens(frame, "r2");
  const body = r2.trimEnd().split("\n").slice(1, -1).join("\n");
  assert.equal(body.startsWith("m: "), true);
  const wCodes = strToCodes(body.slice(3));
  assert.equal(bOut, wCodes, `Bend vs WASM sanitize mismatch on ${JSON.stringify(s)}`);
  cmds.push(`SANITIZE ${codeIn}`);
  checks.push({ kind: "sanitize_6sym", expected: wCodes });
  counts3Way.sanitize_6sym++;
}

// 4. 84 3-way R2_COMPACT vectors covering ALL 14 authority variants incl. mixed low bits (`0x03`, `0x05`, `0x09`, `0x0F`)
for (let t = 0; t < 84; t++) {
  const auth = AUTH_BASES[t % AUTH_BASES.length];
  const bMode = tagOf(B.r2_compact_mode(byteToBendAuth(auth))) === "CompactHeader" ? "compact" : "fence";
  const frame = W.encodeJson({ x: t }, auth, 0x01);
  const wMode = W.projectR2Compact(frame).startsWith("[@") ? "compact" : "fence";
  assert.equal(bMode, wMode, `R2_COMPACT mismatch on auth=0x${auth.toString(16)}`);
  cmds.push(`R2_COMPACT ${auth}`);
  checks.push({ kind: "r2_compact", expected: wMode });
  counts3Way.r2_compact++;
}

// 5. 300 3-way WIRE_INT vectors (Lean <-> Bend <-> WASM)
for (let t = 0; t < 300; t++) {
  const kind = t % 4;
  const val = randU32() % 260;
  let kName, wObj, raw;
  const hdr = [0x43, 0x4F, 0x57, 0x52, 0x01, 0x02, 0x01, 0x00, 0x00];
  if (kind === 0) {
    const n = val % 128;
    kName = "WFix"; wObj = ST("WFix", { n: BigInt(n) });
    raw = new Uint8Array([...hdr, 0x40 + n]);
    cmds.push(`WIRE_INT ${kName} 128 160 ${n}`);
  } else if (kind === 1) {
    kName = "WTagIntPos"; wObj = ST("WTagIntPos", { n: BigInt(val) });
    raw = new Uint8Array([...hdr, 0x03, ...encodeUvarint(BigInt(val) << 1n)]);
    cmds.push(`WIRE_INT ${kName} 128 160 ${val}`);
  } else if (kind === 2) {
    kName = "WTagIntNeg"; wObj = ST("WTagIntNeg", { m: BigInt(val) });
    raw = new Uint8Array([...hdr, 0x03, ...encodeUvarint((BigInt(val) << 1n) | 1n)]);
    cmds.push(`WIRE_INT ${kName} 128 160 ${val}`);
  } else {
    kName = "WTagUint"; wObj = ST("WTagUint", { u: BigInt(val) });
    const realU = BigInt(val) < 160n ? BigInt(val) : (1n << 63n) + (BigInt(val) - 160n);
    raw = new Uint8Array([...hdr, 0x09, ...encodeUvarint(realU)]);
    cmds.push(`WIRE_INT ${kName} 128 160 ${val}`);
  }
  const bOk = fromB(B.verify_canon_int_zero_alloc(128n, 160n, wObj));
  let wOk = false;
  try { W.verifyFrame(raw); wOk = true; } catch { wOk = false; }
  assert.equal(bOk, wOk);
  checks.push({ kind: "wire_int", expected: wOk ? "1" : "0" });
  counts3Way.wire_int++;
}

// 6. 100 3-way IEEE-754 F64_BITS vectors (Lean UInt64 <-> Bend FloatKind <-> WASM raw f64)
const F64_CASES = [
  [0x0000_0000_0000_0000n, 0x0000_0000_0000_0000n, ST("FPosZero")],
  [0x8000_0000_0000_0000n, 0x0000_0000_0000_0000n, ST("FNegZero")],
  [0x3FF0_0000_0000_0000n, 0x3FF0_0000_0000_0000n, ST("FFinite", { bits: 1n })],
  [0xBFF8_0000_0000_0000n, 0xBFF8_0000_0000_0000n, ST("FFinite", { bits: 2n })],
  [0x7FF0_0000_0000_0000n, 0x7FF0_0000_0000_0000n, ST("FInf", { pos: toB(true) })],
  [0xFFF0_0000_0000_0000n, 0xFFF0_0000_0000_0000n, ST("FInf", { pos: toB(false) })],
  [0x7FF8_0000_0000_0000n, 0x7FF8_0000_0000_0000n, ST("FCanonQNaN")],
  [0x7FF8_0000_0000_0001n, 0x7FF8_0000_0000_0000n, ST("FNonCanonNaN", { payload: 1n })],
  [0xFFF8_0000_0000_0000n, 0x7FF8_0000_0000_0000n, ST("FNonCanonNaN", { payload: 2n })],
  [0x7FF0_0000_0000_0001n, 0x7FF8_0000_0000_0000n, ST("FNonCanonNaN", { payload: 3n })],
];
for (let t = 0; t < 100; t++) {
  const [bits, expCanon, fKind] = F64_CASES[t % F64_CASES.length];
  const bOk = fromB(B.verify_canon_f64(fKind));
  const raw = new Uint8Array(18);
  raw.set([0x43, 0x4F, 0x57, 0x52, 0x01, 0x02, 0x01, 0x00, 0x00, 0x04], 0);
  new DataView(raw.buffer).setBigUint64(10, bits, true);
  let wOk = false;
  try { W.verifyFrame(raw); wOk = true; } catch { wOk = false; }
  assert.equal(bOk, wOk);
  cmds.push(`F64_BITS ${bits.toString()}`);
  checks.push({ kind: "f64_bits", expected: `${expCanon.toString()} ${wOk ? 1 : 0}` });
  counts3Way.f64_bits++;
}

// 7. 200 3-way MAP_KEYS vectors (Lean <-> Bend <-> WASM)
for (let t = 0; t < 200; t++) {
  const nkeys = 1 + (randU32() % 5);
  const len = randU32() % (nkeys + 2);
  const ks = [];
  for (let i = 0; i < len; i++) ks.push(randU32() % (nkeys + 1));
  const bOk = fromB(B.verify_map_keys(BigInt(nkeys), toList(ks.map(BigInt)))) &&
    fromB(B.all_dict_keys_used(BigInt(nkeys), toList(ks.map(BigInt))));
  const bytes = [0x43, 0x4F, 0x57, 0x52, 0x01, 0x02, 0x01, 0x00, ...encodeUvarint(nkeys)];
  for (let i = 0; i < nkeys; i++) {
    const s = `k${String(i).padStart(2, "0")}`;
    bytes.push(s.length, ...Buffer.from(s, "utf8"));
  }
  bytes.push(0x08, ...encodeUvarint(ks.length));
  for (const k of ks) bytes.push(...encodeUvarint(k), 0x41);
  let wOk = false;
  try { W.verifyFrame(new Uint8Array(bytes)); wOk = true; } catch { wOk = false; }
  assert.equal(bOk, wOk);
  cmds.push(["MAP_KEYS", String(nkeys), ...ks.map(String)].join(" "));
  checks.push({ kind: "map_keys", expected: wOk ? "1" : "0" });
  counts3Way.map_keys++;
}

// 8. 160 TRUE 3-WAY StatePage HMAC-first error precedence vectors (`L36` / `F13`):
//    Unbiased full 32-state Cartesian product (hasKey x hmacFlag 0..3 x macValid x hdrValid) x 5,
//    verified live against REAL WASM `verifyStatePageBytes` (real HMAC-SHA256!) <-> Bend <-> Lean!
const SP_KEY = "cowrie-3way-hmac-secret-key";
for (let t = 0; t < 160; t++) {
  const hk = (t & 1) === 1;
  const hf = (t >>> 1) & 3;
  const mv = ((t >>> 3) & 1) === 1;
  const hv = ((t >>> 4) & 1) === 1;
  const bErr = tagOf(B.statepage_parse_precedence(toB(hk), BigInt(hf), toB(mv), toB(hv)));
  const sp = makeStatePage(64 + t, 0x33, hf, mv, hv, SP_KEY);
  let wErr = "SpOk";
  try {
    W.verifyStatePageBytes(sp, hk ? SP_KEY : null, true);
  } catch (e) {
    if (e.code === "ERR_AUTH_MAC_MISMATCH") wErr = "SpAuthMacMismatch";
    else if (e.code === "ERR_NON_CANONICAL_FLAGS") wErr = "SpNonCanonicalFlags";
    else wErr = "SpInvalidContract";
  }
  assert.equal(bErr, wErr, `3-way SP_PRECEDENCE mismatch hk=${hk} hf=${hf} mv=${mv} hv=${hv}: bend=${bErr} wasm=${wErr}`);
  cmds.push(`SP_PRECEDENCE ${hk ? 1 : 0} ${hf} ${mv ? 1 : 0} ${hv ? 1 : 0}`);
  checks.push({ kind: "sp_precedence_all16", expected: wErr });
  counts3Way.sp_precedence_all16++;
}

// 9. 120 2-way Lean Int64 ZigZag <-> WASM TAG_INT exact wire encoding (incl. i64::MIN / i64::MAX!)
const ZIGZAG_EDGE = [0n, 1n, -1n, 2n, -2n, 127n, 128n, -128n, 9007199254740991n, -9007199254740991n, -(1n << 63n), (1n << 63n) - 1n];
for (let t = 0; t < 120; t++) {
  const iv = t < ZIGZAG_EDGE.length ? ZIGZAG_EDGE[t] : BigInt((randU32() | 0)) * 1000003n;
  const zz = iv >= 0n ? (iv << 1n) : ((-iv - 1n) << 1n) | 1n;
  if (iv < 0n || iv >= 128n) {
    const raw = new Uint8Array([0x43, 0x4F, 0x57, 0x52, 0x01, 0x02, 0x01, 0x00, 0x00, 0x03, ...encodeUvarint(zz)]);
    W.verifyFrame(raw);
    assert.equal(W.decodeToJson(raw), iv.toString());
  }
  cmds.push(`ZIGZAG ${iv.toString()}`);
  checks.push({ kind: "zigzag64_lean_wasm", expected: `${zz.toString()} ${iv.toString()}` });
  counts2Way.zigzag64_lean_wasm++;
}

// 10. 200 2-way Lean <-> Bend CWZM disjoint-monotonic binary-skip vectors (incl. inverted lo > hi!)
for (let t = 0; t < 200; t++) {
  const nZones = 1 + (randU32() % 8);
  let prevMax = randU32() % 10;
  const zones = [];
  const zSpecs = [];
  for (let i = 0; i < nZones; i++) {
    const mn = prevMax + (randU32() % 5);
    const mx = mn + (randU32() % 10);
    zones.push(ST("Zone", { min_k: BigInt(mn), max_k: BigInt(mx) }));
    zSpecs.push(`${mn}:${mx}`);
    prevMax = mx;
  }
  const a = randU32() % 80;
  const b = randU32() % 80;
  // Every 10th trial intentionally tests inverted lo > hi!
  const lo = (t % 10 === 0 && a !== b) ? Math.max(a, b) : Math.min(a, b);
  const hi = (t % 10 === 0 && a !== b) ? Math.min(a, b) : Math.max(a, b);
  const bRes = fromList(B.query_key_range_pages(toList(zones), BigInt(lo), BigInt(hi))).map(String);
  cmds.push(["CWZM", String(lo), String(hi), ...zSpecs].join(" "));
  checks.push({ kind: "cwzm_disjoint_lean_bend", expected: bRes.length ? bRes.join(",") : "-" });
  counts2Way.cwzm_disjoint_lean_bend++;
}

// 11. 150 2-way Lean <-> Bend LiveIvfStore watermark compaction vectors
for (let t = 0; t < 150; t++) {
  const s0 = t % 7;
  const base = [1, 2];
  const comp = [10 + t, 20 + t];
  const post = [30 + t];
  const all = [...comp, ...post];
  const bCrash = fromList(
    B.live_ivf_recover(
      toList([...base, ...comp].map(BigInt)),
      BigInt(s0 + comp.length),
      B["SPEC.ivf_make_seq_recs"](toList(all.map(BigInt)), BigInt(s0))
    )
  ).map(String);
  cmds.push(`IVF_RECOVER ${s0 + comp.length} ${s0} ${[...base, ...comp].join(",")} ${all.join(",")}`);
  checks.push({ kind: "ivf_watermark_lean_bend", expected: bCrash.join(",") });
  counts2Way.ivf_watermark_lean_bend++;
}

// 12. 96 2-way Lean <-> Bend MCP tool classifier vectors (`L27`) across policy/verb/flags
const OPT_ACTS = [
  ["none", { $: "None" }],
  ["ro", { $: "Some", value: ST("ReadOnly") }],
  ["sh", { $: "Some", value: ST("ShellExec") }],
  ["ne", { $: "Some", value: ST("NetExfil") }],
];
for (let t = 0; t < 96; t++) {
  const [polS, polB] = OPT_ACTS[t % 4];
  const [verbS, verbB] = OPT_ACTS[(t >>> 2) % 3];
  const ar = ((t >>> 4) & 1) === 1;
  const br = ((t >>> 5) & 1) === 1;
  const ta = ((t >>> 6) & 1) === 1;
  const hr = (t % 2) === 1;
  const bAct = B.classify_mcp_tool(polB, verbB, toB(ar), toB(br), toB(ta), toB(hr));
  const bCode = String(B.action_to_code(bAct));
  cmds.push(`MCP_CLASSIFY ${polS} ${verbS} ${ar ? 1 : 0} ${br ? 1 : 0} ${ta ? 1 : 0} ${hr ? 1 : 0}`);
  checks.push({ kind: "mcp_classify_lean_bend", expected: bCode });
  counts2Way.mcp_classify_lean_bend++;
}

// Execute entire batch through compiled native Lean 4 binary in one shot!
const t0 = performance.now();
const proc = spawnSync(leanBin, [], {
  input: cmds.join("\n") + "\n",
  encoding: "utf8",
  maxBuffer: 32 * 1024 * 1024,
});
const leanMs = performance.now() - t0;
assert.equal(proc.status, 0, `cowrie-lean-oracle exited ${proc.status}: ${proc.stderr}`);

const lines = proc.stdout.trimEnd().split("\n");
assert.equal(lines.length, checks.length, `Lean oracle line count mismatch: got ${lines.length} vs ${checks.length}`);

for (let i = 0; i < checks.length; i++) {
  assert.equal(
    lines[i],
    checks[i].expected,
    `LOCKSTEP MISMATCH at vector #${i} (${checks[i].kind}) cmd='${cmds[i]}': lean='${lines[i]}' vs expected='${checks[i].expected}'`
  );
}

const total3Way = Object.values(counts3Way).reduce((a, b) => a + b, 0);
const total2Way = Object.values(counts2Way).reduce((a, b) => a + b, 0);

console.log(JSON.stringify({
  three_way_lockstep: "PASS",
  total_vectors: checks.length,
  true_three_way_vectors: total3Way,
  two_way_vectors: total2Way,
  lean_native_oracle_ms: Number(leanMs.toFixed(2)),
  three_way_breakdown: counts3Way,
  two_way_breakdown: counts2Way,
}));
