/**
 * Zero-dependency ESM JavaScript / Browser / Node.js SDK for `cowrie_core.wasm` (`wasm32-unknown-unknown`).
 * Provides canonical Plane A framing, zero-alloc wire verification, progressive R0/R1/R2 lenses,
 * 16 KB CWRF shard + StatePage HMAC verification, sticky 0x80 taint propagation, and RFC 8032 Ed25519
 * owner-declassification signatures (`COWRIE_DECLASSIFY_V1`).
 */

export const AUTH_SYS_POLICY = 0x01;
export const AUTH_VERIFIED_TOOL = 0x02;
export const AUTH_UNTRUSTED_EXT = 0x04;
export const AUTH_MODEL_SYNTH = 0x08;
export const BIT_TAINTED = 0x80;

export const EPI_OBSERVED = 0x01;
export const EPI_DERIVED = 0x02;
export const EPI_ESTIMATED = 0x03;

export const ACTION_CODES = Object.freeze({
  read_only: 0,
  shell_exec: 1,
  cred_read: 2,
  fs_write: 3,
  net_exfil: 4,
});

const U64_MAX = 0xffff_ffff_ffff_ffffn;

function resolveActionCode(action) {
  if (typeof action !== "string" || !Object.prototype.hasOwnProperty.call(ACTION_CODES, action)) {
    throw new Error(`Invalid Cowrie action '${String(action)}'`);
  }
  return ACTION_CODES[action];
}

function toU64BigInt(value, fieldName) {
  let bi;
  if (typeof value === "bigint") {
    bi = value;
  } else if (typeof value === "number" && Number.isInteger(value) && Number.isSafeInteger(value)) {
    bi = BigInt(value);
  } else {
    throw new TypeError(`${fieldName} must be a safe integer or BigInt`);
  }
  if (bi < 0n || bi > U64_MAX) {
    throw new RangeError(`${fieldName} must be in [0, 2^64 - 1]`);
  }
  return bi;
}

export class CowrieWasmError extends Error {
  constructor(code, rc) {
    super(`${code} (rc=${rc})`);
    this.name = "CowrieWasmError";
    this.code = code;
    this.rc = rc;
  }
}

const enc = new TextEncoder();
const dec = new TextDecoder("utf-8");

export function toHex(u8) {
  let out = "";
  for (let i = 0; i < u8.length; i++) out += u8[i].toString(16).padStart(2, "0");
  return out;
}

export function fromHex(hex) {
  if (typeof hex !== "string" || hex.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(hex)) {
    throw new Error("Invalid hex string");
  }
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(2 * i, 2 * i + 2), 16);
  return out;
}

export async function createCowrieWasm(wasmBytesOrResponse) {
  const result =
    wasmBytesOrResponse instanceof Uint8Array || wasmBytesOrResponse instanceof ArrayBuffer
      ? await WebAssembly.instantiate(wasmBytesOrResponse, {})
      : await WebAssembly.instantiateStreaming(wasmBytesOrResponse, {});
  const ex = result.instance.exports;

  const u8Mem = () => new Uint8Array(ex.memory.buffer);
  const dvMem = () => new DataView(ex.memory.buffer);

  function cstr(ptr) {
    const m = u8Mem();
    if (ptr < 0 || ptr >= m.length) {
      throw new RangeError("Out-of-bounds WASM string pointer");
    }
    let end = ptr;
    while (end < m.length && m[end] !== 0) end++;
    return dec.decode(m.subarray(ptr, end));
  }

  function check(rc) {
    if (rc !== 0) throw new CowrieWasmError(cstr(ex.cowrie_err_name(rc)), rc);
  }

  function withBytes(bytes, fn, zeroize = false) {
    const len = bytes.length;
    const ptr = len > 0 ? ex.cowrie_wasm_alloc(len) : 0;
    if (len > 0 && ptr === 0) {
      throw new Error("WASM allocation failed");
    }
    try {
      if (len > 0) u8Mem().set(bytes, ptr);
      return fn(ptr, len);
    } finally {
      if (len > 0) {
        if (zeroize) u8Mem().fill(0, ptr, ptr + len);
        ex.cowrie_wasm_free(ptr, len);
      }
    }
  }

  function takeCowrieBuf(structPtr) {
    const dv = dvMem();
    const dataPtr = dv.getUint32(structPtr, true);
    const len = dv.getUint32(structPtr + 4, true);
    const cap = dv.getUint32(structPtr + 8, true);
    try {
      return len > 0 ? u8Mem().slice(dataPtr, dataPtr + len) : new Uint8Array(0);
    } finally {
      if (ex.cowrie_buf_free.length === 3) {
        ex.cowrie_buf_free(dataPtr, len, cap);
      } else {
        // wasm32 C-ABI passes >8B structs by pointer (`void cowrie_buf_free(CowrieBuf* byval)`)
        ex.cowrie_buf_free(structPtr);
      }
    }
  }

  return {
    exports: ex,

    sha256Hex(bytes) {
      const u8 = typeof bytes === "string" ? enc.encode(bytes) : bytes;
      const outPtr = ex.cowrie_wasm_alloc(65);
      try {
        check(withBytes(u8, (p, n) => ex.cowrie_sha256_hex(p, n, outPtr)));
        return cstr(outPtr);
      } finally {
        ex.cowrie_wasm_free(outPtr, 65);
      }
    },

    encodeJson(obj, authority = AUTH_VERIFIED_TOOL, epistemic = EPI_OBSERVED) {
      const raw = typeof obj === "string" ? enc.encode(obj) : enc.encode(JSON.stringify(obj));
      const bufStruct = ex.cowrie_wasm_alloc(12);
      try {
        const rc = withBytes(raw, (p, n) => ex.cowrie_encode_json(p, n, authority, epistemic, bufStruct));
        check(rc);
        return takeCowrieBuf(bufStruct);
      } finally {
        ex.cowrie_wasm_free(bufStruct, 12);
      }
    },

    verifyFrame(frameU8) {
      const scratch = ex.cowrie_wasm_alloc(36); // auth(1) + epi(1) + cid(34)
      try {
        const rc = withBytes(frameU8, (p, n) =>
          ex.cowrie_frame_verify(p, n, scratch, scratch + 1, scratch + 2)
        );
        check(rc);
        const m = u8Mem();
        return {
          authority: m[scratch],
          epistemic: m[scratch + 1],
          cid: m.slice(scratch + 2, scratch + 36),
          cidHex: toHex(m.subarray(scratch + 2, scratch + 36)),
        };
      } finally {
        ex.cowrie_wasm_free(scratch, 36);
      }
    },

    decodeToJson(frameU8) {
      const bufStruct = ex.cowrie_wasm_alloc(12);
      try {
        const rc = withBytes(frameU8, (p, n) => ex.cowrie_decode_to_json(p, n, bufStruct));
        check(rc);
        return dec.decode(takeCowrieBuf(bufStruct));
      } finally {
        ex.cowrie_wasm_free(bufStruct, 12);
      }
    },

    projectLens(frameU8, tier = "r2", tensorCount = 0) {
      const tMap = { r0: 0, r1: 1, r2: 2, r3: 3 };
      if (!Object.prototype.hasOwnProperty.call(tMap, tier)) {
        throw new Error(`Unknown Cowrie lens tier: ${tier}`);
      }
      const tCode = tMap[tier];
      const bufStruct = ex.cowrie_wasm_alloc(12);
      try {
        const rc = withBytes(frameU8, (p, n) =>
          ex.cowrie_project_lens(p, n, tCode, tensorCount, bufStruct)
        );
        check(rc);
        return dec.decode(takeCowrieBuf(bufStruct));
      } finally {
        ex.cowrie_wasm_free(bufStruct, 12);
      }
    },

    projectR2Compact(frameU8) {
      const bufStruct = ex.cowrie_wasm_alloc(12);
      try {
        const rc = withBytes(frameU8, (p, n) =>
          ex.cowrie_project_r2_compact(p, n, bufStruct)
        );
        check(rc);
        return dec.decode(takeCowrieBuf(bufStruct));
      } finally {
        ex.cowrie_wasm_free(bufStruct, 12);
      }
    },

    extractField(frameU8, fieldPath) {
      if (typeof fieldPath !== "string") {
        throw new Error("fieldPath must be a string");
      }
      const pathBytes = enc.encode(fieldPath);
      const bufStruct = ex.cowrie_wasm_alloc(12);
      try {
        const rc = withBytes(frameU8, (fp, fn) =>
          withBytes(pathBytes, (pp, pn) => ex.cowrie_extract_field(fp, fn, pp, pn, bufStruct))
        );
        check(rc);
        return dec.decode(takeCowrieBuf(bufStruct));
      } finally {
        ex.cowrie_wasm_free(bufStruct, 12);
      }
    },

    packSingleShard(frameU8) {
      const bufStruct = ex.cowrie_wasm_alloc(12);
      try {
        const rc = withBytes(frameU8, (p, n) => ex.cowrie_shard_pack_single(p, n, bufStruct));
        check(rc);
        return takeCowrieBuf(bufStruct);
      } finally {
        ex.cowrie_wasm_free(bufStruct, 12);
      }
    },

    verifyShard(shardU8) {
      const rootPtr = ex.cowrie_wasm_alloc(34);
      try {
        const rc = withBytes(shardU8, (p, n) => ex.cowrie_shard_verify(p, n, rootPtr));
        check(rc);
        const root = u8Mem().slice(rootPtr, rootPtr + 34);
        return { root, rootHex: toHex(root) };
      } finally {
        ex.cowrie_wasm_free(rootPtr, 34);
      }
    },

    resolvedFrameCid(shardU8, frameIdx = 0) {
      if (!Number.isInteger(frameIdx) || frameIdx < 0) {
        throw new Error(`Invalid frameIdx: ${frameIdx}`);
      }
      const cidPtr = ex.cowrie_wasm_alloc(34);
      try {
        const rc = withBytes(shardU8, (p, n) =>
          ex.cowrie_shard_resolved_frame_cid(p, n, frameIdx, cidPtr)
        );
        check(rc);
        const cid = u8Mem().slice(cidPtr, cidPtr + 34);
        return { cid, cidHex: toHex(cid) };
      } finally {
        ex.cowrie_wasm_free(cidPtr, 34);
      }
    },

    verifyStatePageBytes(spU8, hmacKey = null, verifyHash = true) {
      const keyBytes = hmacKey ? (typeof hmacKey === "string" ? enc.encode(hmacKey) : hmacKey) : new Uint8Array(0);
      const tokPtr = ex.cowrie_wasm_alloc(4);
      try {
        const rc = withBytes(spU8, (sp, sn) =>
          withBytes(
            keyBytes,
            (kp, kn) => ex.cowrie_statepage_verify_bytes(sp, sn, kp, kn, verifyHash ? 1 : 0, tokPtr),
            true
          )
        );
        check(rc);
        return { ctxTokens: dvMem().getUint32(tokPtr, true) };
      } finally {
        ex.cowrie_wasm_free(tokPtr, 4);
      }
    },

    propagateAuthority(baseAuth, parentAuths = []) {
      const u8 = new Uint8Array(parentAuths);
      return withBytes(u8, (p, n) => ex.cowrie_propagate_authority(baseAuth, p, n));
    },

    isTainted(auth) {
      return Boolean(ex.cowrie_is_tainted(auth));
    },

    ed25519Pubkey(seed32) {
      if (!(seed32 instanceof Uint8Array) || seed32.length !== 32) {
        throw new Error("Expected 32-byte Uint8Array seed");
      }
      const outPtr = ex.cowrie_wasm_alloc(32);
      try {
        check(withBytes(seed32, (sp) => ex.cowrie_ed25519_pubkey(sp, outPtr), true));
        return u8Mem().slice(outPtr, outPtr + 32);
      } finally {
        ex.cowrie_wasm_free(outPtr, 32);
      }
    },

    signDeclassification(seed32, targetCid34, action, expiryUnixSecs) {
      if (!(seed32 instanceof Uint8Array) || seed32.length !== 32 || !(targetCid34 instanceof Uint8Array) || targetCid34.length !== 34) {
        throw new Error("Invalid seed or CID length");
      }
      const actCode = resolveActionCode(action);
      const exp = toU64BigInt(expiryUnixSecs, "expiryUnixSecs");
      const sigPtr = ex.cowrie_wasm_alloc(64);
      try {
        const rc = withBytes(
          seed32,
          (sp) =>
            withBytes(targetCid34, (cp) =>
              ex.cowrie_declassify_sign(sp, cp, actCode, exp, sigPtr)
            ),
          true
        );
        check(rc);
        return u8Mem().slice(sigPtr, sigPtr + 64);
      } finally {
        ex.cowrie_wasm_free(sigPtr, 64);
      }
    },

    verifyDeclassification(pubkey32, targetCid34, action, expiryUnixSecs, sig64, nowUnixSecs) {
      if (
        !(pubkey32 instanceof Uint8Array) || pubkey32.length !== 32 ||
        !(targetCid34 instanceof Uint8Array) || targetCid34.length !== 34 ||
        !(sig64 instanceof Uint8Array) || sig64.length !== 64
      ) {
        throw new Error("Invalid pubkey, CID, or signature length");
      }
      const actCode = resolveActionCode(action);
      const exp = toU64BigInt(expiryUnixSecs, "expiryUnixSecs");
      const now = toU64BigInt(nowUnixSecs, "nowUnixSecs");
      const rc = withBytes(pubkey32, (pkp) =>
        withBytes(targetCid34, (cp) =>
          withBytes(sig64, (sgp) =>
            ex.cowrie_declassify_verify(pkp, cp, actCode, exp, sgp, now)
          )
        )
      );
      return rc === 0;
    },
  };
}
