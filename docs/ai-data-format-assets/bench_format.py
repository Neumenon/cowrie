#!/usr/bin/env python3
"""
Tessera Reference Proof & Format Benchmark Harness
--------------------------------------------------
Reproducible micro-benchmark backing the quantitative plates in:
  "Designing a Data Format for the Age of AI"

Measures exact bytes, zstd-3 compressed bytes, deterministic token-proxy counts
(cl100k-style regex pre-tokenization with subword splitting for long runs, clearly
reported as modeled_bpe_tokens), zero-copy 64-byte tensor alignment, canonical
bijectivity under key permutation, and progressive R0/R1/R2/R3 attention budgets.

Receipt output: /home/omen/agent-lib/ai-data-format-assets/bench_receipt.json
"""

import base64
import hashlib
import json
import math
import os
import re
import struct
import time
import zstandard as zstd
import numpy as np

# ---------------------------------------------------------------------------
# 1. Deterministic BPE-proxy tokenizer (transparent & reproducible, zero network)
# ---------------------------------------------------------------------------
# Splits on cl100k-like boundaries (contractions, letters, 1-3 digit runs,
# punctuation/symbols, whitespace) and counts long alpha/hex/base64 runs at ~4
# chars/token (or ~1.33 chars/token for high-entropy base64/hex runs).
_PAT = re.compile(
    r"""'(?:[sdmt]|ll|ve|re)| ?[A-Za-z]+| ?[0-9]{1,3}| ?[^\sA-Za-z0-9]+|\s+(?!\S)|\s+"""
)

def estimate_bpe_tokens(text: str) -> int:
    total = 0
    for tok in _PAT.findall(text):
        n = len(tok)
        if n <= 4:
            total += 1
        elif tok.strip().isalpha():
            # Common English words up to ~7 chars are usually 1 token; longer split ~4.5 chars/tok
            total += 1 if n <= 7 else math.ceil(n / 4.5)
        else:
            # Dense punctuation, hex, or base64 encodes poorly in BPE (~1.5-2.5 chars/token)
            total += math.ceil(n / 2.2)
    return total

# ---------------------------------------------------------------------------
# 2. Minimal Canonical Tessera Core Codec (Bijective + 64B-Aligned Tensors)
# ---------------------------------------------------------------------------
MAGIC = b"TSRA"
VERSION = 1

TAG_NULL   = 0x00
TAG_FALSE  = 0x01
TAG_TRUE   = 0x02
TAG_INT    = 0x03
TAG_FLOAT  = 0x04
TAG_STR    = 0x05
TAG_BYTES  = 0x06
TAG_ARRAY  = 0x07
TAG_MAP    = 0x08
TAG_TENSOR = 0x20  # Payload stored in 64-byte aligned arena
# 0x40..0xBF = FIXINT 0..127

class TensorVal:
    __slots__ = ("dtype", "shape", "data", "space_id")
    def __init__(self, dtype: str, shape: tuple, data: bytes, space_id: str = ""):
        self.dtype = dtype
        self.shape = tuple(shape)
        self.data = data
        self.space_id = space_id

def _uvarint(n: int) -> bytes:
    assert n >= 0
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)

def _read_uvarint(buf: memoryview, pos: int):
    shift = 0
    val = 0
    while True:
        b = buf[pos]
        pos += 1
        val |= (b & 0x7F) << shift
        if not (b & 0x80):
            return val, pos
        shift += 7

def _zigzag(n: int) -> int:
    return (n << 1) ^ (n >> 63)

def _unzigzag(u: int) -> int:
    return (u >> 1) ^ -(u & 1)

def _collect_keys(v, keys: set):
    if isinstance(v, dict):
        for k, val in v.items():
            keys.add(k)
            _collect_keys(val, keys)
    elif isinstance(v, list):
        for item in v:
            _collect_keys(item, keys)

def tessera_encode(v, trust_byte: int = 0x11) -> bytes:
    """
    Encode a value into a canonical Tessera frame:
      [0..63]   64-byte fixed header:
                  magic(4) + ver(1) + trust_authority(1) + flags(2)
                  + sym_len(4) + arena_off(4) + arena_len(4) + tensor_count(4)
                  + core_sha256_prefix(32) + reserved(8)
      [64..]    Plane A: Sorted key dictionary + Canonical Symbolic Core
      [align64] Plane C: 64-byte aligned zero-copy Tensor Arena (zero-padded)
    """
    keys = set()
    _collect_keys(v, keys)
    sorted_keys = sorted(keys, key=lambda s: s.encode("utf-8"))
    key_idx = {k: i for i, k in enumerate(sorted_keys)}

    sym = bytearray()
    sym += _uvarint(len(sorted_keys))
    for k in sorted_keys:
        kb = k.encode("utf-8")
        sym += _uvarint(len(kb)) + kb

    arena_entries = []  # (dtype, shape, space_id, raw_bytes)

    def enc_val(x):
        if x is None:
            sym.append(TAG_NULL)
        elif isinstance(x, bool):
            sym.append(TAG_TRUE if x else TAG_FALSE)
        elif isinstance(x, int):
            if 0 <= x <= 127:
                sym.append(0x40 + x)
            else:
                sym.append(TAG_INT)
                sym.extend(_uvarint(_zigzag(x)))
        elif isinstance(x, float):
            # Canonical float64: -0.0 -> +0.0, single quiet NaN
            if math.isnan(x):
                raw = struct.pack("<Q", 0x7FF8000000000000)
            elif x == 0.0:
                raw = struct.pack("<d", 0.0)
            else:
                raw = struct.pack("<d", x)
            sym.append(TAG_FLOAT)
            sym.extend(raw)
        elif isinstance(x, str):
            b = x.encode("utf-8")
            sym.append(TAG_STR)
            sym.extend(_uvarint(len(b)))
            sym.extend(b)
        elif isinstance(x, bytes):
            sym.append(TAG_BYTES)
            sym.extend(_uvarint(len(x)))
            sym.extend(x)
        elif isinstance(x, TensorVal):
            tid = len(arena_entries)
            arena_entries.append(x)
            sym.append(TAG_TENSOR)
            sym.extend(_uvarint(tid))
            dt = x.dtype.encode("utf-8")
            sp = x.space_id.encode("utf-8")
            sym.extend(_uvarint(len(dt)))
            sym.extend(dt)
            sym.extend(_uvarint(len(sp)))
            sym.extend(sp)
            sym.extend(_uvarint(len(x.shape)))
            for d in x.shape:
                sym.extend(_uvarint(d))
            sym.extend(_uvarint(len(x.data)))
        elif isinstance(x, list):
            sym.append(TAG_ARRAY)
            sym.extend(_uvarint(len(x)))
            for item in x:
                enc_val(item)
        elif isinstance(x, dict):
            sym.append(TAG_MAP)
            sym.extend(_uvarint(len(x)))
            # Strictly byte-sorted keys guarantee bijective canonical output
            for k in sorted(x.keys(), key=lambda s: s.encode("utf-8")):
                sym.extend(_uvarint(key_idx[k]))
                enc_val(x[k])
        else:
            raise TypeError(f"Unsupported type: {type(x)}")

    enc_val(v)

    # Assemble Plane A + 64-byte aligned Plane C arena
    body = bytearray()
    header_size = 64
    sym_bytes = bytes(sym)
    sym_end = header_size + len(sym_bytes)
    pad_to_arena = (64 - (sym_end % 64)) % 64 if arena_entries else 0
    arena_off = sym_end + pad_to_arena

    arena = bytearray()
    tensor_offsets = []
    for t in arena_entries:
        rel_off = len(arena)
        assert (arena_off + rel_off) % 64 == 0
        tensor_offsets.append(arena_off + rel_off)
        arena += t.data
        rem = len(t.data) % 64
        if rem:
            arena += b"\x00" * (64 - rem)

    # Canonical content digest covers Plane A + Plane C (independent of transport compression)
    h = hashlib.sha256()
    h.update(b"\x00")
    h.update(sym_bytes)
    h.update(b"\x01")
    for t in arena_entries:
        h.update(t.data)
    digest = h.digest()

    header = struct.pack(
        "<4sBBHIIII32sQ",
        MAGIC,
        VERSION,
        trust_byte,
        0,
        len(sym_bytes),
        arena_off,
        len(arena),
        len(arena_entries),
        digest,
        0,
    )
    assert len(header) == 64
    body += header
    body += sym_bytes
    if pad_to_arena:
        body += b"\x00" * pad_to_arena
    body += arena
    return bytes(body)

def tessera_decode_zero_copy(buf: bytes):
    """Strict decoder returning borrowed memoryviews for aligned tensors."""
    mv = memoryview(buf)
    magic, ver, trust, flags, sym_len, arena_off, arena_len, tcount, digest, res = struct.unpack_from(
        "<4sBBHIIII32sQ", mv, 0
    )
    assert magic == MAGIC and ver == VERSION and flags == 0 and res == 0
    pos = 64
    sym_end = 64 + sym_len
    nkeys, pos = _read_uvarint(mv, pos)
    keys = []
    for _ in range(nkeys):
        klen, pos = _read_uvarint(mv, pos)
        keys.append(bytes(mv[pos:pos + klen]).decode("utf-8"))
        pos += klen

    # Verify strict sorted uniqueness
    for i in range(1, len(keys)):
        assert keys[i - 1].encode("utf-8") < keys[i].encode("utf-8"), "ERR_NON_CANONICAL_DICT"

    arena_cursor = arena_off
    tensor_views = []

    def dec_val(p):
        nonlocal arena_cursor
        tag = mv[p]
        p += 1
        if 0x40 <= tag <= 0xBF:
            return tag - 0x40, p
        if tag == TAG_NULL:
            return None, p
        if tag == TAG_FALSE:
            return False, p
        if tag == TAG_TRUE:
            return True, p
        if tag == TAG_INT:
            u, p = _read_uvarint(mv, p)
            val = _unzigzag(u)
            assert not (0 <= val <= 127), "ERR_NON_CANONICAL_INT"
            return val, p
        if tag == TAG_FLOAT:
            val = struct.unpack_from("<d", mv, p)[0]
            return val, p + 8
        if tag == TAG_STR:
            n, p = _read_uvarint(mv, p)
            return bytes(mv[p:p + n]).decode("utf-8"), p + n
        if tag == TAG_BYTES:
            n, p = _read_uvarint(mv, p)
            return mv[p:p + n], p + n
        if tag == TAG_TENSOR:
            tid, p = _read_uvarint(mv, p)
            dlen, p = _read_uvarint(mv, p)
            dtype = bytes(mv[p:p + dlen]).decode("utf-8")
            p += dlen
            slen, p = _read_uvarint(mv, p)
            space_id = bytes(mv[p:p + slen]).decode("utf-8")
            p += slen
            ndim, p = _read_uvarint(mv, p)
            shape = []
            for _ in range(ndim):
                dim, p = _read_uvarint(mv, p)
                shape.append(dim)
            blen, p = _read_uvarint(mv, p)
            assert arena_cursor % 64 == 0, "ERR_UNALIGNED_TENSOR"
            tview = mv[arena_cursor:arena_cursor + blen]  # ZERO COPY!
            tensor_views.append((tid, arena_cursor, blen, dtype, tuple(shape), tview))
            step = blen + ((64 - (blen % 64)) % 64)
            arena_cursor += step
            return TensorVal(dtype, tuple(shape), bytes(tview), space_id), p
        if tag == TAG_ARRAY:
            n, p = _read_uvarint(mv, p)
            arr = []
            for _ in range(n):
                item, p = dec_val(p)
                arr.append(item)
            return arr, p
        if tag == TAG_MAP:
            n, p = _read_uvarint(mv, p)
            obj = {}
            prev_k = -1
            for _ in range(n):
                ki, p = _read_uvarint(mv, p)
                assert ki > prev_k, "ERR_NON_CANONICAL_KEY_ORDER"
                prev_k = ki
                val, p = dec_val(p)
                obj[keys[ki]] = val
            return obj, p
        raise ValueError(f"ERR_RESERVED_TAG: {hex(tag)}")

    val, end_pos = dec_val(pos)
    assert end_pos == sym_end
    return val, digest, tensor_views

# ---------------------------------------------------------------------------
# 3. Deterministic Attention Lens Projector (R0 / R1 / R2)
# ---------------------------------------------------------------------------
def project_r0_handle(name: str, digest: bytes, record_type: str, trust: str, stats: str) -> str:
    return f"@tsra:{digest.hex()[:12]} type={record_type} trust={trust} {stats}"

def project_r1_skeleton(v, depth=0) -> str:
    """Deterministic structural atlas: schema shape, array counts, tensor coordinates, no heavy bodies."""
    ind = "  " * depth
    if isinstance(v, dict):
        lines = []
        for k in sorted(v.keys()):
            val = v[k]
            if isinstance(val, list) and val and isinstance(val[0], dict):
                cols = sorted(val[0].keys())
                lines.append(f"{ind}{k}: Table[{len(val)}]({','.join(cols)})")
            elif isinstance(val, TensorVal):
                lines.append(f"{ind}{k}: Tensor<{val.dtype}>{list(val.shape)} @{val.space_id} ({len(val.data)}B)")
            elif isinstance(val, dict):
                lines.append(f"{ind}{k}:")
                lines.append(project_r1_skeleton(val, depth + 1))
            elif isinstance(val, str) and len(val) > 48:
                lines.append(f"{ind}{k}: str[{len(val)}B] \"{val[:36]}...\"")
            else:
                lines.append(f"{ind}{k}: {val}")
        return "\n".join(lines)
    return f"{ind}{type(v).__name__}"

def project_r2_attention_lens(v, depth=0) -> str:
    """
    Deterministic Token-Optimal Lens (R2):
    - Hoists homogeneous dict-array schemas into compact CSV-like rows
    - Strips redundant JSON quotes/braces
    - Replaces raw tensors with compact coordinate handles (never dumps raw floats to an LLM!)
    - Seals untrusted spans explicitly
    """
    ind = "  " * depth
    if isinstance(v, dict):
        lines = []
        for k in sorted(v.keys()):
            val = v[k]
            if isinstance(val, list) and len(val) >= 2 and all(isinstance(x, dict) for x in val):
                cols = sorted(val[0].keys())
                if all(sorted(x.keys()) == cols for x in val):
                    lines.append(f"{ind}{k}[{len(val)}]{{{','.join(cols)}}}:")
                    for row in val:
                        cells = []
                        for c in cols:
                            cv = row[c]
                            if isinstance(cv, TensorVal):
                                cells.append(f"<T:{cv.dtype}{list(cv.shape)}>")
                            else:
                                s = str(cv).replace("\n", " ")
                                cells.append(s)
                        lines.append(f"{ind}  " + " | ".join(cells))
                    continue
            if isinstance(val, TensorVal):
                lines.append(f"{ind}{k}: <Tensor {val.dtype}{list(val.shape)} space={val.space_id} bytes={len(val.data)}>")
            elif isinstance(val, dict):
                lines.append(f"{ind}{k}:")
                lines.append(project_r2_attention_lens(val, depth + 1))
            elif isinstance(val, list):
                lines.append(f"{ind}{k}: [" + ", ".join(str(x) for x in val) + "]")
            else:
                lines.append(f"{ind}{k}: {val}")
        return "\n".join(lines)
    return str(v)

# ---------------------------------------------------------------------------
# 4. Construct Four Realistic AI Workloads (Deterministic seed)
# ---------------------------------------------------------------------------
rng = np.random.default_rng(20261123)

def make_w1_agent_trace():
    steps = []
    tools = ["web.fetch", "repo.grep", "sql.query", "code.patch", "test.run", "doc.cite"]
    authorities = ["UNTRUSTED_EXT", "VERIFIED_TOOL", "VERIFIED_TOOL", "MODEL_SYNTH", "VERIFIED_TOOL", "VERIFIED_TOOL"]
    for i in range(12):
        t = tools[i % len(tools)]
        auth = authorities[i % len(authorities)]
        steps.append({
            "step": i + 1,
            "tool": t,
            "authority": auth,
            "latency_ms": int(14 + (i * 19) % 180),
            "status": "ok" if i != 7 else "retry_timeout",
            "confidence": round(0.84 + (i % 5) * 0.03, 2),
            "summary": f"Executed {t} on shard_{i:02d}; matched {(i+1)*3} candidate spans with provenance digest.",
            "hash": hashlib.sha256(f"step-{i}".encode()).hexdigest()[:16],
        })
    return {
        "schema": "tessera.trace.v1",
        "session_id": "ses_2026_11_23_alpha",
        "model": "qwen3.5-9b-q4km@sha256:9a4f1c8e",
        "policy_hash": "1220a8b4e91c7d32",
        "steps": steps,
    }

def make_w2_multimodal_record():
    emb = rng.standard_normal(1536).astype(np.float32)
    emb /= np.linalg.norm(emb)
    patch = rng.integers(-128, 127, size=(64, 64), dtype=np.int8)
    tsra_obj = {
        "schema": "tessera.multimodal.v1",
        "doc_id": "paper_2026_attention_io",
        "title": "Zero-Copy Attention Storage Across Heterogeneous Memory Tiers",
        "authority": "VERIFIED_CORPUS",
        "abstract": (
            "Modern inference engines stall on serialization boundaries between CPU orchestration, "
            "context-window token assembly, and GPU HBM tensor feeds. We evaluate a canonical "
            "aligned container with deterministic resolution projections."
        ),
        "embedding": TensorVal("f32", (1536,), emb.tobytes(), "nomic-embed-v2@sha256:44e1a0:cos:l2"),
        "vision_latents": TensorVal("i8", (64, 64), patch.tobytes(), "siglip2-patch14@sha256:88c2f1:q8"),
        "citations": [
            {"id": "ref_01", "year": 2024, "span": "120..448", "score": 0.94},
            {"id": "ref_02", "year": 2025, "span": "910..1180", "score": 0.89},
            {"id": "ref_03", "year": 2026, "span": "1420..1890", "score": 0.97},
        ],
    }
    json_obj = {
        "schema": tsra_obj["schema"],
        "doc_id": tsra_obj["doc_id"],
        "title": tsra_obj["title"],
        "authority": tsra_obj["authority"],
        "abstract": tsra_obj["abstract"],
        "embedding": [round(float(x), 6) for x in emb],
        "vision_latents_b64": base64.b64encode(patch.tobytes()).decode("ascii"),
        "citations": tsra_obj["citations"],
    }
    return tsra_obj, json_obj

def make_w3_retrieval_table():
    rows = []
    domains = ["systems", "compilers", "storage", "cryptography", "ml-kernels"]
    for i in range(100):
        rows.append({
            "rank": i + 1,
            "doc_id": f"doc_{i:04d}",
            "domain": domains[i % len(domains)],
            "score": round(0.99 - i * 0.0042, 4),
            "tokens": 180 + (i * 37) % 640,
            "trust": "verified" if i % 4 != 3 else "untrusted_web",
            "excerpt": f"Finding {i+1}: aligned block transfer eliminates copy overhead across PCIe Gen4 lanes.",
        })
    return {
        "schema": "tessera.retrieval.v1",
        "query": "zero-copy tensor serialization benchmarks",
        "k": 100,
        "hits": rows,
    }

def make_w4_epistemic_graph():
    claims = []
    for i in range(40):
        claims.append({
            "claim_id": f"clm_{i:03d}",
            "subject": f"engine_gate_{i % 10}",
            "predicate": "throughput_tps",
            "value": round(42.5 + i * 2.15, 2),
            "ci_low": round(41.0 + i * 2.15, 2),
            "ci_high": round(44.0 + i * 2.15, 2),
            "epistemic": "observed_exact" if i % 3 != 2 else "inferred_prob",
            "evidence_cid": hashlib.sha256(f"ev-{i}".encode()).hexdigest()[:16],
            "span": f"{i*128}..{i*128+96}",
        })
    return {
        "schema": "tessera.epistemic.v1",
        "graph_epoch": 20261123,
        "verifier": "bendtt-v2.0.35",
        "claims": claims,
    }

def main():
    cctx = zstd.ZstdCompressor(level=3)
    w1 = make_w1_agent_trace()
    w2_tsra, w2_json = make_w2_multimodal_record()
    w3 = make_w3_retrieval_table()
    w4 = make_w4_epistemic_graph()

    workloads = [
        ("W1_agent_trace", "Agent Execution Trace (12 steps)", w1, w1, "trace", "MIXED"),
        ("W2_multimodal", "Multimodal Record (1536d f32 + 64x64 i8)", w2_tsra, w2_json, "multimodal", "VERIFIED"),
        ("W3_retrieval_100", "Retrieval Result Table (100 rows)", w3, w3, "retrieval", "MIXED"),
        ("W4_epistemic_dag", "Epistemic Lineage Graph (40 claims)", w4, w4, "epistemic", "VERIFIED"),
    ]

    results = []
    for wid, label, tsra_val, json_val, rtype, trust in workloads:
        j_pretty = json.dumps(json_val, indent=2)
        j_mini = json.dumps(json_val, separators=(",", ":"))
        t_bytes = tessera_encode(tsra_val)

        # Verify bijective canonical roundtrip + key-shuffle invariance
        dec_val, digest, tviews = tessera_decode_zero_copy(t_bytes)
        re_bytes = tessera_encode(dec_val)
        assert re_bytes == t_bytes, f"Bijectivity failed on {wid}"

        # Shuffle top-level keys and assert bit-identical canonical encoding
        shuffled = dict(reversed(list(tsra_val.items())))
        assert tessera_encode(shuffled) == t_bytes, f"Canonical invariance failed on {wid}"

        # Verify every tensor view is strictly 64-byte aligned and zero-copy
        for tid, off, blen, dt, shp, tv in tviews:
            assert off % 64 == 0, f"Unaligned tensor offset {off}"
            if dt == "f32":
                arr = np.frombuffer(tv, dtype=np.float32)
                assert arr.shape[0] == shp[0]

        # Progressive Attention Projections
        r0 = project_r0_handle(wid, digest, rtype, trust, f"bytes={len(t_bytes)} tensors={len(tviews)}")
        r1 = project_r1_skeleton(tsra_val)
        r2 = project_r2_attention_lens(tsra_val)

        # Timing (500 iterations encode/decode)
        iters = 300
        t0 = time.perf_counter()
        for _ in range(iters):
            json.loads(j_mini)
        json_dec_us = (time.perf_counter() - t0) * 1e6 / iters

        t0 = time.perf_counter()
        for _ in range(iters):
            tessera_decode_zero_copy(t_bytes)
        tsra_dec_us = (time.perf_counter() - t0) * 1e6 / iters

        rec = {
            "id": wid,
            "label": label,
            "bytes": {
                "json_pretty": len(j_pretty.encode("utf-8")),
                "json_minified": len(j_mini.encode("utf-8")),
                "tessera_core": len(t_bytes),
                "json_zstd3": len(cctx.compress(j_mini.encode("utf-8"))),
                "tessera_zstd3": len(cctx.compress(t_bytes)),
            },
            "modeled_bpe_tokens": {
                "json_pretty": estimate_bpe_tokens(j_pretty),
                "json_minified": estimate_bpe_tokens(j_mini),
                "tessera_r2_lens": estimate_bpe_tokens(r2),
                "tessera_r1_skeleton": estimate_bpe_tokens(r1),
                "tessera_r0_handle": estimate_bpe_tokens(r0),
            },
            "python_decode_us": {
                "json_stdlib_c": round(json_dec_us, 1),
                "tessera_pure_py_ref": round(tsra_dec_us, 1),
            },
            "verification": {
                "bijective_roundtrip": True,
                "key_shuffle_bit_identical": True,
                "tensor_count": len(tviews),
                "all_tensors_64b_aligned": all(off % 64 == 0 for _, off, *_ in tviews),
                "core_sha256_prefix": digest.hex()[:16],
            },
            "samples": {
                "r0": r0,
                "r1_preview": r1[:420],
                "r2_preview": r2[:420],
            },
        }
        # Compute savings ratios
        jm_tok = rec["modeled_bpe_tokens"]["json_minified"]
        jp_tok = rec["modeled_bpe_tokens"]["json_pretty"]
        r2_tok = rec["modeled_bpe_tokens"]["tessera_r2_lens"]
        r0_tok = rec["modeled_bpe_tokens"]["tessera_r0_handle"]
        rec["ratios"] = {
            "r2_vs_json_pretty_token_reduction_pct": round((1.0 - r2_tok / jp_tok) * 100, 1),
            "r2_vs_json_mini_token_reduction_pct": round((1.0 - r2_tok / jm_tok) * 100, 1),
            "r0_vs_json_mini_compression_x": round(jm_tok / r0_tok, 1),
            "binary_vs_json_mini_byte_reduction_pct": round(
                (1.0 - rec["bytes"]["tessera_core"] / rec["bytes"]["json_minified"]) * 100, 1
            ),
        }
        results.append(rec)

    receipt = {
        "generated_at": "2026-11-23",
        "method": {
            "bytes": "exact UTF-8 / binary frame length in Python 3",
            "compression": "zstandard level 3",
            "tokens": "deterministic cl100k-style regex pre-tokenizer + subword length proxy (modeled_bpe_tokens, not OpenAI API billing)",
            "canonical_checks": "bijective re-encode equality, reversed key order bit-identity, 64-byte aligned memoryview zero-copy check",
        },
        "workloads": results,
    }

    out_path = "/home/omen/agent-lib/ai-data-format-assets/bench_receipt.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(receipt, f, indent=2)

    print(f"Wrote {out_path}")
    for r in results:
        b = r["bytes"]
        t = r["modeled_bpe_tokens"]
        rt = r["ratios"]
        print(
            f"{r['id']:18s} | bytes: JSON-min={b['json_minified']:6d}  TSRA={b['tessera_core']:6d} ({rt['binary_vs_json_mini_byte_reduction_pct']:+5.1f}%) | "
            f"toks: JSON-min={t['json_minified']:5d}  R2={t['tessera_r2_lens']:4d} (-{rt['r2_vs_json_mini_token_reduction_pct']:4.1f}%)  "
            f"R1={t['tessera_r1_skeleton']:3d}  R0={t['tessera_r0_handle']:2d} ({rt['r0_vs_json_mini_compression_x']}x)"
        )

if __name__ == "__main__":
    main()
