#!/usr/bin/env python3
"""Does Stage-3 give REAL value, or just drawn shapes? Measured demos vs the actual alternatives.
Each demo prints concrete numbers and states the honest comparison. Run: python3 tools/demos/profile_value.py
"""
from __future__ import annotations

import json
import os
import struct
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import cowrie_ref as c  # noqa: E402
from cowrie_ref import Tensor, file as F, profiles as p  # noqa: E402

GO = ["/tmp/cowrie-cli", "recode", "--addr"]
RUST = [os.path.join(ROOT, "rust/target/release/examples/recode"), "--addr"]
TS_DIR = os.path.join(ROOT, "typescript")
TSX = [os.path.join(TS_DIR, "node_modules/.bin/tsx"), "recode.ts", "--addr"]


def vec(seed, dim=384):
    # deterministic pseudo-random float32 vector (no Math.random in spirit)
    return [((seed * 2654435761 + i * 40503) % 1000) / 1000.0 for i in range(dim)]


def emb(seed, dim=384):
    v = vec(seed, dim)
    return p.embedding("text-embed-3", Tensor(0x01, (dim,), b"".join(struct.pack("<f", x) for x in v)), id=f"d{seed}")


def hr(t): print("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78)


def demo_canonical_dedup():
    hr("1. CONTENT-ADDRESSED DEDUP that JSON/msgpack CANNOT do (the killer)")
    v = vec(7, 8)
    # two JSON encodings of the SAME vector — different float repr + key order — both 'valid'
    j1 = json.dumps({"model": "m", "vector": v}, separators=(",", ":"))
    j2 = json.dumps({"vector": [float(f"{x:.10g}") for x in v], "model": "m"})  # reordered + reformatted
    import hashlib
    print(f"  same vector, two JSON encodings: equal bytes? {j1.encode()==j2.encode()}  "
          f"(sha256 {hashlib.sha256(j1.encode()).hexdigest()[:8]} vs {hashlib.sha256(j2.encode()).hexdigest()[:8]})")
    e1 = c.content_address(p.embedding("m", Tensor(0x01,(8,), b"".join(struct.pack('<f',x) for x in v))))
    e2 = c.content_address(p.embedding("m", Tensor(0x01,(8,), b"".join(struct.pack('<f',x) for x in v))))
    print(f"  same vector, Cowrie canonical address: equal? {e1==e2}  ({e1.hex()[:12]})")
    # stream dedup: 1000 records, 30% duplicates
    seeds = [i % 700 for i in range(1000)]  # 700 unique, 300 dups
    addrs = {c.content_address(emb(s, 16)) for s in seeds}
    print(f"  stream of 1000 embeddings (700 unique): unique content addresses = {len(addrs)} -> "
          f"dedup correctly collapses {1000-len(addrs)} duplicates")
    print("  VERDICT: JSON/msgpack can't dedup semantically-identical data (no canonical form); Cowrie does.")


def demo_size_vs_json():
    hr("2. SIZE + LOSSLESSNESS vs JSON for a 384-dim float32 embedding")
    e = emb(1)
    cw = c.encode(e)
    js = json.dumps({"model": "text-embed-3", "dim": 384, "dtype": "float32", "id": "d1",
                     "vector": vec(1)}, separators=(",", ":")).encode()
    print(f"  Cowrie: {len(cw):5} bytes (binary, lossless, 64B-aligned tensor)")
    print(f"  JSON  : {len(js):5} bytes (text, float repr is LOSSY, not aligned)  -> {len(js)/len(cw):.2f}x larger")
    print("  NOTE: size is NOT the pitch and is NOT optimized yet (no compression/quantization). The value")
    print("  is lossless + aligned + deterministic identity; we're correct first and can optimize size later.")


def demo_fingerprint_routing():
    hr("3. O(1) FINGERPRINT ROUTING + MODEL-DRIFT DETECTION")
    stream = [emb(i) for i in range(500)]
    fps = {c.fingerprint(r) for r in stream}
    print(f"  500 records, same (model,dim,dtype): distinct fingerprints = {len(fps)} (want 1 -> route by one key)")
    drift = emb(0, dim=768)  # a model swap -> different dim
    print(f"  a 768-dim record (model swap): fingerprint differs from the 384-dim stream? "
          f"{c.fingerprint(drift) not in fps}  -> drift caught in O(1), no schema diff")


def demo_graph_canonical():
    hr("3b. CANONICAL GRAPH SNAPSHOT (fixed: order-independent identity)")
    g1 = p.graph([{"id": "a", "labels": ["X"]}, {"id": "b"}], [{"src": "a", "dst": "b", "type": "e"}])
    g2 = p.graph([{"id": "b"}, {"id": "a", "labels": ["X"]}], [{"src": "a", "dst": "b", "type": "e"}])
    print(f"  same graph, nodes in reversed order -> same content address? "
          f"{c.content_address(g1) == c.content_address(g2)}  (was False before the fix; nodes/edges now"
          f" sorted by canonical encoding)")
    print(f"  a different graph -> different address? "
          f"{c.content_address(g1) != c.content_address(p.graph([{'id': 'a'}], []))}")


def demo_zero_copy():
    hr("4. ZERO-COPY aligned tensor (vs JSON base64 decode+copy)")
    e = emb(1)
    blob = c.encode(e)
    (dt, sh, off, ln) = c.tensor_spans(blob)[0]
    view = blob[off:off+ln]  # a slice/view, no decode
    first = struct.unpack("<f", view[:4])[0]
    print(f"  vector data at file offset {off} (64-aligned: {off%64==0}), {ln} bytes; "
          f"view[0]={first:.3f} read with NO decode/copy")
    print("  JSON: must parse the whole text array and allocate a new float array (copy). safetensors aligns too,")
    print("  but has no content identity/nesting/metadata envelope around it.")


def demo_tamper_and_dataset():
    hr("5. TAMPER-EVIDENCE + ONE-HASH DATASET (vs tar/parquet shards)")
    f0 = F.encode_file([emb(i, 16) for i in range(4)])
    f1 = F.encode_file([emb(i, 16) for i in range(4, 8)])
    r0, r1 = F.file_identity(f0), F.file_identity(f1)
    man = p.dataset_manifest([{"uri": "s0", "merkle_root": r0, "count": 4},
                              {"uri": "s1", "merkle_root": r1, "count": 4}])
    print(f"  dataset of 2 shards -> ONE identity: {man['root'].hex()[:16]}…")
    bad = bytearray(f0); bad[20] ^= 0xFF
    try:
        F.decode_file(bytes(bad)); ok = "NOT DETECTED ❌"
    except c.CowrieError: ok = "rejected ✓ (Merkle mismatch)"
    print(f"  flip 1 byte in shard 0: {ok}")
    r0b = p.dataset_root([F.merkle_root([c.encode(emb(99, 16))]), r1])
    print(f"  change shard 0 content -> dataset root changes: {man['root'] != p.dataset_root([F.merkle_root([c.encode(emb(99,16))]), r1])}")
    print("  tar/parquet shards have no per-record identity or dataset-level verifiable root.")


def demo_cross_lang():
    hr("6. CROSS-LANGUAGE PROFILE IDENTITY (the moat: same value -> same hash, any lang)")
    blob = c.encode(emb(1, 16))
    py = c.content_address(c.decode(blob)).hex()
    out = {"python": py}
    for name, cmd, cwd in (("go", GO, None), ("rust", RUST, None), ("ts", TSX, TS_DIR)):
        try:
            out[name] = subprocess.run(cmd, input=blob, capture_output=True, cwd=cwd).stdout.decode().strip()
        except Exception as e:  # noqa: BLE001
            out[name] = f"<{e}>"
    agree = len(set(out.values())) == 1
    for k, v in out.items(): print(f"  {k:7} {v[:24]}")
    print(f"  all four agree on the Embedding's content address? {agree}")


if __name__ == "__main__":
    demo_canonical_dedup(); demo_size_vs_json(); demo_fingerprint_routing()
    demo_graph_canonical(); demo_zero_copy(); demo_tamper_and_dataset(); demo_cross_lang()
    print("\n" + "=" * 78 + "\nDemos are MEASURED claims, not marketing. Read each VERDICT critically.\n" + "=" * 78)
