#!/usr/bin/env python3
"""Standalone cross-language conformance runner for Cowrie (`libcowrie_core.so`).
Reads frozen golden, fuzz, anti-malleability, DoS, truncation, Unicode, and Merkle vectors
from `tests/vectors.json` (zero external dependencies)."""

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python"))

import cowrie


def main():
    vec_path = ROOT / "tests" / "vectors.json"
    v = json.loads(vec_path.read_text())

    # 1. 50 golden + 100 fuzz canonical frames bit-for-bit CID & metadata verification
    for grp in ("golden", "fuzz"):
        for item in v[grp]:
            raw = bytes.fromhex(item["wire_hex"])
            res = cowrie.verify_frame(raw)
            assert res["authority"] == item["auth"]
            assert res["epistemic"] == item["epi"]
            assert res["cid"].hex() == item["cid_hex"], f"{item['id']} CID mismatch"

    # 2. 12 anti-malleability negative fixtures -> exact ERR_* ABI match
    for neg in v["negatives"]:
        raw = bytes.fromhex(neg["wire_hex"])
        try:
            cowrie.verify_frame(raw)
            raise AssertionError(f"Accepted negative {neg['id']}")
        except cowrie.CowrieError as e:
            assert e.code == neg["expected_err"], f"{neg['id']}: got {e.code}, exp {neg['expected_err']}"

    # 3. 16 KB shard + RFC 6962 Merkle root + 10/10 1-bit flip rejections
    clean_shard = bytes.fromhex(v["clean_shard_hex"])
    root = cowrie.verify_shard(clean_shard)
    assert root.hex() == v["clean_shard_root"]
    for ct in v["corrupt_tests"]:
        mut = bytearray(clean_shard)
        mut[ct["flip_byte_offset"]] ^= 0x01
        try:
            cowrie.verify_shard(bytes(mut))
            raise AssertionError(f"Corrupt shard {ct['id']} accepted!")
        except cowrie.CowrieError as e:
            assert e.code == "ERR_ROOT_MISMATCH"

    # 4. 8 hostile DoS bombs + depth=64 boundary + exhaustive byte truncations + astral UTF-8 trap
    for c in v["dos_cases"]:
        raw = bytes.fromhex(c["wire_hex"])
        try:
            cowrie.verify_frame(raw)
            raise AssertionError(f"DoS {c['id']} accepted!")
        except cowrie.CowrieError as e:
            assert e.code == c["expected_err"], f"{c['id']}: got {e.code}, exp {c['expected_err']}"

    cowrie.verify_frame(bytes.fromhex(v["depth_64_ok_hex"]))

    rich = bytes.fromhex(v["rich_wire_hex"])
    for cut in range(len(rich)):
        try:
            cowrie.verify_frame(rich[:cut])
            raise AssertionError(f"Truncation {cut} accepted!")
        except cowrie.CowrieError as e:
            assert e.code in ("ERR_TRUNCATED", "ERR_ALLOC_LIMIT")

    ures = cowrie.verify_frame(bytes.fromhex(v["unicode_wire_hex"]))
    assert ures["cid"].hex() == v["unicode_cid_hex"]

    # 5. JSON -> Canonical (key-order invariant) -> R0/R1/R2 Lenses + Causal Taint propagation
    doc = {
        "title": "RTX 3090 Verification Ledger",
        "runs": [
            {"suite": "U1", "status": "PASS", "speedup": 1.93},
            {"suite": "U2", "status": "PASS", "speedup": 61.9},
            {"suite": "U3-v1.0", "status": "PASS", "speedup": 16.3},
        ],
        "embedding": {"$tensor": 0, "dtype": "i8", "shape": [512], "space": "nomic-v2:cos"},
    }
    w1 = cowrie.encode_json(doc, cowrie.AUTH_VERIFIED_TOOL, cowrie.EPI_OBSERVED)
    w2 = cowrie.encode_json(dict(reversed(list(doc.items()))), cowrie.AUTH_VERIFIED_TOOL, cowrie.EPI_OBSERVED)
    assert w1 == w2
    assert cowrie.project_lens(w1, "r0", 1).startswith("@")
    assert "Table[3](speedup,status,suite)" in cowrie.project_lens(w1, "r1", 1)
    r2 = cowrie.project_lens(w1, "r2", 1)
    assert "runs[3]{speedup,status,suite}:" in r2 and "<T:i8[512]@nomic-v2:cos>" in r2

    hop1 = cowrie.propagate_authority(cowrie.AUTH_MODEL_SYNTH, [cowrie.AUTH_UNTRUSTED_EXT, cowrie.AUTH_VERIFIED_TOOL])
    hop2 = cowrie.propagate_authority(cowrie.AUTH_VERIFIED_TOOL, [hop1])
    clean = cowrie.propagate_authority(cowrie.AUTH_VERIFIED_TOOL, [cowrie.AUTH_SYS_POLICY, cowrie.AUTH_VERIFIED_TOOL])
    assert cowrie.is_tainted(hop1) and cowrie.is_tainted(hop2) and not cowrie.is_tainted(clean)

    print(
        f"STANDALONE CONFORMANCE PASS (vectors.json): "
        f"{len(v['golden'])} golden + {len(v['fuzz'])} fuzz + {len(v['negatives'])} neg + "
        f"{len(v['corrupt_tests'])} bitflips + {len(v['dos_cases'])} DoS + {len(rich)} truncations + "
        f"Unicode + R0/R1/R2 + Causal Taint = 100% PASS"
    )


if __name__ == "__main__":
    main()
