#!/usr/bin/env python3
"""Dataset-identity parity gate (stream/dataset layer — docs/STREAM-LAYER.md).

`dataset_root` is the top of the identity DAG (dataset -> file roots -> frames). It must be byte-
identical across implementations. For each golden dataset, each impl's `recode --dataset-root` (Merkle
over the ordered shard roots, reusing the §7 construction) must equal the golden `dataset_root`.

Wire for --dataset-root stdin: uvarint(n) then n times (uvarint(len) || blob). Output: 68-hex multihash.

Run: python3 tools/dataset_identity_gate.py   (exit non-zero on any divergence)
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
from cowrie_ref import profiles as p  # noqa: E402
from cowrie_ref.varint import encode_uvarint  # noqa: E402

DS = json.load(open(os.path.join(ROOT, "testdata", "v1_datasets.json")))
GO_CLI = os.environ.get("GO_CLI", "/tmp/cowrie-cli")
RUST = os.environ.get("RUST_RECODE", os.path.join(ROOT, "rust/target/release/examples/recode"))
TS_DIR = os.path.join(ROOT, "typescript")
TSX = os.path.join(TS_DIR, "node_modules/.bin/tsx")
IMPLS = {
    "go":   ([GO_CLI, "recode", "--dataset-root"], None),
    "rust": ([RUST, "--dataset-root"], None),
    "ts":   ([TSX, "recode.ts", "--dataset-root"], TS_DIR),
}


def encode_blobs(roots: list[bytes]) -> bytes:
    out = bytearray(encode_uvarint(len(roots)))
    for r in roots:
        out += encode_uvarint(len(r)) + r
    return bytes(out)


def run(cmd, raw, cwd=None) -> str:
    return subprocess.run(cmd, input=raw, capture_output=True, cwd=cwd).stdout.decode("ascii", "replace").strip()


def main() -> int:
    impls = {"python": lambda roots, want: p.dataset_root(roots).hex()}
    for lang, (cmd, cwd) in IMPLS.items():
        impls[lang] = (lambda cmd, cwd: (lambda roots, want: run(cmd, encode_blobs(roots), cwd)))(cmd, cwd)
    res = {k: [0, 0] for k in impls}
    fails = []
    for name, fx in DS.items():
        roots = [bytes.fromhex(h) for h in fx["shard_roots"]]
        want = fx["dataset_root"]
        for lang, fn in impls.items():
            got = fn(roots, want)
            if got == want:
                res[lang][0] += 1
            else:
                res[lang][1] += 1
                fails.append(f"{lang}/{name}: want {want[:16]}… got {got[:16]}…")
    total = len(DS)
    print(f"Dataset-identity parity gate — {total} datasets (Merkle DAG over file roots)\n")
    print(f"{'impl':<10}{'pass':>6}{'fail':>6}   status")
    green = True
    for lang, (a, b) in res.items():
        if b:
            green = False
        print(f"{lang:<10}{a:>6}{b:>6}   {'✅ GREEN' if b == 0 else '❌ RED'}")
    if fails:
        print("\n" + "\n".join(fails[:12]))
    print("\n" + ("✅ ALL IMPLEMENTATIONS AGREE ON DATASET IDENTITY" if green else "❌ DATASET-IDENTITY GATE RED"))
    return 0 if green else 1


if __name__ == "__main__":
    raise SystemExit(main())
