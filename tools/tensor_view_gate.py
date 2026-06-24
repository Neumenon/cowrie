#!/usr/bin/env python3
"""Tensor zero-copy data-locator parity gate (Phase 2 / Tensor Advantage, option A).

Contract — `recode --tensor-spans`: read a canonical Cowrie message on stdin, decode it, and print
ONE LINE PER TENSOR in document order:

    <data_offset> <data_len> <dtype> <shape>

space-separated, where <shape> is the comma-joined dims or "-" for a rank-0 (scalar) tensor. No
tensors ⇒ empty output. data_offset is the absolute byte offset of the tensor's contiguous LE data.

Every implementation must produce identical lines, matching the golden testdata/v1_tensor_spans.json.
A reader can then mmap the message and view data[data_offset : data_offset+data_len] with no copy.

Run: python3 tools/tensor_view_gate.py   (exit non-zero on any mismatch)
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))

GOLDEN = json.load(open(os.path.join(ROOT, "testdata", "v1_golden.json")))
SPANS = json.load(open(os.path.join(ROOT, "testdata", "v1_tensor_spans.json")))
GO_CLI = os.environ.get("GO_CLI", "/tmp/cowrie-cli")
RUST = os.environ.get("RUST_RECODE", os.path.join(ROOT, "rust/target/release/examples/recode"))
TS_DIR = os.path.join(ROOT, "typescript")
TSX = os.path.join(TS_DIR, "node_modules/.bin/tsx")

IMPLS = {
    "go":   ([GO_CLI, "recode", "--tensor-spans"], None),
    "rust": ([RUST, "--tensor-spans"], None),
    "ts":   ([TSX, "recode.ts", "--tensor-spans"], TS_DIR),
}


def expected_lines(name: str) -> str:
    rows = []
    for s in SPANS[name]:
        shape = ",".join(str(d) for d in s["shape"]) if s["shape"] else "-"
        rows.append(f"{s['data_offset']} {s['data_len']} {s['dtype']} {shape}")
    return "\n".join(rows)


def run(cmd, raw, cwd=None) -> str:
    return subprocess.run(cmd, input=raw, capture_output=True, cwd=cwd).stdout.decode("ascii", "replace").strip()


def main() -> int:
    res = {k: [0, 0] for k in IMPLS}
    fails = []
    total = len(SPANS)
    for name in SPANS:
        raw = bytes.fromhex(GOLDEN[name]["canonical_hex"])
        want = expected_lines(name)
        for lang, (cmd, cwd) in IMPLS.items():
            got = run(cmd, raw, cwd)
            if got == want:
                res[lang][0] += 1
            else:
                res[lang][1] += 1
                fails.append(f"{lang}/{name}: want {want!r} got {got!r}")
    print(f"Tensor zero-copy locator parity gate — {total} tensor-bearing vectors (Phase 2 / A)\n")
    print(f"{'impl':<10}{'pass':>6}{'fail':>6}   status")
    green = True
    for lang, (p, f) in res.items():
        if f:
            green = False
        print(f"{lang:<10}{p:>6}{f:>6}   {'✅ GREEN' if f == 0 else '❌ RED'}")
    if fails:
        print("\n" + "\n".join(fails[:12]))
    print("\n" + ("✅ ALL IMPLEMENTATIONS AGREE ON TENSOR SPANS (zero-copy views)" if green else "❌ TENSOR-VIEW GATE RED"))
    return 0 if green else 1


if __name__ == "__main__":
    raise SystemExit(main())
