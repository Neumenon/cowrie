#!/usr/bin/env python3
"""Structural-fingerprint parity gate (§4 / §4.4 release gate — the one that kills B4).

The §4 fingerprint MUST be byte-identical across implementations: it is an identity. This gate was
MISSING, which is why the legacy ordinal scheme (B4) survived in Go/Rust/TS undetected. For every
golden vector, each impl's `recode --fingerprint` (the FNV-1a-64 over the §4 FPC grammar, printed as
16 lowercase hex chars) must equal the golden `fingerprint64` produced by the Python oracle.

Run: python3 tools/fingerprint_gate.py   (exit non-zero on any divergence)
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import cowrie_ref as c  # noqa: E402

GOLDEN = json.load(open(os.path.join(ROOT, "testdata", "v1_golden.json")))
GO_CLI = os.environ.get("GO_CLI", "/tmp/cowrie-cli")
RUST = os.environ.get("RUST_RECODE", os.path.join(ROOT, "rust/target/release/examples/recode"))
TS_DIR = os.path.join(ROOT, "typescript")
TSX = os.path.join(TS_DIR, "node_modules/.bin/tsx")

IMPLS = {
    "go":   ([GO_CLI, "recode", "--fingerprint"], None),
    "rust": ([RUST, "--fingerprint"], None),
    "ts":   ([TSX, "recode.ts", "--fingerprint"], TS_DIR),
}


def fp(cmd, raw, cwd=None) -> str:
    return subprocess.run(cmd, input=raw, capture_output=True, cwd=cwd).stdout.decode("ascii", "replace").strip().lower()


def main() -> int:
    impls = {"python": lambda raw, name: format(c.fingerprint(c.decode(raw))[0] & 0xFFFFFFFFFFFFFFFF, "016x")}
    for lang, (cmd, cwd) in IMPLS.items():
        impls[lang] = (lambda cmd, cwd: (lambda raw, name: fp(cmd, raw, cwd)))(cmd, cwd)

    res = {k: [0, 0] for k in impls}
    fails = []
    for name, rec in GOLDEN.items():
        raw = bytes.fromhex(rec["canonical_hex"])
        want = format(rec["fingerprint64"] & 0xFFFFFFFFFFFFFFFF, "016x")
        for lang, fn in impls.items():
            got = fn(raw, name)
            if got == want:
                res[lang][0] += 1
            else:
                res[lang][1] += 1
                fails.append(f"{lang}/{name}: want {want} got {got}")

    total = len(GOLDEN)
    print(f"Structural-fingerprint parity gate — {total} vectors (§4 FPC grammar)\n")
    print(f"{'impl':<10}{'pass':>6}{'fail':>6}   status")
    green = True
    for lang, (p, f) in res.items():
        if f:
            green = False
        print(f"{lang:<10}{p:>6}{f:>6}   {'✅ GREEN' if f == 0 else '❌ RED'}")
    if fails:
        print("\n" + "\n".join(fails[:12]))
    print("\n" + ("✅ ALL IMPLEMENTATIONS AGREE ON THE §4 FINGERPRINT" if green else "❌ FINGERPRINT GATE RED (B4 alive)"))
    return 0 if green else 1


if __name__ == "__main__":
    raise SystemExit(main())
