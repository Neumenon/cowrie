#!/usr/bin/env python3
"""Content-address parity gate (SPEC-v1 §3).

The content address is a multihash SHA-256 of a value's canonical wire bytes — the v1 per-value
identity. Every implementation must compute the SAME address for the same value, and it must match
the golden addresses derived from the Python reference.

For each golden vector: feed its canonical bytes to each impl's `recode --addr` (decode -> canonical
re-encode -> multihash SHA-256 -> hex) and compare to testdata/v1_content_addresses.json.

Run: python3 tools/content_address_gate.py   (exit non-zero on any mismatch)
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
ADDR = json.load(open(os.path.join(ROOT, "testdata", "v1_content_addresses.json")))
GO_CLI = os.environ.get("GO_CLI", "/tmp/cowrie-cli")
RUST = os.environ.get("RUST_RECODE", os.path.join(ROOT, "rust/target/release/examples/recode"))
TS_DIR = os.path.join(ROOT, "typescript")
TSX = os.path.join(TS_DIR, "node_modules/.bin/tsx")


def addr_via(cmd, raw, cwd=None) -> str:
    out = subprocess.run(cmd, input=raw, capture_output=True, cwd=cwd).stdout
    return out.decode("ascii", "replace").strip()


def main() -> int:
    impls = {
        "python": lambda raw, name: c.address_of_bytes(raw).hex(),
        "go":     lambda raw, name: addr_via([GO_CLI, "recode", "--addr"], raw),
        "rust":   lambda raw, name: addr_via([RUST, "--addr"], raw),
        "ts":     lambda raw, name: addr_via([TSX, "recode.ts", "--addr"], raw, TS_DIR),
    }
    res = {k: [0, 0] for k in impls}
    fails = []
    for name, rec in GOLDEN.items():
        raw = bytes.fromhex(rec["canonical_hex"])
        want = ADDR[name]
        for lang, fn in impls.items():
            try:
                got = fn(raw, name)
            except Exception as exc:  # noqa: BLE001
                got = f"<error: {exc}>"
            if got == want:
                res[lang][0] += 1
            else:
                res[lang][1] += 1
                fails.append(f"{lang}/{name}: want {want[:18]}… got {got[:18]}…")

    total = len(GOLDEN)
    print(f"Content-address parity gate — {total} vectors (§3 multihash SHA-256)\n")
    print(f"{'impl':<10}{'pass':>6}{'fail':>6}   status")
    green = True
    for lang, (p, f) in res.items():
        if f:
            green = False
        print(f"{lang:<10}{p:>6}{f:>6}   {'✅ GREEN' if f == 0 else '❌ RED'}")
    if fails:
        print("\n" + "\n".join(fails[:12]))
    print("\n" + ("✅ ALL IMPLEMENTATIONS AGREE ON CONTENT ADDRESSES" if green else "❌ CONTENT-ADDRESS GATE RED"))
    return 0 if green else 1


if __name__ == "__main__":
    raise SystemExit(main())
