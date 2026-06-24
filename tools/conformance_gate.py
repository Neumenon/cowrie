#!/usr/bin/env python3
"""Cross-language conformance gate (SPEC-v1 §5).

Every implementation must reproduce the golden vectors from `testdata/v1_golden.json`:
  - encode (Python reference): the oracle that generated the goldens.
  - recode (Go/Rust/TS): decode the golden bytes and re-encode canonical bytes byte-identically.

Build the recode tools first (or let --build do it):
  cd go      && go build -o /tmp/cowrie-cli ./cmd/cowrie
  cd rust    && cargo build --release --example recode
  (TS runs via typescript/node_modules/.bin/tsx)

Run: python3 tools/conformance_gate.py [--build]
Exit non-zero if any cell fails.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLDEN = os.path.join(ROOT, "testdata", "v1_golden.json")
GO_CLI = os.environ.get("GO_CLI", "/tmp/cowrie-cli")
RUST_RECODE = os.environ.get("RUST_RECODE", os.path.join(ROOT, "rust/target/release/examples/recode"))
TS_DIR = os.path.join(ROOT, "typescript")
TSX = os.path.join(TS_DIR, "node_modules/.bin/tsx")


def build() -> None:
    subprocess.run(["go", "build", "-o", GO_CLI, "./cmd/cowrie"], cwd=os.path.join(ROOT, "go"), check=True)
    subprocess.run(["cargo", "build", "--release", "--example", "recode"], cwd=os.path.join(ROOT, "rust"), check=True)


def recode(cmd: list[str], raw: bytes, cwd: str | None = None) -> bytes:
    return subprocess.run(cmd, input=raw, capture_output=True, cwd=cwd).stdout


def main() -> int:
    if "--build" in sys.argv:
        build()
    golden = json.load(open(GOLDEN))

    # Python reference: re-derive each vector and check it matches (the oracle).
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    import cowrie_ref  # noqa: E402
    from cowrie_ref.__main__ import CASES  # noqa: E402

    impls = {
        "python": lambda name, raw: cowrie_ref.encode(CASES[name]),
        "go":     lambda name, raw: recode([GO_CLI, "recode"], raw),
        "rust":   lambda name, raw: recode([RUST_RECODE], raw),
        "ts":     lambda name, raw: recode([TSX, "recode.ts"], raw, cwd=TS_DIR),
    }

    results: dict[str, list[int]] = {k: [0, 0] for k in impls}  # [pass, fail]
    failures: list[str] = []
    for name, rec in golden.items():
        raw = bytes.fromhex(rec["canonical_hex"])
        for lang, fn in impls.items():
            try:
                got = fn(name, raw).hex()
            except Exception as exc:  # noqa: BLE001
                got = f"<error: {exc}>"
            if got == rec["canonical_hex"]:
                results[lang][0] += 1
            else:
                results[lang][1] += 1
                failures.append(f"{lang}/{name}")

    total = len(golden)
    print(f"Cowrie v1 conformance gate — {total} golden vectors\n")
    print(f"{'impl':<10} {'pass':>5} {'fail':>5}   status")
    all_green = True
    for lang, (p, f) in results.items():
        status = "✅ GREEN" if f == 0 else "❌ RED"
        if f:
            all_green = False
        print(f"{lang:<10} {p:>5} {f:>5}   {status}")
    if failures:
        print("\nfailures:", ", ".join(failures[:20]))
    print("\n" + ("✅ ALL IMPLEMENTATIONS CONFORM" if all_green else "❌ GATE RED"))
    return 0 if all_green else 1


if __name__ == "__main__":
    raise SystemExit(main())
