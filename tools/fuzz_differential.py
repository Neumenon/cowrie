#!/usr/bin/env python3
"""Generative differential fuzzer (SPEC-v1 cross-language determinism).

The Python reference generates random canonical values, encodes them, and every other
implementation must recode (decode -> re-encode) to byte-identical output. This stresses the
canonical encoders on inputs the curated golden vectors don't cover: random integer magnitudes
(FIXINT/Int/Uint/BigInt boundaries), unicode object keys (global byte-sort), deep nesting,
floats, and the Core wrapper types.

Usage: python3 tools/fuzz_differential.py [N] [--seed S]
Exit non-zero on the first cross-language byte divergence (printed with a repro seed).
"""
from __future__ import annotations

import json
import os
import random
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import cowrie_ref as c  # noqa: E402
from cowrie_ref import Bitmask, Datetime, Decimal128, Tensor, Uuid  # noqa: E402

GO = [os.environ.get("GO_CLI", "/tmp/cowrie-cli"), "recode"]
RUST = [os.environ.get("RUST_RECODE", os.path.join(ROOT, "rust/target/release/examples/recode"))]
TS_DIR = os.path.join(ROOT, "typescript")
TS = [os.path.join(TS_DIR, "node_modules/.bin/tsx"), "recode.ts"]

# A pool of unicode-ish key fragments to exercise UTF-8 byte-sorting of the dictionary.
KEYCHARS = "abcAZ09_é\U0001F600ÿ中"


def rand_key(rng: random.Random) -> str:
    return "".join(rng.choice(KEYCHARS) for _ in range(rng.randint(0, 5)))


def rand_int(rng: random.Random) -> int:
    # span FIXINT/FIXNEG, Int64, Uint64, and BigInt ranges
    bucket = rng.randint(0, 4)
    if bucket == 0:
        return rng.randint(-16, 127)
    if bucket == 1:
        return rng.randint(-(2**63), 2**63 - 1)
    if bucket == 2:
        return rng.randint(2**63, 2**64 - 1)
    if bucket == 3:
        return rng.randint(2**64, 2**80)
    return -rng.randint(2**63 + 1, 2**80)


def rand_value(rng: random.Random, depth: int):
    leaves = ["null", "bool", "int", "float", "str", "bytes", "decimal", "datetime", "uuid", "bitmask"]
    branches = ["array", "object"]
    kinds = leaves + (branches if depth > 0 else [])
    k = rng.choice(kinds)
    if k == "null":
        return None
    if k == "bool":
        return rng.random() < 0.5
    if k == "int":
        return rand_int(rng)
    if k == "float":
        return rng.choice([rng.uniform(-1e6, 1e6), 0.0, float("inf"), float("-inf"), float("nan"),
                           rng.uniform(-1e300, 1e300)])
    if k == "str":
        return "".join(rng.choice(KEYCHARS) for _ in range(rng.randint(0, 8)))
    if k == "bytes":
        return bytes(rng.randint(0, 255) for _ in range(rng.randint(0, 8)))
    if k == "decimal":
        return Decimal128(rng.randint(-(2**80), 2**80), rng.randint(-40, 40))
    if k == "datetime":
        return Datetime(rng.randint(-(2**63), 2**63 - 1))
    if k == "uuid":
        return Uuid(bytes(rng.randint(0, 255) for _ in range(16)))
    if k == "bitmask":
        return Bitmask(tuple(rng.random() < 0.5 for _ in range(rng.randint(0, 17))))
    if k == "array":
        return [rand_value(rng, depth - 1) for _ in range(rng.randint(0, 5))]
    # object — random (possibly unicode) keys, deduped
    return {rand_key(rng): rand_value(rng, depth - 1) for _ in range(rng.randint(0, 5))}


def recode(cmd, raw, cwd=None) -> bytes:
    return subprocess.run(cmd, input=raw, capture_output=True, cwd=cwd).stdout


def main() -> int:
    n = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 2000
    seed = 1234
    if "--seed" in sys.argv:
        seed = int(sys.argv[sys.argv.index("--seed") + 1])
    rng = random.Random(seed)
    print(f"Differential fuzz: {n} random values, seed={seed}\n")

    diverged = 0
    for i in range(n):
        v = rand_value(rng, depth=rng.randint(0, 4))
        try:
            blob = c.encode(v)
        except Exception as exc:  # noqa: BLE001
            print(f"  [{i}] reference encode FAILED on {v!r}: {exc}")
            diverged += 1
            continue
        exp = blob.hex()
        for lang, cmd, cwd in (("go", GO, None), ("rust", RUST, None), ("ts", TS, TS_DIR)):
            got = recode(cmd, blob, cwd).hex()
            if got != exp:
                diverged += 1
                print(f"  [{i}] DIVERGE {lang}: value={v!r}\n     exp {exp}\n     got {got or '(empty/err)'}")
                if diverged >= 10:
                    print("\n(stopping after 10 divergences)")
                    return 1
    if diverged == 0:
        print(f"✅ {n} random values — Go/Rust/TS all byte-identical to the Python reference. No divergence.")
        return 0
    print(f"\n❌ {diverged} divergence(s) found.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
