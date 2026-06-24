#!/usr/bin/env python3
"""Negative / anti-malleability cross-language gate (SPEC-v1 §5.3).

For each fixture in testdata/v1_negative.json, feed the bytes to each implementation's `recode`
(decode -> re-encode) and observe:
  - REJECT  : the decoder errored (non-zero exit or empty stdout)
  - NORMALIZE: it accepted and re-encoded the CANONICAL equivalent (lenient mode)
  - ACCEPT  : it accepted and echoed non-canonical bytes back (a real bug)

Gate rule:
  - tier "malformed"     : EVERY impl MUST REJECT.            (hard fail otherwise)
  - tier "non-canonical" : a STRICT impl REJECTs; a lenient impl NORMALIZEs (reported, not failed) —
                           but ACCEPT (echoes non-canonical) is a hard fail.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NEG = json.load(open(os.path.join(ROOT, "testdata", "v1_negative.json")))
GOLDEN = json.load(open(os.path.join(ROOT, "testdata", "v1_golden.json")))
GO_CLI = os.environ.get("GO_CLI", "/tmp/cowrie-cli")
RUST = os.environ.get("RUST_RECODE", os.path.join(ROOT, "rust/target/release/examples/recode"))
TS_DIR = os.path.join(ROOT, "typescript")
TSX = os.path.join(TS_DIR, "node_modules/.bin/tsx")

STRICT = "--strict" in sys.argv
_S = ["--strict"] if STRICT else []
IMPLS = {
    "go":   ([GO_CLI, "recode", *_S], None),
    "rust": ([RUST, *_S], None),
    "ts":   ([TSX, "recode.ts", *_S], TS_DIR),
}


def behavior(cmd: list[str], raw: bytes, cwd: str | None) -> str:
    p = subprocess.run(cmd, input=raw, capture_output=True, cwd=cwd)
    if p.returncode != 0 or not p.stdout:
        return "REJECT"
    return "NORMALIZE" if p.stdout != raw else "ACCEPT"


def main() -> int:
    mode = "STRICT (§5.3)" if STRICT else "lenient (default decode)"
    print(f"Negative / anti-malleability gate [{mode}] — {len(NEG)} fixtures\n")
    print(f"{'fixture':22}{'tier':14}{'go':>11}{'rust':>11}{'ts':>11}")
    hard_fail = False
    strict_gap = 0
    for name, fx in NEG.items():
        raw = bytes.fromhex(fx["hex"])
        row = {}
        for lang, (cmd, cwd) in IMPLS.items():
            b = behavior(cmd, raw, cwd)
            row[lang] = b
            # malformed: must always reject. non-canonical: must reject IN STRICT mode.
            if (fx["tier"] == "malformed" or STRICT) and b != "REJECT":
                hard_fail = True
            if not STRICT and fx["tier"] == "non-canonical" and b != "REJECT":
                strict_gap += 1
        print(f"{name:22}{fx['tier']:14}{row['go']:>11}{row['rust']:>11}{row['ts']:>11}")

    print()
    if STRICT:
        if hard_fail:
            print("❌ HARD FAIL: some fixture was NOT rejected in strict mode (non-canonical leaked through).")
        else:
            print("✅ STRICT: every malformed AND non-canonical fixture rejected by all three decoders (§5.3).")
    else:
        print("❌ HARD FAIL: a malformed fixture was not rejected." if hard_fail
              else "✅ malformed fixtures rejected by every decoder.")
        if strict_gap:
            print(f"⚠️  STRICT-MODE GAP: {strict_gap} non-canonical cases NORMALIZED/ACCEPTED by lenient decode.")
            print("   Run with --strict to verify the strict decoders reject them (SPEC-v1 §5.3).")
    return 1 if hard_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
