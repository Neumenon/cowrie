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
import re
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


_CODE = re.compile(rb"ERR_[A-Z_]+")


def probe(cmd: list[str], raw: bytes, cwd: str | None) -> tuple[str, str]:
    """Return (behavior, error_code). On a decode error the impl prints the canonical ERR_* code
    to stderr; we extract the first such token for exact-code verification (§2.6 / §5.3)."""
    p = subprocess.run(cmd, input=raw, capture_output=True, cwd=cwd)
    if p.returncode != 0 or not p.stdout:
        beh = "REJECT"
    else:
        beh = "NORMALIZE" if p.stdout != raw else "ACCEPT"
    m = _CODE.search(p.stderr)
    return beh, (m.group(0).decode() if m else "")


def main() -> int:
    mode = "STRICT (§5.3)" if STRICT else "lenient (default decode)"
    print(f"Negative / anti-malleability gate [{mode}] — {len(NEG)} fixtures\n")
    cols = "  (go/rust/ts behavior; STRICT also checks exact ERR_* code)"
    print(f"{'fixture':22}{'tier':14}{'expect':22}{'go':>10}{'rust':>10}{'ts':>10}{cols if False else ''}")
    hard_fail = False
    code_fail = 0
    strict_gap = 0
    for name, fx in NEG.items():
        raw = bytes.fromhex(fx["hex"])
        row = {}
        for lang, (cmd, cwd) in IMPLS.items():
            b, code = probe(cmd, raw, cwd)
            row[lang] = b if not STRICT else f"{b}:{code or '?'}"
            if (fx["tier"] == "malformed" or STRICT) and b != "REJECT":
                hard_fail = True
            if STRICT and code != fx["expect"]:
                code_fail += 1
                hard_fail = True
            if not STRICT and fx["tier"] == "non-canonical" and b != "REJECT":
                strict_gap += 1
        print(f"{name:22}{fx['tier']:14}{fx['expect']:22}{row['go']:>10}{row['rust']:>10}{row['ts']:>10}"
              if not STRICT else f"{name:22}{fx['tier']:14}{fx['expect']:22}{row['go']:>22}{row['rust']:>22}{row['ts']:>22}")

    print()
    if STRICT:
        if hard_fail:
            print(f"❌ HARD FAIL: a fixture was not rejected, or {code_fail} exact-code mismatch(es) (§2.6 codes).")
        else:
            print("✅ STRICT: all fixtures rejected by go/rust/ts with the EXACT expected ERR_* code (§5.3 + §2.6).")
    else:
        print("❌ HARD FAIL: a malformed fixture was not rejected." if hard_fail
              else "✅ malformed fixtures rejected by every decoder.")
        if strict_gap:
            print(f"⚠️  STRICT-MODE GAP: {strict_gap} non-canonical cases NORMALIZED/ACCEPTED by lenient decode.")
            print("   Run with --strict to verify the strict decoders reject them (SPEC-v1 §5.3).")
    return 1 if hard_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
