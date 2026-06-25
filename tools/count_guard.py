#!/usr/bin/env python3
"""Monotonic conformance-count guard (1.0 freeze gate).

Conformance coverage must only ever GROW. This pins a floor under every corpus so a regression that
silently deletes vectors/fixtures (which would make a green gate meaningless) fails CI. Raising a floor
is intentional and allowed; lowering one is the bug this catches.

Run: python3 tools/count_guard.py   (exit non-zero if any corpus dropped below its floor)
"""
from __future__ import annotations

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Floors pinned at the v1.0-freeze counts. Bump these up when coverage grows; never down.
FLOOR = {
    "testdata/v1_golden.json": 73,            # positive conformance vectors (full Core + boundaries)
    "testdata/v1_negative.json": 27,          # anti-malleability fixtures (8 malformed + 13 non-canonical + invalid-utf8)
    "testdata/v1_content_addresses.json": 73, # §3 content addresses
    "testdata/v1_tensor_spans.json": 8,       # §2.5 tensor zero-copy spans
    "testdata/v1_files.json": 7,              # §7 file/Merkle fixtures
}


def main() -> int:
    bad = []
    print("Monotonic conformance-count guard (1.0):")
    for rel, floor in FLOOR.items():
        n = len(json.load(open(os.path.join(ROOT, rel))))
        ok = n >= floor
        print(f"  {rel:38} {n:>4}  (floor {floor}) {'ok' if ok else 'SHRANK ❌'}")
        if not ok:
            bad.append(f"{rel}: {n} < floor {floor}")
    if bad:
        print("\n❌ COVERAGE SHRANK — conformance corpus must only grow:\n   " + "\n   ".join(bad))
        return 1
    print("\n✅ coverage holds at or above the frozen floor.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
