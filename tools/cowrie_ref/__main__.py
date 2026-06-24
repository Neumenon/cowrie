"""CLI:

    python -m cowrie_ref gen          # (re)generate testdata/v1_golden.json and self-check
    python -m cowrie_ref fp '<json>'  # canonical hex + fingerprint of a JSON value
"""
from __future__ import annotations

import json
import os
import sys

from . import decode, encode, fingerprint, roundtrip_ok

# JSON-expressible golden cases (exercise every canonical edge in the subset).
CASES: dict[str, object] = {
    "null": None, "true": True, "false": False,
    "int_0": 0, "int_1": 1, "int_127": 127, "int_128": 128, "int_neg1": -1,
    "int_neg16": -16, "int_neg17": -17, "uint_big": 2**63 + 5,
    "float_1_5": 1.5, "float_zero": 0.0,
    "str_hi": "hi", "str_unicode": "héllo",
    "arr_123": [1, 2, 3], "arr_empty": [], "obj_empty": {},
    "obj_a1": {"a": 1}, "obj_ba": {"b": 1, "a": 2},
    "nested": {"x": [1, 2], "y": {"z": 3}},
}

_GOLDEN = os.path.join(os.path.dirname(__file__), "..", "..", "testdata", "v1_golden.json")


def _gen() -> int:
    out: dict[str, object] = {}
    for name, value in CASES.items():
        blob = encode(value)
        assert roundtrip_ok(value), f"round-trip failed: {name}"
        assert decode(blob) == value, f"value mismatch: {name}"
        h64, h32 = fingerprint(value)
        out[name] = {"value": value, "canonical_hex": blob.hex(), "fingerprint64": h64, "fingerprint32": h32}
        print(f"{name:14} {blob.hex():<52} fp64={h64:016x}")
    with open(os.path.normpath(_GOLDEN), "w") as fh:
        json.dump(out, fh, indent=2)
    print(f"\nwrote {os.path.normpath(_GOLDEN)}  ({len(out)} vectors, all round-trip-verified)")
    return 0


def _fp(arg: str) -> int:
    value = json.loads(arg)
    blob = encode(value)
    h64, h32 = fingerprint(value)
    print(f"canonical : {blob.hex()}")
    print(f"fp64      : {h64:#018x}")
    print(f"roundtrip : {'ok' if roundtrip_ok(value) else 'FAIL'}")
    return 0


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[1] == "gen":
        return _gen()
    if len(argv) >= 3 and argv[1] == "fp":
        return _fp(argv[2])
    print(__doc__)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
