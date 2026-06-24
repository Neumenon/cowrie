#!/usr/bin/env python3
"""Generate the negative / anti-malleability fixture suite (SPEC-v1 §5.3) and verify the Python
reference rejects each one in strict mode with the expected error code.

Two tiers:
  - "malformed": structurally invalid; EVERY decoder (strict or lenient) MUST reject.
  - "non-canonical": well-formed but not canonical; STRICT decoders MUST reject, lenient MAY accept
    (and normalize). The cross-language negative gate uses the tier to know what to assert.

Writes testdata/v1_negative.json: {name: {hex, expect, tier}}.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cowrie_ref as c  # noqa: E402

H = bytes.fromhex("434f57520100")  # COWR + version 1 + compression 0
D0 = H + b"\x00"  # ... + DictLen 0


def hx(b: bytes) -> str:
    return b.hex()


# (name, bytes, expected_error, tier)
NEGATIVES: list[tuple[str, bytes, str, str]] = [
    # --- malformed: all decoders must reject ---
    ("bad_magic",        bytes.fromhex("58585858010000") + b"\x00", "ERR_INVALID_MAGIC",   "malformed"),
    ("bad_version",      bytes.fromhex("434f5752020000") + b"\x00", "ERR_INVALID_VERSION", "malformed"),
    ("truncated_string", D0 + b"\x05",                              "ERR_TRUNCATED",       "malformed"),
    ("reserved_tag_0f",  D0 + b"\x0f",                              "ERR_RESERVED_TAG",    "malformed"),
    ("varint_overflow",  D0 + b"\x05" + bytes([0x80] * 9 + [0x02]), "ERR_INVALID_VARINT",  "malformed"),
    ("dictidx_oob",      H + b"\x01\x01a" + b"\xd1\x05\x41",        "ERR_INVALID_FIELD_ID","malformed"),
    ("trailing_data",    D0 + b"\x00\x00",                          "ERR_TRAILING_DATA",   "malformed"),
    # --- non-canonical: strict rejects, lenient may normalize ---
    ("int_not_fixint",   D0 + b"\x03\x02",                          "ERR_NON_CANONICAL",   "non-canonical"),  # Int(1)
    ("uint_fits_int",    D0 + b"\x09\x05",                          "ERR_NON_CANONICAL",   "non-canonical"),  # Uint(5)
    ("bigint_small",     D0 + b"\x0d\x01\x05",                      "ERR_NON_CANONICAL",   "non-canonical"),  # BigInt(5)
    ("array_not_fixarr", D0 + b"\x06\x02\x41\x42",                  "ERR_NON_CANONICAL",   "non-canonical"),  # Array(2)[1,2]
    ("neg_zero",         D0 + b"\x04" + bytes.fromhex("0000000000000080"), "ERR_NON_CANONICAL", "non-canonical"),
    ("noncanonical_nan", D0 + b"\x04" + bytes.fromhex("010000000000f87f"), "ERR_NON_CANONICAL", "non-canonical"),
    ("unsorted_dict",    H + b"\x02\x01z\x01a" + b"\xd1\x00\x41",   "ERR_NON_CANONICAL",   "non-canonical"),  # dict [z,a]
    ("nonascending_idx", H + b"\x01\x01a" + b"\xd2\x00\x41\x00\x42","ERR_NON_CANONICAL",   "non-canonical"),  # idx 0 twice
    ("bitmask_trailing", D0 + b"\x24\x03\x0d",                      "ERR_NON_CANONICAL",   "non-canonical"),  # count 3, bit 3 set
]


def main() -> int:
    out: dict[str, dict] = {}
    for name, blob, expect, tier in NEGATIVES:
        # 1. strict MUST reject with the expected code
        try:
            c.decode(blob, strict=True)
            print(f"FAIL: {name} was ACCEPTED in strict mode (expected {expect})")
            return 1
        except c.CowrieError as e:
            if e.code != expect:
                print(f"FAIL: {name} strict raised {e.code}, expected {expect}")
                return 1
        # 2. non-canonical fixtures must be accepted leniently (proves they are well-formed)
        if tier == "non-canonical":
            try:
                c.decode(blob, strict=False)
            except c.CowrieError as e:
                print(f"FAIL: {name} (non-canonical) rejected even leniently: {e.code}")
                return 1
        out[name] = {"hex": hx(blob), "expect": expect, "tier": tier}
        print(f"ok  {name:20} {tier:14} -> {expect}")
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "testdata", "v1_negative.json")
    json.dump(out, open(path, "w"), indent=2)
    print(f"\nwrote {path} ({len(out)} negative fixtures, all verified against the Python reference)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
