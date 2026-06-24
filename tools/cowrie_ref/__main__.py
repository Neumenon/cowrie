"""CLI:

    python -m cowrie_ref gen          # (re)generate testdata/v1_golden.json and self-check
    python -m cowrie_ref fp '<json>'  # canonical hex + fingerprint of a JSON value
"""
from __future__ import annotations

import json
import os
import struct
import sys

from . import Bitmask, Datetime, Decimal128, Extension, Tensor, Uuid, decode, encode, fingerprint, roundtrip_ok, value_eq

# Golden cases — exercise every canonical edge across the full Core.
CASES: dict[str, object] = {
    "null": None, "true": True, "false": False,
    "int_0": 0, "int_1": 1, "int_127": 127, "int_128": 128, "int_neg1": -1,
    "int_neg16": -16, "int_neg17": -17, "uint_big": 2**63 + 5,
    "bigint_pos": 2**70, "bigint_neg": -(2**70),
    "float_1_5": 1.5, "float_zero": 0.0,
    "str_hi": "hi", "str_unicode": "héllo", "bytes_3": b"\x01\x02\x03",
    "decimal_12345_2": Decimal128(12345, 2), "decimal_zero": Decimal128(0, 0),
    "datetime": Datetime(1_700_000_000_000_000_000),
    "uuid": Uuid(bytes.fromhex("00112233445566778899aabbccddeeff")),
    "tensor_f32_2x3": Tensor(0x01, (2, 3), b"\x00" * 24),
    "tensor_scalar_f64": Tensor(0x0C, (), struct.pack("<d", 1.0)),
    "bitmask_101": Bitmask((True, False, True)),
    "extension": Extension(7, b"\xde\xad"),
    "arr_123": [1, 2, 3], "arr_empty": [], "obj_empty": {},
    "obj_a1": {"a": 1}, "obj_ba": {"b": 1, "a": 2},
    "nested": {"x": [1, 2], "y": {"z": 3}},
    # --- boundary / edge cases (coverage expansion) ---
    "int64_min": -(2**63), "int64_max": 2**63 - 1, "uint64_max": 2**64 - 1,
    "bigint_above_uint64": 2**64, "bigint_below_int64": -(2**63) - 1,
    "float_neg": -1.5, "float_nan": float("nan"), "float_inf": float("inf"),
    "float_neg_inf": float("-inf"), "float_subnormal": 5e-324, "float_max": 1.7976931348623157e308,
    "str_empty": "", "str_4byte": "\U00010000", "bytes_empty": b"",
    "decimal_neg_one": Decimal128(-1, 0), "decimal_neg_scale": Decimal128(123, -2),
    "decimal_big_scale": Decimal128(3, 64), "decimal_big_negscale": Decimal128(7, -100),
    "decimal_neg_coeff": Decimal128(-12345, 2), "decimal_max_coeff": Decimal128(2**127 - 1, 0),
    "datetime_epoch": Datetime(0), "datetime_neg": Datetime(-1),
    "datetime_max": Datetime(2**63 - 1), "datetime_min": Datetime(-(2**63)),
    "arr_15": list(range(15)), "arr_16": list(range(16)),
    "obj_15": {f"k{i:02d}": i for i in range(15)}, "obj_16": {f"k{i:02d}": i for i in range(16)},
    "tensor_int8_3": Tensor(0x04, (3,), b"\x01\x02\x03"),
    "tensor_bool_4": Tensor(0x0D, (4,), b"\x01\x00\x01\x00"),
    "tensor_empty_dim": Tensor(0x04, (0, 3), b""),
    "tensor_binary_9": Tensor(0x14, (9,), b"\xff\x01"),
    "tensor_qint4_3": Tensor(0x10, (3,), b"\x21\x03"),
    "bitmask_0": Bitmask(()), "bitmask_1": Bitmask((True,)),
    "bitmask_8": Bitmask((True,) * 8), "bitmask_9": Bitmask((True,) * 9),
    "dfs_global_sort": {"z": {"a": 1}},
    "tensors_nested": {"m": Tensor(0x04, (2,), b"\x01\x02"),
                       "n": [Tensor(0x01, (1,), b"\x00\x00\x80\x3f")]},
}


def _safe(value: object) -> object:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return repr(value)

_GOLDEN = os.path.join(os.path.dirname(__file__), "..", "..", "testdata", "v1_golden.json")


def _gen() -> int:
    out: dict[str, object] = {}
    for name, value in CASES.items():
        blob = encode(value)
        assert roundtrip_ok(value), f"round-trip failed: {name}"
        assert value_eq(decode(blob), value), f"value mismatch: {name}"
        h64, h32 = fingerprint(value)
        out[name] = {"value": _safe(value), "canonical_hex": blob.hex(), "fingerprint64": h64, "fingerprint32": h32}
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
