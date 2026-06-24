"""Value model + wire tags (SPEC-v1 §1, §2.3).

The JSON-expressible Core maps onto Python natives, so values read naturally:

    None -> Null   bool -> Bool   int -> Int/Uint/BigInt (by range)   float -> Float
    str  -> String  bytes -> Bytes   list -> Array   dict -> Object

(Tensor / Bitmask / Decimal128 / UUID / Datetime / Extension get explicit wrapper types when this
reference grows beyond the JSON subset.)
"""
from __future__ import annotations

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
UINT64_MAX = 2**64 - 1

# §2.3 wire tags (subset)
T_NULL, T_FALSE, T_TRUE = 0x00, 0x01, 0x02
T_INT, T_FLOAT, T_STRING, T_ARRAY, T_OBJECT, T_BYTES, T_UINT = 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09
FIXINT, FIXARRAY, FIXMAP, FIXNEG = 0x40, 0xC0, 0xD0, 0xE0  # range bases

MAGIC = b"COWR"
VERSION = 0x01
COMPRESSION_NONE = 0x00

# §4.1 fingerprint codes (a namespace distinct from wire tags)
FPC = {
    "null": 0x00, "bool": 0x01, "int": 0x02, "uint": 0x03, "bigint": 0x04, "float": 0x05,
    "decimal": 0x06, "string": 0x07, "bytes": 0x08, "uuid": 0x09, "datetime": 0x0A,
    "array": 0x0B, "object": 0x0C, "tensor": 0x0D, "bitmask": 0x0E, "extension": 0x0F,
}


def number_domain(n: int) -> str:
    """Which §1.1 number variant an int belongs to (drives both encode tag and fingerprint)."""
    if INT64_MIN <= n <= INT64_MAX:
        return "int"
    if INT64_MAX < n <= UINT64_MAX:
        return "uint"
    return "bigint"


def sorted_keys(obj: dict) -> list[str]:
    """Object keys in canonical order: ascending by raw UTF-8 bytes (§2.4)."""
    return sorted(obj, key=lambda k: k.encode("utf-8"))
