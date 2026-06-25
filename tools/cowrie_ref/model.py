"""Value model + wire tags (SPEC-v1 §1, §2.3).

The JSON-expressible Core maps onto Python natives; the remaining Core variants get small frozen
wrapper types so a value still reads naturally:

    None -> Null   bool -> Bool   int -> Int/Uint/BigInt (by range)   float -> Float
    str  -> String  bytes -> Bytes   list -> Array   dict -> Object
    Decimal128 · Datetime · Uuid · Tensor · Bitmask · Extension  (wrapper types below)
"""
from __future__ import annotations

from dataclasses import dataclass

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
UINT64_MAX = 2**64 - 1

# §2.3 wire tags
T_NULL, T_FALSE, T_TRUE = 0x00, 0x01, 0x02
T_INT, T_FLOAT, T_STRING, T_ARRAY, T_OBJECT = 0x03, 0x04, 0x05, 0x06, 0x07
T_BYTES, T_UINT, T_DECIMAL, T_DATETIME, T_UUID = 0x08, 0x09, 0x0A, 0x0B, 0x0C
T_BIGINT, T_EXTENSION, T_TENSOR, T_BITMASK = 0x0D, 0x0E, 0x20, 0x24
FIXINT, FIXARRAY, FIXMAP, FIXNEG = 0x40, 0xC0, 0xD0, 0xE0  # range bases

MAGIC = b"COWR"
VERSION = 0x01
COMPRESSION_NONE = 0x00
TENSOR_ALIGN = 64  # §2.5: tensor data starts at a 64-byte boundary (canonical, zero-padded, verified)
import struct as _struct
CANONICAL_NAN = _struct.unpack("<d", bytes.fromhex("000000000000f87f"))[0]  # the one canonical quiet NaN (§3)
MAX_RANK = 32       # §2.7
MAX_DEPTH = 1000    # §2.7
# §2.7 size caps (normative; match Go DefaultMax*, Rust gen2 max_*_len, TS Limits.MAX_*_LEN).
# Oversized input is rejected with ERR_TOO_LARGE — the cross-language token the others surface.
MAX_ARRAY = 1_000_000   # §2.7 MaxArray: array element count
MAX_OBJECT = 1_000_000  # §2.7 MaxObject: object field count
MAX_DICT = 1_000_000    # §2.7 MaxDict: header dictionary entry count
MAX_STRING = 10_000_000  # §2.7 MaxString: 10MB (decoded UTF-8 byte length)
MAX_BYTES = 50_000_000   # §2.7 MaxBytes: 50MB
MAX_EXT = 1_000_000      # §2.7 MaxExt: 1MB extension payload

# §2.5 DType -> (wire value, bytes_per_elem | None for sub-byte) ; sub-byte handled separately
DTYPE = {
    "float32": 0x01, "float16": 0x02, "bfloat16": 0x03, "int8": 0x04, "int16": 0x05,
    "int32": 0x06, "int64": 0x07, "uint8": 0x08, "uint16": 0x09, "uint32": 0x0A,
    "uint64": 0x0B, "float64": 0x0C, "bool": 0x0D,
    "qint4": 0x10, "qint2": 0x11, "qint3": 0x12, "ternary": 0x13, "binary": 0x14,
}
DTYPE_NAME = {v: k for k, v in DTYPE.items()}
DTYPE_BITS = {
    0x01: 32, 0x02: 16, 0x03: 16, 0x04: 8, 0x05: 16, 0x06: 32, 0x07: 64, 0x08: 8,
    0x09: 16, 0x0A: 32, 0x0B: 64, 0x0C: 64, 0x0D: 8, 0x10: 4, 0x11: 2, 0x12: 3, 0x13: 2, 0x14: 1,
}

# §4.1 fingerprint codes (a namespace distinct from wire tags)
FPC = {
    "null": 0x00, "bool": 0x01, "int": 0x02, "uint": 0x03, "bigint": 0x04, "float": 0x05,
    "decimal": 0x06, "string": 0x07, "bytes": 0x08, "uuid": 0x09, "datetime": 0x0A,
    "array": 0x0B, "object": 0x0C, "tensor": 0x0D, "bitmask": 0x0E, "extension": 0x0F,
}


# ---- wrapper value types (frozen so they hash/compare by value) ----
@dataclass(frozen=True)
class Decimal128:
    coefficient: int  # signed, |coeff| < 2**127
    scale: int        # svarint

    def canonical(self) -> "Decimal128":
        """Lowest terms: strip trailing-zero coefficient into a smaller scale (§3)."""
        c, s = self.coefficient, self.scale
        if c == 0:
            return Decimal128(0, 0)
        while c % 10 == 0:
            c //= 10
            s -= 1
        return Decimal128(c, s)


@dataclass(frozen=True)
class Datetime:
    nanos: int  # int64 ns since Unix epoch UTC


@dataclass(frozen=True)
class Uuid:
    value: bytes  # exactly 16 bytes, RFC-4122 field order


@dataclass(frozen=True)
class Tensor:
    dtype: int                # a DTYPE wire value
    shape: tuple[int, ...]
    data: bytes


@dataclass(frozen=True)
class Bitmask:
    bits: tuple[bool, ...]


@dataclass(frozen=True)
class Extension:
    ext_type: int
    payload: bytes


def number_domain(n: int) -> str:
    """Which §1.1 number variant an int belongs to (drives encode tag and fingerprint)."""
    if INT64_MIN <= n <= INT64_MAX:
        return "int"
    if INT64_MAX < n <= UINT64_MAX:
        return "uint"
    return "bigint"


def sorted_keys(obj: dict) -> list[str]:
    """Object keys in canonical order: ascending by raw UTF-8 bytes (§2.4)."""
    return sorted(obj, key=lambda k: k.encode("utf-8"))


def min_twos_complement_le(n: int) -> bytes:
    """Minimal-length little-endian two's-complement bytes for a signed int (§2.3 BigInt)."""
    length = max(1, (n.bit_length() + 8) // 8)
    while True:
        try:
            return n.to_bytes(length, "little", signed=True)
        except OverflowError:
            length += 1
