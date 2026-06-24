"""Cowrie v1 — clean-room reference implementation.

Written from ``docs/SPEC-v1.md`` alone (no reference to the Go/Rust/TS impls, which diverge). The
spec is the oracle: this package *defines* the canonical bytes + fingerprints that conformant
implementations must reproduce, and it generates the golden vector suite.

    >>> from cowrie_ref import encode, decode, fingerprint
    >>> blob = encode({"a": 1})
    >>> decode(blob)
    {'a': 1}
    >>> fingerprint({"a": 1})[0]
    9141471042555069906
"""
from __future__ import annotations

from .decode import decode
from .encode import encode, encode_value
from .errors import CowrieError
from .fingerprint import fingerprint
from .model import Bitmask, Decimal128, Datetime, Extension, Tensor, Uuid

__all__ = [
    "encode", "encode_value", "decode", "fingerprint", "CowrieError", "roundtrip_ok", "value_eq",
    "Bitmask", "Decimal128", "Datetime", "Extension", "Tensor", "Uuid",
]
__version__ = "0.1.0"


def value_eq(a: object, b: object) -> bool:
    """Semantic equality (§1 domains), NaN-aware and structural."""
    import math
    if isinstance(a, float) and isinstance(b, float):
        return a == b or (math.isnan(a) and math.isnan(b))
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(value_eq(x, y) for x, y in zip(a, b))
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(value_eq(a[k], b[k]) for k in a)
    return a == b


def roundtrip_ok(value: object) -> bool:
    """Bijectivity check (§3): ``encode`` is stable through a strict decode."""
    blob = encode(value)
    return encode(decode(blob)) == blob and value_eq(decode(blob), value)
