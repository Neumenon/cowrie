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

__all__ = ["encode", "encode_value", "decode", "fingerprint", "CowrieError", "roundtrip_ok"]
__version__ = "0.1.0"


def roundtrip_ok(value: object) -> bool:
    """Bijectivity check (§3): ``encode`` is stable through a strict decode."""
    blob = encode(value)
    return encode(decode(blob)) == blob and decode(blob) == value
