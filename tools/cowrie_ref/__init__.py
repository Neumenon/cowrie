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

from .decode import decode, tensor_spans
from .encode import encode, encode_value
from .errors import CowrieError
from .fingerprint import fingerprint
from .model import Bitmask, Decimal128, Datetime, Extension, Tensor, Uuid

# §7 file / dataset identity — part of the public v1 surface (re-exported lazily below to avoid an
# import cycle: ``file`` and ``profiles`` import ``encode`` from this package at module load).
__all__ = [
    "encode", "encode_value", "decode", "fingerprint", "CowrieError", "roundtrip_ok", "value_eq",
    "content_address", "address_of_bytes", "tensor_spans",
    "Bitmask", "Decimal128", "Datetime", "Extension", "Tensor", "Uuid",
    # §7 file container + dataset/stream identity
    "encode_file", "decode_file", "file_identity", "merkle_root", "dataset_root",
]
__version__ = "0.1.0"


def __getattr__(name: str):  # PEP 562 lazy re-export of the §7 helpers (breaks the import cycle)
    if name in ("encode_file", "decode_file", "file_identity", "merkle_root"):
        from . import file as _file
        return getattr(_file, name)
    if name == "dataset_root":
        from . import profiles as _profiles
        return _profiles.dataset_root
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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


def address_of_bytes(canonical: bytes) -> bytes:
    """Content address (§3) of already-canonical, uncompressed wire bytes: a **multihash** SHA-256
    — ``b'\\x12\\x20'`` (sha2-256, 32-byte length) followed by the 32 digest bytes."""
    import hashlib
    return b"\x12\x20" + hashlib.sha256(canonical).digest()


def content_address(value: object) -> bytes:
    """Content address (§3) of a value: encode it canonically, then ``address_of_bytes``. This is the
    v1 per-value identity — equal canonical bytes ⇒ equal address, across every conforming impl.
    (A Bytes value is itself a Python ``bytes``, so we MUST encode, never hash the value directly.)"""
    return address_of_bytes(encode(value))


def roundtrip_ok(value: object) -> bool:
    """Bijectivity check (§3): ``encode`` is stable through a strict decode."""
    blob = encode(value)
    return encode(decode(blob)) == blob and value_eq(decode(blob), value)
