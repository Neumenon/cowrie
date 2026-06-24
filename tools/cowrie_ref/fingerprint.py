"""Schema fingerprint (SPEC-v1 §4).

FNV-1a-64 over a per-variant contribution grammar that captures *type structure only*, using the
spec-pinned fingerprint codes (§4.1) — never a host enum/``iota`` (the B4 root bug) and never values.
"""
from __future__ import annotations

from . import model as m
from .varint import encode_uvarint

_FNV_OFFSET = 0xCBF29CE484222325
_FNV_PRIME = 0x100000001B3
_MASK64 = 0xFFFFFFFFFFFFFFFF


def _fnv1a64(data: bytes) -> int:
    h = _FNV_OFFSET
    for byte in data:
        h = ((h ^ byte) * _FNV_PRIME) & _MASK64
    return h


def _fp(value: object) -> bytes:
    if value is None:
        return bytes([m.FPC["null"]])
    if isinstance(value, bool):
        return bytes([m.FPC["bool"]])
    if isinstance(value, int):
        return bytes([m.FPC[m.number_domain(value)]])  # int / uint / bigint by §1.1 domain
    if isinstance(value, float):
        return bytes([m.FPC["float"]])
    if isinstance(value, str):
        return bytes([m.FPC["string"]])
    if isinstance(value, (bytes, bytearray)):
        return bytes([m.FPC["bytes"]])
    if isinstance(value, list):
        return bytes([m.FPC["array"]]) + encode_uvarint(len(value)) + b"".join(_fp(x) for x in value)
    if isinstance(value, dict):
        out = bytearray([m.FPC["object"]])
        out += encode_uvarint(len(value))
        for key in m.sorted_keys(value):
            raw = key.encode("utf-8")
            out += encode_uvarint(len(raw)) + raw + _fp(value[key])
        return bytes(out)
    raise TypeError(f"unsupported value type: {type(value).__name__}")


def fingerprint(value: object) -> tuple[int, int]:
    """Return ``(fingerprint64, fingerprint32)`` for the value's type structure (§4.3)."""
    h64 = _fnv1a64(_fp(value))
    return h64, h64 & 0xFFFFFFFF
