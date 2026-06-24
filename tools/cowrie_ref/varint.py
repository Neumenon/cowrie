"""LEB128 varints (SPEC-v1 §2.1).

``uvarint`` — unsigned, minimal, ≤10 bytes. ``svarint`` — zigzag of a 64-bit signed value, then
``uvarint``. Decoders here are *strict*: non-minimal (overlong) or over-length encodings raise
``ERR_INVALID_VARINT`` so canonical identity holds.
"""
from __future__ import annotations

from .errors import CowrieError, ERR_INVALID_VARINT, ERR_TRUNCATED

_MAX_UVARINT_BYTES = 10  # a 64-bit value is at most 10 LEB128 bytes


def encode_uvarint(n: int) -> bytes:
    if n < 0:
        raise ValueError("uvarint requires n >= 0")
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        out.append(byte | 0x80 if n else byte)
        if not n:
            return bytes(out)


def encode_svarint(n: int) -> bytes:
    """Zigzag-encode a value in the int64 range, then uvarint."""
    if not -(2**63) <= n <= 2**63 - 1:
        raise ValueError("svarint requires an int64 value")
    zigzag = (n << 1) ^ (n >> 63)  # arithmetic shift; valid across the int64 range in Python
    return encode_uvarint(zigzag)


def decode_uvarint(data: bytes, pos: int) -> tuple[int, int]:
    """Return ``(value, new_pos)``; raise on truncation, over-length, or non-minimal encoding."""
    result = 0
    shift = 0
    start = pos
    while True:
        if pos - start >= _MAX_UVARINT_BYTES:
            raise CowrieError(ERR_INVALID_VARINT, "varint too long")
        if pos >= len(data):
            raise CowrieError(ERR_TRUNCATED, "varint")
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            # minimality: the final byte must be non-zero unless the value is the single byte 0x00.
            if byte == 0 and pos - start > 1:
                raise CowrieError(ERR_INVALID_VARINT, "overlong varint")
            # 64-bit overflow: minimality does NOT catch a 10-byte form with bits past bit 63.
            if result > 0xFFFFFFFFFFFFFFFF:
                raise CowrieError(ERR_INVALID_VARINT, "uvarint overflows 64 bits")
            return result, pos
        shift += 7


def decode_svarint(data: bytes, pos: int) -> tuple[int, int]:
    zigzag, pos = decode_uvarint(data, pos)
    return (zigzag >> 1) ^ -(zigzag & 1), pos
