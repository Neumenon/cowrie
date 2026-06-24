"""Canonical encoder (SPEC-v1 §2, §3).

Produces the one canonical byte-string for a value: smallest int/array/map form, byte-sorted
dictionary, dict-coded object fields. ``encode`` emits a full headered stream; ``encode_value``
emits just the value body (used by the round-trip checks).
"""
from __future__ import annotations

import struct

from . import model as m
from .varint import encode_svarint, encode_uvarint


def _collect_keys(value: object, acc: set[str]) -> None:
    if isinstance(value, dict):
        for key, sub in value.items():
            acc.add(key)
            _collect_keys(sub, acc)
    elif isinstance(value, list):
        for item in value:
            _collect_keys(item, acc)


def encode_value(value: object, dict_index: dict[str, int], base: int = 0) -> bytes:
    # ``base`` = absolute byte offset where this value's first byte lands in the message; used to
    # align tensor data to a 64-byte boundary (§2.5).
    # bool must precede int (bool is an int subclass in Python).
    if value is None:
        return bytes([m.T_NULL])
    if value is True:
        return bytes([m.T_TRUE])
    if value is False:
        return bytes([m.T_FALSE])
    if isinstance(value, int):
        domain = m.number_domain(value)
        if 0 <= value <= 127:
            return bytes([m.FIXINT + value])
        if -16 <= value <= -1:
            return bytes([m.FIXNEG + (-1 - value)])
        if domain == "int":
            return bytes([m.T_INT]) + encode_svarint(value)
        if domain == "uint":
            return bytes([m.T_UINT]) + encode_uvarint(value)
        tc = m.min_twos_complement_le(value)  # BigInt
        return bytes([m.T_BIGINT]) + encode_uvarint(len(tc)) + tc
    if isinstance(value, float):
        if value == 0.0:
            value = 0.0  # normalize -0.0 -> +0.0 (§3)
        elif value != value:
            value = m.CANONICAL_NAN  # normalize any NaN -> the one canonical quiet NaN (§3)
        return bytes([m.T_FLOAT]) + struct.pack("<d", value)
    if isinstance(value, str):
        raw = value.encode("utf-8")
        return bytes([m.T_STRING]) + encode_uvarint(len(raw)) + raw
    if isinstance(value, (bytes, bytearray)):
        return bytes([m.T_BYTES]) + encode_uvarint(len(value)) + bytes(value)
    if isinstance(value, m.Decimal128):
        d = value.canonical()
        return bytes([m.T_DECIMAL]) + encode_svarint(d.scale) + d.coefficient.to_bytes(16, "little", signed=True)
    if isinstance(value, m.Datetime):
        return bytes([m.T_DATETIME]) + struct.pack("<q", value.nanos)
    if isinstance(value, m.Uuid):
        if len(value.value) != 16:
            raise ValueError("UUID must be 16 bytes")
        return bytes([m.T_UUID]) + value.value
    if isinstance(value, m.Extension):
        return bytes([m.T_EXTENSION]) + encode_uvarint(value.ext_type) + encode_uvarint(len(value.payload)) + value.payload
    if isinstance(value, m.Tensor):
        nelem = 1
        for d in value.shape:
            nelem *= d
        want = (nelem * m.DTYPE_BITS[value.dtype] + 7) // 8
        if len(value.data) != want:
            raise ValueError(f"tensor dataLen {len(value.data)} != expected {want}")
        out = bytearray([m.T_TENSOR, value.dtype, len(value.shape)])
        for d in value.shape:
            out += encode_uvarint(d)
        out += encode_uvarint(len(value.data))
        pad = (-(base + len(out))) % m.TENSOR_ALIGN  # zero-pad so data starts 64B-aligned (§2.5)
        out += b"\x00" * pad
        out += value.data
        return bytes(out)
    if isinstance(value, m.Bitmask):
        count = len(value.bits)
        buf = bytearray((count + 7) // 8)
        for i, bit in enumerate(value.bits):
            if bit:
                buf[i >> 3] |= 1 << (i & 7)  # LSB-first
        return bytes([m.T_BITMASK]) + encode_uvarint(count) + bytes(buf)
    if isinstance(value, list):
        head = bytes([m.FIXARRAY + len(value)]) if len(value) <= 15 else bytes([m.T_ARRAY]) + encode_uvarint(len(value))
        buf = bytearray(head)
        for item in value:
            buf += encode_value(item, dict_index, base + len(buf))
        return bytes(buf)
    if isinstance(value, dict):
        keys = m.sorted_keys(value)
        head = bytes([m.FIXMAP + len(keys)]) if len(keys) <= 15 else bytes([m.T_OBJECT]) + encode_uvarint(len(keys))
        buf = bytearray(head)
        for k in keys:
            buf += encode_uvarint(dict_index[k])
            buf += encode_value(value[k], dict_index, base + len(buf))
        return bytes(buf)
    raise TypeError(f"unsupported value type: {type(value).__name__}")


def encode(value: object) -> bytes:
    """Encode ``value`` to a full canonical, uncompressed Cowrie v1 stream (§2.2)."""
    keys: set[str] = set()
    _collect_keys(value, keys)
    dict_keys = sorted(keys, key=lambda k: k.encode("utf-8"))
    dict_index = {k: i for i, k in enumerate(dict_keys)}

    header = bytearray(m.MAGIC)
    header.append(m.VERSION)
    header.append(m.COMPRESSION_NONE)
    header += encode_uvarint(len(dict_keys))
    for key in dict_keys:
        raw = key.encode("utf-8")
        header += encode_uvarint(len(raw)) + raw
    return bytes(header) + encode_value(value, dict_index, base=len(header))
