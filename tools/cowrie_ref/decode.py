"""Strict decoder (SPEC-v1 §2, §3, §5.3).

Decodes a canonical stream back to Python values and, in strict mode (the default), rejects any
well-formed-but-non-canonical input with ``ERR_NON_CANONICAL`` rather than silently accepting it —
this is what makes ``encode(decode(x)) == x`` a real conformance gate.
"""
from __future__ import annotations

import struct
import sys

from . import model as _m0

# Ensure our explicit MAX_DEPTH check (ERR_TOO_DEEP) fires before CPython's own recursion limit,
# so over-deep input is a clean decode error, never a RecursionError crash.
sys.setrecursionlimit(max(sys.getrecursionlimit(), _m0.MAX_DEPTH * 8 + 1000))

from . import model as m
from .errors import (
    CowrieError, ERR_DUPLICATE_KEY, ERR_INVALID_FIELD_ID, ERR_INVALID_MAGIC, ERR_INVALID_TAG,
    ERR_INVALID_UTF8, ERR_INVALID_VERSION, ERR_NON_CANONICAL, ERR_TOO_DEEP, ERR_TOO_LARGE, ERR_RESERVED_TAG,
    ERR_TRAILING_DATA, ERR_TRUNCATED, ERR_UNSUPPORTED_COMPRESSION,
)
from .varint import decode_svarint, decode_uvarint

_CANONICAL_NAN = struct.unpack("<d", bytes.fromhex("000000000000f87f"))[0]


class _Decoder:
    def __init__(self, data: bytes, strict: bool = True) -> None:
        self.data = data
        self.pos = 0
        self.strict = strict
        self.keys: list[str] = []
        self.spans: list[tuple[int, tuple[int, ...], int, int]] = []  # (dtype, shape, data_offset, data_len)

    # -- low-level reads --
    def _take(self, n: int) -> bytes:
        if self.pos + n > len(self.data):
            raise CowrieError(ERR_TRUNCATED)
        chunk = self.data[self.pos : self.pos + n]
        self.pos += n
        return chunk

    def _uvarint(self) -> int:
        value, self.pos = decode_uvarint(self.data, self.pos)
        return value

    def _svarint(self) -> int:
        value, self.pos = decode_svarint(self.data, self.pos)
        return value

    def _bad(self, code: str, detail: str = "") -> CowrieError:
        return CowrieError(code, detail)

    # -- header (§2.2) --
    def header(self) -> None:
        if self._take(4) != m.MAGIC:
            raise self._bad(ERR_INVALID_MAGIC)
        if self._take(1)[0] != m.VERSION:
            raise self._bad(ERR_INVALID_VERSION)
        if self._take(1)[0] != m.COMPRESSION_NONE:
            raise self._bad(ERR_UNSUPPORTED_COMPRESSION, "this reference decodes uncompressed only")
        n = self._uvarint()
        prev: bytes | None = None
        for _ in range(n):
            raw = self._take(self._uvarint())
            try:
                key = raw.decode("utf-8")
            except UnicodeDecodeError:
                raise self._bad(ERR_INVALID_UTF8, "dict key")
            if self.strict and prev is not None:
                if raw == prev:
                    raise self._bad(ERR_DUPLICATE_KEY, "duplicate dictionary key")
                if raw < prev:
                    raise self._bad(ERR_NON_CANONICAL, "dictionary not byte-sorted")
            prev = raw
            self.keys.append(key)

    # -- values (§2.3) --
    def value(self, depth: int = 0) -> object:
        if depth > m.MAX_DEPTH:
            raise self._bad(ERR_TOO_DEEP, "nesting depth > MAX_DEPTH")
        tag = self._take(1)[0]
        if tag == m.T_NULL:
            return None
        if tag == m.T_FALSE:
            return False
        if tag == m.T_TRUE:
            return True
        if m.FIXINT <= tag <= m.FIXINT + 127:
            return tag - m.FIXINT
        if m.FIXNEG <= tag <= m.FIXNEG + 15:
            return -1 - (tag - m.FIXNEG)
        if tag == m.T_INT:
            v = self._svarint()
            if self.strict and -16 <= v <= 127:
                raise self._bad(ERR_NON_CANONICAL, "int in FIXINT/FIXNEG range")
            return v
        if tag == m.T_UINT:
            v = self._uvarint()
            if self.strict and v <= m.INT64_MAX:
                raise self._bad(ERR_NON_CANONICAL, "uint fits Int")
            return v
        if tag == m.T_FLOAT:
            v = struct.unpack("<d", self._take(8))[0]
            if self.strict:
                raw = self.data[self.pos - 8 : self.pos]
                if raw == bytes.fromhex("0000000000000080"):
                    raise self._bad(ERR_NON_CANONICAL, "negative zero")
                if v != v and raw != bytes.fromhex("000000000000f87f"):
                    raise self._bad(ERR_NON_CANONICAL, "non-canonical NaN")
            return v
        if tag == m.T_STRING:
            raw = self._take(self._uvarint())
            try:
                return raw.decode("utf-8")
            except UnicodeDecodeError:
                raise self._bad(ERR_INVALID_UTF8)
        if tag == m.T_BYTES:
            return self._take(self._uvarint())
        if tag == m.T_BIGINT:
            raw = self._take(self._uvarint())
            v = int.from_bytes(raw, "little", signed=True)
            if self.strict:
                if m.INT64_MIN <= v <= m.UINT64_MAX:
                    raise self._bad(ERR_NON_CANONICAL, "bigint fits Int/Uint")
                if raw != m.min_twos_complement_le(v):
                    raise self._bad(ERR_NON_CANONICAL, "non-minimal bigint")
            return v
        if tag == m.T_DECIMAL:
            scale = self._svarint()
            coeff = int.from_bytes(self._take(16), "little", signed=True)
            dec = m.Decimal128(coeff, scale)
            if self.strict and dec != dec.canonical():
                raise self._bad(ERR_NON_CANONICAL, "decimal not lowest terms")
            return dec
        if tag == m.T_DATETIME:
            return m.Datetime(struct.unpack("<q", self._take(8))[0])
        if tag == m.T_UUID:
            return m.Uuid(self._take(16))
        if tag == m.T_EXTENSION:
            ext_type = self._uvarint()
            return m.Extension(ext_type, self._take(self._uvarint()))
        if tag == m.T_TENSOR:
            dtype = self._take(1)[0]
            if dtype not in m.DTYPE_BITS:
                raise self._bad(ERR_INVALID_TAG, f"dtype 0x{dtype:02x}")
            rank = self._take(1)[0]
            if rank > m.MAX_RANK:
                raise self._bad(ERR_TOO_LARGE, "tensor rank > MAX_RANK")
            shape = tuple(self._uvarint() for _ in range(rank))
            nelem = 1
            for d in shape:
                nelem *= d
            bits = nelem * m.DTYPE_BITS[dtype]
            dlen = self._uvarint()
            pad = (-self.pos) % m.TENSOR_ALIGN  # data is 64B-aligned (§2.5)
            pad_bytes = self._take(pad)
            if self.strict and any(pad_bytes):
                raise self._bad(ERR_NON_CANONICAL, "tensor alignment padding not zero")
            data_off = self.pos
            data = self._take(dlen)
            self.spans.append((dtype, shape, data_off, dlen))
            if len(data) != (bits + 7) // 8:
                raise self._bad(ERR_NON_CANONICAL, "tensor dataLen mismatch")
            if self.strict and bits % 8 and data and data[-1] >> (bits % 8):
                raise self._bad(ERR_NON_CANONICAL, "tensor sub-byte padding not zero")
            return m.Tensor(dtype, shape, data)
        if tag == m.T_BITMASK:
            count = self._uvarint()
            data = self._take((count + 7) // 8)
            if self.strict and count % 8 and data and data[-1] >> (count % 8):
                raise self._bad(ERR_NON_CANONICAL, "bitmask trailing bits not zero")
            bits = tuple(bool(data[i >> 3] >> (i & 7) & 1) for i in range(count))
            return m.Bitmask(bits)
        if tag == m.T_ARRAY or m.FIXARRAY <= tag <= m.FIXARRAY + 15:
            count = (tag - m.FIXARRAY) if tag != m.T_ARRAY else self._uvarint()
            if self.strict and tag == m.T_ARRAY and count <= 15:
                raise self._bad(ERR_NON_CANONICAL, "array should use FIXARRAY")
            return [self.value(depth + 1) for _ in range(count)]
        if tag == m.T_OBJECT or m.FIXMAP <= tag <= m.FIXMAP + 15:
            count = (tag - m.FIXMAP) if tag != m.T_OBJECT else self._uvarint()
            if self.strict and tag == m.T_OBJECT and count <= 15:
                raise self._bad(ERR_NON_CANONICAL, "object should use FIXMAP")
            obj: dict[str, object] = {}
            last = -1
            for _ in range(count):
                idx = self._uvarint()
                if idx >= len(self.keys):
                    raise self._bad(ERR_INVALID_FIELD_ID, str(idx))
                if self.strict and idx <= last:
                    raise self._bad(ERR_NON_CANONICAL, "object fields not in ascending dictIdx")
                last = idx
                obj[self.keys[idx]] = self.value(depth + 1)
            return obj
        raise self._bad(ERR_RESERVED_TAG, f"0x{tag:02x}")


def decode(data: bytes, strict: bool = True) -> object:
    """Decode a Cowrie v1 stream. In strict mode, non-canonical input is rejected (§5.3)."""
    dec = _Decoder(data, strict=strict)
    dec.header()
    root = dec.value()
    if dec.pos != len(data):
        raise CowrieError(ERR_TRAILING_DATA)
    return root


def tensor_spans(data: bytes, strict: bool = True) -> list[tuple[int, tuple[int, ...], int, int]]:
    """Locate every tensor's ``(dtype, shape, data_offset, data_len)`` within a canonical message, in
    document order. ``data_offset`` is the ABSOLUTE byte offset of the tensor's contiguous little-endian
    data run — a zero-copy reader mmaps the message and views ``data[data_offset : data_offset+data_len]``
    with no decode/copy. Non-breaking: reads the existing canonical layout (no alignment yet, see
    docs/PHASE2-TENSOR.md)."""
    dec = _Decoder(data, strict=strict)
    dec.header()
    dec.value()
    if dec.pos != len(data):
        raise CowrieError(ERR_TRAILING_DATA)
    return dec.spans
