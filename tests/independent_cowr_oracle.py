#!/usr/bin/env python3
"""Independent zero-dependency pure-Python `COWR v1` canonical frame verifier & CID oracle.

Implements every Plane A wire invariant from `SPEC.md` §2 without calling `libcowrie_core.so`
or any Rust/WASM code, eliminating shared-implementation blind spots in conformance testing.
"""

from __future__ import annotations

import hashlib
import math
import struct
from typing import Any

MAGIC_FRAME = b"COWR"
VERSION = 1
MAX_DEPTH = 64
MAX_DECODED_BYTES = 64 * 1024 * 1024
MAX_KEY_LEN = 65536
CANON_QNAN_BITS = 0x7FF8_0000_0000_0000

TAG_NULL = 0x00
TAG_FALSE = 0x01
TAG_TRUE = 0x02
TAG_INT = 0x03
TAG_FLOAT64 = 0x04
TAG_STR = 0x05
TAG_BYTES = 0x06
TAG_ARRAY = 0x07
TAG_MAP = 0x08
TAG_UINT = 0x09
TAG_TENSOR_REF = 0x0A

DTYPE_BYTES = {
    0: 4,  # f32
    1: 2,  # f16
    2: 2,  # bf16
    3: 1,  # i8
    4: 1,  # u8
    5: 8,  # i64
    6: 8,  # f64
    7: 1,  # q4 (packed 2/byte -> ceil(elems/2))
    8: 1,  # q8
}


class OracleError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _decode_uvarint_strict(buf: bytes, pos: int) -> tuple[int, int]:
    val = 0
    shift = 0
    start = pos
    while True:
        if pos >= len(buf):
            raise OracleError("ERR_TRUNCATED")
        b = buf[pos]
        pos += 1
        if shift == 63 and b > 1:
            raise OracleError("ERR_NON_CANONICAL_VARINT")
        if shift > 63:
            raise OracleError("ERR_NON_CANONICAL_VARINT")
        val |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            if pos - start > 1 and b == 0:
                raise OracleError("ERR_NON_CANONICAL_VARINT")
            return val, pos
        shift += 7


def _checked_tensor_min_bytes(dtype: int, dims: list[int]) -> int:
    if dtype not in DTYPE_BYTES or len(dims) > 8:
        raise OracleError("ERR_INVALID_CONTRACT")
    elems = 1
    for d in dims:
        elems *= d
        if elems > (1 << 64) - 1:
            raise OracleError("ERR_ALLOC_LIMIT")
    if dtype == 7:  # q4
        return (elems + 1) // 2
    out = elems * DTYPE_BYTES[dtype]
    if out > (1 << 64) - 1:
        raise OracleError("ERR_ALLOC_LIMIT")
    return out


def verify_and_decode_cowr(buf: bytes) -> dict[str, Any]:
    """Independently verify and decode a strict Plane A `COWR v1` frame.

    Returns `{"authority": int, "epistemic": int, "cid": bytes, "value": Any}`
    or raises `OracleError("ERR_*")` matching Cowrie's frozen error ABI.
    """
    n = len(buf)
    if n < 8:
        raise OracleError("ERR_TRUNCATED")
    if buf[0:4] != MAGIC_FRAME:
        raise OracleError("ERR_BAD_MAGIC")
    if buf[4] != VERSION:
        raise OracleError("ERR_UNSUPPORTED_VER")
    auth = buf[5]
    epi = buf[6]
    flags = buf[7]
    if flags != 0 or (auth & ~0x8F) != 0 or (auth & 0x0F) == 0 or epi not in (1, 2, 3):
        raise OracleError("ERR_NON_CANONICAL_FLAGS")

    pos = 8
    nkeys, pos = _decode_uvarint_strict(buf, pos)
    if nkeys > n - pos:
        raise OracleError("ERR_ALLOC_LIMIT")

    budget = 0

    def charge(amt: int) -> None:
        nonlocal budget
        budget += amt
        if budget > MAX_DECODED_BYTES:
            raise OracleError("ERR_ALLOC_LIMIT")

    keys: list[str] = []
    key_lens: list[int] = []
    prev_kb: bytes | None = None
    for _ in range(nkeys):
        klen, pos = _decode_uvarint_strict(buf, pos)
        if klen > n - pos:
            raise OracleError("ERR_TRUNCATED")
        if klen > MAX_KEY_LEN:
            raise OracleError("ERR_ALLOC_LIMIT")
        charge(klen + 16)
        kb = buf[pos : pos + klen]
        pos += klen
        if prev_kb is not None:
            if kb == prev_kb:
                raise OracleError("ERR_DUPLICATE_KEY")
            if kb < prev_kb:
                raise OracleError("ERR_UNSORTED_DICT")
        prev_kb = kb
        try:
            keys.append(kb.decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise OracleError("ERR_INVALID_UTF8") from exc
        key_lens.append(klen)

    used = [False] * nkeys

    def dec(cur_pos: int, depth: int) -> tuple[Any, int]:
        if depth > MAX_DEPTH:
            raise OracleError("ERR_MAX_DEPTH")
        if cur_pos >= n:
            raise OracleError("ERR_TRUNCATED")
        charge(32)
        tag = buf[cur_pos]
        cur_pos += 1

        if 0x40 <= tag <= 0xBF:
            return tag - 0x40, cur_pos
        if tag == TAG_NULL:
            return None, cur_pos
        if tag == TAG_FALSE:
            return False, cur_pos
        if tag == TAG_TRUE:
            return True, cur_pos
        if tag == TAG_INT:
            u, cur_pos = _decode_uvarint_strict(buf, cur_pos)
            v = (u >> 1) ^ (-(u & 1))
            if 0 <= v <= 127:
                raise OracleError("ERR_NON_CANONICAL_INT")
            return v, cur_pos
        if tag == TAG_UINT:
            u, cur_pos = _decode_uvarint_strict(buf, cur_pos)
            if u <= (1 << 63) - 1:
                raise OracleError("ERR_NON_CANONICAL_UINT")
            return u, cur_pos
        if tag == TAG_FLOAT64:
            if cur_pos + 8 > n:
                raise OracleError("ERR_TRUNCATED")
            bits = struct.unpack_from("<Q", buf, cur_pos)[0]
            cur_pos += 8
            if bits == 0x8000_0000_0000_0000:
                raise OracleError("ERR_NON_CANONICAL_FLOAT")
            if (bits & 0x7FF0_0000_0000_0000) == 0x7FF0_0000_0000_0000 and (bits & 0x000F_FFFF_FFFF_FFFF) != 0:
                if bits != CANON_QNAN_BITS:
                    raise OracleError("ERR_NON_CANONICAL_FLOAT")
            val = struct.unpack("<d", struct.pack("<Q", bits))[0]
            return val, cur_pos
        if tag == TAG_STR:
            slen, cur_pos = _decode_uvarint_strict(buf, cur_pos)
            if slen > n - cur_pos:
                raise OracleError("ERR_TRUNCATED")
            charge(slen)
            raw = buf[cur_pos : cur_pos + slen]
            cur_pos += slen
            try:
                return raw.decode("utf-8"), cur_pos
            except UnicodeDecodeError as exc:
                raise OracleError("ERR_INVALID_UTF8") from exc
        if tag == TAG_BYTES:
            blen, cur_pos = _decode_uvarint_strict(buf, cur_pos)
            if blen > n - cur_pos:
                raise OracleError("ERR_TRUNCATED")
            charge(blen)
            raw = buf[cur_pos : cur_pos + blen]
            cur_pos += blen
            return ("bytes", raw), cur_pos
        if tag == TAG_ARRAY:
            cnt, cur_pos = _decode_uvarint_strict(buf, cur_pos)
            if cnt > n - cur_pos:
                raise OracleError("ERR_ALLOC_LIMIT")
            items = []
            for _ in range(cnt):
                item, cur_pos = dec(cur_pos, depth + 1)
                items.append(item)
            return items, cur_pos
        if tag == TAG_MAP:
            cnt, cur_pos = _decode_uvarint_strict(buf, cur_pos)
            if cnt * 2 > n - cur_pos:
                raise OracleError("ERR_ALLOC_LIMIT")
            prev_idx = -1
            out = {}
            for _ in range(cnt):
                ki, cur_pos = _decode_uvarint_strict(buf, cur_pos)
                if ki >= nkeys:
                    raise OracleError("ERR_BAD_DICT_INDEX")
                if ki == prev_idx:
                    raise OracleError("ERR_DUPLICATE_KEY")
                if ki < prev_idx:
                    raise OracleError("ERR_UNSORTED_MAP_KEYS")
                prev_idx = ki
                used[ki] = True
                charge(key_lens[ki] + 16)
                val, cur_pos = dec(cur_pos, depth + 1)
                out[keys[ki]] = val
            return out, cur_pos
        if tag == TAG_TENSOR_REF:
            arena_idx, cur_pos = _decode_uvarint_strict(buf, cur_pos)
            if arena_idx > 0xFFFF_FFFF:
                raise OracleError("ERR_INVALID_CONTRACT")
            if cur_pos + 2 > n:
                raise OracleError("ERR_TRUNCATED")
            dtype = buf[cur_pos]
            ndim = buf[cur_pos + 1]
            cur_pos += 2
            if dtype not in DTYPE_BYTES or ndim > 8:
                raise OracleError("ERR_INVALID_CONTRACT")
            dims = []
            for _ in range(ndim):
                d, cur_pos = _decode_uvarint_strict(buf, cur_pos)
                dims.append(d)
            _checked_tensor_min_bytes(dtype, dims)
            slen, cur_pos = _decode_uvarint_strict(buf, cur_pos)
            if slen == 0 or slen > 256:
                raise OracleError("ERR_INVALID_CONTRACT")
            if slen > n - cur_pos:
                raise OracleError("ERR_TRUNCATED")
            charge(slen)
            try:
                space = buf[cur_pos : cur_pos + slen].decode("utf-8")
            except UnicodeDecodeError as exc:
                raise OracleError("ERR_INVALID_UTF8") from exc
            cur_pos += slen
            return ("tensor_ref", arena_idx, dtype, dims, space), cur_pos
        raise OracleError("ERR_RESERVED_TAG")

    value, pos = dec(pos, 0)
    if pos != n:
        raise OracleError("ERR_TRAILING_BYTES")
    if not all(used):
        raise OracleError("ERR_UNUSED_DICT_KEY")

    cid = b"\x12\x20" + hashlib.sha256(buf).digest()
    return {"authority": auth, "epistemic": epi, "cid": cid, "value": value}


if __name__ == "__main__":
    import json
    from pathlib import Path

    vec_path = Path(__file__).resolve().parent / "vectors.json"
    v = json.loads(vec_path.read_text())
    for grp in ("golden", "fuzz"):
        for item in v[grp]:
            out = verify_and_decode_cowr(bytes.fromhex(item["wire_hex"]))
            assert out["authority"] == item["auth"]
            assert out["epistemic"] == item["epi"]
            assert out["cid"].hex() == item["cid_hex"]
    for neg in v["negatives"] + v["dos_cases"]:
        try:
            verify_and_decode_cowr(bytes.fromhex(neg["wire_hex"]))
            raise AssertionError(f"Expected {neg['expected_err']} for {neg['id']}")
        except OracleError as e:
            assert e.code == neg["expected_err"], f"{neg['id']}: got {e.code}, expected {neg['expected_err']}"
    print("INDEPENDENT PYTHON COWR v1 ORACLE PASS (150 valid + 20 negative/DoS)")
