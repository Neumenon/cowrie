#!/usr/bin/env python3
"""Cowrie v1 reference encoder — a clean-room implementation of the canonical encoder + §4
fingerprint, written ONLY from docs/SPEC-v1.md (no reference to the Go/Rust/TS impls, which
diverge). It exists to GENERATE GOLDEN VECTORS from the spec, so conformance is measured
against the spec, not against any implementation. Subset: the JSON-expressible Core
(Null/Bool/Int/Uint/Float/String/Array/Object). Tensors/Bytes/etc. to follow.
"""
import struct, json

# ---- §2.1 varints ----
def uvarint(n: int) -> bytes:
    assert n >= 0
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)

def svarint(n: int) -> bytes:                       # zigzag then uvarint (§2.1)
    return uvarint((n << 1) ^ (n >> 63) if n >= 0 else ((n << 1) ^ ~0))

# zigzag for negatives done explicitly to avoid Python's arbitrary-width >>:
def svarint(n: int) -> bytes:
    zz = (n << 1) ^ (n >> 63) if n >= 0 else ((-n) << 1) - 1
    return uvarint(zz)

INT64_MAX = 2**63 - 1
UINT64_MAX = 2**64 - 1
INT64_MIN = -(2**63)

# ---- canonical encoder (§2.3, §2.3.1) ----
def collect_keys(v, acc):
    if isinstance(v, dict):
        for k, val in v.items():
            acc.add(k); collect_keys(val, acc)
    elif isinstance(v, list):
        for x in v: collect_keys(x, acc)

def enc_value(v, didx) -> bytes:
    if v is None: return b'\x00'
    if v is True: return b'\x02'
    if v is False: return b'\x01'
    if isinstance(v, bool): pass
    if isinstance(v, int):
        if 0 <= v <= 127: return bytes([0x40 + v])                  # FIXINT
        if -16 <= v <= -1: return bytes([0xE0 + (-1 - v)])          # FIXNEG
        if INT64_MIN <= v <= INT64_MAX: return b'\x03' + svarint(v) # Int
        if INT64_MAX < v <= UINT64_MAX: return b'\x09' + uvarint(v) # Uint
        raise ValueError("BigInt not in subset")
    if isinstance(v, float):
        if v == 0.0: v = 0.0                                        # -0 -> +0
        return b'\x04' + struct.pack('<d', v)                      # Float64 LE
    if isinstance(v, str):
        b = v.encode('utf-8'); return b'\x05' + uvarint(len(b)) + b # String
    if isinstance(v, list):
        body = b''.join(enc_value(x, didx) for x in v)
        hdr = bytes([0xC0 + len(v)]) if len(v) <= 15 else b'\x06' + uvarint(len(v))
        return hdr + body                                          # (FIX)ARRAY
    if isinstance(v, dict):
        keys = sorted(v.keys(), key=lambda k: k.encode('utf-8'))   # byte-sorted
        body = b''.join(uvarint(didx[k]) + enc_value(v[k], didx) for k in keys)
        hdr = bytes([0xD0 + len(keys)]) if len(keys) <= 15 else b'\x07' + uvarint(len(keys))
        return hdr + body                                          # (FIX)MAP
    raise ValueError(f"unsupported: {type(v)}")

def encode(v) -> bytes:
    keyset = set(); collect_keys(v, keyset)
    dict_keys = sorted(keyset, key=lambda k: k.encode('utf-8'))    # global sorted dict
    didx = {k: i for i, k in enumerate(dict_keys)}
    hdr = b'COWR' + b'\x01' + b'\x00'                              # magic + ver + compression(none)
    hdr += uvarint(len(dict_keys))
    for k in dict_keys:
        kb = k.encode('utf-8'); hdr += uvarint(len(kb)) + kb
    return hdr + enc_value(v, didx)

# ---- §4 fingerprint ----
FNV_OFFSET = 0xcbf29ce484222325
FNV_PRIME = 0x100000001b3
def fnv1a64(b: bytes) -> int:
    h = FNV_OFFSET
    for x in b:
        h = ((h ^ x) * FNV_PRIME) & 0xFFFFFFFFFFFFFFFF
    return h

def fp(v) -> bytes:                                                # §4.2 over the §1.1 value model
    if v is None: return b'\x00'
    if isinstance(v, bool): return b'\x01'
    if isinstance(v, int):
        if INT64_MIN <= v <= INT64_MAX: return b'\x02'             # Int
        if v <= UINT64_MAX: return b'\x03'                         # Uint
        return b'\x04'                                             # BigInt
    if isinstance(v, float): return b'\x05'
    if isinstance(v, str): return b'\x07'
    if isinstance(v, list):
        return b'\x0B' + uvarint(len(v)) + b''.join(fp(x) for x in v)
    if isinstance(v, dict):
        keys = sorted(v.keys(), key=lambda k: k.encode('utf-8'))
        out = b'\x0C' + uvarint(len(keys))
        for k in keys:
            kb = k.encode('utf-8'); out += uvarint(len(kb)) + kb + fp(v[k])
        return out
    raise ValueError

def fingerprint(v):
    h = fnv1a64(fp(v)); return h, h & 0xFFFFFFFF

# ---- golden vectors ----
CASES = {
    "null": None, "true": True, "false": False,
    "int_0": 0, "int_1": 1, "int_127": 127, "int_128": 128, "int_neg1": -1,
    "int_neg16": -16, "int_neg17": -17, "uint_big": 2**63 + 5,
    "float_1_5": 1.5, "float_zero": 0.0,
    "str_hi": "hi",
    "arr_123": [1, 2, 3],
    "obj_a1": {"a": 1},
    "obj_ba": {"b": 1, "a": 2},
    "nested": {"x": [1, 2], "y": {"z": 3}},
}
if __name__ == "__main__":
    out = {}
    for name, v in CASES.items():
        b = encode(v); h64, h32 = fingerprint(v)
        out[name] = {"value": v, "canonical_hex": b.hex(), "fingerprint64": h64, "fingerprint32": h32}
        print(f"{name:14} {b.hex():<48} fp64={h64:016x}")
    json.dump(out, open("testdata/v1_golden.json", "w"), indent=2)
    print("\nwrote testdata/v1_golden.json")
