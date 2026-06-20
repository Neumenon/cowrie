"""wheel_smoke.py — Conformance smoke test for the installed cowrie wheel.

Run against the installed package (not the source tree). All test values are
constructed inline so this script has no dependency on the repo's testdata/
or test helpers.

Usage:
    # Require native (Cython) extension — used in cibuildwheel:
    COWRIE_REQUIRE_NATIVE=1 python wheel_smoke.py

    # Pure-Python mode (no native assertion) — for local dev:
    COWRIE_PUREPYTHON=1 python wheel_smoke.py
"""
import os
import struct
import sys


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def _pass(label: str) -> None:
    print(f"PASS: {label}")


# ── 1. Native extension assertion ────────────────────────────────────────────

if os.environ.get("COWRIE_REQUIRE_NATIVE"):
    from cowrie.gen2 import _HAS_NATIVE
    if not _HAS_NATIVE:
        _fail("COWRIE_REQUIRE_NATIVE=1 but _HAS_NATIVE is False — C extension not loaded")
    _pass("native extension loaded (_HAS_NATIVE=True)")
else:
    print("NOTE: COWRIE_REQUIRE_NATIVE not set — skipping native assertion")

# ── Import the real API ───────────────────────────────────────────────────────

from cowrie.gen2 import (
    encode, decode,
    encode_framed, decode_framed,
    Value, DType, TensorData, Type,
    Compression,
)

# ── 2. Round-trip: scalar int ────────────────────────────────────────────────

v_in = Value.int64(42)
v_out = decode(encode(v_in))
if v_out.type != Type.INT64:
    _fail(f"int64 round-trip: wrong type {v_out.type}")
if v_out.data != 42:
    _fail(f"int64 round-trip: expected 42, got {v_out.data}")
_pass("round-trip int64(42)")

# ── 3. Round-trip: string ────────────────────────────────────────────────────

v_in = Value.string("hello, cowrie")
v_out = decode(encode(v_in))
if v_out.type != Type.STRING:
    _fail(f"string round-trip: wrong type {v_out.type}")
if v_out.data != "hello, cowrie":
    _fail(f"string round-trip: expected 'hello, cowrie', got {v_out.data!r}")
_pass("round-trip string('hello, cowrie')")

# ── 4. Round-trip: object/dict ───────────────────────────────────────────────

v_in = Value.object({"key": Value.int64(99), "val": Value.string("ok")})
v_out = decode(encode(v_in))
if v_out.type != Type.OBJECT:
    _fail(f"object round-trip: wrong type {v_out.type}")
if v_out.data["key"].data != 99:
    _fail(f"object round-trip: key 'key' expected 99, got {v_out.data['key'].data}")
if v_out.data["val"].data != "ok":
    _fail(f"object round-trip: key 'val' expected 'ok', got {v_out.data['val'].data!r}")
_pass("round-trip object({key:99, val:'ok'})")

# ── 5. Round-trip: array ─────────────────────────────────────────────────────

v_in = Value.array([Value.int64(1), Value.int64(2), Value.int64(3)])
v_out = decode(encode(v_in))
if v_out.type != Type.ARRAY:
    _fail(f"array round-trip: wrong type {v_out.type}")
got = [item.data for item in v_out.data]
if got != [1, 2, 3]:
    _fail(f"array round-trip: expected [1,2,3], got {got}")
_pass("round-trip array([1,2,3])")

# ── 6. Round-trip: float32 tensor ────────────────────────────────────────────

raw = struct.pack("<ff", 1.0, 2.0)
v_in = Value.tensor(DType.FLOAT32, [2], raw)
v_out = decode(encode(v_in))
if v_out.type != Type.TENSOR:
    _fail(f"float32 tensor round-trip: wrong type {v_out.type}")
t = v_out.data
if t.dtype != DType.FLOAT32:
    _fail(f"float32 tensor round-trip: wrong dtype {t.dtype}")
if t.shape != [2]:
    _fail(f"float32 tensor round-trip: wrong shape {t.shape}")
got_vals = struct.unpack("<ff", t.data)
if got_vals != (1.0, 2.0):
    _fail(f"float32 tensor round-trip: expected (1.0,2.0), got {got_vals}")
_pass("round-trip float32 tensor([1.0, 2.0], shape=[2])")

# ── 7. Round-trip: rank-0 scalar tensor ──────────────────────────────────────
# shape=[] means one element; elem_size=4 for float32, so 4 bytes of data

raw0 = struct.pack("<f", 3.14)
v_in = Value.tensor(DType.FLOAT32, [], raw0)
v_out = decode(encode(v_in))
if v_out.type != Type.TENSOR:
    _fail(f"rank-0 tensor round-trip: wrong type {v_out.type}")
t0 = v_out.data
if t0.shape != []:
    _fail(f"rank-0 tensor round-trip: expected shape=[], got {t0.shape}")
if t0.dtype != DType.FLOAT32:
    _fail(f"rank-0 tensor round-trip: wrong dtype {t0.dtype}")
(got_scalar,) = struct.unpack("<f", t0.data)
if abs(got_scalar - 3.14) > 1e-5:
    _fail(f"rank-0 tensor round-trip: expected ~3.14, got {got_scalar}")
_pass("round-trip rank-0 float32 scalar tensor (shape=[])")

# ── 8. Round-trip: bool tensor ───────────────────────────────────────────────
# DType.BOOL: 1 byte per element

raw_bool = bytes([1, 0, 1])  # True, False, True
v_in = Value.tensor(DType.BOOL, [3], raw_bool)
v_out = decode(encode(v_in))
if v_out.type != Type.TENSOR:
    _fail(f"bool tensor round-trip: wrong type {v_out.type}")
tb = v_out.data
if tb.dtype != DType.BOOL:
    _fail(f"bool tensor round-trip: wrong dtype {tb.dtype}")
if tb.shape != [3]:
    _fail(f"bool tensor round-trip: expected shape=[3], got {tb.shape}")
got_bools = list(tb.data)
if got_bools != [1, 0, 1]:
    _fail(f"bool tensor round-trip: expected [1,0,1], got {got_bools}")
_pass("round-trip bool tensor([True,False,True], shape=[3])")

# ── 9. Round-trip: bigint ────────────────────────────────────────────────────
# 12345678901234567890 in big-endian two's complement (9 bytes, unsigned here)

big_val = 12345678901234567890
big_bytes = big_val.to_bytes(9, "big")
v_in = Value.bigint(big_bytes)
v_out = decode(encode(v_in))
if v_out.type != Type.BIGINT:
    _fail(f"bigint round-trip: wrong type {v_out.type}")
if v_out.data != big_bytes:
    _fail(f"bigint round-trip: bytes mismatch")
recovered = int.from_bytes(v_out.data, "big")
if recovered != big_val:
    _fail(f"bigint round-trip: expected {big_val}, got {recovered}")
_pass("round-trip bigint(12345678901234567890)")

# ── 10. Round-trip: compressed frame (gzip, no external deps) ────────────────
# encode_framed with GZIP; small payloads fall back to raw (threshold 256 B),
# so we use a tensor large enough to compress.

large_data = struct.pack("<" + "f" * 100, *[float(i) for i in range(100)])
v_large = Value.tensor(DType.FLOAT32, [100], large_data)
framed = encode_framed(v_large, Compression.GZIP)
v_out = decode_framed(framed)
if v_out.type != Type.TENSOR:
    _fail(f"gzip framed round-trip: wrong type {v_out.type}")
if v_out.data.shape != [100]:
    _fail(f"gzip framed round-trip: expected shape=[100], got {v_out.data.shape}")
recovered_vals = struct.unpack("<100f", v_out.data.data)
expected_vals = tuple(float(i) for i in range(100))
if recovered_vals != expected_vals:
    _fail(f"gzip framed round-trip: tensor data mismatch")
_pass("round-trip gzip-framed float32 tensor (shape=[100])")

# ── 11. Golden bytes: int64(42) ──────────────────────────────────────────────
# Wire: magic=534a version=02 flags=00 dict_len=00 FIXINT(42)=6a
# FIXINT_BASE=0x40, so 42 -> 0x40+42=0x6a

GOLDEN_INT42 = bytes.fromhex("534a0200006a")
got = encode(Value.int64(42))
if got != GOLDEN_INT42:
    _fail(f"golden int64(42): expected {GOLDEN_INT42.hex()}, got {got.hex()}")
_pass(f"golden bytes int64(42) == {GOLDEN_INT42.hex()}")

# ── 12. Golden bytes: string("hi") ───────────────────────────────────────────
# Wire: magic=534a version=02 flags=00 dict_len=00 TAG_STRING=05 len=02 'h''i'

GOLDEN_STRING_HI = bytes.fromhex("534a02000005026869")
got = encode(Value.string("hi"))
if got != GOLDEN_STRING_HI:
    _fail(f"golden string('hi'): expected {GOLDEN_STRING_HI.hex()}, got {got.hex()}")
_pass(f"golden bytes string('hi') == {GOLDEN_STRING_HI.hex()}")

# ── 13. Golden bytes: null ───────────────────────────────────────────────────
# Wire: magic=534a version=02 flags=00 dict_len=00 TAG_NULL=00

GOLDEN_NULL = bytes.fromhex("534a02000000")
got = encode(Value.null())
if got != GOLDEN_NULL:
    _fail(f"golden null: expected {GOLDEN_NULL.hex()}, got {got.hex()}")
_pass(f"golden bytes null == {GOLDEN_NULL.hex()}")

# ── 14. Malformed input raises ───────────────────────────────────────────────

try:
    decode(b"\x00bad")
    _fail("malformed decode: expected ValueError, got no exception")
except ValueError:
    _pass("malformed input b'\\x00bad' raises ValueError")
except Exception as e:
    _fail(f"malformed decode: expected ValueError, got {type(e).__name__}: {e}")

# ── Done ─────────────────────────────────────────────────────────────────────

print("\nAll checks passed.")
