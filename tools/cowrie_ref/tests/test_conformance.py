"""Conformance tests for the Cowrie v1 reference (SPEC-v1 §5).

Run: ``cd tools && python -m pytest cowrie_ref/tests`` (or ``python -m cowrie_ref.tests.test_conformance``).
Covers round-trip identity, fingerprint-is-structure-only, golden-vector stability, and the
strict-mode anti-malleability rejections (§5.3).
"""
from __future__ import annotations

import json
import os

import pytest

import cowrie_ref as c
from cowrie_ref.__main__ import CASES, _GOLDEN
from cowrie_ref.errors import ERR_NON_CANONICAL, ERR_TRAILING_DATA, ERR_INVALID_VARINT, ERR_INVALID_MAGIC


@pytest.mark.parametrize("name", list(CASES))
def test_roundtrip(name: str) -> None:
    value = CASES[name]
    blob = c.encode(value)
    assert c.decode(blob) == value                  # value survives
    assert c.encode(c.decode(blob)) == blob         # bytes are stable (bijectivity, §3)
    assert c.roundtrip_ok(value)


def test_fingerprint_is_structure_only() -> None:
    # different Int values share a fingerprint; domain change does not.
    assert c.fingerprint(0) == c.fingerprint(127) == c.fingerprint(-5)
    assert c.fingerprint(0) != c.fingerprint(2**63 + 1)   # Int vs Uint
    assert c.fingerprint([1, 2]) != c.fingerprint([1, 2, 3])  # length is structure (tuple semantics)


def test_golden_stable() -> None:
    """The committed golden vectors must match the reference exactly (catches accidental drift)."""
    with open(os.path.normpath(_GOLDEN)) as fh:
        golden = json.load(fh)
    for name, rec in golden.items():
        assert c.encode(CASES[name]).hex() == rec["canonical_hex"], name
        assert c.fingerprint(CASES[name])[0] == rec["fingerprint64"], name


def _hx(s: str) -> bytes:
    return bytes.fromhex(s.replace(" ", ""))


# §5.3 anti-malleability: well-formed but non-canonical input MUST be rejected in strict mode,
# and accepted (as the same value) in lenient mode.
NON_CANONICAL = {
    "int_in_fixint_range": ("434f57520100 0003 02", 1),          # Int(0x03) carrying 1
    "uint_fits_int":       ("434f57520100 0009 05", 5),          # Uint carrying 5
    "array_not_fixarray":  ("434f57520100 0006 02 4142", [1, 2]),# Array(0x06) count 2 (<=15)
    "object_not_fixmap":   ("434f57520100 0101 61 07 01 0041", {"a": 1}),  # Object(0x07) count 1
    "negative_zero":       ("434f57520100 0004 0000000000000080", 0.0),
    "overlong_varint":     ("434f57520100 0005 8100 41", None),  # String len overlong -> ERR_INVALID_VARINT
}


@pytest.mark.parametrize("name", list(NON_CANONICAL))
def test_strict_rejects_noncanonical(name: str) -> None:
    blob = _hx(NON_CANONICAL[name][0])
    with pytest.raises(c.CowrieError):
        c.decode(blob, strict=True)


def test_lenient_accepts_some_noncanonical() -> None:
    for name in ("int_in_fixint_range", "uint_fits_int", "array_not_fixarray", "object_not_fixmap"):
        blob, expect = _hx(NON_CANONICAL[name][0]), NON_CANONICAL[name][1]
        assert c.decode(blob, strict=False) == expect, name


# strict rejections specific to the extended Core.
CORE_NON_CANONICAL = {
    "bigint_fits_int": "434f57520100 000d 01 05",                       # BigInt carrying 5 (fits Int)
    "decimal_not_lowest": "434f57520100 000a 00 64000000000000000000000000000000",  # 100*10^0 (foldable)
    "bitmask_trailing_bit": "434f57520100 0024 03 0d",                  # count 3 but bit 3 set
}


@pytest.mark.parametrize("name", list(CORE_NON_CANONICAL))
def test_strict_rejects_core_noncanonical(name: str) -> None:
    blob = _hx(CORE_NON_CANONICAL[name])
    with pytest.raises(c.CowrieError) as ei:
        c.decode(blob, strict=True)
    assert ei.value.code == ERR_NON_CANONICAL


def test_tensor_datalen_mismatch_rejected() -> None:
    # Tensor f32 shape [2] needs 8 bytes; give 4 -> rejected even leniently (structural).
    bad = _hx("434f57520100 0020 01 01 02 04 00000000")
    with pytest.raises(c.CowrieError):
        c.decode(bad, strict=False)


def test_trailing_data_rejected() -> None:
    blob = c.encode({"a": 1}) + b"\x00"
    with pytest.raises(c.CowrieError) as ei:
        c.decode(blob)
    assert ei.value.code == ERR_TRAILING_DATA


def test_bad_magic_rejected() -> None:
    with pytest.raises(c.CowrieError) as ei:
        c.decode(b"XXXX\x01\x00\x00\x00")
    assert ei.value.code == ERR_INVALID_MAGIC


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
