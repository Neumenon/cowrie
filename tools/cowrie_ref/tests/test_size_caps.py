"""SPEC-v1 §2.7 size caps: the strict decoder MUST reject oversized inputs with ERR_TOO_LARGE,
matching the Go/Rust/TS implementations (which surface the same cross-language ERR_TOO_LARGE token
for MaxArray/MaxObject/MaxString/MaxBytes/MaxExt/MaxDict). Caps are monkeypatched low so the test
stays cheap — it asserts the COUNT/LENGTH check fires before any large allocation."""
from __future__ import annotations

import pytest

import cowrie_ref as c
from cowrie_ref import model as m
from cowrie_ref.varint import encode_uvarint

_HEADER = m.MAGIC + bytes([m.VERSION, m.COMPRESSION_NONE])


def _no_dict() -> bytes:
    """Header with an empty dictionary (uvarint count 0)."""
    return _HEADER + encode_uvarint(0)


def test_array_over_cap_rejected(monkeypatch) -> None:
    monkeypatch.setattr(m, "MAX_ARRAY", 4, raising=False)
    # T_ARRAY with a declared count one past the cap (no elements need to follow:
    # the count check must fire before any per-element read).
    stream = _no_dict() + bytes([m.T_ARRAY]) + encode_uvarint(5)
    with pytest.raises(c.CowrieError) as ei:
        c.decode(stream, strict=True)
    assert ei.value.code == "ERR_TOO_LARGE"


def test_object_over_cap_rejected(monkeypatch) -> None:
    monkeypatch.setattr(m, "MAX_OBJECT", 4, raising=False)
    stream = _no_dict() + bytes([m.T_OBJECT]) + encode_uvarint(5)
    with pytest.raises(c.CowrieError) as ei:
        c.decode(stream, strict=True)
    assert ei.value.code == "ERR_TOO_LARGE"


def test_dict_over_cap_rejected(monkeypatch) -> None:
    monkeypatch.setattr(m, "MAX_DICT", 4, raising=False)
    # Header dictionary declares more entries than the cap; the check must fire on the
    # declared count, before reading entries.
    stream = _HEADER + encode_uvarint(5)
    with pytest.raises(c.CowrieError) as ei:
        c.decode(stream, strict=True)
    assert ei.value.code == "ERR_TOO_LARGE"


def test_string_over_cap_rejected(monkeypatch) -> None:
    monkeypatch.setattr(m, "MAX_STRING", 8, raising=False)
    stream = _no_dict() + bytes([m.T_STRING]) + encode_uvarint(9)
    with pytest.raises(c.CowrieError) as ei:
        c.decode(stream, strict=True)
    assert ei.value.code == "ERR_TOO_LARGE"


def test_bytes_over_cap_rejected(monkeypatch) -> None:
    monkeypatch.setattr(m, "MAX_BYTES", 8, raising=False)
    stream = _no_dict() + bytes([m.T_BYTES]) + encode_uvarint(9)
    with pytest.raises(c.CowrieError) as ei:
        c.decode(stream, strict=True)
    assert ei.value.code == "ERR_TOO_LARGE"


def test_ext_over_cap_rejected(monkeypatch) -> None:
    monkeypatch.setattr(m, "MAX_EXT", 8, raising=False)
    stream = _no_dict() + bytes([m.T_EXTENSION]) + encode_uvarint(1) + encode_uvarint(9)
    with pytest.raises(c.CowrieError) as ei:
        c.decode(stream, strict=True)
    assert ei.value.code == "ERR_TOO_LARGE"


def test_default_caps_present() -> None:
    """The §2.7 constants exist with the normative values (matches Go/Rust/TS)."""
    assert m.MAX_ARRAY == 1_000_000
    assert m.MAX_OBJECT == 1_000_000
    assert m.MAX_DICT == 1_000_000
    assert m.MAX_STRING == 10_000_000
    assert m.MAX_BYTES == 50_000_000
    assert m.MAX_EXT == 1_000_000
