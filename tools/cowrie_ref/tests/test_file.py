"""Cowrie file / frame container + Merkle file identity (§7)."""
from __future__ import annotations

import json
import os
import struct

import pytest

import cowrie_ref as c
from cowrie_ref import Bitmask, Datetime, Decimal128, Tensor, Uuid
from cowrie_ref import file as F

# The value lists that generated testdata/v1_files.json (kept in sync with the generator).
FILES = {
    "file_empty":  [],
    "file_single": [{"a": 1}],
    "file_three":  [None, {"a": 1}, [1, 2, 3]],
    "file_dup":    [{"a": 1}, {"a": 1}],
    "file_five":   [True, 127, "héllo", {"z": {"a": 1}}, [1, 2]],
    "file_mixed":  [Decimal128(12345, 2), Datetime(0), Uuid(bytes(range(16))),
                    Tensor(0x04, (2, 3), bytes(6)), Bitmask((True, False, True))],
}
_GOLDEN = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "testdata", "v1_files.json"))
GOLDEN = json.load(open(_GOLDEN))


@pytest.mark.parametrize("name", list(FILES))
def test_file_matches_golden(name: str) -> None:
    vals = FILES[name]
    blob = F.encode_file(vals)
    assert blob.hex() == GOLDEN[name]["file_hex"], name
    frames = [c.encode(v) for v in vals]
    assert F.merkle_root(frames).hex() == GOLDEN[name]["merkle_root"], name
    assert F.merkle_root(frames)[:2] == b"\x12\x20"           # multihash sha2-256
    assert F.decode_file(blob) == frames                       # round-trip via footer index
    assert F.file_identity(blob).hex() == GOLDEN[name]["merkle_root"]


def test_file_deterministic() -> None:
    for vals in FILES.values():
        assert F.encode_file(vals) == F.encode_file(vals)


def test_distinct_frame_lists_distinct_roots() -> None:
    # count-binding + ordering: these must all differ
    roots = {F.merkle_root([c.encode(v) for v in FILES[n]]) for n in FILES}
    assert len(roots) == len(FILES)
    assert F.merkle_root([c.encode({"a": 1})]) != F.merkle_root([c.encode({"a": 1}), c.encode({"a": 1})])


def _reject(blob: bytes, code: str) -> None:
    with pytest.raises(c.CowrieError) as ei:
        F.decode_file(blob)
    assert ei.value.code == code


def test_canonical_and_tamper_rejections() -> None:
    blob = F.encode_file(FILES["file_three"])
    b = bytearray(blob); b[5] = 1; _reject(bytes(b), "ERR_NON_CANONICAL")          # reserved != 0
    b = bytearray(blob); b[-1] = 0; _reject(bytes(b), "ERR_INVALID_MAGIC")          # bad end magic
    b = bytearray(blob); b[0] = 0; _reject(bytes(b), "ERR_INVALID_MAGIC")           # bad file magic
    # flip a byte inside the first frame -> Merkle mismatch
    b = bytearray(blob); b[10] ^= 0xFF; _reject(bytes(b), "ERR_NON_CANONICAL")
    # corrupt the footer index so it no longer mirrors the body
    (fo,) = struct.unpack("<Q", blob[-12:-4])
    b = bytearray(blob); b[fo + 1] ^= 0xFF; _reject(bytes(b), "ERR_NON_CANONICAL")
