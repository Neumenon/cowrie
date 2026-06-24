"""Content address (§3): multihash SHA-256 of canonical wire bytes. The reference must reproduce
the golden addresses, and address(value) must equal address_of_bytes(canonical) (bijectivity)."""
from __future__ import annotations

import json
import os

import pytest

import cowrie_ref as c

_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "testdata")
GOLDEN = json.load(open(os.path.normpath(os.path.join(_DIR, "v1_golden.json"))))
ADDR = json.load(open(os.path.normpath(os.path.join(_DIR, "v1_content_addresses.json"))))


@pytest.mark.parametrize("name", list(ADDR))
def test_address_matches_golden(name: str) -> None:
    raw = bytes.fromhex(GOLDEN[name]["canonical_hex"])
    expect = bytes.fromhex(ADDR[name])
    assert c.address_of_bytes(raw) == expect
    assert c.content_address(c.decode(raw)) == expect  # value -> encode -> address == golden
    assert expect[:2] == b"\x12\x20" and len(expect) == 34  # multihash sha2-256, 32-byte digest
