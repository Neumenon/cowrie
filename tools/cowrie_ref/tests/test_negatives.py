"""The Python reference MUST reject every negative fixture in strict mode with its exact error
code, and accept the non-canonical ones in lenient mode (SPEC-v1 §5.3)."""
from __future__ import annotations

import json
import os

import pytest

import cowrie_ref as c

_NEG = os.path.join(os.path.dirname(__file__), "..", "..", "..", "testdata", "v1_negative.json")
NEGATIVES = json.load(open(os.path.normpath(_NEG)))


@pytest.mark.parametrize("name", list(NEGATIVES))
def test_strict_rejects_with_expected_code(name: str) -> None:
    fx = NEGATIVES[name]
    with pytest.raises(c.CowrieError) as ei:
        c.decode(bytes.fromhex(fx["hex"]), strict=True)
    assert ei.value.code == fx["expect"], name


@pytest.mark.parametrize("name", [n for n, f in NEGATIVES.items() if f["tier"] == "non-canonical"])
def test_lenient_accepts_noncanonical(name: str) -> None:
    # non-canonical fixtures are well-formed: lenient decode must not raise.
    c.decode(bytes.fromhex(NEGATIVES[name]["hex"]), strict=False)
