#!/usr/bin/env python3
"""Meta-test: prove the conformance gate has TEETH.

The conformance gate (tools/conformance_gate.py) passes a cell when an engine
*reproduces* the golden canonical bytes:  recode(golden_bytes) == golden_bytes.

A gate is only worth anything if it goes RED on corrupted input. This canary
takes real golden vectors, flips/truncates exactly the bytes a sloppy or
malicious encoder might get wrong, and asserts that the conformance check would
FAIL — i.e. at least one of {Go recode, Rust recode, Python re-encode} no longer
reproduces the *original* golden bytes (it produces different bytes, errors, or
emits nothing).

If a corrupted vector were to sail through green, *that* would be the real bug:
this test would fail and flag it.

Run with the project venv:
    /tmp/cowrie-venv/bin/python -m pytest tools/test_gate_canary.py -v
"""
from __future__ import annotations

import json
import os
import subprocess

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLDEN = os.path.join(ROOT, "testdata", "v1_golden.json")
GO_CLI = os.environ.get("GO_CLI", "/tmp/cowrie-cli")
RUST_RECODE = os.environ.get(
    "RUST_RECODE", os.path.join(ROOT, "rust/target/release/examples/recode")
)

_GOLDEN = json.load(open(GOLDEN))


def _recode(cmd: list[str], raw: bytes) -> bytes:
    """Mirror conformance_gate.recode(): feed raw bytes on stdin, take stdout.

    On a decode error the engines write to stderr and emit nothing on stdout,
    so the returned bytes (b"") will not equal the original golden bytes —
    exactly the signal the gate keys on.
    """
    return subprocess.run(cmd, input=raw, capture_output=True).stdout


def _reproduces_original(original: bytes, corrupted: bytes) -> dict[str, bool]:
    """For each engine, does recode(corrupted) reproduce the ORIGINAL bytes?

    The conformance gate compares an engine's recode output against the golden
    canonical bytes. We replay that comparison but against the *original* bytes
    so that "True" means the corruption slipped through undetected by that engine.
    """
    results: dict[str, bool] = {}
    for lang, cmd in (("go", [GO_CLI, "recode"]), ("rust", [RUST_RECODE])):
        try:
            out = _recode(cmd, corrupted)
        except Exception:  # noqa: BLE001 — a crash is also "did not reproduce"
            out = b"<error>"
        results[lang] = out == original
    return results


def _assert_gate_would_go_red(name: str, original: bytes, corrupted: bytes) -> None:
    assert corrupted != original, "corruption must actually change the bytes"
    repro = _reproduces_original(original, corrupted)
    # Gate has teeth iff at least one engine fails to reproduce the original.
    assert not all(repro.values()), (
        f"GATE HAS NO TEETH for {name!r}: every engine reproduced the original "
        f"golden bytes from corrupted input {corrupted.hex()} "
        f"(original {original.hex()}). repro={repro}"
    )


# ---------------------------------------------------------------------------
# Corruption builders
# ---------------------------------------------------------------------------

def _flip_last_byte(b: bytes) -> bytes:
    """Flip a bit in the trailing VALUE byte (the payload, not the header)."""
    return b[:-1] + bytes([b[-1] ^ 0x01])


def _flip_magic_byte(b: bytes) -> bytes:
    """Flip the first MAGIC byte ('C' of 'COWR') — a header corruption."""
    return bytes([b[0] ^ 0x01]) + b[1:]


def _truncate(b: bytes) -> bytes:
    """Drop the final byte — a truncated input."""
    return b[:-1]


# Vectors chosen so the last byte is a genuine value byte (header is the
# 7-byte COWR magic + version + flags prefix common to all v1 vectors).
VALUE_FLIP_VECTORS = ["int_1", "int_127", "int_neg17", "uint_big"]
MAGIC_FLIP_VECTORS = ["null", "int_1", "int_128"]
TRUNCATE_VECTORS = ["int_128", "uint_big", "bigint_pos"]


# Sanity: the chosen vectors actually exist in the golden set.
@pytest.mark.parametrize(
    "name", sorted(set(VALUE_FLIP_VECTORS + MAGIC_FLIP_VECTORS + TRUNCATE_VECTORS))
)
def test_vector_present_in_golden(name: str) -> None:
    assert name in _GOLDEN, f"missing golden vector {name!r}"


@pytest.mark.parametrize("name", VALUE_FLIP_VECTORS)
def test_value_byte_corruption_is_detected(name: str) -> None:
    """A flipped VALUE byte must not silently reproduce the original."""
    original = bytes.fromhex(_GOLDEN[name]["canonical_hex"])
    _assert_gate_would_go_red(name, original, _flip_last_byte(original))


@pytest.mark.parametrize("name", MAGIC_FLIP_VECTORS)
def test_magic_byte_corruption_is_detected(name: str) -> None:
    """A flipped MAGIC/header byte must be rejected (decode error)."""
    original = bytes.fromhex(_GOLDEN[name]["canonical_hex"])
    _assert_gate_would_go_red(name, original, _flip_magic_byte(original))


@pytest.mark.parametrize("name", TRUNCATE_VECTORS)
def test_truncated_input_is_detected(name: str) -> None:
    """A truncated input must be rejected, not happily padded back."""
    original = bytes.fromhex(_GOLDEN[name]["canonical_hex"])
    _assert_gate_would_go_red(name, original, _truncate(original))


def test_canary_self_check_uncorrupted_roundtrips() -> None:
    """Control: uncorrupted golden bytes DO reproduce (engines aren't just
    broken / always-failing). Guards against a false sense of teeth."""
    name = "int_1"
    original = bytes.fromhex(_GOLDEN[name]["canonical_hex"])
    repro = _reproduces_original(original, original)
    assert all(repro.values()), (
        f"engines failed to reproduce UNcorrupted golden {name!r}; canary is "
        f"meaningless if recode is just broken. repro={repro}"
    )
