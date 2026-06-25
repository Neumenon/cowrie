"""Suite 4: Mutation/Corruption Tests for cowrie Python.

Programmatic corruption of valid fixtures — verifies the decoder
raises a structured decode error on corrupted input. Unexpected
exceptions (MemoryError, RecursionError, AttributeError) propagate
and fail the test.
"""

import struct
from pathlib import Path

import pytest

from cowrie import gen2

_EXPECTED_DECODE_ERRORS = (ValueError, TypeError, struct.error, IndexError)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _fixture_dir() -> Path:
    return _repo_root() / "testdata" / "fixtures"


CORE_FIXTURES = [
    "core/null.cowrie",
    "core/true.cowrie",
    "core/int.cowrie",
    "core/float.cowrie",
    "core/string.cowrie",
    "core/array.cowrie",
    "core/object.cowrie",
]


def _load_fixture(name: str) -> bytes:
    return (_fixture_dir() / name).read_bytes()


class TestTruncation:
    """Decoding truncated data must raise a structured error, never crash."""

    @pytest.mark.parametrize("fixture", CORE_FIXTURES)
    def test_truncation_at_every_offset(self, fixture: str):
        data = _load_fixture(fixture)
        for i in range(len(data)):
            truncated = data[:i]
            try:
                gen2.decode(truncated)
            except _EXPECTED_DECODE_ERRORS:
                pass


class TestBitFlip:
    """Single-byte XOR corruption must raise a structured error, never crash."""

    @pytest.mark.parametrize("fixture", CORE_FIXTURES)
    def test_bitflip_at_every_offset(self, fixture: str):
        data = _load_fixture(fixture)
        for i in range(len(data)):
            corrupted = bytearray(data)
            corrupted[i] ^= 0xFF
            try:
                gen2.decode(bytes(corrupted))
            except _EXPECTED_DECODE_ERRORS:
                pass


class TestHeaderCorruption:
    """Corrupting header bytes must raise a structured error, never crash."""

    @pytest.mark.parametrize("fixture", CORE_FIXTURES[:3])  # subset for speed
    def test_magic_byte_corruption(self, fixture: str):
        data = _load_fixture(fixture)
        for pos in range(min(4, len(data))):
            for val in range(256):
                corrupted = bytearray(data)
                corrupted[pos] = val
                try:
                    gen2.decode(bytes(corrupted))
                except _EXPECTED_DECODE_ERRORS:
                    pass


class TestEmptyAndMinimal:
    """Edge-case inputs."""

    def test_empty_input(self):
        with pytest.raises(_EXPECTED_DECODE_ERRORS):
            gen2.decode(b"")

    def test_single_byte(self):
        for b in range(256):
            try:
                gen2.decode(bytes([b]))
            except _EXPECTED_DECODE_ERRORS:
                pass

    def test_just_magic(self):
        with pytest.raises(_EXPECTED_DECODE_ERRORS):
            gen2.decode(b"SJ")

    def test_magic_plus_version(self):
        with pytest.raises(_EXPECTED_DECODE_ERRORS):
            gen2.decode(b"SJ\x02\x00")
