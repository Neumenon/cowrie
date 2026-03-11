"""Suite 4: Mutation/Corruption Tests for cowrie Python.

Programmatic corruption of valid fixtures — verifies the decoder
never crashes on corrupted input (only returns errors).
"""

from pathlib import Path

import pytest

from cowrie import gen2


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
    """Decoding truncated data must raise, never crash."""

    @pytest.mark.parametrize("fixture", CORE_FIXTURES)
    def test_truncation_at_every_offset(self, fixture: str):
        data = _load_fixture(fixture)
        for i in range(len(data)):
            truncated = data[:i]
            try:
                gen2.decode(truncated)
            except Exception:
                pass  # Any exception is fine — no crash


class TestBitFlip:
    """Single-byte XOR corruption must not crash the decoder."""

    @pytest.mark.parametrize("fixture", CORE_FIXTURES)
    def test_bitflip_at_every_offset(self, fixture: str):
        data = _load_fixture(fixture)
        for i in range(len(data)):
            corrupted = bytearray(data)
            corrupted[i] ^= 0xFF
            try:
                gen2.decode(bytes(corrupted))
            except Exception:
                pass  # Any exception is fine — no crash


class TestHeaderCorruption:
    """Corrupting header bytes must not crash."""

    @pytest.mark.parametrize("fixture", CORE_FIXTURES[:3])  # subset for speed
    def test_magic_byte_corruption(self, fixture: str):
        data = _load_fixture(fixture)
        for pos in range(min(4, len(data))):
            for val in range(256):
                corrupted = bytearray(data)
                corrupted[pos] = val
                try:
                    gen2.decode(bytes(corrupted))
                except Exception:
                    pass


class TestEmptyAndMinimal:
    """Edge-case inputs."""

    def test_empty_input(self):
        with pytest.raises(Exception):
            gen2.decode(b"")

    def test_single_byte(self):
        for b in range(256):
            try:
                gen2.decode(bytes([b]))
            except Exception:
                pass

    def test_just_magic(self):
        try:
            gen2.decode(b"SJ")
        except Exception:
            pass

    def test_magic_plus_version(self):
        try:
            gen2.decode(b"SJ\x02\x00")
        except Exception:
            pass
