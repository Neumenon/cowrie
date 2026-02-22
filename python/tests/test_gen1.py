"""Gen1 codec tests."""

import pytest
from cowrie.gen1 import encode, decode


class TestEmptyContainers:
    """Test empty container round-trips."""

    def test_empty_array(self):
        """Empty array [] should round-trip correctly."""
        original = []
        encoded = encode(original)
        decoded = decode(encoded)
        assert decoded == original

    def test_empty_object(self):
        """Empty object {} should round-trip correctly."""
        original = {}
        encoded = encode(original)
        decoded = decode(encoded)
        assert decoded == original

    def test_empty_string(self):
        """Empty string should round-trip correctly."""
        original = ""
        encoded = encode(original)
        decoded = decode(encoded)
        assert decoded == original

    def test_empty_bytes(self):
        """Empty bytes should round-trip correctly."""
        original = b""
        encoded = encode(original)
        decoded = decode(encoded)
        assert decoded == original


class TestPrimitives:
    """Test primitive value encoding/decoding."""

    def test_null(self):
        assert decode(encode(None)) is None

    def test_true(self):
        assert decode(encode(True)) is True

    def test_false(self):
        assert decode(encode(False)) is False

    def test_positive_int(self):
        assert decode(encode(42)) == 42

    def test_negative_int(self):
        assert decode(encode(-42)) == -42

    def test_zero(self):
        assert decode(encode(0)) == 0

    def test_float(self):
        result = decode(encode(3.14159))
        assert abs(result - 3.14159) < 0.0001

    def test_string(self):
        assert decode(encode("hello")) == "hello"

    def test_unicode(self):
        assert decode(encode("hello 世界 🌍")) == "hello 世界 🌍"


class TestContainers:
    """Test container encoding/decoding."""

    def test_simple_array(self):
        original = [1, 2, 3]
        assert decode(encode(original)) == original

    def test_simple_object(self):
        original = {"name": "Alice", "age": 30}
        decoded = decode(encode(original))
        assert decoded["name"] == "Alice"
        assert decoded["age"] == 30

    def test_nested_object(self):
        original = {"user": {"name": "Bob", "address": {"city": "NYC"}}}
        decoded = decode(encode(original))
        assert decoded["user"]["address"]["city"] == "NYC"


class TestProtoTensors:
    """Test proto-tensor (homogeneous array) encoding."""

    def test_int64_array(self):
        """Homogeneous int arrays should round-trip correctly."""
        original = [1, 2, 3, 4, 5, 6, 7, 8]
        decoded = decode(encode(original))
        assert decoded == original

    def test_float64_array(self):
        """Homogeneous float arrays should round-trip correctly."""
        original = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        decoded = decode(encode(original))
        for i, (orig, dec) in enumerate(zip(original, decoded)):
            assert abs(orig - dec) < 0.0001, f"Mismatch at index {i}"
