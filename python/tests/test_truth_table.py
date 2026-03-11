"""
Truth table tests for cowrie gen2 codec.

Reads truth_cases.json and tests all cowrie cases covering:
- roundtrip encoding/decoding
- float specials (NaN, Inf, -0.0)
- duplicate map keys (last-writer-wins)
- unknown tags, trailing garbage, truncated input
- empty containers, nested depth, large integers
"""

import base64
import json
import math
import struct
from pathlib import Path

import pytest

from cowrie import gen2
from cowrie.gen2 import (
    MAGIC,
    VERSION,
    Tag,
    Type,
    Value,
    encode,
    encode_uvarint,
    decode,
    to_any,
)


def _cogs_root() -> Path:
    """Return the cogs workspace root (parent of cowrie repo)."""
    return Path(__file__).resolve().parents[3]


def _load_cowrie_cases():
    """Load cowrie truth cases from truth_cases.json."""
    path = _cogs_root() / "testdata" / "robustness" / "truth_cases.json"
    data = json.loads(path.read_text())
    return data["cowrie"]["cases"]


CASES = _load_cowrie_cases()
CASE_BY_ID = {c["id"]: c for c in CASES}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_value(inp: dict) -> Value:
    """Build a gen2.Value from a truth-case input descriptor."""
    t = inp["type"]

    if t == "null":
        return Value.null()
    elif t == "bool":
        return Value.bool_(inp["value"])
    elif t == "int64":
        return Value.int64(inp["value"])
    elif t == "float64":
        raw = inp["value"]
        if raw == "NaN":
            return Value.float64(float("nan"))
        elif raw == "+Inf":
            return Value.float64(float("inf"))
        elif raw == "-Inf":
            return Value.float64(float("-inf"))
        elif raw == "-0.0":
            return Value.float64(-0.0)
        else:
            return Value.float64(float(raw))
    elif t == "string":
        return Value.string(inp["value"])
    elif t == "bytes":
        raw_b64 = inp.get("value_base64", "")
        return Value.bytes_(base64.b64decode(raw_b64) if raw_b64 else b"")
    elif t == "array":
        items = [gen2.from_any(item) for item in inp["value"]]
        return Value.array(items)
    elif t == "object":
        entries = inp.get("entries", [])
        # Build dict from entries (last-writer-wins for dups in Python dict)
        members = {}
        for key, val in entries:
            members[key] = gen2.from_any(val)
        return Value.object(members)
    elif t == "nested_arrays":
        depth = inp["depth"]
        v = Value.array([])
        for _ in range(depth):
            v = Value.array([v])
        return v
    else:
        raise ValueError(f"Unknown input type: {t}")


def _build_duplicate_key_wire(entries) -> bytes:
    """Manually build wire bytes for an object with duplicate keys.

    Python dicts deduplicate, so we must construct the binary by hand.
    """
    buf = bytearray()

    # Collect unique keys in order of first appearance
    seen = {}
    unique_keys = []
    for key, _val in entries:
        if key not in seen:
            seen[key] = len(unique_keys)
            unique_keys.append(key)

    # Header: magic + version + flags
    buf.extend(MAGIC)
    buf.append(VERSION)
    buf.append(0)  # flags

    # Dictionary
    buf.extend(encode_uvarint(len(unique_keys)))
    for key in unique_keys:
        encoded = key.encode("utf-8")
        buf.extend(encode_uvarint(len(encoded)))
        buf.extend(encoded)

    # Object with duplicate keys: use FIXMAP tag for small counts
    count = len(entries)
    if count <= 15:
        buf.append(Tag.FIXMAP_BASE + count)
    else:
        buf.append(Tag.OBJECT)
        buf.extend(encode_uvarint(count))

    for key, val in entries:
        idx = seen[key]
        buf.extend(encode_uvarint(idx))
        # Encode the value inline - handle simple int values
        if isinstance(val, int) and 0 <= val <= 127:
            buf.append(Tag.FIXINT_BASE + val)
        elif isinstance(val, int):
            buf.append(Tag.INT64)
            zigzag = (val << 1) ^ (val >> 63) if val >= 0 else ((val << 1) ^ (val >> 63)) & 0xFFFFFFFFFFFFFFFF
            buf.extend(encode_uvarint(zigzag))
        elif isinstance(val, str):
            buf.append(Tag.STRING)
            encoded = val.encode("utf-8")
            buf.extend(encode_uvarint(len(encoded)))
            buf.extend(encoded)
        elif val is None:
            buf.append(Tag.NULL)
        elif isinstance(val, bool):
            buf.append(Tag.TRUE if val else Tag.FALSE)
        else:
            raise ValueError(f"Unsupported value type in duplicate key builder: {type(val)}")

    return bytes(buf)


def _assert_value_matches(case: dict, result) -> None:
    """Assert the decoded result matches the expected value in a truth case."""
    expect = case["expect"]

    if "is_nan" in expect and expect["is_nan"]:
        assert isinstance(result, float), f"{case['id']}: expected float, got {type(result)}"
        assert math.isnan(result), f"{case['id']}: expected NaN"
        return

    if "is_positive_inf" in expect and expect["is_positive_inf"]:
        assert result == float("inf"), f"{case['id']}: expected +Inf, got {result}"
        return

    if "is_negative_inf" in expect and expect["is_negative_inf"]:
        assert result == float("-inf"), f"{case['id']}: expected -Inf, got {result}"
        return

    if "negative_zero" in expect and expect["negative_zero"]:
        assert isinstance(result, float), f"{case['id']}: expected float, got {type(result)}"
        assert result == 0.0, f"{case['id']}: expected 0.0, got {result}"
        assert math.copysign(1.0, result) < 0, f"{case['id']}: expected negative zero"
        return

    if "value_base64" in expect:
        expected_b64 = expect["value_base64"]
        assert result == expected_b64, f"{case['id']}: expected b64 {expected_b64!r}, got {result!r}"
        return

    if "value" in expect:
        expected = expect["value"]
        # Handle large integers: to_any() stringifies values outside safe-int range
        if isinstance(expected, int) and abs(expected) > 9007199254740991:
            assert result == str(expected), (
                f"{case['id']}: expected {expected} (as string), got {result!r}"
            )
        else:
            assert result == expected, f"{case['id']}: expected {expected!r}, got {result!r}"
        return

    raise AssertionError(f"{case['id']}: no expected value found in case")


# ---------------------------------------------------------------------------
# Test functions
# ---------------------------------------------------------------------------

def _get_case(case_id: str) -> dict:
    return CASE_BY_ID[case_id]


@pytest.mark.parametrize(
    "case",
    [c for c in CASES if c["action"] == "roundtrip"],
    ids=lambda c: c["id"],
)
def test_roundtrip(case):
    """Roundtrip: build value -> encode -> decode -> compare."""
    value = _build_value(case["input"])
    data = encode(value)
    decoded = decode(data)
    result = to_any(decoded)
    _assert_value_matches(case, result)


def test_duplicate_map_keys():
    """Duplicate keys: last-writer-wins on decode."""
    case = _get_case("duplicate_map_keys")
    entries = case["input"]["entries"]
    wire = _build_duplicate_key_wire(entries)
    decoded = decode(wire)
    result = to_any(decoded)
    expected = case["expect"]["value"]
    assert result == expected, f"duplicate_map_keys: expected {expected}, got {result}"


def test_unknown_tag_rejected():
    """Unknown tag byte in payload must return error."""
    case = _get_case("unknown_tag_rejected")
    raw_hex = case["input"]["value"]
    data = bytes.fromhex(raw_hex)
    with pytest.raises(Exception):
        decode(data)


def test_trailing_garbage_rejected():
    """Extra bytes after valid root value must be rejected."""
    case = _get_case("trailing_garbage_rejected")
    assert not case["expect"]["ok"]

    # Encode a simple value, then append garbage
    value = Value.int64(42)
    data = encode(value)
    garbage = data + b"\xff"
    with pytest.raises(Exception):
        decode(garbage)


def test_truncated_input_rejected():
    """Truncated input must return error, not panic."""
    case = _get_case("truncated_input_rejected")
    assert not case["expect"]["ok"]

    # Encode a value, then truncate
    value = Value.string("hello world, this is a longer string to ensure truncation is meaningful")
    data = encode(value)
    truncated = data[: len(data) // 2]
    with pytest.raises(Exception):
        decode(truncated)


def test_empty_input_rejected():
    """Empty (zero-length) input returns error."""
    case = _get_case("empty_input_rejected")
    assert not case["expect"]["ok"]
    with pytest.raises(Exception):
        decode(b"")


@pytest.mark.parametrize(
    "case",
    [c for c in CASES if c["action"] == "roundtrip_depth"],
    ids=lambda c: c["id"],
)
def test_roundtrip_depth(case):
    """Nested arrays to specified depth roundtrip successfully."""
    depth = case["input"]["depth"]
    value = _build_value(case["input"])
    data = encode(value)
    decoded = decode(data)

    # Walk the nested structure to verify depth
    v = decoded
    for _ in range(depth):
        assert v.type == Type.ARRAY, f"Expected ARRAY at nesting level, got {v.type}"
        assert len(v.data) == 1, f"Expected 1-element array, got {len(v.data)}"
        v = v.data[0]
    # Innermost should be empty array
    assert v.type == Type.ARRAY and len(v.data) == 0, "Innermost should be empty array"
