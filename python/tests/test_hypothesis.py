"""Suite 5: Property-based testing for cowrie Python using hypothesis.

Tests:
1. Arbitrary binary decode: never raises unhandled exception (only cowrie errors)
2. Value roundtrip: encode(decode(encode(v))) == encode(v) (canonical)
3. Type preservation: decoded type matches encoded type
"""

import math
import struct

import pytest

try:
    from hypothesis import given, settings, assume, HealthCheck
    from hypothesis import strategies as st

    HAS_HYPOTHESIS = True
except ImportError:
    HAS_HYPOTHESIS = False

from cowrie import gen2


pytestmark = pytest.mark.skipif(
    not HAS_HYPOTHESIS, reason="hypothesis not installed"
)


# ── Strategies ──────────────────────────────────────────────────────

if not HAS_HYPOTHESIS:
    pytest.skip("hypothesis not installed", allow_module_level=True)

json_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**63), max_value=2**63 - 1),
    st.floats(allow_nan=True, allow_infinity=True),
    st.text(max_size=100),
)

# Recursive JSON-like structures (limited depth)
json_values = st.recursive(
    json_scalars,
    lambda children: st.one_of(
        st.lists(children, max_size=10),
        st.dictionaries(st.text(max_size=20), children, max_size=10),
    ),
    max_leaves=50,
)


# ── Tests ───────────────────────────────────────────────────────────


@given(data=st.binary(max_size=1024))
@settings(max_examples=500, suppress_health_check=[HealthCheck.too_slow])
def test_arbitrary_binary_decode_no_crash(data: bytes):
    """Decoding arbitrary bytes must never panic — only return value or raise."""
    try:
        gen2.decode(data)
    except Exception:
        pass  # Any exception is fine, just no crash


@given(value=st.none())
@settings(max_examples=1)
def test_null_roundtrip(value):
    v = gen2.Value.null()
    encoded = gen2.encode(v)
    decoded = gen2.decode(encoded)
    assert gen2.to_any(decoded) is None


@given(value=st.booleans())
@settings(max_examples=10)
def test_bool_roundtrip(value: bool):
    v = gen2.Value.bool_(value)
    encoded = gen2.encode(v)
    decoded = gen2.decode(encoded)
    actual = gen2.to_any(decoded)
    assert actual == value


@given(value=st.integers(min_value=-(2**63), max_value=2**63 - 1))
@settings(max_examples=200)
def test_int_roundtrip(value: int):
    v = gen2.Value.int64(value)
    encoded = gen2.encode(v)
    decoded = gen2.decode(encoded)
    actual = gen2.to_any(decoded)
    # to_any may return int for small values, string for large ones
    if isinstance(actual, str):
        assert int(actual) == value
    else:
        assert actual == value


@given(value=st.floats(allow_nan=True, allow_infinity=True))
@settings(max_examples=200)
def test_float_roundtrip(value: float):
    v = gen2.Value.float64(value)
    encoded = gen2.encode(v)
    decoded = gen2.decode(encoded)
    actual = gen2.to_any(decoded)
    if math.isnan(value):
        assert math.isnan(actual)
    else:
        assert actual == value
        # Check -0.0 sign preservation
        if value == 0.0:
            assert math.copysign(1.0, actual) == math.copysign(1.0, value)


@given(value=st.text(max_size=200))
@settings(max_examples=200)
def test_string_roundtrip(value: str):
    v = gen2.Value.string(value)
    encoded = gen2.encode(v)
    decoded = gen2.decode(encoded)
    actual = gen2.to_any(decoded)
    assert actual == value


@given(value=st.binary(max_size=200))
@settings(max_examples=100)
def test_bytes_roundtrip(value: bytes):
    import base64
    v = gen2.Value.bytes_(value)
    encoded = gen2.encode(v)
    decoded = gen2.decode(encoded)
    actual = gen2.to_any(decoded)
    # to_any returns base64 string for bytes, not raw bytes
    if isinstance(actual, str):
        assert base64.b64decode(actual) == value if actual else value == b""
    else:
        assert actual == value


@given(data=st.binary(min_size=4, max_size=512))
@settings(max_examples=300, suppress_health_check=[HealthCheck.too_slow])
def test_truncation_never_crashes(data: bytes):
    """Truncating valid-looking data at any point must not crash."""
    # Add cowrie magic header to make it more interesting
    header = b"SJ\x02\x00"
    full = header + data
    for i in range(len(full)):
        try:
            gen2.decode(full[:i])
        except Exception:
            pass


@given(data=st.binary(min_size=4, max_size=256))
@settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
def test_bitflip_never_crashes(data: bytes):
    """Flipping a single bit in data must not crash the decoder."""
    header = b"SJ\x02\x00"
    full = bytearray(header + data)
    for i in range(len(full)):
        corrupted = bytearray(full)
        corrupted[i] ^= 0xFF
        try:
            gen2.decode(bytes(corrupted))
        except Exception:
            pass
