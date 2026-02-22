#!/usr/bin/env python3
"""Generate adversarial test vectors for SJSON security testing."""

import os
import struct

# Gen1 Type Tags
TAG_NULL = 0x00
TAG_FALSE = 0x01
TAG_TRUE = 0x02
TAG_INT64 = 0x03
TAG_FLOAT64 = 0x04
TAG_STRING = 0x05
TAG_BYTES = 0x06
TAG_ARRAY = 0x07
TAG_OBJECT = 0x08
TAG_INT64_ARRAY = 0x09
TAG_FLOAT64_ARRAY = 0x0A
TAG_STRING_ARRAY = 0x0B


def write_uvarint(n: int) -> bytes:
    """Encode unsigned varint."""
    result = bytearray()
    while n >= 0x80:
        result.append((n & 0x7F) | 0x80)
        n >>= 7
    result.append(n)
    return bytes(result)


def zigzag_encode(n: int) -> int:
    """Zigzag encode signed int."""
    return ((n << 1) ^ (n >> 63)) & 0xFFFFFFFFFFFFFFFF


def make_dir(name: str):
    """Create subdirectory."""
    os.makedirs(name, exist_ok=True)


def main():
    # Create subdirectories
    make_dir("empty")
    make_dir("boundary")
    make_dir("depth")
    make_dir("overflow")
    make_dir("utf8")

    # =========================================================
    # Empty Container Tests
    # =========================================================

    # Empty array []
    with open("empty/empty_array.sjson", "wb") as f:
        f.write(bytes([TAG_ARRAY]) + write_uvarint(0))

    # Empty object {}
    with open("empty/empty_object.sjson", "wb") as f:
        f.write(bytes([TAG_OBJECT]) + write_uvarint(0))

    # Empty string ""
    with open("empty/empty_string.sjson", "wb") as f:
        f.write(bytes([TAG_STRING]) + write_uvarint(0))

    # Empty bytes
    with open("empty/empty_bytes.sjson", "wb") as f:
        f.write(bytes([TAG_BYTES]) + write_uvarint(0))

    # Empty int64 array
    with open("empty/empty_int64_array.sjson", "wb") as f:
        f.write(bytes([TAG_INT64_ARRAY]) + write_uvarint(0))

    # Empty float64 array
    with open("empty/empty_float64_array.sjson", "wb") as f:
        f.write(bytes([TAG_FLOAT64_ARRAY]) + write_uvarint(0))

    # Empty string array
    with open("empty/empty_string_array.sjson", "wb") as f:
        f.write(bytes([TAG_STRING_ARRAY]) + write_uvarint(0))

    print("Created empty container tests")

    # =========================================================
    # Boundary Value Tests
    # =========================================================

    # INT64_MAX
    int64_max = 9223372036854775807
    with open("boundary/int64_max.sjson", "wb") as f:
        f.write(bytes([TAG_INT64]) + write_uvarint(zigzag_encode(int64_max)))

    # INT64_MIN
    int64_min = -9223372036854775808
    with open("boundary/int64_min.sjson", "wb") as f:
        f.write(bytes([TAG_INT64]) + write_uvarint(zigzag_encode(int64_min)))

    # Float64 positive infinity
    with open("boundary/float64_inf.sjson", "wb") as f:
        f.write(bytes([TAG_FLOAT64]) + struct.pack("<d", float("inf")))

    # Float64 negative infinity
    with open("boundary/float64_neg_inf.sjson", "wb") as f:
        f.write(bytes([TAG_FLOAT64]) + struct.pack("<d", float("-inf")))

    # Float64 NaN
    with open("boundary/float64_nan.sjson", "wb") as f:
        f.write(bytes([TAG_FLOAT64]) + struct.pack("<d", float("nan")))

    # Float64 subnormal (smallest positive subnormal)
    with open("boundary/float64_subnormal.sjson", "wb") as f:
        # Smallest positive subnormal: 2^-1074 = 5e-324
        f.write(bytes([TAG_FLOAT64]) + struct.pack("<d", 5e-324))

    # Float64 zero
    with open("boundary/float64_zero.sjson", "wb") as f:
        f.write(bytes([TAG_FLOAT64]) + struct.pack("<d", 0.0))

    # Float64 negative zero
    with open("boundary/float64_neg_zero.sjson", "wb") as f:
        f.write(bytes([TAG_FLOAT64]) + struct.pack("<d", -0.0))

    print("Created boundary value tests")

    # =========================================================
    # Depth Tests (nesting)
    # =========================================================

    def make_nested_array(depth: int) -> bytes:
        """Create binary SJSON for nested array of given depth."""
        result = bytearray()
        for _ in range(depth):
            result.append(TAG_ARRAY)
            result.extend(write_uvarint(1))  # count = 1
        result.append(TAG_NULL)  # innermost value
        return bytes(result)

    # Nested array at max depth (1000)
    with open("depth/nested_array_1000.sjson", "wb") as f:
        f.write(make_nested_array(1000))

    # Nested array exceeding max depth (1001)
    with open("depth/nested_array_1001.sjson", "wb") as f:
        f.write(make_nested_array(1001))

    # Nested objects at max depth
    def make_nested_object(depth: int) -> bytes:
        """Create binary SJSON for nested object of given depth."""
        result = bytearray()
        for i in range(depth):
            result.append(TAG_OBJECT)
            result.extend(write_uvarint(1))  # count = 1
            key = f"k{i}"
            result.extend(write_uvarint(len(key)))
            result.extend(key.encode())
        result.append(TAG_NULL)  # innermost value
        return bytes(result)

    with open("depth/nested_object_1000.sjson", "wb") as f:
        f.write(make_nested_object(1000))

    with open("depth/nested_object_1001.sjson", "wb") as f:
        f.write(make_nested_object(1001))

    print("Created depth tests")

    # =========================================================
    # Overflow Tests
    # =========================================================

    # Invalid varint (too many continuation bytes - 11 bytes is invalid)
    with open("overflow/invalid_varint.bin", "wb") as f:
        # 11 bytes with continuation bits set
        f.write(bytes([TAG_ARRAY]))
        f.write(bytes([0x80] * 10 + [0x01]))

    # Maximum valid varint (10 bytes, max uint64)
    with open("overflow/max_varint.bin", "wb") as f:
        # This is a valid 10-byte varint for UINT64_MAX
        f.write(bytes([TAG_ARRAY]))
        f.write(bytes([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]))

    # Int64Array with count that would overflow when * 8
    # count = 0x2000000000000001 (causes count*8 overflow to a small number)
    with open("overflow/int64_array_overflow.bin", "wb") as f:
        f.write(bytes([TAG_INT64_ARRAY]))
        # Varint for 0x2000000000000001
        f.write(bytes([0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x20]))

    print("Created overflow tests")

    # =========================================================
    # UTF-8 Tests
    # =========================================================

    # Invalid continuation byte (0x80 without leading byte)
    with open("utf8/invalid_continuation.bin", "wb") as f:
        f.write(bytes([TAG_STRING]))
        f.write(write_uvarint(1))
        f.write(bytes([0x80]))  # Invalid standalone continuation

    # Overlong encoding (ASCII 'A' = 0x41 encoded as 2 bytes)
    with open("utf8/overlong_encoding.bin", "wb") as f:
        f.write(bytes([TAG_STRING]))
        f.write(write_uvarint(2))
        f.write(bytes([0xC1, 0x81]))  # Overlong encoding of 'A'

    # Truncated multi-byte sequence
    with open("utf8/truncated_sequence.bin", "wb") as f:
        f.write(bytes([TAG_STRING]))
        f.write(write_uvarint(1))
        f.write(bytes([0xE0]))  # 3-byte sequence start, but only 1 byte provided

    # Invalid UTF-8 byte (0xFE and 0xFF are never valid)
    with open("utf8/invalid_byte_fe.bin", "wb") as f:
        f.write(bytes([TAG_STRING]))
        f.write(write_uvarint(1))
        f.write(bytes([0xFE]))

    print("Created UTF-8 tests")

    print("\nAll test vectors generated successfully!")
    print("\nTest categories:")
    print("  - empty/: Empty container round-trip tests (should pass)")
    print("  - boundary/: Boundary value tests (should pass)")
    print("  - depth/: Nesting depth tests (1000 should pass, 1001 should fail)")
    print("  - overflow/: Integer overflow tests (should fail safely)")
    print("  - utf8/: Invalid UTF-8 tests (should fail safely)")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    main()
