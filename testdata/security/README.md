# Cowrie Security Test Vectors

This directory contains adversarial test vectors for testing decoder security.
These inputs are designed to trigger edge cases and potential vulnerabilities.

## Test Categories

### 1. Integer Overflow Tests (`overflow/`)
- `huge_array_count.bin` - Array with count that overflows when multiplied by element size
- `huge_int64_array.bin` - Int64Array with count that causes `count * 8` overflow
- `max_varint.bin` - 10-byte varint at maximum value (edge case)
- `invalid_varint.bin` - Malformed varint (too many continuation bytes)

### 2. Depth Tests (`depth/`)
- `nested_1000.bin` - Array nested exactly 1000 levels (at limit)
- `nested_1001.bin` - Array nested 1001 levels (should fail)
- `nested_objects_1000.bin` - Objects nested 1000 levels

### 3. Size Limit Tests (`size/`)
- `max_string.bin` - String at maximum allowed size
- `huge_string.bin` - String exceeding size limit
- `max_array.bin` - Array at maximum element count
- `huge_array.bin` - Array exceeding element count limit

### 4. Invalid UTF-8 Tests (`utf8/`)
- `invalid_string_continuation.bin` - String with invalid UTF-8 continuation byte
- `truncated_string.bin` - String with incomplete UTF-8 sequence
- `overlong_encoding.bin` - String with overlong UTF-8 encoding

### 5. Tag Tests (`tags/`)
- `unknown_tag.bin` - Document with unknown type tag
- `invalid_tag_context.bin` - Valid tag in invalid context

### 6. Empty Container Tests (`empty/`)
- `empty_array.cowrie` - Empty array `[]`
- `empty_object.cowrie` - Empty object `{}`
- `empty_string.cowrie` - Empty string `""`
- `empty_bytes.cowrie` - Empty bytes

### 7. Boundary Value Tests (`boundary/`)
- `int64_max.cowrie` - INT64_MAX (9223372036854775807)
- `int64_min.cowrie` - INT64_MIN (-9223372036854775808)
- `float64_nan.cowrie` - Float64 NaN
- `float64_inf.cowrie` - Float64 Infinity
- `float64_neg_inf.cowrie` - Float64 -Infinity
- `float64_subnormal.cowrie` - Float64 subnormal number

## Usage

Each implementation should:
1. Successfully decode valid edge-case inputs
2. Reject invalid inputs with appropriate errors
3. Not crash or hang on any input
4. Not allocate excessive memory on malicious inputs

## Generating Binary Test Vectors

```python
# Example: Create nested array test
import struct

def make_nested_array(depth):
    """Create binary Cowrie data for nested array."""
    # Tag 0x07 = ARRAY, count 1
    result = b''
    for _ in range(depth):
        result += b'\x07\x01'  # ARRAY tag + count=1
    result += b'\x00'  # NULL at deepest level
    return result

# Write to file
with open('nested_1001.bin', 'wb') as f:
    f.write(make_nested_array(1001))
```

## Cross-Language Verification

All test vectors should produce identical results across:
- Go
- Rust
- Python
- C
- TypeScript
