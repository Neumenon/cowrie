"""
Cowrie v2 - Binary JSON++ Codec with Extended Types.

Cowrie extends JSON with:
  - Explicit integer types (int64, uint64)
  - Decimal128 for high-precision decimals
  - Native binary data (no base64)
  - Datetime64 (nanosecond timestamps)
  - UUID128 (native UUIDs)
  - BigInt (arbitrary precision)
  - Dictionary-coded object keys

Usage:
    import cowrie

    # Encode Python data to Cowrie bytes
    data = {"name": "Alice", "age": 30, "active": True}
    encoded = cowrie.encode(data)

    # Decode Cowrie bytes back to Python
    decoded = cowrie.decode(encoded)

    # Use Value objects for extended types
    val = cowrie.Object(
        ("id", cowrie.UUID(uuid.uuid4())),
        ("created", cowrie.Datetime(datetime.now())),
        ("score", cowrie.Decimal128(Decimal("3.14159"))),
    )
"""

from .codec import (
    encode, decode, encode_to, decode_from,
    encode_compressed, decode_compressed, save, load,
)
from .types import (
    # Type enum
    Type,
    # Value class
    Value,
    # Constructors
    Null, Bool, Int64, Uint64, Float64,
    String, Bytes, Array, Object,
    Decimal128, Datetime64, UUID128, BigInt,
    # Aliases
    Datetime, UUID,
    # From Python helpers
    from_python, to_python,
)

__version__ = "0.1.0"
__all__ = [
    # Core functions
    "encode", "decode", "encode_to", "decode_from",
    # Compression
    "encode_compressed", "decode_compressed", "save", "load",
    # Types
    "Type", "Value",
    # Constructors
    "Null", "Bool", "Int64", "Uint64", "Float64",
    "String", "Bytes", "Array", "Object",
    "Decimal128", "Datetime64", "UUID128", "BigInt",
    "Datetime", "UUID",
    # Converters
    "from_python", "to_python",
]
