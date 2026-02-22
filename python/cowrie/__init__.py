"""Cowrie - Structured JSON binary codec.

This package provides two codecs for binary JSON serialization:

- **gen1**: Lightweight codec with proto-tensor support (11 core types + graph types)
- **gen2**: Full Cowrie v2 with ML extensions (18+ types, dictionary coding)

Quick Start
-----------

Gen1 (Lightweight):
    >>> from cowrie import gen1
    >>> data = gen1.encode({"name": "Alice", "scores": [1.0, 2.0, 3.0]})
    >>> decoded = gen1.decode(data)

Gen2 (Full):
    >>> from cowrie import gen2
    >>> data = gen2.encode({"name": "Alice", "embedding": gen2.Tensor([0.1, 0.2], "float32")})
    >>> decoded = gen2.decode(data)

Feature Comparison
------------------

| Feature | Gen1 | Gen2 |
|---------|------|------|
| Core types | 11 | 13+ |
| ML types | proto-tensors | Tensor, Image, Audio, etc. |
| Dictionary coding | No | Yes |
| Graph types | 6 | Adjlist |
"""

__version__ = "0.1.0"

from . import gen1
from . import gen2

# Re-export common functions from gen2 for convenience
from .gen2 import (
    encode,
    decode,
    from_json,
    to_json,
    from_any,
    to_any,
    Value,
    DType,
    TensorData,
)

__all__ = [
    "gen1",
    "gen2",
    "encode",
    "decode",
    "from_json",
    "to_json",
    "from_any",
    "to_any",
    "Value",
    "DType",
    "TensorData",
]
