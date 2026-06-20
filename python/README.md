# cowrie-py

A binary codec for JSON-like and ML-native data. Extends JSON with explicit integer types (int64, uint64), Decimal128, native binary (no base64), Datetime64, UUID128, BigInt, tensors, images, audio, and graph types. Pure-Python with an optional Cython accelerator (~10× faster encode/decode for common types when built; always falls back to pure Python).

## Install

```
pip install cowrie-py
```

## Usage

```python
import cowrie

# Encode and decode
data = cowrie.encode(cowrie.Value.from_dict({"x": 1, "y": 2.5}))
value = cowrie.decode(data)

# JSON bridge (NaN/Inf rejected here; valid in binary encoding)
json_str = cowrie.to_json(value)
value2 = cowrie.from_json(json_str)

# Convert plain Python objects
value3 = cowrie.from_any({"scores": [0.1, 0.2, 0.3], "label": "cat"})
plain = cowrie.to_any(value3)

# Tensors (gen2)
from cowrie import gen2
tensor_val = gen2.Value.make_tensor(gen2.TensorData(
    dtype=gen2.DType.FLOAT32,
    shape=[3],
    data=b"\x00\x00\x80\x3f\x00\x00\x00\x40\x00\x00\x40\x40",
))
buf = gen2.encode(tensor_val)
```

## NaN / Inf policy

NaN and Inf are **allowed** in Cowrie binary encoding (they round-trip faithfully). They are **rejected** by the JSON bridge (`to_json` / `from_json`) because JSON has no representation for them.

## Links

- Main repo: [github.com/Neumenon/cowrie](https://github.com/Neumenon/cowrie)
