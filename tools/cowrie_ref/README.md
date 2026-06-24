# cowrie_ref — Cowrie v1 reference implementation

A clean-room implementation of **Cowrie v1**, written from [`docs/SPEC-v1.md`](../../docs/SPEC-v1.md)
alone. The spec is the oracle: this package *defines* the canonical bytes and fingerprints that every
conformant implementation (Go/Rust/TS/…) must reproduce, and it generates the golden vector suite.

It is deliberately small and exact, not fast — its job is to be obviously correct.

## Layout
| module | responsibility |
|---|---|
| `errors.py` | the §2.6 error codes (one exception, stable `.code`) |
| `varint.py` | LEB128 uvarint/svarint, strict (rejects overlong) — §2.1 |
| `model.py` | value model + wire tags + fingerprint codes — §1, §2.3, §4.1 |
| `encode.py` | canonical encoder — §2, §3 |
| `decode.py` | strict decoder (rejects non-canonical) + lenient mode — §2, §5.3 |
| `fingerprint.py` | schema fingerprint grammar — §4 |
| `tests/` | round-trip, golden stability, anti-malleability |

## Use
```python
from cowrie_ref import encode, decode, fingerprint, roundtrip_ok
blob = encode({"a": 1})            # -> canonical bytes
decode(blob)                       # -> {'a': 1}   (strict: rejects non-canonical input)
fingerprint({"a": 1})             # -> (fp64, fp32)
```

```bash
python -m cowrie_ref gen           # regenerate ../../testdata/v1_golden.json (self-checking)
python -m cowrie_ref fp '{"a":1}'  # canonical hex + fingerprint of a JSON value
python -m pytest cowrie_ref/tests  # 32 conformance tests
```

## Scope
Covers the **full v1 Core**: Null, Bool, Int, Uint, BigInt, Float, Decimal128, String, Bytes, UUID,
Datetime, Array, Object, Tensor, Bitmask, Extension — encode + strict decode + fingerprint, with
32 round-trip-verified golden vectors and anti-malleability rejection across every type.
