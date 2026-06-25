# Cowrie — Go

Binary "JSON++" codec (Cowrie v2) for Go.

## Installation

```bash
go get github.com/Neumenon/cowrie/go
```

## Quick Start

```go
import cowrie "github.com/Neumenon/cowrie/go"

// Build a value and encode
v := cowrie.Object(
    cowrie.Member{Key: "name",  Value: cowrie.String("Alice")},
    cowrie.Member{Key: "score", Value: cowrie.Float64(0.95)},
)
data, err := cowrie.Encode(v)

// Decode
decoded, err := cowrie.Decode(data)
fmt.Println(decoded.Get("name").String()) // Alice

// JSON bridge (round-trip through JSON without data loss)
v2, err := cowrie.FromJSON([]byte(`{"action":"search","k":42}`))
jsonOut, err := cowrie.ToJSON(v2)
```

## Types

| Type | Constructor | Wire tag |
|------|------------|----------|
| null | `Null()` | 0x00 |
| bool | `Bool(b)` | 0x01/0x02 |
| int64 | `Int64(i)` | 0x03 / FIXINT |
| uint64 | `Uint64(u)` | 0x09 |
| float64 | `Float64(f)` | 0x04 |
| decimal128 | `NewDecimal128(scale, coef)` | 0x0A |
| string | `String(s)` | 0x05 |
| bytes | `Bytes(b)` | 0x08 |
| datetime64 | `Datetime64(nanos)` / `Datetime(t)` | 0x0B |
| uuid128 | `UUID128(uuid)` | 0x0C |
| bigint | `BigInt(b)` | 0x0D |
| array | `Array(items...)` | 0x06 / FIXARRAY |
| object | `Object(members...)` | 0x07 / FIXMAP |
| tensor | `Tensor(dtype, dims, data)` | 0x20 |
| tensor_ref | `TensorRef(storeID, key)` | 0x21 |
| image | `Image(format, w, h, data)` | 0x22 |
| audio | `Audio(encoding, rate, ch, data)` | 0x23 |
| bitmask | `Bitmask(count, bits)` | 0x24 |
| node | `Node(id, labels, props)` | 0x35 |
| edge | `Edge(from, to, typ, props)` | 0x36 |
| node_batch | `NodeBatch(nodes)` | 0x37 |
| edge_batch | `EdgeBatch(edges)` | 0x38 |

Tags 0x30 (Adjlist), 0x31 (RichText), 0x32 (Delta), 0x39 (GraphShard) are reserved.
Encoders must not emit them. Decoders skip them silently (return null).

`FlagHasColumnHints = 0x08` is reserved; the per-column hints feature was cut and
the code lives in `attic/`. The flag bit is ignored on decode.

## Encoding

```go
// Simple encode
data, err := cowrie.Encode(v)

// Deterministic (sorted keys) — required for hashing / content-addressing
data, err := cowrie.EncodeWithOptions(v, cowrie.EncodeOptions{Deterministic: true})

// Append to existing buffer (zero extra allocation)
dst, err := cowrie.EncodeAppend(dst, v)

// Stream to io.Writer
err := cowrie.EncodeTo(w, v)
```

## Decoding

```go
// Default security limits (depth 1000, 50 MB bytes, etc.)
v, err := cowrie.Decode(data)

// Custom limits
opts := cowrie.DefaultDecodeOptions()
opts.MaxBytesLen = 10 * 1024 * 1024 // 10 MB
v, err := cowrie.DecodeWithOptions(data, opts)

// From io.Reader (50 MB cap)
v, err := cowrie.DecodeFrom(r)

// From io.Reader with explicit cap
v, err := cowrie.DecodeFromLimited(r, 10*1024*1024)
```

Unknown `TagExt` extensions are preserved by default (`UnknownExtKeep`) so payloads
round-trip without data loss. Set `OnUnknownExt: cowrie.UnknownExtSkipAsNull` or
`UnknownExtError` to change this.

## JSON Bridge

`FromJSON` / `ToJSON` convert between Cowrie values and JSON without schema loss.
`FromJSONEnriched` additionally infers ISO 8601 timestamps → `datetime64`,
UUID strings → `uuid128`, and base64 blobs in `data`/`payload`/`blob` fields → `bytes`.

```go
v, _ := cowrie.FromJSON([]byte(`{"ts":"2024-01-15T10:30:00Z"}`))
// v.Get("ts").Type() == cowrie.TypeString  (strict mode)

v, _ := cowrie.FromJSONEnriched([]byte(`{"ts":"2024-01-15T10:30:00Z"}`))
// v.Get("ts").Type() == cowrie.TypeDatetime64  (enriched mode)
```

## Tensor Example

```go
// Encode a 384-element float32 embedding
data := make([]byte, 384*4) // raw little-endian float32 bytes
v := cowrie.Tensor(cowrie.DTypeFloat32, []uint64{384}, data)
buf, _ := cowrie.Encode(v)

// Decode and get a zero-copy []float32 view
decoded, _ := cowrie.Decode(buf)
td := decoded.Tensor()
floats, ok := td.ViewFloat32() // zero-copy slice into td.Data
```

## Any-Value API

The `EncodeAny` / `DecodeAny` API bridges untyped Go values. Numeric slices are
automatically tensorized (use `AnyOptions{TensorizeSlices: true}`, which is the default).

```go
result, err := cowrie.EncodeAny(map[string]any{
    "name":      "Alice",
    "embedding": []float32{0.1, 0.2, 0.3},
})
// embedding is encoded as TagTensor (0x20), not an array of floats
```

## Schema Fingerprint

```go
fp := cowrie.SchemaFingerprint64(v)  // 64-bit FNV-1a over type structure
fp32 := cowrie.SchemaFingerprint32(v) // low 32 bits
```

Two values with identical field names and types produce the same fingerprint
regardless of their data. Useful for schema-based routing and detecting schema drift.

## Security Limits

All decode paths apply limits by default:

| Limit | Default |
|-------|---------|
| Max nesting depth | 1 000 |
| Max array length | 1 000 000 |
| Max object fields | 1 000 000 |
| Max string bytes | 10 MB |
| Max bytes length | 50 MB |
| Max ext payload | 1 MB |
| Max dictionary entries | 1 000 000 |
| Max tensor rank | 32 |

Pass `DecodeOptions` to override. Set a field to `-1` for unlimited (not recommended
for untrusted input). Use `DecodeFromLimited` to cap total input size.

## License

MIT
