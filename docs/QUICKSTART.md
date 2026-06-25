# Cowrie Quickstart

Cowrie is a deterministic, content-addressable, AI-native binary codec.

Its one invariant: **equal value ⇒ equal canonical bytes ⇒ equal hash** — and those bytes are
*byte-identical* across Python, Go, Rust, and TypeScript. Magic `COWR`, version 1. A value's
**content address** is the multihash SHA-256 of its canonical bytes (spec §3).

Status: **0.9.0**, pre-1.0. The wire format is a release candidate and not yet frozen. Nothing is
published to a registry yet, so the snippets below install from the `phase0-spec` branch.

The authoritative spec is [`docs/SPEC-v1.md`](./SPEC-v1.md).

---

## Install (today, from `phase0-spec`)

```bash
# Python — gives both the `cowrie` console script and `import cowrie_ref`
pip install "git+https://github.com/Neumenon/cowrie.git@phase0-spec#subdirectory=tools"

# Go — git-native one-liner
go get github.com/Neumenon/cowrie/go@phase0-spec
```

```toml
# Rust — in Cargo.toml
[dependencies]
cowrie-rs = { git = "https://github.com/Neumenon/cowrie.git", branch = "phase0-spec" }
```

```bash
# JS — npm cannot install a git subdirectory, so build a local tarball
git clone -b phase0-spec https://github.com/Neumenon/cowrie.git
cd cowrie/typescript
npm install && npm run build && npm pack      # -> cowrie-codec-0.9.0.tgz
npm install /path/to/cowrie/typescript/cowrie-codec-0.9.0.tgz
```

Once packages are published, installs become the usual one-liners:

```bash
pip install cowrie-ref
npm install cowrie-codec
cargo add cowrie-rs
go get github.com/Neumenon/cowrie/go@v0.9.0
```

| Language | Package / module                       | Registry  |
|----------|----------------------------------------|-----------|
| Python   | `cowrie-ref` (import `cowrie_ref`)     | PyPI      |
| JS / TS  | `cowrie-codec`                         | npm       |
| Rust     | `cowrie-rs`                            | crates.io |
| Go       | `github.com/Neumenon/cowrie/go`        | go modules |

---

## The core loop: encode → decode → content-address

Every binding does the same three things, just with different value-construction ergonomics
(explained right after). Throughout we use the value `{"name": "Alice", "scores": [1, 2, 3]}`.

### Python — plain objects go straight in

Python's reference impl reads ordinary dicts, lists, ints, strings, etc. directly. No wrapper type.

```python
from cowrie_ref import encode, decode, content_address

value = {"name": "Alice", "scores": [1, 2, 3]}

blob = encode(value)            # -> canonical bytes
back = decode(blob)             # -> {"name": "Alice", "scores": [1, 2, 3]}
addr = content_address(value)   # -> bytes (multihash SHA-256, §3)

print(addr.hex())
# 122091f7d42a00c157c37f0929b15e90d8c785dbe50581cc651aafc338f6e5e8aad1
```

There is also a `cowrie` CLI:

```bash
cowrie recode --addr < wire     # other flags: --strict --file-id --file-recode
                                #              --tensor-spans --fingerprint --dataset-root
```

### JavaScript / TypeScript — wrap with `fromAny` first

```javascript
const { fromAny, encode, decode, toAny, contentAddress } = require("cowrie-codec");

const value = fromAny({ name: "Alice", scores: [1, 2, 3] }); // plain JS -> typed Value
const wire = encode(value);                                  // canonical bytes
const back = toAny(decode(wire));                            // { name: "Alice", scores: [1,2,3] }
const addr = contentAddress(value);                          // multihash SHA-256 (§3)

console.log(Buffer.from(addr).toString("hex"));
// 122091f7d42a00c157c37f0929b15e90d8c785dbe50581cc651aafc338f6e5e8aad1
```

The root export also includes `contentAddressHex`, `addressOfBytes`, `schemaFingerprint64`,
`fileIdentity`, `merkleRoot`, `encodeFile`, `decodeFile`, and `tensorSpans`.

### Go — build a typed `Value` with constructors

```go
import (
    "fmt"

    cowrie "github.com/Neumenon/cowrie/go"
)

func main() {
    v := cowrie.Object(
        cowrie.Member{Key: "name", Value: cowrie.String("Alice")},
        cowrie.Member{Key: "scores", Value: cowrie.Array(
            cowrie.Int64(1), cowrie.Int64(2), cowrie.Int64(3),
        )},
    )

    data, _ := cowrie.Encode(v)              // canonical bytes
    dec, _ := cowrie.Decode(data)            // *cowrie.Value
    addr := cowrie.AddressOfBytes(data)      // []byte (raw multihash, §3)

    fmt.Println(cowrie.ContentAddressHex(v)) // hex string, or hex.EncodeToString(addr)
    // 122091f7d42a00c157c37f0929b15e90d8c785dbe50581cc651aafc338f6e5e8aad1
}
```

`AddressOfBytes` / `ContentAddress` return raw bytes; use `AddressOfBytesHex` /
`ContentAddressHex` (or `encoding/hex`) for the hex string.

### Rust — build a `gen2::Value`

```rust
use cowrie_rs::gen2::{self, Value};

fn main() -> Result<(), cowrie_rs::gen2::CowrieError> {
    let v = Value::object(vec![
        ("name", Value::String("Alice".into())),
        ("scores", Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])),
    ]);

    let wire = gen2::encode(&v)?;                 // canonical bytes
    let _dec = gen2::decode(&wire)?;              // Value
    let _addr = gen2::address_of_bytes(&wire);    // Vec<u8> (raw multihash, §3)

    println!("{}", gen2::content_address_hex(&v)?);
    // 122091f7d42a00c157c37f0929b15e90d8c785dbe50581cc651aafc338f6e5e8aad1
    Ok(())
}
```

`content_address(&v)` returns the raw `Vec<u8>`; `content_address_hex(&v)` gives the hex string.

---

## Why the four APIs differ

The codec works on a single, precise type lattice (null, bool, signed int, unsigned int, float,
string, bytes, array, object, and the AI-native extensions). Each binding exposes that lattice in
the way that is natural — and *unambiguous* — for its host language:

- **Python** accepts native objects directly (`dict`, `list`, `int`, `str`, ...). Python's runtime
  types already map cleanly onto the lattice, so no construction step is needed.
- **Go** uses explicit constructors — `cowrie.Object`/`cowrie.Member`, `cowrie.Array`,
  `cowrie.Int64`, `cowrie.String`, `cowrie.Float64` — that build a `*cowrie.Value`. Go has no
  untyped literal that spans the lattice, so you name the type at construction.
- **Rust** uses the `gen2::Value` enum — `Value::Object`, `Value::Array`, `Value::Int(i64)`,
  `Value::String`, `Value::Float(f64)`. The enum variant *is* the type tag.
- **JavaScript** must wrap input with `fromAny(...)` before encoding (and unwrap with `toAny(...)`
  after decoding). **This is the load-bearing difference:** a JS `number` cannot distinguish an
  integer from a float — `1` and `1.0` are the same `number`. If JS values were encoded directly,
  `1` could canonicalize as an int in one program and a float in another, breaking the
  "equal value ⇒ equal bytes" invariant. `fromAny` resolves the int/float ambiguity once, up front,
  so the resulting `Value` is unambiguous and canonical.

In every binding the result of construction is the same canonical `Value`, so `encode` produces the
same bytes and `content_address` produces the same hash.

---

## Cross-language identity demo

Encode `{"name": "Alice", "scores": [1, 2, 3]}` in any of the four implementations and you get the
**same content address** — proof of the invariant:

```
122091f7d42a00c157c37f0929b15e90d8c785dbe50581cc651aafc338f6e5e8aad1
```

| Language | Call                                         | Hex content address |
|----------|----------------------------------------------|---------------------|
| Python   | `content_address(value).hex()`               | `122091f7…e5e8aad1` |
| JS       | `Buffer.from(contentAddress(v)).toString("hex")` | `122091f7…e5e8aad1` |
| Go       | `cowrie.ContentAddressHex(v)`                | `122091f7…e5e8aad1` |
| Rust     | `gen2::content_address_hex(&v)?`             | `122091f7…e5e8aad1` |

Same value, same canonical bytes, same hash — across four languages, four runtimes, byte for byte.
That is the whole point of Cowrie.

This identity is enforced, not aspirational: 11 standing conformance gates
(`bash tools/run_all_gates.sh`, plus CI in `.github/workflows/conformance.yml`) fail the build the
moment any implementation drifts. Green is law.
