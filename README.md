# Cowrie — deterministic, content-addressable binary codec for AI data

[![conformance](https://github.com/Neumenon/cowrie/actions/workflows/conformance.yml/badge.svg)](https://github.com/Neumenon/cowrie/actions/workflows/conformance.yml)

Cowrie v1 (`COWR`, version 1) is a **deterministic, content-addressable, AI-native binary codec**.
It exists to make data identity exact and portable: there is exactly **one** canonical byte-string
per value, it is the same in every language, and its hash is its address.

## The identity invariant

> **equal value ⇒ equal canonical bytes ⇒ equal hash — byte-identical across Python, Go, Rust, and TypeScript.**

Canonicalization is **mandatory and decoder-enforced** (unlike CBOR's optional canonical mode): there
is no second valid encoding of the same value. The content address is a multihash SHA-256 over the
canonical bytes (spec §3). As proof, all four implementations emit the same content address for
`{"name":"Alice","scores":[1,2,3]}`:

```
122091f7d42a00c157c37f0929b15e90d8c785dbe50581cc651aafc338f6e5e8aad1
```

That invariant is what makes Cowrie useful as a substrate for content-addressable storage, dedup,
cross-service caching, and tamper-evident dataset identity.

## At a glance — install + usage

`@phase0-spec` installs the clean v1 codec from the branch being promoted to `main`. Once published,
swap to the registry forms shown below each cell. See **[INSTALL.md](INSTALL.md)** and
**[docs/QUICKSTART.md](docs/QUICKSTART.md)** for full detail (per-platform notes, publishing, CLI flags).

| Language | Install (today, from branch) | Install (after publish) |
| --- | --- | --- |
| **Python** | `pip install "git+https://github.com/Neumenon/cowrie.git@phase0-spec#subdirectory=tools"` | `pip install cowrie-ref` |
| **Go** | `go get github.com/Neumenon/cowrie/go@phase0-spec` | `go get github.com/Neumenon/cowrie/go@v0.9.0` |
| **Rust** | `cowrie-rs = { git = "https://github.com/Neumenon/cowrie.git", branch = "phase0-spec" }` | `cargo add cowrie-rs` |
| **JS/TS** | clone + build + pack (see below) | `npm install cowrie-codec` |

> **JS note:** npm cannot install a git subdirectory. Build the tarball once:
> ```bash
> git clone -b phase0-spec https://github.com/Neumenon/cowrie.git
> cd cowrie/typescript && npm install && npm run build && npm pack   # -> cowrie-codec-0.9.0.tgz
> npm install /path/to/cowrie-codec-0.9.0.tgz
> ```

The APIs are idiomatic per language and differ deliberately — Python takes plain objects, JS needs
`fromAny` (its numbers can't distinguish int from float), Go and Rust use explicit value constructors.

**Python** — plain objects go in directly:

```python
from cowrie_ref import encode, decode, content_address, fingerprint

blob = encode({"name": "Alice", "scores": [1, 2, 3]})
value = decode(blob)
addr = content_address({"name": "Alice", "scores": [1, 2, 3]}).hex()
```

**TypeScript / JavaScript** — wrap with `fromAny` first:

```javascript
const { fromAny, encode, decode, toAny, contentAddress } = require("cowrie-codec");

const v = fromAny({ name: "Alice", scores: [1, 2, 3] });
const wire = encode(v);
const value = toAny(decode(wire));
const addr = contentAddress(v);
```

**Go** — explicit value constructors:

```go
import cowrie "github.com/Neumenon/cowrie/go"

v := cowrie.Object(
    cowrie.Member{Key: "name", Value: cowrie.String("Alice")},
    cowrie.Member{Key: "scores", Value: cowrie.Array(
        cowrie.Int64(1), cowrie.Int64(2), cowrie.Int64(3))},
)
data, _ := cowrie.Encode(v)
addr := cowrie.AddressOfBytes(data)
dec, _ := cowrie.Decode(data)
```

**Rust**:

```rust
use cowrie_rs::gen2;

let v = gen2::Value::String("Alice".into());
let wire = gen2::encode(&v)?;
let addr = gen2::address_of_bytes(&wire);   // or gen2::content_address(&v)
let dec = gen2::decode(&wire)?;
```

The Python package also installs a `cowrie` CLI:

```bash
cowrie recode [--strict|--addr|--file-id|--file-recode|--tensor-spans|--fingerprint|--dataset-root] < wire
```

## Conformance — green is law

Cross-language parity is **machine-enforced**, not asserted. The Python reference is the executable
oracle; Go, Rust, and TypeScript must agree with it byte-for-byte. Eleven standing gates run on every
push via CI (`.github/workflows/conformance.yml`), and a red gate **blocks merge**. Run them locally
with the single source-of-truth runner:

```bash
bash tools/run_all_gates.sh
```

The gates cover positive conformance, content-address (§3), file/Merkle identity (§7), zero-copy
tensor spans, the structural fingerprint (§4), dataset identity, strict + lenient rejection of
malformed/non-canonical input (exact `ERR_*` codes), differential fuzzing, and a canary that proves a
1-byte corruption turns the gate red. Full status and how each count was verified live in
**[CONFORMANCE.md](CONFORMANCE.md)**.

## Specification

**[docs/SPEC-v1.md](docs/SPEC-v1.md)** is the authoritative, reference-executable wire format. It is the
single source of truth for the bytes; this README is only an introduction.

## Status

**0.9.0 — pre-1.0 release candidate.** All four implementations are at 0.9.0 and pass every conformance
gate. The wire format is **not yet frozen**: it is stable and fixture-pinned, but reserves the right to
change before 1.0. New capabilities are added as profiles/conventions over the locked Core, never as new
wire tags. Registry packages: PyPI `cowrie-ref`, npm `cowrie-codec`, crates.io `cowrie-rs`, Go module
`github.com/Neumenon/cowrie/go`.

## License

MIT
