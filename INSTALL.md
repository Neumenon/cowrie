# Installing Cowrie

Cowrie is a deterministic, content-addressable, AI-native binary codec.
The invariant: **equal value ⇒ equal canonical bytes ⇒ equal hash**, byte-identical
across Python, Go, Rust, and TypeScript. (Magic `COWR`, version 1; content address =
multihash SHA-256.)

> **Version: 0.9.0 — pre-1.0 release candidate.** The wire format is **not yet frozen**.
> Pin exact versions and expect breaking changes before 1.0.

Cowrie is a monorepo (`github.com/Neumenon/cowrie`). The clean v1 tree lives on branch
**`phase0-spec`**. Each language has its own subdirectory:

| Language   | Subdir        | Package          | Registry  |
|------------|---------------|------------------|-----------|
| Python     | `tools/`      | `cowrie-ref`     | PyPI      |
| TypeScript | `typescript/` | `cowrie-codec`   | npm       |
| Rust       | `rust/`       | `cowrie-rs`      | crates.io |
| Go         | `go/`         | `github.com/Neumenon/cowrie/go` | Go modules |

---

## Today (from source / git, pre-publish)

**Nothing is published to a registry yet.** Install directly from the `phase0-spec` branch.

### Python

```sh
pip install "git+https://github.com/Neumenon/cowrie.git@phase0-spec#subdirectory=tools"
```

Gives you the `cowrie` console script **and** `import cowrie_ref`.

### Go

```sh
go get github.com/Neumenon/cowrie/go@phase0-spec
```

A true one-liner — Go modules are git-native, so the subdir module resolves directly.

### Rust

Add to `Cargo.toml`:

```toml
[dependencies]
cowrie-rs = { git = "https://github.com/Neumenon/cowrie.git", branch = "phase0-spec" }
```

### TypeScript / JavaScript — the rough edge

**npm cannot install a git subdirectory.** There is no one-liner; you must clone, build,
and pack a tarball, then install that:

```sh
git clone -b phase0-spec https://github.com/Neumenon/cowrie.git
cd cowrie/typescript
npm install
npm run build
npm pack            # produces cowrie-codec-0.9.0.tgz
```

Then, from your own project:

```sh
npm install /path/to/cowrie/typescript/cowrie-codec-0.9.0.tgz
```

---

## After publish (registries)

Once the packages are published, installation is the standard per-ecosystem command:

```sh
pip install cowrie-ref                              # Python
npm install cowrie-codec                            # TypeScript / JS
cargo add cowrie-rs                                 # Rust
go get github.com/Neumenon/cowrie/go@v0.9.0         # Go
```

> Go release tags take the form `go/vX.Y.Z` (subdir module). Use `@v0.9.0` once tagged.

---

## Quick sanity check

The APIs are idiomatic per language and **differ** (notably: JS needs `fromAny` first,
because JS numbers can't distinguish int from float). The snippet below encodes
`{"name":"Alice","scores":[1,2,3]}`; all four implementations emit the same content
address: `122091f7d42a00c157c37f0929b15e90d8c785dbe50581cc651aafc338f6e5e8aad1`.

### Python

```python
from cowrie_ref import encode, decode, content_address, fingerprint

blob = encode({"name": "Alice", "scores": [1, 2, 3]})
value = decode(blob)
addr = content_address({"name": "Alice", "scores": [1, 2, 3]}).hex()
```

CLI:

```sh
cowrie recode [--strict|--addr|--file-id|--file-recode|--tensor-spans|--fingerprint|--dataset-root] < wire
```

### TypeScript / JavaScript

```js
const { fromAny, encode, decode, toAny, contentAddress } = require("cowrie-codec");

const v = fromAny({ name: "Alice", scores: [1, 2, 3] });
const wire = encode(v);
const value = toAny(decode(wire));
const addr = contentAddress(v);
```

The root export also includes `schemaFingerprint64`, `fileIdentity`, `merkleRoot`,
`decodeFile`, `encodeFile`, `tensorSpans`, `addressOfBytes`, and `contentAddressHex`.

### Go

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

### Rust

```rust
use cowrie_rs::gen2;

let v = gen2::Value::String("Alice".into());
let wire = gen2::encode(&v)?;
let addr = gen2::address_of_bytes(&wire);   // also gen2::content_address(&v)
let dec = gen2::decode(&wire)?;
```

---

## Verifying a build

Cross-language conformance is enforced by 11 standing gates:

```sh
bash tools/run_all_gates.sh
```

CI runs these on every change (`.github/workflows/conformance.yml`) — green is law, no
merge while red.
