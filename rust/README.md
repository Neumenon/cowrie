# cowrie-rs

Rust implementation of **Cowrie v1** — a deterministic, content-addressable, AI-native binary codec.

Cowrie's core invariant: **equal value ⇒ equal canonical bytes ⇒ equal hash**, byte-identical across
Python, Go, Rust, and TypeScript. Magic `COWR`, format version 1. The content address is a multihash
SHA-256 of the canonical wire bytes (see [SPEC §3](../docs/SPEC-v1.md)).

> **Status:** `0.9.0` — pre-1.0 release candidate. The wire format is **not yet frozen** and may change
> before 1.0.

## Install

Nothing is published to crates.io yet. **Today**, depend on the crate from the `phase0-spec` branch:

```toml
# Cargo.toml
[dependencies]
cowrie-rs = { git = "https://github.com/Neumenon/cowrie.git", branch = "phase0-spec" }
```

After the first publish to crates.io you'll be able to use the one-liner instead:

```bash
cargo add cowrie-rs   # only works AFTER publish — not yet available
```

## Usage

```rust
use cowrie_rs::gen2;

// Build a value, encode to canonical wire bytes, and compute its content address.
let v = gen2::Value::String("Alice".into());
let wire = gen2::encode(&v)?;

let addr = gen2::address_of_bytes(&wire); // multihash SHA-256 of the wire bytes
// or directly from the value:
let addr2 = gen2::content_address(&v)?;

let dec = gen2::decode(&wire)?;
assert_eq!(dec, v);
# Ok::<(), cowrie_rs::gen2::CowrieError>(())
```

Encoding the canonical document `{"name":"Alice","scores":[1,2,3]}` produces the content address

```
122091f7d42a00c157c37f0929b15e90d8c785dbe50581cc651aafc338f6e5e8aad1
```

in every implementation — Python, Go, Rust, and TypeScript all agree, byte for byte.

## Cross-language

Cowrie is a monorepo with one canonical wire format and four implementations:

| Language   | Package / module                | Source dir    |
|------------|---------------------------------|---------------|
| Rust       | `cowrie-rs` (crates.io)         | `rust/`       |
| Python     | `cowrie-ref` (PyPI)             | `tools/`      |
| TypeScript | `cowrie-codec` (npm)            | `typescript/` |
| Go         | `github.com/Neumenon/cowrie/go` | `go/`         |

## Docs

- [Root README](../README.md) — project overview and the other implementations.
- [docs/SPEC-v1.md](../docs/SPEC-v1.md) — the authoritative v1 wire-format specification.

## License

MIT
