# cowrie-rs

Rust implementation of Cowrie, a deterministic binary codec for JSON-like and ML-native data.

## Install

```bash
cargo add cowrie-rs
```

## Usage

```rust
use cowrie_rs::gen2;

let value = gen2::Value::String("Alice".into());
let encoded = gen2::encode(&value)?;
let decoded = gen2::decode(&encoded)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

See the main repository for the wire spec and cross-language conformance notes: https://github.com/Neumenon/cowrie
