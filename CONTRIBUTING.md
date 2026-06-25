# Contributing

Thanks for helping improve Cowrie. This repository contains four maintained implementations: Go, Rust, Python, and TypeScript.

## Ground rules

- Keep wire-format behavior deterministic and cross-language compatible.
- Do not trust README claims when changing behavior; update fixtures/specs/tests together.
- Gen2 reserved tags `0x30-0x32` and `0x39` must be skipped by decoders and not emitted by encoders.
- NaN/Inf are allowed in Cowrie binary encoding and rejected by the JSON bridge.

## Before opening a PR

Run the gates relevant to your change:

```bash
# Go
cd go && go vet ./... && go test ./...

# Rust
cd rust && cargo clippy --all-targets -- -D warnings && cargo test

# Python
cd python && pip install -e ".[dev]" && mypy cowrie/ --ignore-missing-imports && pytest tests/

# TypeScript
cd typescript && npm ci && npm run build && npm test
```

For wire-format changes, run the full cross-language conformance suite from the repo root — the same
gates CI enforces (Python reference + Go + Rust + TS must agree byte-for-byte):

```bash
(cd go && go build -o /tmp/cowrie-cli ./cmd/cowrie)
(cd rust && cargo build --release --example recode)
bash tools/run_all_gates.sh
```

This runs all 11 gates: count-guard, the Python-reference tests, positive conformance, content-address
parity, file-identity parity, tensor-view parity, §4 fingerprint parity, dataset-identity parity,
negative (lenient + strict), and the differential fuzzer.

## Adding fixture cases

1. Generate or add the `.cowrie`/JSON input under `testdata/fixtures/`.
2. Add a manifest entry with `kind` (`decode`, `decode_framed`, or `from_json`).
3. Add expected JSON where there is a canonical projection.
4. Add pinned parity coverage in Rust and TypeScript when the shared harness does not execute that language directly.
