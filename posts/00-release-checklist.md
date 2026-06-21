# Release Checklist

## Current Package Names & Versions

| Registry | Package | Version | Manifest |
|----------|---------|---------|----------|
| npm | `cowrie-codec` | 2.1.2 | `typescript/package.json` |
| PyPI | `cowrie-py` | 2.1.2 | `python/pyproject.toml` |
| crates.io | `cowrie-rs` | 2.1.2 | `rust/Cargo.toml` |
| Go | `github.com/Neumenon/cowrie/go/v2` | `go/v2.1.2` | `go/go.mod` |

Go is released under two tags pushed together: the canonical `v<version>` tag (e.g. `v2.1.2`) and the subdir-module tag `go/v<version>` (e.g. `go/v2.1.2`) that the Go module proxy resolves for `github.com/Neumenon/cowrie/go/v2`.

---

## Release Flow

**`./release.sh` is the single release authority.** CI publishes all registries automatically when the `v*` tag lands — manual per-registry publish commands are not used and would double-publish against the tag-triggered CI.

### 1. Ensure manifests are at the target version

All four manifests must declare the same version before running the script:

- `typescript/package.json`
- `python/pyproject.toml`
- `rust/Cargo.toml`
- `go/go.mod` (module path version suffix must match the major)

### 2. Sync to clean main

```bash
git checkout main && git pull --ff-only origin main
```

The release script enforces this — it will abort if the working tree is dirty or if `main` is not in sync with `origin/main`.

### 3. Run the release script

```bash
./release.sh <version>   # e.g. ./release.sh 2.1.2
```

The script runs a PREFLIGHT + TAG-ONLY sequence:

1. Asserts `main` is clean and in sync with origin.
2. Verifies every manifest declares `<version>`.
3. Runs all 4 language test suites (Go, Python, TypeScript, Rust).
4. Runs the 47-case cross-language fixture harness.
5. Pushes `v<version>` and `go/v<version>` tags.

### 4. CI publishes all registries

On the `v*` tag, CI triggers automatically:

- **`ci.yml`** → publishes `cowrie-codec` to npm and `cowrie-rs` to crates.io.
- **`publish-pypi.yml`** → publishes `cowrie-py` to PyPI as native wheels built with `cibuildwheel` (the Cython `_cext` accelerator, built against NumPy 2.x), plus an sdist fallback. Each wheel is smoke-tested with `COWRIE_REQUIRE_NATIVE=1`, so a tag cannot publish unless the native extension builds and loads. NumPy is a runtime dependency.

No manual publish commands needed.

### 5. (Optional) Create a GitHub release

```bash
gh release create v<version> --generate-notes
```

---

## Post-Release Verification

Confirm the packages landed correctly:

```bash
# npm
npm install cowrie-codec@<version> && node -e "require('cowrie-codec'); console.log('ok')"

# PyPI
pip install cowrie-py==<version> && python -c "import cowrie; print('ok')"

# crates.io  (search.crates.io updates within a few minutes)
cargo add cowrie-rs@<version>

# Go
go get github.com/Neumenon/cowrie/go/v2@go/v<version>
```

---

## Known Issues

1. **Shard Python/C not implemented**: Only npm and crates.io packages exist for Shard.
