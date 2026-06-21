# Release Checklist

## Current Package Names & Versions

| Registry | Package | Version | Manifest |
|----------|---------|---------|----------|
| npm | `cowrie-codec` | 2.1.2 | `typescript/package.json` |
| PyPI | `cowrie-py` | 2.1.2 | `python/pyproject.toml` |
| crates.io | `cowrie-rs` | 2.1.2 | `rust/Cargo.toml` |
| Go | `github.com/Neumenon/cowrie/go/v2` | `v2.1.2` (tagged as `go/v2.1.2`) | `go/go.mod` |

Go is released under two tags pushed together: the canonical `v<version>` tag (e.g. `v2.1.2`) and the subdir-module tag `go/v<version>` (e.g. `go/v2.1.2`) that the Go module proxy resolves for `github.com/Neumenon/cowrie/go/v2`.

---

## Release Flow

**`./release.sh` is the single release authority.** CI publishes all registries automatically when the `v*` tag lands — manual per-registry publish commands are not used and would double-publish against the tag-triggered CI.

### 1. Ensure manifests are at the target version

The three registry manifests must declare the target patch version before running the script:

- `typescript/package.json`
- `python/pyproject.toml`
- `rust/Cargo.toml`

The Go module does not carry the patch version in `go/go.mod`; it must keep the `/v2` major suffix and the release script pushes the required `go/v<version>` subdir tag.

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
2. Verifies registry manifests declare `<version>` and both release tags are unused.
3. Runs spec/fixture manifest validators.
4. Runs static gates (`go vet`, Python mypy, TypeScript typecheck/build, Rust clippy).
5. Runs all 4 language test suites (Go, Python, TypeScript, Rust).
6. Runs the 55-case cross-language fixture harness.
7. Runs package probes (`npm pack --dry-run`, `cargo publish --dry-run`, Python build + twine check).
8. Pushes `v<version>` and `go/v<version>` tags.

### 4. CI publishes all registries

On the `v*` tag, CI triggers automatically:

- **`ci.yml`** → publishes `cowrie-codec` to npm and `cowrie-rs` to crates.io.
- **`publish-pypi.yml`** → publishes `cowrie-py` to PyPI as Linux/macOS native wheels built with `cibuildwheel` (the Cython `_cext` accelerator, built against NumPy 2.x), plus an sdist fallback. Each wheel is smoke-tested with `COWRIE_REQUIRE_NATIVE=1`, so a tag cannot publish unless the native extension builds and loads. Windows installs use the pure-Python fallback until native MSVC/zlib wheels are validated. NumPy is a runtime dependency.

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

# PyPI (Linux/macOS native wheels should load the Cython accelerator)
pip install cowrie-py==<version> && COWRIE_REQUIRE_NATIVE=1 python -c "from cowrie.gen2 import _HAS_NATIVE; assert _HAS_NATIVE; import cowrie; print('ok')"

# crates.io  (search.crates.io updates within a few minutes)
cargo add cowrie-rs@<version>

# Go
go get github.com/Neumenon/cowrie/go/v2@v<version>
```

---

## Known Issues

1. **Windows PyPI native wheels are not published in v2.1.2**: Windows users install from the sdist and use the pure-Python fallback until native MSVC/zlib wheels are validated.
