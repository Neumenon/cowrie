# Release Checklist

## Current Package Names & Versions

### Cowrie (`cowrie-final/`)
| Registry | Package | Version | File |
|----------|---------|---------|------|
| npm | `cowrie-codec` | 0.1.1 | `typescript/package.json` |
| PyPI | `cowrie-py` | 0.1.1 | `python/pyproject.toml` |
| crates.io | `cowrie-rs` | 0.1.1 | `rust/Cargo.toml` |
| Go | `github.com/Neumenon/cowrie` | untagged | `go/go.mod` |
| C | (source-only) | 0.1.0 | `c/CMakeLists.txt` |

### Glyph (`glyph/`)
| Registry | Package | Version | File |
|----------|---------|---------|------|
| npm | `cowrie-glyph` | 0.3.0 | `js/package.json` |
| PyPI | `glyph-py` | 1.0.0 | `py/pyproject.toml` |
| crates.io | `glyph-rs` | 0.1.0 | `rust/glyph-codec/Cargo.toml` |
| Go | `github.com/Neumenon/glyph` | untagged | `go/go.mod` |

### Shard (`Agent-GO/cowrie/shard-*`)
| Registry | Package | Version | File |
|----------|---------|---------|------|
| npm | `shard-format` | 0.1.0 | `shard-ts/package.json` |
| crates.io | `shard-format` | 0.1.0 | `shard-rs/Cargo.toml` |
| Go | embedded in ucodec | n/a | n/a |
| PyPI | not yet | - | - |
| C | not yet | - | - |

---

## Pre-Release

### 1. Version Bump
Bump version in ALL files for the target package:
- `package.json` (npm)
- `pyproject.toml` (PyPI)
- `Cargo.toml` (crates.io)
- Git tag (Go)

### 2. Run Tests (ALL languages must pass)

**Cowrie:**
```bash
cd cowrie-final
cd go && go test ./... && cd ..
cd python && pip install -e ".[dev]" && pytest tests/ && cd ..
cd typescript && npm ci && npm test && cd ..
cd rust && cargo test && cd ..
cd c && mkdir -p build && cd build && cmake .. && make && ctest --output-on-failure && cd ../..
```

**Glyph:**
```bash
cd glyph
cd go && go test ./... && cd ..
cd py && pytest tests/ && cd ..
cd js && npm ci && npm test && cd ..
cd rust/glyph-codec && cargo test && cd ../..
cd c/glyph-codec && make clean && make test && cd ../..
```

### 3. Git Tag
```bash
git tag -a v0.X.Y -m "Release v0.X.Y"
git push origin v0.X.Y
```

---

## Publish Order

Publish in this order (npm first — easiest to unpublish if broken; crates.io last — permanent):

### 1. npm
```bash
cd typescript && npm publish --access public
```

### 2. PyPI
```bash
cd python && python -m build && twine upload dist/*
```

### 3. crates.io
```bash
cd rust && cargo publish
```

### 4. Go
Just needs the git tag pushed. Go proxy picks it up automatically.

---

## Post-Release

### Verify installs
```bash
# Cowrie
npm install cowrie-codec && node -e "const c = require('cowrie-codec'); console.log('ok')"
pip install cowrie-py && python -c "import cowrie; print('ok')"
cargo add cowrie-rs

# Glyph
npm install cowrie-glyph && node -e "const g = require('cowrie-glyph'); console.log('ok')"
pip install glyph-py && python -c "import glyph; print('ok')"
cargo add glyph-rs
```

### Update Agent-GO
After tagging a glyph release, update `Agent-GO/go.mod`:
```
# Replace local path with tagged version
require github.com/Neumenon/glyph v0.X.Y
# Remove: replace github.com/Neumenon/glyph => ../glyph/go
```

### GitHub Release
Create release on GitHub with changelog notes.

---

## Known Issues

1. **Stale glyph copies in cowrie-final**: `cowrie-final/typescript/glyph/` (`glyph-codec`) and `cowrie-final/python/glyph/` (`glyph-serial`) are dead code — never published. Canonical packages publish from `glyph/` repo. Consider deleting these stale dirs.

2. **Glyph go.mod has stale replace**: `glyph/go/go.mod` has `replace github.com/phenomenon0/Agent-GO => ../../Agent-GO` (for `bridge.go` build tag). Must remove before tagging for Go proxy.

3. **Shard Python/C not implemented**: Only npm and crates.io packages exist.

4. **Existing release script**: `cowrie-final/release.sh` exists — check if it's up to date before using.
