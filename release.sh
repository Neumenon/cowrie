#!/bin/bash
set -euo pipefail

# Cowrie release driver — PREFLIGHT + TAG ONLY.
#
# This script does NOT publish to any registry. Publishing is owned entirely by
# the tag-triggered CI workflows:
#   - ci.yml            -> npm + crates.io (on the v* tag)
#   - publish-pypi.yml  -> PyPI            (on the v* tag, gated by test/build jobs)
# A single release authority avoids the double-publish race that manual
# `cargo publish` / `twine upload` / `npm publish` from this script would cause.
#
# Before tagging, this script guarantees:
#   - you are on a clean `main` that is in sync with origin/main
#   - registry package manifests already declare ${VERSION}
#   - both release tags (`v${VERSION}` and `go/v${VERSION}`) are unused
#   - spec/fixture validators, static gates, tests, fixtures, and package probes pass
# Then it pushes the release tags and hands off to CI.

VERSION="${1:?Usage: ./release.sh <version> (e.g., 2.1.2)}"
TMPDIR_RELEASE="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_RELEASE"' EXIT

echo "=== Cowrie Release v${VERSION} (preflight + tag) ==="
echo ""

# Step 0: repo must be a clean main in sync with origin, tags must be new.
echo "--- Checking repository state ---"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
[[ "$BRANCH" == "main" ]]            || { echo "FAIL: release from 'main' (on '$BRANCH')"; exit 1; }
[[ -z "$(git status --porcelain)" ]] || { echo "FAIL: working tree is dirty — commit/stash first"; exit 1; }
git fetch origin main --tags
[[ "$(git rev-parse @)" == "$(git rev-parse origin/main)" ]] \
    || { echo "FAIL: local main != origin/main — pull/push first"; exit 1; }
for tag in "v${VERSION}" "go/v${VERSION}"; do
    ! git rev-parse "$tag" >/dev/null 2>&1 \
        || { echo "FAIL: tag $tag already exists"; exit 1; }
done
echo "On main, clean, in sync with origin; v${VERSION} and go/v${VERSION} are unused."
echo ""

# Step 1: registry manifests must already declare ${VERSION}.
echo "--- Checking manifest versions ---"
check_version() {
    [[ "$2" == "$VERSION" ]] || { echo "FAIL: $1 declares '$2', expected '$VERSION'"; exit 1; }
    echo "  $1: $2 OK"
}
check_version "rust/Cargo.toml"         "$(python3 - <<'PY'
import pathlib, re
m = re.search(r'^version\s*=\s*"([^"]+)"', pathlib.Path('rust/Cargo.toml').read_text(), re.M)
print(m.group(1) if m else '')
PY
)"
check_version "python/pyproject.toml"   "$(python3 - <<'PY'
import pathlib, tomllib
print(tomllib.loads(pathlib.Path('python/pyproject.toml').read_text())['project']['version'])
PY
)"
check_version "typescript/package.json" "$(python3 - <<'PY'
import json, pathlib
print(json.loads(pathlib.Path('typescript/package.json').read_text())['version'])
PY
)"
echo ""

# Step 2: spec and fixture manifest validators.
echo "--- Validating spec and fixture manifest ---"
python3 tools/spec/verify_sections.py
python3 tools/fixtures/validate_manifest.py
echo ""

# Step 3: static gates mirror CI fail-fast checks.
echo "--- Running static gates ---"
echo "[Go vet]";         (cd go && go vet ./...) || { echo "FAIL: go vet"; exit 1; }
echo "[Python mypy]";    (cd python && python -m pip install -q -e ".[dev]" && python -m mypy cowrie/ --ignore-missing-imports) || { echo "FAIL: Python mypy"; exit 1; }
echo "[TypeScript]";     (cd typescript && npm ci && npm run build && npx tsc --noEmit) || { echo "FAIL: TypeScript build/typecheck"; exit 1; }
echo "[Rust clippy]";    (cd rust && cargo clippy --all-targets -- -D warnings) || { echo "FAIL: Rust clippy"; exit 1; }
echo ""

# Step 4: all four language test suites.
echo "--- Running all test suites ---"
echo "[Go]";         (cd go && go test ./...)               || { echo "FAIL: Go tests"; exit 1; }
echo "[Rust]";       (cd rust && cargo test)                || { echo "FAIL: Rust tests"; exit 1; }
echo "[Python]";     (cd python && python -m pytest tests/) || { echo "FAIL: Python tests"; exit 1; }
echo "[TypeScript]"; (cd typescript && npm test)            || { echo "FAIL: TypeScript tests"; exit 1; }
echo ""

# Step 5: cross-language fixture harness — the strongest conformance gate.
echo "--- Cross-language fixture harness ---"
GO_CLI="$TMPDIR_RELEASE/cowrie-cli"
(cd go && go build -o "$GO_CLI" ./cmd/cowrie) || { echo "FAIL: building Go CLI"; exit 1; }
GO_CLI="$GO_CLI" python3 testdata/fixtures/validate_fixtures.py \
    || { echo "FAIL: cross-language fixture harness"; exit 1; }
echo ""

# Step 6: package probes for every publish surface.
echo "--- Package probes ---"
echo "[npm pack]";        (cd typescript && npm pack --dry-run) || { echo "FAIL: npm pack"; exit 1; }
echo "[cargo dry-run]";   (cd rust && cargo package --list > "$TMPDIR_RELEASE/cargo-package-files.txt" && cargo publish --dry-run) || { echo "FAIL: cargo publish --dry-run"; exit 1; }
echo "[Python build]";    (cd python && python -m pip install -q build twine && python -m build --outdir "$TMPDIR_RELEASE/python-dist" && python -m twine check "$TMPDIR_RELEASE"/python-dist/*) || { echo "FAIL: Python build/twine check"; exit 1; }
echo ""

echo "All gates passed."
echo ""

# Step 7: confirm, then tag. The v* tag triggers the publish workflows.
echo "--- Pre-tag checklist ---"
echo "  Version: ${VERSION}"
echo "  Go:      github.com/Neumenon/cowrie/go/v2@v${VERSION}  (tag: go/v${VERSION})"
echo "  Rust:    cowrie-rs@${VERSION}    (published by CI)"
echo "  Python:  cowrie-py==${VERSION}   (published by CI)"
echo "  TS:      cowrie-codec@${VERSION} (published by CI)"
echo ""
read -p "Create and push release tags? CI will publish all registries. (y/N) " confirm
[[ "$confirm" == "y" || "$confirm" == "Y" ]] || { echo "Aborted."; exit 0; }

# Nested Go module tagging: the module is github.com/Neumenon/cowrie/go/v2
# (subdirectory "go/"). Go's proxy resolves it from a tag named go/v<version>,
# NOT the bare v<version> tag. We push BOTH:
#   v${VERSION}    — canonical release tag; triggers CI publish (npm/crates/PyPI)
#   go/v${VERSION} — required for `go get .../go/v2@v${VERSION}` (does NOT trigger
#                    the publish workflows, which match v* only)
echo ""
echo "--- Tagging v${VERSION} and go/v${VERSION} ---"
git tag "v${VERSION}"
git tag "go/v${VERSION}"
git push origin "v${VERSION}" "go/v${VERSION}"

echo ""
echo "=== Tags pushed. CI is now publishing v${VERSION}. ==="
echo "Watch:  gh run watch   (or the Actions tab)"
echo "Go:     go get github.com/Neumenon/cowrie/go/v2@v${VERSION}"
echo "Notes:  gh release create v${VERSION} --title 'Cowrie v${VERSION}' --notes-file CHANGELOG.md"
