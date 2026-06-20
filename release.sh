#!/bin/bash
set -euo pipefail

# Cowrie release driver — PREFLIGHT + TAG ONLY.
#
# This script does NOT publish to any registry. Publishing is owned entirely by
# the tag-triggered CI workflows:
#   - ci.yml          -> npm + crates.io   (on the v* tag)
#   - publish-pypi.yml -> PyPI             (on the v* tag, gated by the test matrix)
# A single release authority avoids the double-publish race that manual
# `cargo publish` / `twine upload` / `npm publish` from this script would cause
# (the v* tag this script pushes triggers those workflows).
#
# Before tagging, this script guarantees:
#   - you are on a clean `main` that is in sync with origin/main
#   - every package manifest already declares ${VERSION}
#   - all four language test suites pass
#   - the cross-language fixture harness passes (the strongest conformance gate)
# Then it pushes the release tags and hands off to CI.

VERSION="${1:?Usage: ./release.sh <version> (e.g., 2.1.2)}"

echo "=== Cowrie Release v${VERSION} (preflight + tag) ==="
echo ""

# Step 0: repo must be a clean main in sync with origin, tag must be new.
echo "--- Checking repository state ---"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
[[ "$BRANCH" == "main" ]]            || { echo "FAIL: release from 'main' (on '$BRANCH')"; exit 1; }
[[ -z "$(git status --porcelain)" ]] || { echo "FAIL: working tree is dirty — commit/stash first"; exit 1; }
git fetch origin main --tags
[[ "$(git rev-parse @)" == "$(git rev-parse origin/main)" ]] \
    || { echo "FAIL: local main != origin/main — pull/push first"; exit 1; }
! git rev-parse "v${VERSION}" >/dev/null 2>&1 \
    || { echo "FAIL: tag v${VERSION} already exists"; exit 1; }
echo "On main, clean, in sync with origin; v${VERSION} is unused."
echo ""

# Step 1: every manifest must already declare ${VERSION} (no surprise mismatches).
echo "--- Checking manifest versions ---"
check_version() {
    [[ "$2" == "$VERSION" ]] || { echo "FAIL: $1 declares '$2', expected '$VERSION'"; exit 1; }
    echo "  $1: $2 OK"
}
check_version "rust/Cargo.toml"         "$(grep -m1 '^version' rust/Cargo.toml | sed 's/.*"\(.*\)".*/\1/')"
check_version "python/pyproject.toml"   "$(grep -m1 '^version' python/pyproject.toml | sed 's/.*"\(.*\)".*/\1/')"
check_version "typescript/package.json" "$(grep -m1 '"version"' typescript/package.json | sed 's/.*"version"[^"]*"\([^"]*\)".*/\1/')"
echo ""

# Step 2: all four language test suites.
echo "--- Running all test suites ---"
echo "[Go]";         (cd go && go test ./...)               || { echo "FAIL: Go tests"; exit 1; }
echo "[Rust]";       (cd rust && cargo test)                || { echo "FAIL: Rust tests"; exit 1; }
echo "[Python]";     (cd python && python -m pytest tests/) || { echo "FAIL: Python tests"; exit 1; }
echo "[TypeScript]"; (cd typescript && npm test)            || { echo "FAIL: TypeScript tests"; exit 1; }
echo ""

# Step 3: cross-language fixture harness — the strongest conformance gate.
echo "--- Cross-language fixture harness ---"
GO_CLI="$(mktemp -d)/cowrie-cli"
(cd go && go build -o "$GO_CLI" ./cmd/cowrie) || { echo "FAIL: building Go CLI"; exit 1; }
GO_CLI="$GO_CLI" python3 testdata/fixtures/validate_fixtures.py \
    || { echo "FAIL: cross-language fixture harness"; exit 1; }
echo ""
echo "All gates passed."
echo ""

# Step 4: confirm, then tag. The v* tag triggers the publish workflows.
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
