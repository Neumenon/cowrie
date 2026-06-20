#!/bin/bash
set -euo pipefail

VERSION="${1:?Usage: ./release.sh <version> (e.g., 0.1.2)}"

echo "=== Cowrie Release v${VERSION} ==="
echo ""

# Step 1: Run all tests
echo "--- Running all test suites ---"
echo "[Go]"
(cd go && go test ./...) || { echo "FAIL: Go tests"; exit 1; }
echo "[Rust]"
(cd rust && cargo test) || { echo "FAIL: Rust tests"; exit 1; }
echo "[Python]"
(cd python && python -m pytest tests/) || { echo "FAIL: Python tests"; exit 1; }
echo "[TypeScript]"
(cd typescript && npm test) || { echo "FAIL: TypeScript tests"; exit 1; }
echo ""
echo "All tests passed."
echo ""

# Step 2: Confirm version
echo "--- Pre-publish checklist ---"
echo "  Version: ${VERSION}"
echo "  Go:      github.com/Neumenon/cowrie/go/v2@v${VERSION}  (tag: go/v${VERSION})"
echo "  Rust:    cowrie-rs@${VERSION}"
echo "  Python:  cowrie-py==${VERSION}"
echo "  TS:      cowrie-codec@${VERSION}"
echo ""
read -p "Proceed with publishing? (y/N) " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Aborted."
    exit 0
fi

# Step 3: Publish to registries
# All publishes must succeed before tagging. Any failure exits immediately.
echo ""
echo "--- Publishing to Rust (crates.io) ---"
(cd rust && cargo publish) || { echo "FAIL: Rust publish failed"; exit 1; }

echo ""
echo "--- Publishing to Python (PyPI) ---"
(cd python && python -m build && twine upload dist/*) || { echo "FAIL: Python publish failed"; exit 1; }

echo ""
echo "--- Publishing to npm ---"
(cd typescript && npm publish) || { echo "FAIL: npm publish failed"; exit 1; }

# Step 4: Tag and push — only reached if ALL publishes succeeded above.
#
# Tagging strategy for nested Go module:
#   The Go module is github.com/Neumenon/cowrie/go/v2 (subdirectory "go/").
#   Go's module proxy resolves a tag for a module in subdirectory <dir>/ from
#   a tag named <dir>/v<version> (e.g. go/v0.1.2), NOT the bare v<version> tag.
#   We push BOTH tags:
#     v${VERSION}       — canonical release tag for the repo, Rust, Python, npm
#     go/v${VERSION}    — required for `go get github.com/Neumenon/cowrie/go/v2@v${VERSION}`
echo ""
echo "--- Tagging v${VERSION} and go/v${VERSION} ---"
git tag "v${VERSION}"
git tag "go/v${VERSION}"
git push origin main "v${VERSION}" "go/v${VERSION}"

echo ""
echo "=== Release v${VERSION} complete ==="
echo "Go module: go get github.com/Neumenon/cowrie/go/v2@v${VERSION}"
echo "Create GitHub release: gh release create v${VERSION} --title 'Cowrie v${VERSION}' --notes-file CHANGELOG.md"
