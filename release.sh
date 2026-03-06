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
echo "[C]"
(cd c && mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && make && ctest --output-on-failure) || { echo "FAIL: C tests"; exit 1; }
echo ""
echo "All tests passed."
echo ""

# Step 2: Confirm version
echo "--- Pre-publish checklist ---"
echo "  Version: ${VERSION}"
echo "  Go:      github.com/Neumenon/cowrie@v${VERSION}"
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
echo ""
echo "--- Publishing to Rust (crates.io) ---"
(cd rust && cargo publish) || { echo "WARN: Rust publish failed"; }

echo ""
echo "--- Publishing to Python (PyPI) ---"
(cd python && python -m build && twine upload dist/*) || { echo "WARN: Python publish failed"; }

echo ""
echo "--- Publishing to npm ---"
(cd typescript && npm publish) || { echo "WARN: npm publish failed"; }

# Step 4: Tag and push
echo ""
echo "--- Tagging v${VERSION} ---"
git tag "v${VERSION}"
git push origin main --tags

echo ""
echo "=== Release v${VERSION} complete ==="
echo "Go module will be available after git tag is pushed."
echo "Create GitHub release: gh release create v${VERSION} --title 'Cowrie v${VERSION}' --notes-file CHANGELOG.md"
