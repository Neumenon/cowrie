#!/usr/bin/env bash
# Single source-of-truth gate runner for Cowrie v1 cross-language conformance.
#
# Runs EVERY gate in order from the repo root and exits non-zero on the FIRST
# failure. This is the LAW: the GitHub Actions CI (.github/workflows/conformance.yml)
# invokes exactly this script, so "green locally" == "green in CI".
#
# Gates (in order):
#   (a) pytest the Python reference  (tools/cowrie_ref/tests)
#   (b) conformance gate             (68 positive vectors x 4 langs)
#   (b2) content-address gate        (§3 multihash SHA-256 identity, 4 langs agree)
#   (b3) file-identity gate          (§7 file/Merkle identity + canonical re-encode, 4 langs)
#   (c) negative gate (lenient)      (16 anti-malleability fixtures)
#   (d) negative gate (--strict)     (strict decoders must reject non-canonical)
#   (e) differential fuzz (150)      (generative cross-lang determinism)
#
# Usage:  bash tools/run_all_gates.sh
set -euo pipefail

# --- locate repo root (this script lives in <root>/tools) -------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT}"

# --- recode tool locations (gates honour these env vars) --------------------
export GO_CLI="${GO_CLI:-/tmp/cowrie-cli}"
export RUST_RECODE="${RUST_RECODE:-${ROOT}/rust/target/release/examples/recode}"

# Python interpreter: prefer the dedicated venv (PEP-668 blocks system pip),
# fall back to python3 if it's not present (e.g. a CI runner that already
# installed pytest + the reference into the active environment).
PY="${COWRIE_PY:-/tmp/cowrie-venv/bin/python}"
if [[ ! -x "${PY}" ]]; then
  PY="python3"
fi

# Ensure the clean-room reference package is importable for pytest and the gates.
export PYTHONPATH="${ROOT}/tools${PYTHONPATH:+:${PYTHONPATH}}"

# --- build the Go CLI only if the prebuilt binary is missing ----------------
if [[ ! -x "${GO_CLI}" ]]; then
  echo ">> Go CLI missing at ${GO_CLI} — building ./cmd/cowrie ..."
  ( cd "${ROOT}/go" && go build -o "${GO_CLI}" ./cmd/cowrie )
fi

# The Rust example is slow to build; we reuse the prebuilt binary and fail loud
# if it is absent rather than silently rebuilding.
if [[ ! -x "${RUST_RECODE}" ]]; then
  echo "!! Rust recode binary missing at ${RUST_RECODE}" >&2
  echo "!! Build it once with: ( cd rust && cargo build --release --example recode )" >&2
  exit 1
fi

echo "=============================================================="
echo " Cowrie v1 — run_all_gates"
echo "   ROOT        = ${ROOT}"
echo "   GO_CLI      = ${GO_CLI}"
echo "   RUST_RECODE = ${RUST_RECODE}"
echo "   PYTHON      = ${PY}"
echo "=============================================================="

fail() {
  echo ""
  echo "##############################################################"
  echo "## ❌ GATE FAILED: $1"
  echo "## (run aborted — fix this before any merge)"
  echo "##############################################################"
  exit 1
}

run() {
  local label="$1"; shift
  echo ""
  echo "--------------------------------------------------------------"
  echo ">> [${label}] $*"
  echo "--------------------------------------------------------------"
  "$@" || fail "${label}"
}

# (a) Python reference unit tests (the executable spec).
run "pytest cowrie_ref" "${PY}" -m pytest tools/cowrie_ref/tests -q

# (b) Positive conformance: 68 golden vectors across python/go/rust/ts.
run "conformance_gate" "${PY}" tools/conformance_gate.py

# (b2) Content-address parity (§3): all impls agree on the multihash SHA-256 identity.
run "content_address_gate" "${PY}" tools/content_address_gate.py

# (b3) File / Merkle identity parity (§7): all impls agree on file identity + canonical re-encode.
run "file_identity_gate" "${PY}" tools/file_identity_gate.py

# (c) Negative gate, lenient decode: malformed fixtures must be rejected.
run "negative_gate (lenient)" "${PY}" tools/negative_gate.py

# (d) Negative gate, strict decode: non-canonical must also be rejected.
run "negative_gate --strict" "${PY}" tools/negative_gate.py --strict

# (e) Generative differential fuzz across all languages.
run "fuzz_differential 150" "${PY}" tools/fuzz_differential.py 150

echo ""
echo "=============================================================="
echo " ✅✅✅  ALL GATES PASS  ✅✅✅"
echo "   pytest · conformance · content-address · file-identity · negative · negative --strict · fuzz"
echo "=============================================================="
exit 0
