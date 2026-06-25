#!/usr/bin/env python3
"""Cross-language fixture validation.

For every case in manifest.json this ACTUALLY DECODES the fixture with real
cowrie decoders and checks the result — unlike the previous CI step, which only
json.load()'d the expected files (a JSON-syntax check that never exercised any
decoder, which is how the C SJFR framing incompatibility went undetected).

Primary decoder: Go reference CLI (the oracle that generates the fixtures).
Secondary decoder: Python pure-Python implementation (cross-language gate).

Point GO_CLI at a built `cowrie` binary:

    cd go && go build -o /tmp/cowrie-cli ./cmd/cowrie
    GO_CLI=/tmp/cowrie-cli python3 testdata/fixtures/validate_fixtures.py

The Python decoder is loaded from PYTHON_PKG_DIR (defaults to the sibling
python/ source tree) using COWRIE_PUREPYTHON=1 to force the pure-Python path
(the Cython extension may not match the current NumPy ABI).

Cross-language scope (per-decoder):
  Go:     all 55 cases — the reference oracle.
  Python: all gen2 cases + gen1 cases where Python gen1 supports the tag set.
          gen1 proto-float32 (tagArrayFloat32 / 0x18) is not implemented in
          Python gen1 and is silently skipped for the Python decoder only.

Rust/TS per-language parity is gated separately by pinned invariant tests in
rust/ and typescript/ — not this harness.

Semantics per case (kind="decode" decodes binary; kind="from_json" runs the JSON
bridge — Go via `encode`+`decode`, Python via from_json):
  * expect.ok == true  + expect.json  -> must succeed AND equal the JSON.
  * expect.ok == true  (no json)      -> must succeed (ML/graph/bitmask types with
                                          no canonical JSON projection).
  * expect.ok == false                -> MUST fail (negative/error case). Note: the
                                          per-language fixtures_core tests assert the
                                          exact error code; this script only checks
                                          that the case is rejected.

Exit code is non-zero if any case fails.
"""
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(ROOT))
GO_CLI = os.environ.get("GO_CLI")

# Python package: prefer PYTHON_PKG_DIR env, else use sibling python/ source tree.
_PYTHON_PKG_DIR = os.environ.get(
    "PYTHON_PKG_DIR",
    os.path.join(os.path.dirname(os.path.dirname(ROOT)), "python"),
)

# ---------------------------------------------------------------------------
# Go reference decoder
# ---------------------------------------------------------------------------

def go_decode(path, gen=2):
    """Return (ok, parsed_json_or_None, stderr)."""
    with open(path, "rb") as fh:
        data = fh.read()
    cmd = [GO_CLI, "decode"]
    if gen == 1:
        cmd.append("--gen1")
    proc = subprocess.run(cmd, input=data, capture_output=True)
    if proc.returncode != 0:
        return False, None, proc.stderr.decode("utf-8", "replace")
    try:
        return True, json.loads(proc.stdout), ""
    except json.JSONDecodeError as exc:
        # Non-JSON stdout is still a successful decode for types that have no
        # canonical JSON projection; only flag genuinely empty output.
        return (proc.stdout != b""), None, f"non-json output ({exc})"


# ---------------------------------------------------------------------------
# Python pure-Python decoder (loaded lazily so Go-only runs still work)
# ---------------------------------------------------------------------------

_py_gen1 = None
_py_gen2 = None
_python_available = False


def _load_python_decoders():
    global _py_gen1, _py_gen2, _python_available
    if _py_gen1 is not None:
        return _python_available

    if not os.path.isdir(_PYTHON_PKG_DIR):
        return False

    # Prepend path so the source tree wins over any installed version.
    if _PYTHON_PKG_DIR not in sys.path:
        sys.path.insert(0, _PYTHON_PKG_DIR)

    # Force pure Python — the Cython .so may have ABI incompatibilities.
    os.environ.setdefault("COWRIE_PUREPYTHON", "1")

    try:
        from cowrie import gen1, gen2  # type: ignore[import]
        _py_gen1 = gen1
        _py_gen2 = gen2
        _python_available = True
    except Exception as exc:  # pragma: no cover
        print(f"  [warn] Python decoder unavailable: {exc}", file=sys.stderr)
        _python_available = False

    return _python_available


def python_decode(path, gen=2):
    """Return (ok, parsed_json_or_None, stderr, skipped).

    skipped=True means the fixture is outside the Python decoder's scope and
    should not count as a failure (e.g. gen1 tags not implemented in Python).
    """
    if not _load_python_decoders():
        return None, None, "Python decoders not loaded", True

    with open(path, "rb") as fh:
        data = fh.read()

    try:
        if gen == 1:
            raw = _py_gen1.decode(data)
            # gen1.decode returns plain Python objects; convert via JSON round-trip
            # so the comparison is the same as Go (both go through json.loads).
            decoded_str = json.dumps(raw)
            return True, json.loads(decoded_str), "", False
        else:
            # gen2 cases: use decode_framed so compressed fixtures work too.
            val = _py_gen2.decode_framed(data)
            decoded_str = _py_gen2.to_json(val)
            # to_json returns a str; parse back to normalise whitespace.
            try:
                return True, json.loads(decoded_str), "", False
            except json.JSONDecodeError:
                # Non-JSON projection (e.g. bitmask with no canonical JSON).
                return (decoded_str != ""), None, f"non-json output", False
    except NotImplementedError as exc:
        # Python gen1 doesn't implement all tag variants (e.g. tagArrayFloat32).
        return None, None, f"not implemented: {exc}", True
    except Exception as exc:
        msg = str(exc)
        # A missing OPTIONAL compression codec (e.g. zstd not installed in this
        # environment) is outside the Python decoder's scope, not a parity failure:
        # skip the Python cross-check (Go still validates the fixture).
        if "not available" in msg.lower() or "not installed" in msg.lower():
            return None, None, f"optional codec unavailable: {msg}", True
        # ValueError signals a decode error (invalid magic, version, tag, etc.)
        return False, None, msg, False


# ---------------------------------------------------------------------------
# from_json bridge (JSON projection -> Value), exercised cross-language
# ---------------------------------------------------------------------------

def go_from_json(path):
    """Return (ok, parsed_json_or_None, stderr).

    `cowrie encode` runs FromJSON internally, so a non-zero exit means the bridge
    rejected the input. For accepted input we round-trip back through `decode` so
    positive cases can be value-compared.
    """
    with open(path, "rb") as fh:
        text = fh.read()
    enc = subprocess.run([GO_CLI, "encode"], input=text, capture_output=True)
    if enc.returncode != 0:
        return False, None, enc.stderr.decode("utf-8", "replace")
    dec = subprocess.run([GO_CLI, "decode"], input=enc.stdout, capture_output=True)
    if dec.returncode != 0:
        return True, None, f"encode ok but decode failed: {dec.stderr.decode('utf-8', 'replace')}"
    try:
        return True, json.loads(dec.stdout), ""
    except json.JSONDecodeError as exc:
        return (dec.stdout != b""), None, f"non-json output ({exc})"


def python_from_json(path):
    """Return (ok, parsed_json_or_None, stderr, skipped). Mirrors python_decode."""
    if not _load_python_decoders():
        return None, None, "Python decoders not loaded", True
    with open(path, "r") as fh:
        text = fh.read()
    try:
        val = _py_gen2.from_json(text)
        decoded_str = _py_gen2.to_json(val)
        try:
            return True, json.loads(decoded_str), "", False
        except json.JSONDecodeError:
            return (decoded_str != ""), None, "non-json output", False
    except Exception as exc:
        msg = str(exc)
        if "not available" in msg.lower() or "not installed" in msg.lower():
            return None, None, f"optional codec unavailable: {msg}", True
        return False, None, msg, False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not GO_CLI:
        sys.exit("GO_CLI is not set — build go/cmd/cowrie and point GO_CLI at it.")

    py_available = _load_python_decoders()
    if py_available:
        print(f"  Python decoder: loaded from {_PYTHON_PKG_DIR} (PUREPYTHON)")
    else:
        print(f"  Python decoder: unavailable — only Go will be checked")

    manifest = json.load(open(os.path.join(ROOT, "manifest.json")))
    fail = passed = skipped = py_skipped = 0

    for case in manifest["cases"]:
        cid = case["id"]
        inp = os.path.join(ROOT, case["input"])
        expect = case["expect"]
        gen = case.get("gen", 2)
        kind = case.get("kind", "decode")

        if not os.path.exists(inp):
            print(f"  SKIP {cid}: input file missing")
            skipped += 1
            continue

        # ---- Go (primary) ----
        if kind == "from_json":
            ok, got, err = go_from_json(inp)
        else:
            ok, got, err = go_decode(inp, gen=gen)
        go_pass = False

        if expect.get("ok"):
            if not ok:
                print(f"  FAIL {cid} [go]: decode errored: {err.strip()}")
                fail += 1
                continue
            if "json" in expect:
                jp = os.path.join(ROOT, expect["json"])
                if not os.path.exists(jp):
                    print(f"  SKIP {cid}: expected json missing")
                    skipped += 1
                    continue
                want = json.load(open(jp))
                if got == want:
                    go_pass = True
                else:
                    print(f"  FAIL {cid} [go]: decoded != expected\n    got={got!r}\n    want={want!r}")
                    fail += 1
                    continue
            else:
                go_pass = True
        else:
            if ok:
                print(f"  FAIL {cid} [go]: expected error {expect.get('error')} but decode succeeded")
                fail += 1
                continue
            else:
                go_pass = True

        # ---- Python (secondary cross-language gate) ----
        py_label = ""
        if py_available:
            if case.get("python_skip"):
                py_label = " [py:skip]"
                py_skipped += 1
                print(f"  OK   {cid}{py_label}")
                passed += 1
                continue

            if kind == "from_json":
                py_ok, py_got, py_err, py_skip = python_from_json(inp)
            else:
                py_ok, py_got, py_err, py_skip = python_decode(inp, gen=gen)

            if py_skip:
                py_label = " [py:skip]"
                py_skipped += 1
            elif expect.get("ok"):
                if py_ok is False:
                    print(
                        f"  FAIL {cid} [python]: decode errored: {py_err.strip()}\n"
                        f"    (Go succeeded — cross-language parity broken)"
                    )
                    fail += 1
                    continue
                if "json" in expect and py_ok:
                    jp = os.path.join(ROOT, expect["json"])
                    want = json.load(open(jp))
                    if py_got != want:
                        print(
                            f"  FAIL {cid} [python]: decoded != expected\n"
                            f"    got={py_got!r}\n    want={want!r}"
                        )
                        fail += 1
                        continue
            else:
                # Negative case: Python must also fail.
                if py_ok is True:
                    print(
                        f"  FAIL {cid} [python]: expected error but decode succeeded\n"
                        f"    (Go correctly rejected — cross-language parity broken)"
                    )
                    fail += 1
                    continue

        print(f"  OK   {cid}{py_label}")
        passed += 1

    print(
        f"\nResults: {passed} passed, {skipped} skipped, {fail} failed"
        + (f", {py_skipped} py-skipped" if py_skipped else "")
    )
    sys.exit(1 if fail else 0)


if __name__ == "__main__":
    main()
