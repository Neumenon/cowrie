#!/usr/bin/env python3
"""Cross-language fixture validation.

For every case in manifest.json this ACTUALLY DECODES the fixture with a real
cowrie decoder and checks the result — unlike the previous CI step, which only
json.load()'d the expected files (a JSON-syntax check that never exercised any
decoder, which is how the C SJFR framing incompatibility went undetected).

By default it uses the Go reference decoder (the oracle that generates the
fixtures). Point GO_CLI at a built `cowrie` binary:

    cd go && go build -o /tmp/cowrie-cli ./cmd/cowrie
    GO_CLI=/tmp/cowrie-cli python3 testdata/fixtures/validate_fixtures.py

Semantics per case (all 34 are kind="decode"):
  * expect.ok == true  + expect.json  -> decode must succeed AND equal the JSON.
  * expect.ok == true  (no json)      -> decode must succeed (ML/graph/bitmask
                                          types with no canonical JSON projection).
  * expect.ok == false                -> decode MUST fail (negative/error case).

Exit code is non-zero if any case fails.
"""
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
GO_CLI = os.environ.get("GO_CLI")


def go_decode(path):
    """Return (ok, parsed_json_or_None, stderr)."""
    with open(path, "rb") as fh:
        data = fh.read()
    proc = subprocess.run([GO_CLI, "decode"], input=data, capture_output=True)
    if proc.returncode != 0:
        return False, None, proc.stderr.decode("utf-8", "replace")
    try:
        return True, json.loads(proc.stdout), ""
    except json.JSONDecodeError as exc:
        # Non-JSON stdout is still a successful decode for types that have no
        # canonical JSON projection; only flag genuinely empty output.
        return (proc.stdout != b""), None, f"non-json output ({exc})"


def main():
    if not GO_CLI:
        sys.exit("GO_CLI is not set — build go/cmd/cowrie and point GO_CLI at it.")

    manifest = json.load(open(os.path.join(ROOT, "manifest.json")))
    fail = passed = skipped = 0

    for case in manifest["cases"]:
        cid = case["id"]
        inp = os.path.join(ROOT, case["input"])
        expect = case["expect"]

        if not os.path.exists(inp):
            print(f"  SKIP {cid}: input file missing")
            skipped += 1
            continue

        ok, got, err = go_decode(inp)

        if expect.get("ok"):
            if not ok:
                print(f"  FAIL {cid}: decode errored: {err.strip()}")
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
                    print(f"  OK   {cid}")
                    passed += 1
                else:
                    print(f"  FAIL {cid}: decoded != expected\n    got={got!r}\n    want={want!r}")
                    fail += 1
            else:
                print(f"  OK   {cid} (decode-only)")
                passed += 1
        else:
            if ok:
                print(f"  FAIL {cid}: expected error {expect.get('error')} but decode succeeded")
                fail += 1
            else:
                print(f"  OK   {cid} (correctly rejected: {expect.get('error')})")
                passed += 1

    print(f"\nResults: {passed} passed, {skipped} skipped, {fail} failed")
    sys.exit(1 if fail else 0)


if __name__ == "__main__":
    main()
