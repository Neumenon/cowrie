#!/usr/bin/env python3
"""File / Merkle identity parity gate (SPEC-v1 §7).

For each golden file in testdata/v1_files.json, every implementation must:
  - --file-id      : compute the SAME Merkle root (== golden merkle_root), and
  - --file-recode  : decode + re-encode the file to byte-identical output (== file_hex).

This proves cross-language agreement on file-level content identity AND canonical-file stability.

Run: python3 tools/file_identity_gate.py   (exit non-zero on any mismatch)
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
from cowrie_ref import file as F  # noqa: E402

FILES = json.load(open(os.path.join(ROOT, "testdata", "v1_files.json")))
GO_CLI = os.environ.get("GO_CLI", "/tmp/cowrie-cli")
RUST = os.environ.get("RUST_RECODE", os.path.join(ROOT, "rust/target/release/examples/recode"))
TS_DIR = os.path.join(ROOT, "typescript")
TSX = os.path.join(TS_DIR, "node_modules/.bin/tsx")

# (label, file-id argv, file-recode argv, cwd)
IMPLS = {
    "go":   ([GO_CLI, "recode", "--file-id"], [GO_CLI, "recode", "--file-recode"], None),
    "rust": ([RUST, "--file-id"], [RUST, "--file-recode"], None),
    "ts":   ([TSX, "recode.ts", "--file-id"], [TSX, "recode.ts", "--file-recode"], TS_DIR),
}


def run(cmd, raw, cwd=None) -> bytes:
    return subprocess.run(cmd, input=raw, capture_output=True, cwd=cwd).stdout


def main() -> int:
    res = {k: [0, 0] for k in ("python", *IMPLS)}  # [id_pass, recode_pass]
    fails = []
    total = len(FILES)
    for name, fx in FILES.items():
        raw = bytes.fromhex(fx["file_hex"])
        want_root = fx["merkle_root"]
        # python reference
        try:
            if F.file_identity(raw).hex() == want_root:
                res["python"][0] += 1
            else:
                fails.append(f"python id/{name}")
            if F.encode_file([__import__("cowrie_ref").decode(f) for f in F.decode_file(raw)]).hex() == fx["file_hex"]:
                res["python"][1] += 1
            else:
                fails.append(f"python recode/{name}")
        except Exception as exc:  # noqa: BLE001
            fails.append(f"python/{name}: {exc}")
        # other langs
        for lang, (idc, rec, cwd) in IMPLS.items():
            got_id = run(idc, raw, cwd).decode("ascii", "replace").strip()
            if got_id == want_root:
                res[lang][0] += 1
            else:
                fails.append(f"{lang} id/{name}: want {want_root[:16]}… got {got_id[:16]}…")
            got_rec = run(rec, raw, cwd).hex()
            if got_rec == fx["file_hex"]:
                res[lang][1] += 1
            else:
                fails.append(f"{lang} recode/{name}")

    print(f"File / Merkle identity parity gate — {total} golden files (§7)\n")
    print(f"{'impl':<10}{'file-id':>9}{'file-recode':>13}   status")
    green = True
    for lang, (idp, rp) in res.items():
        ok = idp == total and rp == total
        green = green and ok
        print(f"{lang:<10}{idp:>4}/{total:<4}{rp:>8}/{total:<4}   {'✅ GREEN' if ok else '❌ RED'}")
    if fails:
        print("\n" + "\n".join(fails[:15]))
    print("\n" + ("✅ ALL IMPLEMENTATIONS AGREE ON FILE IDENTITY" if green else "❌ FILE-IDENTITY GATE RED"))
    return 0 if green else 1


if __name__ == "__main__":
    raise SystemExit(main())
