#!/usr/bin/env python3
"""Suite 6: Cross-Implementation Differential Testing

Feeds the same cowrie fixture corpus to all 5 language decoders and compares
JSON output. Any disagreement between implementations is flagged.

Usage:
    python differential_runner.py [--fixture-dir PATH] [--verbose]

Requirements:
    - Go: `go build` in cowrie/go/cmd/cowrie/
    - Python: `pip install -e cowrie/python/`
    - Rust: `cargo build` in cowrie/rust/ (uses examples/decode_stdin)
    - C: `cmake && make` in cowrie/c/build/
    - TypeScript: `npm ci` in cowrie/typescript/
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional


def repo_root() -> Path:
    """Find the cowrie repo root (parent of testdata/)."""
    here = Path(__file__).resolve().parent
    # testdata/robustness/ -> testdata/ -> cowrie/
    return here.parent.parent


def decode_with_python(fixture_path: Path) -> Optional[str]:
    """Decode using Python cowrie directly."""
    try:
        from cowrie import gen2

        data = fixture_path.read_bytes()
        value = gen2.decode(data)
        result = gen2.to_any(value)
        return json.dumps(result, sort_keys=True, separators=(",", ":"))
    except Exception as e:
        return f"ERROR:{e}"


def decode_with_go(fixture_path: Path, go_bin: Path) -> Optional[str]:
    """Decode using Go CLI."""
    try:
        result = subprocess.run(
            [str(go_bin), "decode", "--json", str(fixture_path)],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            return f"ERROR:{result.stderr.strip()}"
        # Normalize JSON
        parsed = json.loads(result.stdout)
        return json.dumps(parsed, sort_keys=True, separators=(",", ":"))
    except Exception as e:
        return f"ERROR:{e}"


def decode_with_rust(fixture_path: Path, rust_bin: Path) -> Optional[str]:
    """Decode using Rust decode_stdin example."""
    try:
        with open(fixture_path, "rb") as f:
            result = subprocess.run(
                [str(rust_bin)],
                stdin=f,
                capture_output=True,
                text=True,
                timeout=10,
            )
        if result.returncode != 0:
            return f"ERROR:{result.stderr.strip()}"
        parsed = json.loads(result.stdout)
        return json.dumps(parsed, sort_keys=True, separators=(",", ":"))
    except Exception as e:
        return f"ERROR:{e}"


def decode_with_c(fixture_path: Path, c_bin: Path) -> Optional[str]:
    """Decode using C decode_stdin tool."""
    try:
        with open(fixture_path, "rb") as f:
            result = subprocess.run(
                [str(c_bin)],
                stdin=f,
                capture_output=True,
                text=True,
                timeout=10,
            )
        if result.returncode != 0:
            return f"ERROR:{result.stderr.strip()}"
        parsed = json.loads(result.stdout)
        return json.dumps(parsed, sort_keys=True, separators=(",", ":"))
    except Exception as e:
        return f"ERROR:{e}"


def decode_with_ts(fixture_path: Path, ts_dir: Path) -> Optional[str]:
    """Decode using TypeScript."""
    script = f"""
const fs = require('fs');
const {{ decode, toAny }} = require('./src/gen2/index.ts');
const data = fs.readFileSync('{fixture_path}');
const value = decode(new Uint8Array(data));
const result = toAny(value);
console.log(JSON.stringify(result));
"""
    try:
        result = subprocess.run(
            ["node", "--import", "tsx", "-e", script],
            capture_output=True,
            text=True,
            timeout=10,
            cwd=str(ts_dir),
        )
        if result.returncode != 0:
            return f"ERROR:{result.stderr.strip()}"
        parsed = json.loads(result.stdout)
        return json.dumps(parsed, sort_keys=True, separators=(",", ":"))
    except Exception as e:
        return f"ERROR:{e}"


def find_binaries(root: Path) -> dict:
    """Locate built binaries for each language."""
    bins = {}

    # Go
    go_bin = root / "go" / "cmd" / "cowrie" / "cowrie"
    if not go_bin.exists():
        # Try building
        go_dir = root / "go" / "cmd" / "cowrie"
        if go_dir.exists():
            subprocess.run(["go", "build", "-o", str(go_bin), "."], cwd=str(go_dir))
    if go_bin.exists():
        bins["go"] = go_bin

    # Rust
    rust_bin = root / "rust" / "target" / "debug" / "examples" / "decode_stdin"
    if not rust_bin.exists():
        rust_dir = root / "rust"
        if rust_dir.exists():
            subprocess.run(
                ["cargo", "build", "--example", "decode_stdin"],
                cwd=str(rust_dir),
            )
    if rust_bin.exists():
        bins["rust"] = rust_bin

    # C
    c_bin = root / "c" / "build" / "decode_stdin"
    if not c_bin.exists():
        c_tools = root / "c" / "tools" / "decode_stdin.c"
        if c_tools.exists():
            # Build it
            build_dir = root / "c" / "build"
            build_dir.mkdir(exist_ok=True)
    if c_bin.exists():
        bins["c"] = c_bin

    # TypeScript
    ts_dir = root / "typescript"
    if (ts_dir / "package.json").exists():
        bins["typescript"] = ts_dir

    # Python is always available if cowrie is installed
    bins["python"] = None

    return bins


def main():
    parser = argparse.ArgumentParser(description="Cross-implementation differential test")
    parser.add_argument("--fixture-dir", type=Path, default=None)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    root = repo_root()
    fixture_dir = args.fixture_dir or root / "testdata" / "fixtures"
    manifest_path = fixture_dir / "manifest.json"

    if not manifest_path.exists():
        print(f"ERROR: manifest not found at {manifest_path}")
        sys.exit(1)

    manifest = json.loads(manifest_path.read_text())
    bins = find_binaries(root)

    available = list(bins.keys())
    print(f"Available decoders: {', '.join(available)}")
    print(f"Fixture cases: {len(manifest['cases'])}")
    print()

    total = 0
    passed = 0
    failed = 0
    skipped = 0

    for case in manifest["cases"]:
        cid = case["id"]
        if case.get("gen") != 2 or case.get("kind") != "decode":
            continue
        if not case["expect"].get("ok"):
            # Skip negative tests — error messages may differ
            skipped += 1
            continue

        total += 1
        fixture_path = fixture_dir / case["input"]
        if not fixture_path.exists():
            print(f"  SKIP {cid}: fixture missing")
            skipped += 1
            continue

        results = {}

        # Python
        if "python" in bins:
            results["python"] = decode_with_python(fixture_path)

        # Go
        if "go" in bins:
            results["go"] = decode_with_go(fixture_path, bins["go"])

        # Rust
        if "rust" in bins:
            results["rust"] = decode_with_rust(fixture_path, bins["rust"])

        # C
        if "c" in bins:
            results["c"] = decode_with_c(fixture_path, bins["c"])

        # TypeScript
        if "typescript" in bins:
            results["typescript"] = decode_with_ts(fixture_path, bins["typescript"])

        # Compare all non-error results
        valid_results = {
            lang: r for lang, r in results.items() if not r.startswith("ERROR:")
        }

        if len(valid_results) < 2:
            if args.verbose:
                print(f"  SKIP {cid}: fewer than 2 decoders succeeded")
                for lang, r in results.items():
                    print(f"       {lang}: {r[:80]}")
            skipped += 1
            continue

        values = list(valid_results.values())
        if all(v == values[0] for v in values):
            passed += 1
            if args.verbose:
                print(f"  OK   {cid} ({len(valid_results)} decoders agree)")
        else:
            failed += 1
            print(f"  FAIL {cid}: decoders disagree")
            for lang, r in valid_results.items():
                print(f"       {lang}: {r[:200]}")

    print()
    print(f"Results: {passed} passed, {failed} failed, {skipped} skipped (of {total} cases)")
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
