#!/usr/bin/env python3
"""Verify SPEC.md contains required sections for cross-language parity."""
from __future__ import annotations

from pathlib import Path
import sys

REQUIRED_SECTIONS = [
    "### Header Flags",
    "### Column Hints",
    "### TagExt (Extension Envelope)",
    "### Compression Framing Rules",
    "### Security Limits and Decode Options",
    "### Tensor Rank Limits",
    "### Error Codes (Canonical for Fixtures)",
]


def main() -> int:
    spec_path = Path(__file__).resolve().parents[2] / "SPEC.md"
    if not spec_path.exists():
        print(f"SPEC.md not found at {spec_path}")
        return 1

    content = spec_path.read_text(encoding="utf-8")
    missing = [section for section in REQUIRED_SECTIONS if section not in content]
    if missing:
        print("Missing required SPEC sections:")
        for section in missing:
            print(f"- {section}")
        return 1

    print("SPEC.md contains all required sections.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
