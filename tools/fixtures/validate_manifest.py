#!/usr/bin/env python3
"""Validate testdata/fixtures/manifest.json schema and referenced files."""
from __future__ import annotations

from pathlib import Path
import json
import sys

ALLOWED_ERROR_CODES = {
    "ERR_INVALID_MAGIC",
    "ERR_INVALID_VERSION",
    "ERR_TRUNCATED",
    "ERR_INVALID_TAG",
    "ERR_INVALID_FIELD_ID",
    "ERR_INVALID_UTF8",
    "ERR_INVALID_VARINT",
    "ERR_TOO_DEEP",
    "ERR_TOO_LARGE",
    "ERR_DICT_TOO_LARGE",
    "ERR_STRING_TOO_LARGE",
    "ERR_BYTES_TOO_LARGE",
    "ERR_EXT_TOO_LARGE",
    "ERR_RANK_TOO_LARGE",
    "ERR_UNSUPPORTED_COMPRESSION",
    "ERR_DECOMPRESSED_TOO_LARGE",
    "ERR_DECOMPRESSED_MISMATCH",
    "ERR_UNKNOWN_EXTENSION",
}

ALLOWED_KINDS = {"decode", "encode", "roundtrip"}
ALLOWED_GENS = {1, 2}


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    manifest_path = repo_root / "testdata" / "fixtures" / "manifest.json"
    if not manifest_path.exists():
        print(f"Manifest not found at {manifest_path}")
        return 1

    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "cases" not in data:
        print("Manifest must be a JSON object with a 'cases' list.")
        return 1

    cases = data["cases"]
    if not isinstance(cases, list):
        print("'cases' must be a list.")
        return 1

    seen_ids: set[str] = set()
    errors: list[str] = []

    for idx, case in enumerate(cases):
        if not isinstance(case, dict):
            errors.append(f"case[{idx}] must be an object")
            continue

        case_id = case.get("id")
        if not isinstance(case_id, str) or not case_id.strip():
            errors.append(f"case[{idx}].id must be a non-empty string")
        elif case_id in seen_ids:
            errors.append(f"duplicate case id: {case_id}")
        else:
            seen_ids.add(case_id)

        gen = case.get("gen")
        if gen not in ALLOWED_GENS:
            errors.append(f"case[{idx}].gen must be 1 or 2")

        kind = case.get("kind")
        if kind not in ALLOWED_KINDS:
            errors.append(f"case[{idx}].kind must be one of {sorted(ALLOWED_KINDS)}")

        input_path = case.get("input")
        if not isinstance(input_path, str) or not input_path:
            errors.append(f"case[{idx}].input must be a non-empty string")
        else:
            ref = (repo_root / "testdata" / "fixtures" / input_path).resolve()
            if not ref.exists():
                errors.append(f"case[{idx}].input file not found: {input_path}")

        expect = case.get("expect")
        if not isinstance(expect, dict) or "ok" not in expect:
            errors.append(f"case[{idx}].expect must be an object with 'ok' field")
        else:
            ok = expect.get("ok")
            if not isinstance(ok, bool):
                errors.append(f"case[{idx}].expect.ok must be boolean")
            if ok:
                if "error" in expect:
                    errors.append(f"case[{idx}].expect.error must be omitted when ok=true")
                expect_json = expect.get("json")
                if expect_json is not None:
                    if not isinstance(expect_json, str) or not expect_json:
                        errors.append(f"case[{idx}].expect.json must be a non-empty string when provided")
                    else:
                        ref = (repo_root / "testdata" / "fixtures" / expect_json).resolve()
                        if not ref.exists():
                            errors.append(f"case[{idx}].expect.json file not found: {expect_json}")
            else:
                err = expect.get("error")
                if err not in ALLOWED_ERROR_CODES:
                    errors.append(f"case[{idx}].expect.error must be one of allowed codes")
                if "json" in expect:
                    errors.append(f"case[{idx}].expect.json must be omitted when ok=false")

    if errors:
        print("Manifest validation failed:")
        for err in errors:
            print(f"- {err}")
        return 1

    print("Manifest validation OK.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
