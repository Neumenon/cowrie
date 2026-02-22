import json
from pathlib import Path

from sjson import gen2


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _map_error_code(err: Exception) -> str:
    msg = str(err)
    if isinstance(err, gen2.SecurityLimitExceeded):
        return "ERR_TOO_LARGE"
    if "Invalid magic bytes" in msg:
        return "ERR_INVALID_MAGIC"
    if "Unsupported version" in msg:
        return "ERR_INVALID_VERSION"
    if "Unexpected end of data" in msg or "Data too short" in msg or "Incomplete varint" in msg:
        return "ERR_TRUNCATED"
    if msg.startswith("Invalid tag"):
        return "ERR_INVALID_TAG"
    return ""


def test_fixtures_core_decode():
    repo = _repo_root()
    manifest_path = repo / "testdata" / "fixtures" / "manifest.json"
    manifest = json.loads(manifest_path.read_text())

    for case in manifest.get("cases", []):
        if case.get("gen") != 2 or case.get("kind") != "decode":
            continue

        input_path = repo / "testdata" / "fixtures" / case["input"]
        data = input_path.read_bytes()

        if case["expect"]["ok"]:
            value = gen2.decode(data)
            actual = gen2.to_any(value)

            expected_json = case["expect"].get("json")
            if expected_json:
                expected_path = repo / "testdata" / "fixtures" / expected_json
                expected = json.loads(expected_path.read_text())
                assert actual == expected, f"{case['id']}: expected {expected} got {actual}"
        else:
            try:
                gen2.decode(data)
            except Exception as exc:
                code = _map_error_code(exc)
                expected = case["expect"].get("error")
                assert code == expected, f"{case['id']}: expected {expected}, got {code} ({exc})"
                continue
            raise AssertionError(f"{case['id']}: expected error but decode succeeded")
