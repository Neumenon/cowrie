import json
from pathlib import Path

from cowrie import gen2


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
    if "trailing" in msg.lower():
        return "ERR_TRAILING_DATA"
    if "Invalid channel count" in msg:
        return "ERR_INVALID_AUDIO_CHANNELS"
    if "Invalid sample rate" in msg:
        return "ERR_INVALID_AUDIO_RATE"
    return ""


def test_fixtures_core_decode():
    repo = _repo_root()
    fixtures = repo / "testdata" / "fixtures"
    manifest = json.loads((fixtures / "manifest.json").read_text())

    for case in manifest.get("cases", []):
        kind = case.get("kind")
        if case.get("gen") != 2 or kind not in ("decode", "from_json"):
            continue

        input_path = fixtures / case["input"]

        # decode = binary wire -> Value; from_json = JSON projection -> Value.
        def produce():
            if kind == "from_json":
                return gen2.from_json(input_path.read_text())
            return gen2.decode(input_path.read_bytes())

        if case["expect"]["ok"]:
            value = produce()
            expected_json = case["expect"].get("json")
            if expected_json:
                expected = json.loads((fixtures / expected_json).read_text())
                actual = gen2.to_any(value)
                assert actual == expected, f"{case['id']}: expected {expected} got {actual}"
        else:
            try:
                produce()
            except Exception as exc:
                code = _map_error_code(exc)
                expected = case["expect"].get("error")
                assert code == expected, f"{case['id']}: expected {expected}, got {code} ({exc})"
                continue
            raise AssertionError(f"{case['id']}: expected error but {kind} succeeded")
