"""``recode`` CLI — the Python v1 (COWR) command, a peer to the Go/Rust/TS ``recode`` tools.

Reads wire/file bytes on **stdin** and supports the EXACT same contracts the other languages'
``recode`` CLIs use (see ``rust/examples/recode.rs`` and ``go/cmd/cowrie`` ``recodeCmd``). It is a
thin shell over the existing :mod:`cowrie_ref` primitives — it NEVER reimplements the codec:

  bare ``recode``      decode (lenient §5.3) -> re-encode canonical bytes to stdout
  ``--strict``         strict decode (§5.3): reject non-canonical, print ``ERR_*`` to stderr, exit 1
  ``--addr``           content address (§3): multihash SHA-256 hex of the canonical bytes
  ``--fingerprint``    schema fingerprint64 (§4) as 16 lowercase hex chars
  ``--tensor-spans``   Phase-2 zero-copy tensor locator: one line per tensor, document order,
                       ``<data_offset> <data_len> <dtype> <shape>`` (shape ``-`` for rank-0)
  ``--file-id``        Cowrie FILE (§7): decode+verify, print merkle_root as lowercase hex
  ``--file-recode``    Cowrie FILE (§7): decode -> re-encode -> raw bytes to stdout
  ``--dataset-root``   read ``uvarint(n) || n*(uvarint(len) || blob)``; print §7 dataset_root hex

``--strict`` may combine with ``--addr``/``--fingerprint`` (the underlying decode is then strict).
``STRICT=1`` in the environment also turns on strict mode (parity with the cross-language harness).
"""
from __future__ import annotations

import os
import sys

from . import (
    address_of_bytes,
    content_address,  # noqa: F401  (re-exported convenience; address_of_bytes is what we use)
    decode,
    encode,
    fingerprint,
    tensor_spans,
)
from . import file as _file
from . import profiles as _profiles
from .errors import CowrieError
from .varint import decode_uvarint


def _read_stdin() -> bytes:
    return sys.stdin.buffer.read()


def _write_stdout(data: bytes) -> None:
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()


def _fail(err: CowrieError) -> int:
    """Print the canonical SPEC-v1 §2.6 ``ERR_*`` code (+ detail) to stderr; the cross-language gate
    extracts the first ``/ERR_[A-Z_]+/`` token from stderr, so we lead with the code (mirrors the
    Go/Rust ``recode`` tools)."""
    sys.stderr.write(f"{err.code}: {err}\n")
    return 1


def _shape_str(shape: tuple[int, ...]) -> str:
    return ",".join(str(d) for d in shape) if shape else "-"


def main(argv: list[str]) -> int:
    flags = set(argv)
    strict = "--strict" in flags or os.environ.get("STRICT") in ("1", "true")

    # --- §7 dataset/stream-layer Merkle root: uvarint(n) || n*(uvarint(len) || blob) ---
    if "--dataset-root" in flags:
        data = _read_stdin()
        pos = 0
        n, pos = decode_uvarint(data, pos)
        blobs: list[bytes] = []
        for _ in range(n):
            ln, pos = decode_uvarint(data, pos)
            blobs.append(data[pos : pos + ln])
            pos += ln
        # Reuse the §7 Merkle construction (profiles.dataset_root) — do not reimplement.
        print(_profiles.dataset_root(blobs).hex())
        return 0

    # --- Phase-2 zero-copy tensor locator: one line per tensor, document order ---
    if "--tensor-spans" in flags:
        data = _read_stdin()
        try:
            spans = tensor_spans(data, strict=strict)
        except CowrieError as e:
            return _fail(e)
        out = "".join(
            f"{data_off} {data_len} {dtype} {_shape_str(shape)}\n"
            for (dtype, shape, data_off, data_len) in spans
        )
        _write_stdout(out.encode("ascii"))
        return 0

    # --- Cowrie FILE container modes (§7) ---
    if "--file-id" in flags or "--file-recode" in flags:
        data = _read_stdin()
        try:
            frames = _file.decode_file(data, verify=True)
            if "--file-id" in flags:
                print(_file.merkle_root(frames).hex())
            else:
                # Re-encode the file; canonical input reproduces byte-for-byte. Each frame is a
                # complete canonical §2.2 value, so decode it back to a value then re-encode_file.
                values = [decode(f) for f in frames]
                _write_stdout(_file.encode_file(values))
        except CowrieError as e:
            return _fail(e)
        return 0

    # --- value modes: decode (lenient or strict) then act ---
    data = _read_stdin()
    try:
        value = decode(data, strict=strict)
    except CowrieError as e:
        return _fail(e)

    if "--fingerprint" in flags:
        h64, _ = fingerprint(value)
        print(f"{h64:016x}")
        return 0

    canonical = encode(value)

    if "--addr" in flags:
        print(address_of_bytes(canonical).hex())
        return 0

    _write_stdout(canonical)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
