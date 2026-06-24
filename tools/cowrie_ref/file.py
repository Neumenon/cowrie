"""Cowrie file / frame container and Merkle file identity (SPEC-v1 §7).

A **Cowrie file** packages an ordered list of **frames** (each frame is one complete, canonical
§2.2 COWR value's wire bytes) with a sealed footer index for O(1) random access, and a **Merkle
root** over the frames that is the file's content identity.

Layout (all multi-byte ints little-endian; uvarint = §2.1):

    "CWRF" (0x43 0x57 0x52 0x46)        file magic
    version  u8 (0x01)
    reserved u8 (0x00)
    uvarint  frame_count
    repeat frame_count times:
        uvarint frame_len
        frame_bytes                      (a canonical COWR value, §2.2; uncompressed)
    FOOTER (starts at footer_offset):
        uvarint frame_count              (repeated, footer is self-contained)
        repeat: uvarint frame_offset, uvarint frame_len   (offset = file-start to frame_bytes)
        merkle_root  34 bytes            (multihash SHA-256, §3)
    u64 LE   footer_offset               (file-start to FOOTER; enables seek-from-end)
    "CWRF"                               end magic

Merkle (RFC 6962 domain separation, promote-odd — never duplicate; the count is bound into the
root so two different frame lists can never share a root, even structurally):
    leaf(f)    = SHA-256(0x00 || f)
    node(a, b) = SHA-256(0x01 || a || b)
    tree_digest = promote-odd reduction of the leaves   (empty file: SHA-256(b""))
    root_digest = SHA-256(0x02 || uvarint(frame_count) || tree_digest)
    merkle_root = 0x12 0x20 || root_digest              (multihash sha2-256)

File identity = merkle_root. Equal ordered frame contents ⇒ equal identity, cross-language.
Per-frame content address (§3) remains multihash(SHA-256(frame_bytes)) — note the Merkle LEAF is
domain-separated (0x00 prefix) and is therefore deliberately distinct from the content address.
"""
from __future__ import annotations

import hashlib
import struct

from . import encode
from .errors import CowrieError, ERR_INVALID_MAGIC, ERR_TRUNCATED, ERR_NON_CANONICAL
from .varint import decode_uvarint, encode_uvarint

FILE_MAGIC = b"CWRF"
FILE_VERSION = 1
MAX_FRAMES = 1 << 32  # §7 frame-count bound (DoS guard)


# ---------------------------------------------------------------- Merkle -----
def _leaf(frame: bytes) -> bytes:
    return hashlib.sha256(b"\x00" + frame).digest()


def _node(a: bytes, b: bytes) -> bytes:
    return hashlib.sha256(b"\x01" + a + b).digest()


def _tree_digest(frames: list[bytes]) -> bytes:
    if not frames:
        return hashlib.sha256(b"").digest()
    level = [_leaf(f) for f in frames]
    while len(level) > 1:
        nxt = []
        for i in range(0, len(level) - 1, 2):
            nxt.append(_node(level[i], level[i + 1]))
        if len(level) % 2 == 1:
            nxt.append(level[-1])  # promote the odd node unchanged (RFC 6962, not duplicate)
        level = nxt
    return level[0]


def merkle_root(frames: list[bytes]) -> bytes:
    """Multihash SHA-256 Merkle root over the ordered frame byte-blobs (§7), with the frame count
    bound into the root (0x02 domain) so distinct frame lists can never share a root."""
    root = hashlib.sha256(b"\x02" + encode_uvarint(len(frames)) + _tree_digest(frames)).digest()
    return b"\x12\x20" + root


# ---------------------------------------------------------------- encode -----
def encode_file(values: list[object]) -> bytes:
    """Build a Cowrie file from a list of values (each encoded as one canonical frame)."""
    frames = [encode(v) for v in values]
    out = bytearray()
    out += FILE_MAGIC
    out.append(FILE_VERSION)
    out.append(0x00)  # reserved
    out += encode_uvarint(len(frames))
    offsets = []
    for f in frames:
        out += encode_uvarint(len(f))
        offsets.append(len(out))  # offset to frame_bytes (after its length prefix)
        out += f
    footer_offset = len(out)
    # footer
    out += encode_uvarint(len(frames))
    for off, f in zip(offsets, frames):
        out += encode_uvarint(off)
        out += encode_uvarint(len(f))
    out += merkle_root(frames)
    out += struct.pack("<Q", footer_offset)
    out += FILE_MAGIC
    return bytes(out)


# ---------------------------------------------------------------- decode -----
def decode_file(data: bytes, *, verify: bool = True) -> list[bytes]:
    """Return the ordered frame byte-blobs, fully validated (§7). The BODY is the source of truth:
    frames are read sequentially via their length prefixes, and the footer MUST exactly mirror that
    layout (count, contiguous offsets/lengths) — so there is exactly one canonical file byte-string
    per ordered value list. With verify=True (default) the stored Merkle root must also match."""
    if len(data) < 4 + 1 + 1 + 1 + 8 + 4:
        raise CowrieError(ERR_TRUNCATED, "file too short")
    if data[:4] != FILE_MAGIC:
        raise CowrieError(ERR_INVALID_MAGIC, "bad file magic")
    if data[-4:] != FILE_MAGIC:
        raise CowrieError(ERR_INVALID_MAGIC, "bad end magic")
    if data[4] != FILE_VERSION:
        raise CowrieError(ERR_INVALID_MAGIC, "bad file version")
    if data[5] != 0x00:
        raise CowrieError(ERR_NON_CANONICAL, "reserved byte must be 0")
    (footer_offset,) = struct.unpack("<Q", data[-12:-4])
    if not (6 < footer_offset <= len(data) - 12):
        raise CowrieError(ERR_TRUNCATED, "footer offset out of range")

    # --- body: read frames sequentially (the source of truth) ---
    pos = 6
    n, pos = decode_uvarint(data, pos)
    if n > MAX_FRAMES:
        raise CowrieError(ERR_TRUNCATED, "frame count exceeds limit")
    frames: list[bytes] = []
    layout: list[tuple[int, int]] = []
    for _ in range(n):
        flen, pos = decode_uvarint(data, pos)
        if pos + flen > footer_offset:
            raise CowrieError(ERR_TRUNCATED, "frame extends past footer")
        layout.append((pos, flen))
        frames.append(data[pos:pos + flen])
        pos += flen
    if pos != footer_offset:
        raise CowrieError(ERR_NON_CANONICAL, "body does not end exactly at the footer")

    # --- footer: must mirror the body layout exactly (canonical) ---
    fcount, pos = decode_uvarint(data, pos)
    if fcount != n:
        raise CowrieError(ERR_NON_CANONICAL, "footer frame count != header")
    for (off, flen) in layout:
        foff, pos = decode_uvarint(data, pos)
        flen2, pos = decode_uvarint(data, pos)
        if foff != off or flen2 != flen:
            raise CowrieError(ERR_NON_CANONICAL, "footer index does not mirror body layout")
    stored_root = data[pos:pos + 34]
    pos += 34
    if pos != len(data) - 12:
        raise CowrieError(ERR_NON_CANONICAL, "footer length not canonical")
    if verify and merkle_root(frames) != stored_root:
        raise CowrieError(ERR_NON_CANONICAL, "merkle root mismatch (tampered or non-canonical)")
    return frames


def file_identity(data: bytes) -> bytes:
    """The file's content identity = its verified Merkle root (34-byte multihash)."""
    frames = decode_file(data, verify=True)
    return merkle_root(frames)
