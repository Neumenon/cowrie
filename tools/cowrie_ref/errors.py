"""Canonical Cowrie v1 error codes (SPEC-v1 §2.6).

A single exception type carrying a stable ``code`` string; the conformance suite asserts on the
code, never the message.
"""
from __future__ import annotations


class CowrieError(Exception):
    """A decode/encode failure carrying a stable SPEC-v1 §2.6 error code."""

    __slots__ = ("code",)

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code


# §2.6 canonical codes (the subset this reference exercises).
ERR_INVALID_MAGIC = "ERR_INVALID_MAGIC"
ERR_INVALID_VERSION = "ERR_INVALID_VERSION"
ERR_UNSUPPORTED_COMPRESSION = "ERR_UNSUPPORTED_COMPRESSION"
ERR_TRUNCATED = "ERR_TRUNCATED"
ERR_INVALID_VARINT = "ERR_INVALID_VARINT"
ERR_INVALID_TAG = "ERR_INVALID_TAG"
ERR_RESERVED_TAG = "ERR_RESERVED_TAG"
ERR_INVALID_UTF8 = "ERR_INVALID_UTF8"
ERR_INVALID_FIELD_ID = "ERR_INVALID_FIELD_ID"
ERR_DUPLICATE_KEY = "ERR_DUPLICATE_KEY"
ERR_TRAILING_DATA = "ERR_TRAILING_DATA"
ERR_NON_CANONICAL = "ERR_NON_CANONICAL"
