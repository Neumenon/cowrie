"""Tensor zero-copy data-locator (Phase 2 / Tensor Advantage, option A).

tensor_spans(canonical) reports each tensor's (dtype, shape, data_offset, data_len); the bytes at
that range MUST equal the decoded tensor's contiguous data (the zero-copy view), and the located
spans must match the golden."""
from __future__ import annotations

import json
import os

import pytest

import cowrie_ref as c

_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "testdata")
GOLDEN = json.load(open(os.path.normpath(os.path.join(_DIR, "v1_golden.json"))))
SPANS = json.load(open(os.path.normpath(os.path.join(_DIR, "v1_tensor_spans.json"))))


def _tensors(v, out):
    if isinstance(v, c.Tensor):
        out.append(v)
    elif isinstance(v, list):
        for x in v:
            _tensors(x, out)
    elif isinstance(v, dict):
        for x in v.values():
            _tensors(x, out)
    return out


@pytest.mark.parametrize("name", list(SPANS))
def test_spans_match_golden_and_are_zero_copy(name: str) -> None:
    raw = bytes.fromhex(GOLDEN[name]["canonical_hex"])
    spans = c.tensor_spans(raw)
    assert [{"dtype": d, "shape": list(s), "data_offset": o, "data_len": l}
            for (d, s, o, l) in spans] == SPANS[name]
    tensors = _tensors(c.decode(raw), [])
    assert len(spans) == len(tensors)
    for (dtype, shape, off, dlen), t in zip(spans, tensors):
        assert dtype == t.dtype and tuple(shape) == tuple(t.shape)
        assert raw[off:off + dlen] == t.data        # the zero-copy slice IS the tensor data


def test_no_tensor_no_spans() -> None:
    assert c.tensor_spans(bytes.fromhex(GOLDEN["null"]["canonical_hex"])) == []
    assert c.tensor_spans(bytes.fromhex(GOLDEN["obj_a1"]["canonical_hex"])) == []
