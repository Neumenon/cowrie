"""Stream/dataset layer: dataset_root identity + lazy-verify protocol, with adversarial attacks
(swap / reorder / drop / tamper) that MUST be rejected. (docs/STREAM-LAYER.md)"""
from __future__ import annotations

import json
import os

import cowrie_ref as c
from cowrie_ref import file as F, profiles as p

_DS = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "testdata", "v1_datasets.json"))
GOLDEN = json.load(open(_DS))


def _shard(seed, n=3):
    return F.encode_file([{"k": seed * 10 + i} for i in range(n)])


def test_dataset_root_matches_golden_and_order_count_sensitive() -> None:
    files = {i: _shard(i) for i in range(4)}
    roots = {i: F.file_identity(files[i]) for i in range(4)}
    layouts = {"ds_single": [0], "ds_three": [0, 1, 2], "ds_dup": [0, 0], "ds_order": [2, 0, 1]}
    for name, idxs in layouts.items():
        rs = [roots[i] for i in idxs]
        assert p.dataset_root(rs).hex() == GOLDEN[name]["dataset_root"], name
        assert p.dataset_root(rs)[:2] == b"\x12\x20"
    # order- and count-binding
    assert p.dataset_root([roots[0], roots[1], roots[2]]) != p.dataset_root([roots[2], roots[0], roots[1]])
    assert p.dataset_root([roots[0]]) != p.dataset_root([roots[0], roots[0]])


def test_lazy_verify_accepts_good_rejects_attacks() -> None:
    files = [_shard(i) for i in range(3)]
    roots = [F.file_identity(f) for f in files]
    trusted = p.dataset_root(roots)                       # the one trusted dataset identity

    # step 1 (order+count): honest manifest verifies; any tamper to the root list is caught BEFORE data
    assert p.verify_dataset(roots, trusted) is True
    assert p.verify_dataset([roots[1], roots[0], roots[2]], trusted) is False   # SWAP/REORDER
    assert p.verify_dataset(roots[:2], trusted) is False                         # DROP a shard
    assert p.verify_dataset(roots + [roots[0]], trusted) is False                # ADD a shard

    # step 2 (per-shard): a good shard file verifies; a tampered one does not
    assert p.verify_shard(files[1], roots[1]) is True
    bad = bytearray(files[1]); bad[20] ^= 0xFF
    assert p.verify_shard(bytes(bad), roots[1]) is False                         # TAMPERED shard rejected
    assert p.verify_shard(files[1], roots[0]) is False                           # wrong-position shard rejected

    # the swap attack end-to-end: serving shard files in swapped positions is caught at step 1
    swapped_roots = [roots[1], roots[0], roots[2]]
    assert p.verify_dataset(swapped_roots, trusted) is False
