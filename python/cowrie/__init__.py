"""Python ctypes binding over the single audited Rust `libcowrie_core.so` (v1.2)."""

import ctypes
import json
import os
from pathlib import Path
from typing import List, Sequence

AUTH_SYS_POLICY    = 0x01
AUTH_VERIFIED_TOOL = 0x02
AUTH_UNTRUSTED_EXT = 0x04
AUTH_MODEL_SYNTH   = 0x08
BIT_TAINTED        = 0x80

EPI_OBSERVED  = 0x01
EPI_DERIVED   = 0x02
EPI_ESTIMATED = 0x03

ACTION_CODES = {
    "read_only": 0,
    "shell_exec": 1,
    "cred_read": 2,
    "fs_write": 3,
    "net_exfil": 4,
}


class CowrieBuf(ctypes.Structure):
    _fields_ = [
        ("data", ctypes.POINTER(ctypes.c_uint8)),
        ("len", ctypes.c_size_t),
        ("cap", ctypes.c_size_t),
    ]


class CowrieIvfHit(ctypes.Structure):
    _fields_ = [
        ("orig_id", ctypes.c_uint32),
        ("score", ctypes.c_float),
        ("conf", ctypes.c_float),
        ("auth", ctypes.c_uint8),
    ]


class CowrieError(Exception):
    def __init__(self, code: str, rc: int):
        super().__init__(f"{code} (rc={rc})")
        self.code = code
        self.rc = rc


def _find_lib() -> str:
    env = os.environ.get("COWRIE_LIB")
    if env and os.path.exists(env):
        return env
    candidates = [
        "/tmp/cowrie-cargo-target/release/libcowrie_core.so",
        "/tmp/cowrie-cargo-target/debug/libcowrie_core.so",
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    raise FileNotFoundError("libcowrie_core.so not found; set COWRIE_LIB or build with cargo")


_LIB = ctypes.CDLL(_find_lib())

_LIB.cowrie_buf_free.argtypes = [CowrieBuf]
_LIB.cowrie_err_name.argtypes = [ctypes.c_int]
_LIB.cowrie_err_name.restype = ctypes.c_char_p

_LIB.cowrie_frame_verify.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_uint8), ctypes.POINTER(ctypes.c_uint8), ctypes.c_char_p,
]
_LIB.cowrie_frame_verify.restype = ctypes.c_int

_LIB.cowrie_encode_json.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t, ctypes.c_uint8, ctypes.c_uint8, ctypes.POINTER(CowrieBuf)
]
_LIB.cowrie_encode_json.restype = ctypes.c_int

_LIB.cowrie_decode_to_json.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(CowrieBuf)]
_LIB.cowrie_decode_to_json.restype = ctypes.c_int

_LIB.cowrie_project_lens.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t, ctypes.c_uint8, ctypes.c_size_t, ctypes.POINTER(CowrieBuf)
]
_LIB.cowrie_project_lens.restype = ctypes.c_int

_LIB.cowrie_shard_verify.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p]
_LIB.cowrie_shard_verify.restype = ctypes.c_int

_LIB.cowrie_propagate_authority.argtypes = [ctypes.c_uint8, ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t]
_LIB.cowrie_propagate_authority.restype = ctypes.c_uint8

_LIB.cowrie_is_tainted.argtypes = [ctypes.c_uint8]
_LIB.cowrie_is_tainted.restype = ctypes.c_uint8

_LIB.cowrie_has_shani.argtypes = []
_LIB.cowrie_has_shani.restype = ctypes.c_uint8

_LIB.cowrie_sha256_hex.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p]
_LIB.cowrie_sha256_hex.restype = ctypes.c_int

_LIB.cowrie_model_fingerprint.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
_LIB.cowrie_model_fingerprint.restype = ctypes.c_int

_LIB.cowrie_statepage_save_ext.argtypes = [
    ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
    ctypes.c_char_p, ctypes.c_uint16,
    ctypes.c_uint32, ctypes.c_uint8, ctypes.c_uint8,
    ctypes.c_void_p, ctypes.c_size_t,
]
_LIB.cowrie_statepage_save_ext.restype = ctypes.c_int

_LIB.cowrie_statepage_open_full.argtypes = [
    ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
    ctypes.c_int, ctypes.c_int, ctypes.c_uint8,
    ctypes.POINTER(ctypes.c_void_p),
]
_LIB.cowrie_statepage_open_full.restype = ctypes.c_int
_LIB.cowrie_statepage_data.argtypes = [ctypes.c_void_p]
_LIB.cowrie_statepage_data.restype = ctypes.c_void_p
_LIB.cowrie_statepage_len.argtypes = [ctypes.c_void_p]
_LIB.cowrie_statepage_len.restype = ctypes.c_size_t
_LIB.cowrie_statepage_ctx_tokens.argtypes = [ctypes.c_void_p]
_LIB.cowrie_statepage_ctx_tokens.restype = ctypes.c_uint32
_LIB.cowrie_statepage_kv_quant.argtypes = [ctypes.c_void_p]
_LIB.cowrie_statepage_kv_quant.restype = ctypes.c_uint8
_LIB.cowrie_statepage_ssm_dtype.argtypes = [ctypes.c_void_p]
_LIB.cowrie_statepage_ssm_dtype.restype = ctypes.c_uint8
_LIB.cowrie_statepage_hmac_verified.argtypes = [ctypes.c_void_p]
_LIB.cowrie_statepage_hmac_verified.restype = ctypes.c_uint8
_LIB.cowrie_statepage_hash_mode.argtypes = [ctypes.c_void_p]
_LIB.cowrie_statepage_hash_mode.restype = ctypes.c_uint8
_LIB.cowrie_statepage_close.argtypes = [ctypes.c_void_p]

_LIB.cowrie_ivf_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
_LIB.cowrie_ivf_open.restype = ctypes.c_int
_LIB.cowrie_ivf_search_trusted.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_int8),
    ctypes.c_size_t,
    ctypes.c_size_t,
    ctypes.c_size_t,
    ctypes.c_float,
    ctypes.POINTER(CowrieIvfHit),
    ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_size_t),
    ctypes.POINTER(ctypes.c_size_t),
]
_LIB.cowrie_ivf_search_trusted.restype = ctypes.c_int
_LIB.cowrie_ivf_close.argtypes = [ctypes.c_void_p]

_LIB.cowrie_ed25519_pubkey.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
_LIB.cowrie_ed25519_pubkey.restype = ctypes.c_int
_LIB.cowrie_declassify_sign.argtypes = [
    ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint8, ctypes.c_uint64, ctypes.c_char_p
]
_LIB.cowrie_declassify_sign.restype = ctypes.c_int
_LIB.cowrie_declassify_verify.argtypes = [
    ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint8, ctypes.c_uint64, ctypes.c_char_p, ctypes.c_uint64
]
_LIB.cowrie_declassify_verify.restype = ctypes.c_int

_LIB.cowrie_wal_open.argtypes = [ctypes.c_char_p, ctypes.c_uint8, ctypes.POINTER(ctypes.c_void_p)]
_LIB.cowrie_wal_open.restype = ctypes.c_int
_LIB.cowrie_wal_append_frame.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.c_uint8,
    ctypes.POINTER(ctypes.c_uint64), ctypes.c_char_p
]
_LIB.cowrie_wal_append_frame.restype = ctypes.c_int
_LIB.cowrie_wal_append_tombstone.argtypes = [
    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint8, ctypes.POINTER(ctypes.c_uint64)
]
_LIB.cowrie_wal_append_tombstone.restype = ctypes.c_int
_LIB.cowrie_wal_len.argtypes = [ctypes.c_void_p]
_LIB.cowrie_wal_len.restype = ctypes.c_size_t
_LIB.cowrie_wal_live_len.argtypes = [ctypes.c_void_p]
_LIB.cowrie_wal_live_len.restype = ctypes.c_size_t
_LIB.cowrie_wal_compact_shard.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
_LIB.cowrie_wal_compact_shard.restype = ctypes.c_int
_LIB.cowrie_wal_close.argtypes = [ctypes.c_void_p]

class CowrieZonedInput(ctypes.Structure):
    _fields_ = [
        ("primary_key", ctypes.c_int64),
        ("secondary_val", ctypes.c_int64),
        ("frame_data", ctypes.c_void_p),
        ("frame_len", ctypes.c_size_t),
    ]


_LIB.cowrie_zoned_write.argtypes = [
    ctypes.c_char_p, ctypes.POINTER(CowrieZonedInput), ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(ctypes.c_uint32), ctypes.c_char_p
]
_LIB.cowrie_zoned_write.restype = ctypes.c_int
_LIB.cowrie_zoned_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
_LIB.cowrie_zoned_open.restype = ctypes.c_int
_LIB.cowrie_zoned_query_keys.argtypes = [
    ctypes.c_void_p, ctypes.c_int64, ctypes.c_int64, ctypes.c_uint8, ctypes.c_uint8,
    ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_size_t)
]
_LIB.cowrie_zoned_query_keys.restype = ctypes.c_int
_LIB.cowrie_zoned_query_secondary.argtypes = [
    ctypes.c_void_p, ctypes.c_int64, ctypes.c_int64, ctypes.c_uint8, ctypes.c_uint8,
    ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_size_t)
]
_LIB.cowrie_zoned_query_secondary.restype = ctypes.c_int
_LIB.cowrie_zoned_close.argtypes = [ctypes.c_void_p]


def _check(rc: int):
    if rc != 0:
        name = _LIB.cowrie_err_name(rc).decode("ascii")
        raise CowrieError(name, rc)


def _take_buf(buf: CowrieBuf) -> bytes:
    try:
        if buf.len == 0:
            return b""
        return ctypes.string_at(buf.data, buf.len)
    finally:
        _LIB.cowrie_buf_free(buf)


def has_shani() -> bool:
    return bool(_LIB.cowrie_has_shani())


def sha256_hex(data: bytes) -> str:
    out = ctypes.create_string_buffer(65)
    rc = _LIB.cowrie_sha256_hex(data, len(data), out)
    _check(rc)
    return out.value.decode("ascii")


def fingerprint_model(path: str) -> str:
    out = ctypes.create_string_buffer(65)
    rc = _LIB.cowrie_model_fingerprint(path.encode("utf-8"), out)
    _check(rc)
    return out.value.decode("ascii")


def save_statepage(
    path: str,
    model_sha: str,
    doc_cid: str,
    ctx_tokens: int,
    state_bytes,
    kv_quant: int = 0,
    ssm_dtype: int = 0,
    runtime_id: str = "",
    layout_ver: int = 0,
):
    if isinstance(state_bytes, (bytes, bytearray)):
        nbytes = len(state_bytes)
        ptr = ctypes.cast(ctypes.c_char_p(bytes(state_bytes)) if isinstance(state_bytes, bytes) else (ctypes.c_char * nbytes).from_buffer(state_bytes), ctypes.c_void_p)
    else:
        mv = memoryview(state_bytes)
        nbytes = mv.nbytes
        c_buf = (ctypes.c_uint8 * nbytes).from_buffer(mv)
        ptr = ctypes.cast(c_buf, ctypes.c_void_p)
    rc = _LIB.cowrie_statepage_save_ext(
        path.encode("utf-8"),
        model_sha.encode("ascii"),
        doc_cid.encode("ascii"),
        runtime_id.encode("ascii") if runtime_id else None,
        layout_ver,
        ctx_tokens,
        kv_quant,
        ssm_dtype,
        ptr,
        nbytes,
    )
    _check(rc)


class MappedStatePageView:
    """Zero-copy `mmap` handle over a verified 16 KB `.state.cowrie` shard (compatible with `torch.frombuffer`)."""

    def __init__(
        self,
        path: str,
        expected_model_sha: str = "",
        expected_runtime_id: str = "",
        expected_kv_quant: int = -1,
        expected_ssm_dtype: int = -1,
        verify_hash: bool = True,
    ):
        self._h = ctypes.c_void_p()
        rc = _LIB.cowrie_statepage_open_full(
            path.encode("utf-8"),
            expected_model_sha.encode("ascii") if expected_model_sha else None,
            expected_runtime_id.encode("ascii") if expected_runtime_id else None,
            expected_kv_quant,
            expected_ssm_dtype,
            1 if verify_hash else 0,
            ctypes.byref(self._h),
        )
        _check(rc)
        self.state_len = int(_LIB.cowrie_statepage_len(self._h))
        self.ctx_tokens = int(_LIB.cowrie_statepage_ctx_tokens(self._h))
        self.kv_quant = int(_LIB.cowrie_statepage_kv_quant(self._h))
        self.ssm_dtype = int(_LIB.cowrie_statepage_ssm_dtype(self._h))
        self.hmac_verified = bool(_LIB.cowrie_statepage_hmac_verified(self._h))
        self.hash_mode = int(_LIB.cowrie_statepage_hash_mode(self._h))
        addr = _LIB.cowrie_statepage_data(self._h)
        self._carr = (ctypes.c_uint8 * self.state_len).from_address(addr)

    @property
    def buffer(self):
        return memoryview(self._carr)

    def close(self):
        if getattr(self, "_h", None) and self._h.value:
            self._carr = None
            _LIB.cowrie_statepage_close(self._h)
            self._h = ctypes.c_void_p()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __del__(self):
        self.close()


def verify_statepage_file(
    path: str,
    expected_model_sha: str = "",
    expected_kv_quant: int = -1,
    verify_hash: bool = True,
    expected_runtime_id: str = "",
    expected_ssm_dtype: int = -1,
):
    with MappedStatePageView(
        path,
        expected_model_sha=expected_model_sha,
        expected_runtime_id=expected_runtime_id,
        expected_kv_quant=expected_kv_quant,
        expected_ssm_dtype=expected_ssm_dtype,
        verify_hash=verify_hash,
    ) as mp:
        return {
            "state_len": mp.state_len,
            "ctx_tokens": mp.ctx_tokens,
            "kv_quant": mp.kv_quant,
            "ssm_dtype": mp.ssm_dtype,
            "hmac_verified": mp.hmac_verified,
            "hash_mode": mp.hash_mode,
        }


def ed25519_pubkey(seed_32: bytes) -> bytes:
    assert len(seed_32) == 32
    out = ctypes.create_string_buffer(32)
    _check(_LIB.cowrie_ed25519_pubkey(seed_32, out))
    return out.raw


def sign_declassification(seed_32: bytes, target_cid_34: bytes, action: str, expiry_unix: int) -> bytes:
    assert len(seed_32) == 32 and len(target_cid_34) == 34
    out = ctypes.create_string_buffer(64)
    _check(_LIB.cowrie_declassify_sign(seed_32, target_cid_34, ACTION_CODES[action], expiry_unix, out))
    return out.raw


def verify_declassification(pubkey_32: bytes, target_cid_34: bytes, action: str, expiry_unix: int, sig_64: bytes, now_unix: int) -> bool:
    assert len(pubkey_32) == 32 and len(target_cid_34) == 34 and len(sig_64) == 64
    rc = _LIB.cowrie_declassify_verify(pubkey_32, target_cid_34, ACTION_CODES[action], expiry_unix, sig_64, now_unix)
    return rc == 0


class Wal:
    """Crash-safe hash-chained `.cwwal` store over C-ABI."""

    def __init__(self, path: str, recover_torn_tail: bool = True):
        self._h = ctypes.c_void_p()
        _check(_LIB.cowrie_wal_open(path.encode("utf-8"), 1 if recover_torn_tail else 0, ctypes.byref(self._h)))

    def append_frame(self, frame: bytes, fsync: bool = False):
        seq = ctypes.c_uint64(0)
        cid = ctypes.create_string_buffer(34)
        _check(_LIB.cowrie_wal_append_frame(self._h, frame, len(frame), 1 if fsync else 0, ctypes.byref(seq), cid))
        return int(seq.value), cid.raw

    def append_tombstone(self, cid_34: bytes, fsync: bool = False) -> int:
        assert len(cid_34) == 34
        seq = ctypes.c_uint64(0)
        _check(_LIB.cowrie_wal_append_tombstone(self._h, cid_34, 1 if fsync else 0, ctypes.byref(seq)))
        return int(seq.value)

    def live_len(self) -> int:
        return int(_LIB.cowrie_wal_live_len(self._h))

    def __len__(self) -> int:
        return int(_LIB.cowrie_wal_len(self._h))

    def compact_to_shard(self, out_path: str) -> bytes:
        root = ctypes.create_string_buffer(34)
        _check(_LIB.cowrie_wal_compact_shard(self._h, out_path.encode("utf-8"), root))
        return root.raw

    def close(self):
        if getattr(self, "_h", None) and self._h.value:
            _LIB.cowrie_wal_close(self._h)
            self._h = ctypes.c_void_p()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __del__(self):
        self.close()


def write_zoned_shard(path: str, records):
    """Write `(primary_key, secondary_val, frame_bytes)` records into a 16 KB `CWZM` shard."""
    n = len(records)
    arr = (CowrieZonedInput * n)()
    keepalive = []
    for i, (pk, sec, fb) in enumerate(records):
        buf = ctypes.create_string_buffer(fb, len(fb))
        keepalive.append(buf)
        arr[i].primary_key = int(pk)
        arr[i].secondary_val = int(sec)
        arr[i].frame_data = ctypes.cast(buf, ctypes.c_void_p)
        arr[i].frame_len = len(fb)
    tot = ctypes.c_uint64(0)
    pages = ctypes.c_uint32(0)
    root = ctypes.create_string_buffer(34)
    _check(_LIB.cowrie_zoned_write(path.encode("utf-8"), arr, n, ctypes.byref(tot), ctypes.byref(pages), root))
    return int(tot.value), int(pages.value), root.raw


class ZonedShard:
    """16 KB page-targeted Zone-Map shard reader (`CWZM`) over C-ABI."""

    def __init__(self, path: str):
        self._h = ctypes.c_void_p()
        _check(_LIB.cowrie_zoned_open(path.encode("utf-8"), ctypes.byref(self._h)))

    def query_keys(self, lo: int, hi: int, trusted_only: bool = True, verify_merkle: bool = True):
        cnt = ctypes.c_size_t(0)
        bread = ctypes.c_size_t(0)
        _check(_LIB.cowrie_zoned_query_keys(
            self._h, lo, hi, 1 if trusted_only else 0, 1 if verify_merkle else 0,
            ctypes.byref(cnt), ctypes.byref(bread)
        ))
        return int(cnt.value), int(bread.value)

    def query_secondary(self, lo: int, hi: int, trusted_only: bool = True, verify_merkle: bool = True):
        cnt = ctypes.c_size_t(0)
        bread = ctypes.c_size_t(0)
        _check(_LIB.cowrie_zoned_query_secondary(
            self._h, lo, hi, 1 if trusted_only else 0, 1 if verify_merkle else 0,
            ctypes.byref(cnt), ctypes.byref(bread)
        ))
        return int(cnt.value), int(bread.value)

    def close(self):
        if getattr(self, "_h", None) and self._h.value:
            _LIB.cowrie_zoned_close(self._h)
            self._h = ctypes.c_void_p()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __del__(self):
        self.close()


class IvfIndex:
    """Zero-copy out-of-core co-located 16 KB IVF-INT8 (`CWIV`) reader via C-ABI."""

    def __init__(self, path: str):
        self._h = ctypes.c_void_p()
        rc = _LIB.cowrie_ivf_open(path.encode("utf-8"), ctypes.byref(self._h))
        _check(rc)

    def search_trusted(self, query_i8: Sequence[int], nprobe: int = 16, top_k: int = 20, conf_thresh: float = 0.0):
        dim = len(query_i8)
        q_arr = (ctypes.c_int8 * dim)(*query_i8)
        hits_arr = (CowrieIvfHit * top_k)()
        n_out = ctypes.c_size_t(0)
        b_out = ctypes.c_size_t(0)
        rc = _LIB.cowrie_ivf_search_trusted(
            self._h, q_arr, dim, nprobe, top_k, conf_thresh,
            hits_arr, top_k, ctypes.byref(n_out), ctypes.byref(b_out)
        )
        _check(rc)
        hits: List[dict] = []
        for i in range(n_out.value):
            h = hits_arr[i]
            hits.append({"orig_id": int(h.orig_id), "score": float(h.score), "conf": float(h.conf), "auth": int(h.auth)})
        return hits, int(b_out.value)

    def close(self):
        if getattr(self, "_h", None) and self._h.value:
            _LIB.cowrie_ivf_close(self._h)
            self._h = ctypes.c_void_p()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def __del__(self):
        self.close()


def encode_json(obj, authority: int = AUTH_VERIFIED_TOOL, epistemic: int = EPI_OBSERVED) -> bytes:
    raw = obj.encode("utf-8") if isinstance(obj, str) else json.dumps(obj, separators=(",", ":")).encode("utf-8")
    out = CowrieBuf()
    rc = _LIB.cowrie_encode_json(raw, len(raw), authority, epistemic, ctypes.byref(out))
    _check(rc)
    return _take_buf(out)


def verify_frame(frame: bytes):
    auth = ctypes.c_uint8(0)
    epi = ctypes.c_uint8(0)
    cid = ctypes.create_string_buffer(34)
    rc = _LIB.cowrie_frame_verify(frame, len(frame), ctypes.byref(auth), ctypes.byref(epi), cid)
    _check(rc)
    return {"authority": auth.value, "epistemic": epi.value, "cid": cid.raw}


def decode_to_json(frame: bytes) -> str:
    out = CowrieBuf()
    rc = _LIB.cowrie_decode_to_json(frame, len(frame), ctypes.byref(out))
    _check(rc)
    return _take_buf(out).decode("utf-8")


def project_lens(frame: bytes, tier: str = "r2", tensor_count: int = 0) -> str:
    tmap = {"r0": 0, "r1": 1, "r2": 2}
    out = CowrieBuf()
    rc = _LIB.cowrie_project_lens(frame, len(frame), tmap[tier], tensor_count, ctypes.byref(out))
    _check(rc)
    return _take_buf(out).decode("utf-8")


def pack_single_shard(frame: bytes) -> bytes:
    _LIB.cowrie_shard_pack_single.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(CowrieBuf)]
    _LIB.cowrie_shard_pack_single.restype = ctypes.c_int32
    out = CowrieBuf()
    rc = _LIB.cowrie_shard_pack_single(frame, len(frame), ctypes.byref(out))
    _check(rc)
    return _take_buf(out)


def verify_shard(shard: bytes) -> bytes:
    root = ctypes.create_string_buffer(34)
    rc = _LIB.cowrie_shard_verify(shard, len(shard), root)
    _check(rc)
    return root.raw


def propagate_authority(base_auth: int, parent_auths) -> int:
    arr = (ctypes.c_uint8 * len(parent_auths))(*parent_auths)
    return int(_LIB.cowrie_propagate_authority(base_auth, arr, len(parent_auths)))


def is_tainted(authority: int) -> bool:
    return bool(_LIB.cowrie_is_tainted(authority))
