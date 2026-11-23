"""Python ctypes binding over the single audited Rust `libcowrie_core.so`."""

import ctypes
import json
import os
from pathlib import Path

AUTH_SYS_POLICY    = 0x01
AUTH_VERIFIED_TOOL = 0x02
AUTH_UNTRUSTED_EXT = 0x04
AUTH_MODEL_SYNTH   = 0x08
BIT_TAINTED        = 0x80

EPI_OBSERVED  = 0x01
EPI_DERIVED   = 0x02
EPI_ESTIMATED = 0x03


class CowrieBuf(ctypes.Structure):
    _fields_ = [
        ("data", ctypes.POINTER(ctypes.c_uint8)),
        ("len", ctypes.c_size_t),
        ("cap", ctypes.c_size_t),
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

_LIB.cowrie_statepage_save.argtypes = [
    ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
    ctypes.c_uint32, ctypes.c_uint8, ctypes.c_uint8,
    ctypes.c_char_p, ctypes.c_size_t,
]
_LIB.cowrie_statepage_save.restype = ctypes.c_int

_LIB.cowrie_statepage_open_checked.argtypes = [
    ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int, ctypes.c_uint8,
    ctypes.POINTER(ctypes.c_void_p),
]
_LIB.cowrie_statepage_open_checked.restype = ctypes.c_int
_LIB.cowrie_statepage_len.argtypes = [ctypes.c_void_p]
_LIB.cowrie_statepage_len.restype = ctypes.c_size_t
_LIB.cowrie_statepage_kv_quant.argtypes = [ctypes.c_void_p]
_LIB.cowrie_statepage_kv_quant.restype = ctypes.c_uint8
_LIB.cowrie_statepage_hash_mode.argtypes = [ctypes.c_void_p]
_LIB.cowrie_statepage_hash_mode.restype = ctypes.c_uint8
_LIB.cowrie_statepage_close.argtypes = [ctypes.c_void_p]


def has_shani() -> bool:
    return bool(_LIB.cowrie_has_shani())


def save_statepage(
    path: str,
    model_sha: str,
    doc_cid: str,
    ctx_tokens: int,
    state_bytes: bytes,
    kv_quant: int = 0,
    ssm_dtype: int = 0,
):
    rc = _LIB.cowrie_statepage_save(
        path.encode("utf-8"),
        model_sha.encode("ascii"),
        doc_cid.encode("ascii"),
        ctx_tokens,
        kv_quant,
        ssm_dtype,
        state_bytes,
        len(state_bytes),
    )
    _check(rc)


def verify_statepage_file(
    path: str,
    expected_model_sha: str = "",
    expected_kv_quant: int = -1,
    verify_hash: bool = True,
):
    h = ctypes.c_void_p()
    rc = _LIB.cowrie_statepage_open_checked(
        path.encode("utf-8"),
        expected_model_sha.encode("ascii") if expected_model_sha else None,
        expected_kv_quant,
        1 if verify_hash else 0,
        ctypes.byref(h),
    )
    _check(rc)
    try:
        return {
            "state_len": int(_LIB.cowrie_statepage_len(h)),
            "kv_quant": int(_LIB.cowrie_statepage_kv_quant(h)),
            "hash_mode": int(_LIB.cowrie_statepage_hash_mode(h)),
        }
    finally:
        _LIB.cowrie_statepage_close(h)


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
