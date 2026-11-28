"""Python ctypes binding over the single audited Rust `libcowrie_core` shared library (v1.2)."""

import ctypes
import json
import os
import stat
import sys
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

_U64_MAX = (1 << 64) - 1


def _lib_filename() -> str:
    if sys.platform == "darwin":
        return "libcowrie_core.dylib"
    if sys.platform == "win32":
        return "cowrie_core.dll"
    return "libcowrie_core.so"


def _is_safe_shared_lib(path: Path, require_owner_check: bool = False) -> bool:
    try:
        st = os.lstat(path)
    except OSError:
        return False
    if not stat.S_ISREG(st.st_mode):
        return False
    if require_owner_check and hasattr(os, "geteuid"):
        euid = os.geteuid()
        egid = os.getegid()
        if st.st_uid not in (euid, 0):
            return False
        if (st.st_mode & 0o002) != 0:
            return False
        if (st.st_mode & 0o020) != 0 and st.st_gid != egid:
            return False
    return True


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


def _load_compatible_lib(path: Path):
    """Load a candidate `libcowrie_core` and verify it exports the required v1.2 ABI symbols."""
    lib = ctypes.CDLL(str(path))
    if not hasattr(lib, "cowrie_query_frame_slice"):
        return None
    return lib


def _find_lib() -> str:
    for var in ("COWRIE_LIB", "COWRIE_LIB_PATH"):
        env = os.environ.get(var)
        if env and os.path.exists(env):
            return env
    lib_name = _lib_filename()
    pkg_dir = Path(__file__).resolve().parent
    cargo_target = Path(os.environ.get("CARGO_TARGET_DIR", "/tmp/cowrie-cargo-target"))

    ordered_candidates = [
        (pkg_dir / lib_name, False),
        (cargo_target / "release" / lib_name, True),
        (cargo_target / "debug" / lib_name, True),
        (Path.home() / ".local" / "lib" / lib_name, False),
        (Path("/usr/local/lib") / lib_name, False),
    ]
    fallback_path = None
    for cand, req_owner in ordered_candidates:
        if not _is_safe_shared_lib(cand, require_owner_check=req_owner):
            continue
        try:
            if _load_compatible_lib(cand) is not None:
                return str(cand)
            if fallback_path is None:
                fallback_path = str(cand)
        except OSError:
            continue
    if fallback_path is not None:
        return fallback_path
    raise FileNotFoundError(
        f"{lib_name} not found; set COWRIE_LIB (or COWRIE_LIB_PATH) or build with `make build`"
    )


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

_LIB.cowrie_project_r2_compact.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(CowrieBuf)
]
_LIB.cowrie_project_r2_compact.restype = ctypes.c_int

_LIB.cowrie_extract_field.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(CowrieBuf)
]
_LIB.cowrie_extract_field.restype = ctypes.c_int

_LIB.cowrie_alpha_handle.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
_LIB.cowrie_alpha_handle.restype = ctypes.c_int

_LIB.cowrie_project_virtual_atlas.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t, ctypes.c_size_t, ctypes.POINTER(CowrieBuf)
]
_LIB.cowrie_project_virtual_atlas.restype = ctypes.c_int

_LIB.cowrie_query_frame_slice.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_size_t, ctypes.c_size_t,
    ctypes.c_size_t, ctypes.c_size_t,
    ctypes.POINTER(CowrieBuf),
]
_LIB.cowrie_query_frame_slice.restype = ctypes.c_int

_LIB.cowrie_shard_pack_single.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(CowrieBuf)]
_LIB.cowrie_shard_pack_single.restype = ctypes.c_int

_LIB.cowrie_shard_verify.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p]
_LIB.cowrie_shard_verify.restype = ctypes.c_int

_LIB.cowrie_shard_resolved_frame_cid.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t, ctypes.c_size_t, ctypes.c_char_p
]
_LIB.cowrie_shard_resolved_frame_cid.restype = ctypes.c_int

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
_LIB.cowrie_statepage_open_strict.argtypes = [
    ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
    ctypes.c_int, ctypes.c_int, ctypes.c_uint8,
    ctypes.POINTER(ctypes.c_void_p),
]
_LIB.cowrie_statepage_open_strict.restype = ctypes.c_int
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
_LIB.cowrie_statepage_model_sha.argtypes = [ctypes.c_void_p]
_LIB.cowrie_statepage_model_sha.restype = ctypes.c_char_p
_LIB.cowrie_statepage_doc_cid.argtypes = [ctypes.c_void_p]
_LIB.cowrie_statepage_doc_cid.restype = ctypes.c_char_p
_LIB.cowrie_statepage_state_sha.argtypes = [ctypes.c_void_p]
_LIB.cowrie_statepage_state_sha.restype = ctypes.c_char_p
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
_LIB.cowrie_wal_live_frame_at.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(CowrieBuf)]
_LIB.cowrie_wal_live_frame_at.restype = ctypes.c_int
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
    keepalive = None
    if isinstance(state_bytes, bytes):
        nbytes = len(state_bytes)
        keepalive = state_bytes
        ptr = ctypes.cast(ctypes.c_char_p(keepalive), ctypes.c_void_p) if nbytes > 0 else ctypes.c_void_p(0)
    elif isinstance(state_bytes, bytearray):
        nbytes = len(state_bytes)
        keepalive = (ctypes.c_uint8 * nbytes).from_buffer(state_bytes) if nbytes > 0 else None
        ptr = ctypes.cast(keepalive, ctypes.c_void_p) if keepalive is not None else ctypes.c_void_p(0)
    else:
        mv = memoryview(state_bytes)
        nbytes = mv.nbytes
        if nbytes == 0:
            ptr = ctypes.c_void_p(0)
        elif not mv.readonly and mv.c_contiguous:
            keepalive = (ctypes.c_uint8 * nbytes).from_buffer(mv)
            ptr = ctypes.cast(keepalive, ctypes.c_void_p)
        else:
            raw_copy = mv.tobytes()
            keepalive = raw_copy
            ptr = ctypes.cast(ctypes.c_char_p(keepalive), ctypes.c_void_p)
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
    """Zero-copy read-only `mmap` handle over a verified 16 KB `.state.cowrie` shard."""

    def __init__(
        self,
        path: str,
        expected_model_sha: str = "",
        expected_runtime_id: str = "",
        expected_kv_quant: int = -1,
        expected_ssm_dtype: int = -1,
        verify_hash: bool = True,
        expected_doc_cid: str = "",
    ):
        self._h = ctypes.c_void_p()
        rc = _LIB.cowrie_statepage_open_strict(
            path.encode("utf-8"),
            expected_model_sha.encode("ascii") if expected_model_sha else None,
            expected_doc_cid.encode("ascii") if expected_doc_cid else None,
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
        self.model_sha = _LIB.cowrie_statepage_model_sha(self._h).decode("ascii")
        self.doc_cid = _LIB.cowrie_statepage_doc_cid(self._h).decode("ascii")
        self.state_sha = _LIB.cowrie_statepage_state_sha(self._h).decode("ascii")
        addr = _LIB.cowrie_statepage_data(self._h)
        self._carr = (ctypes.c_uint8 * self.state_len).from_address(addr)

    @property
    def buffer(self):
        if self._carr is None:
            raise ValueError("MappedStatePageView is closed")
        return memoryview(self._carr).toreadonly()

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
    expected_doc_cid: str = "",
):
    with MappedStatePageView(
        path,
        expected_model_sha=expected_model_sha,
        expected_runtime_id=expected_runtime_id,
        expected_kv_quant=expected_kv_quant,
        expected_ssm_dtype=expected_ssm_dtype,
        verify_hash=verify_hash,
        expected_doc_cid=expected_doc_cid,
    ) as mp:
        return {
            "state_len": mp.state_len,
            "ctx_tokens": mp.ctx_tokens,
            "kv_quant": mp.kv_quant,
            "ssm_dtype": mp.ssm_dtype,
            "hmac_verified": mp.hmac_verified,
            "hash_mode": mp.hash_mode,
            "model_sha": mp.model_sha,
            "doc_cid": mp.doc_cid,
            "state_sha": mp.state_sha,
        }


def _validate_action_and_u64(action: str, *timestamps: int) -> int:
    if action not in ACTION_CODES:
        raise ValueError(f"Unknown Cowrie action: {action!r}")
    for ts in timestamps:
        if not isinstance(ts, int) or isinstance(ts, bool) or ts < 0 or ts > _U64_MAX:
            raise ValueError(f"Timestamp out of uint64 range: {ts!r}")
    return ACTION_CODES[action]


def ed25519_pubkey(seed_32: bytes) -> bytes:
    if not isinstance(seed_32, (bytes, bytearray)) or len(seed_32) != 32:
        raise ValueError("seed_32 must be exactly 32 bytes")
    out = ctypes.create_string_buffer(32)
    _check(_LIB.cowrie_ed25519_pubkey(bytes(seed_32), out))
    return out.raw


def sign_declassification(seed_32: bytes, target_cid_34: bytes, action: str, expiry_unix: int) -> bytes:
    if not isinstance(seed_32, (bytes, bytearray)) or len(seed_32) != 32:
        raise ValueError("seed_32 must be exactly 32 bytes")
    if not isinstance(target_cid_34, (bytes, bytearray)) or len(target_cid_34) != 34:
        raise ValueError("target_cid_34 must be exactly 34 bytes")
    act_code = _validate_action_and_u64(action, expiry_unix)
    out = ctypes.create_string_buffer(64)
    _check(_LIB.cowrie_declassify_sign(bytes(seed_32), bytes(target_cid_34), act_code, expiry_unix, out))
    return out.raw


def verify_declassification(pubkey_32: bytes, target_cid_34: bytes, action: str, expiry_unix: int, sig_64: bytes, now_unix: int) -> bool:
    if not isinstance(pubkey_32, (bytes, bytearray)) or len(pubkey_32) != 32:
        raise ValueError("pubkey_32 must be exactly 32 bytes")
    if not isinstance(target_cid_34, (bytes, bytearray)) or len(target_cid_34) != 34:
        raise ValueError("target_cid_34 must be exactly 34 bytes")
    if not isinstance(sig_64, (bytes, bytearray)) or len(sig_64) != 64:
        raise ValueError("sig_64 must be exactly 64 bytes")
    act_code = _validate_action_and_u64(action, expiry_unix, now_unix)
    rc = _LIB.cowrie_declassify_verify(bytes(pubkey_32), bytes(target_cid_34), act_code, expiry_unix, bytes(sig_64), now_unix)
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
        if not isinstance(cid_34, (bytes, bytearray)) or len(cid_34) != 34:
            raise ValueError("cid_34 must be exactly 34 bytes")
        seq = ctypes.c_uint64(0)
        _check(_LIB.cowrie_wal_append_tombstone(self._h, bytes(cid_34), 1 if fsync else 0, ctypes.byref(seq)))
        return int(seq.value)

    def live_len(self) -> int:
        return int(_LIB.cowrie_wal_live_len(self._h))

    def live_frames(self) -> List[bytes]:
        n = self.live_len()
        out_list: List[bytes] = []
        for i in range(n):
            buf = CowrieBuf()
            _check(_LIB.cowrie_wal_live_frame_at(self._h, i, ctypes.byref(buf)))
            out_list.append(_take_buf(buf))
        return out_list

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
    tmap = {"r0": 0, "r1": 1, "r2": 2, "r3": 3}
    if tier not in tmap:
        raise ValueError(f"Unknown Cowrie lens tier: {tier!r} (expected r0, r1, r2, or r3)")
    out = CowrieBuf()
    rc = _LIB.cowrie_project_lens(frame, len(frame), tmap[tier], tensor_count, ctypes.byref(out))
    _check(rc)
    return _take_buf(out).decode("utf-8")


def project_r2_compact(frame: bytes) -> str:
    """Trust-tiered R2-Compact lens (B2-a): compact single-line header for clean verified/policy frames, full Mealy fence otherwise."""
    out = CowrieBuf()
    rc = _LIB.cowrie_project_r2_compact(frame, len(frame), ctypes.byref(out))
    _check(rc)
    return _take_buf(out).decode("utf-8")


def extract_field(frame: bytes, path: str) -> str:
    """Strictly verify a COWR frame and extract the exact value at `path` (`/a/0/b` or `a[0].b`) as lossless JSON."""
    if not isinstance(path, str):
        raise ValueError("path must be str")
    pb = path.encode("utf-8")
    out = CowrieBuf()
    rc = _LIB.cowrie_extract_field(frame, len(frame), pb, len(pb), ctypes.byref(out))
    _check(rc)
    return _take_buf(out).decode("utf-8")


def alpha_handle(cid_34: bytes) -> str:
    """Return the deterministic 10-letter Alpha32 handle (`@abcdefghij`) for a 34-byte CID."""
    if not isinstance(cid_34, (bytes, bytearray)) or len(cid_34) != 34:
        raise ValueError("cid_34 must be exactly 34 bytes")
    out = ctypes.create_string_buffer(12)
    _check(_LIB.cowrie_alpha_handle(bytes(cid_34), out))
    return out.value.decode("ascii")


def project_virtual_atlas(frame: bytes, max_chars: int = 1800) -> str:
    """Project a canonical COWR frame into a bounded pre-append virtualization atlas (Mealy-fenced when untrusted/tainted)."""
    out = CowrieBuf()
    rc = _LIB.cowrie_project_virtual_atlas(frame, len(frame), int(max_chars), ctypes.byref(out))
    _check(rc)
    return _take_buf(out).decode("utf-8")


def query_frame_slice(
    frame: bytes,
    pointer: str | None = None,
    grep: str | None = None,
    lines: tuple[int, int] | list[int] | None = None,
    offset: int = 0,
    max_chars: int = 2400,
) -> str:
    """Bounded `cowrie_get` slice query over a canonical COWR frame (pointer, grep, 1-based inclusive lines, offset, max_chars)."""
    pb = pointer.encode("utf-8") if pointer else b""
    gb = grep.encode("utf-8") if grep else b""
    line_start = 0
    line_end = 0
    if lines is not None:
        if len(lines) < 1:
            raise ValueError("lines must have 1 or 2 positive integers")
        line_start = int(lines[0])
        line_end = int(lines[1]) if len(lines) >= 2 else line_start + 30
        if line_start < 1 or line_end < 1:
            raise ValueError("lines must be 1-based positive integers")
    out = CowrieBuf()
    rc = _LIB.cowrie_query_frame_slice(
        frame, len(frame),
        pb if pb else None, len(pb),
        gb if gb else None, len(gb),
        line_start, line_end,
        max(0, int(offset)), max(0, int(max_chars)),
        ctypes.byref(out),
    )
    _check(rc)
    return _take_buf(out).decode("utf-8")


def pack_single_shard(frame: bytes) -> bytes:
    out = CowrieBuf()
    rc = _LIB.cowrie_shard_pack_single(frame, len(frame), ctypes.byref(out))
    _check(rc)
    return _take_buf(out)


def verify_shard(shard: bytes) -> bytes:
    root = ctypes.create_string_buffer(34)
    rc = _LIB.cowrie_shard_verify(shard, len(shard), root)
    _check(rc)
    return root.raw


def resolved_frame_cid(shard: bytes, frame_idx: int = 0) -> bytes:
    """Compute the repack-stable 34-byte resolved CID committing to frame descriptor + referenced Plane C tensors."""
    if not isinstance(frame_idx, int) or frame_idx < 0:
        raise ValueError("frame_idx must be a non-negative int")
    cid = ctypes.create_string_buffer(34)
    rc = _LIB.cowrie_shard_resolved_frame_cid(shard, len(shard), frame_idx, cid)
    _check(rc)
    return cid.raw


def propagate_authority(base_auth: int, parent_auths) -> int:
    arr = (ctypes.c_uint8 * len(parent_auths))(*parent_auths)
    return int(_LIB.cowrie_propagate_authority(base_auth, arr, len(parent_auths)))


def is_tainted(authority: int) -> bool:
    return bool(_LIB.cowrie_is_tainted(authority))
