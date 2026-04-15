"""
Cowrie native C extension via ctypes.

Provides C-speed encode/decode by loading libcowrie_gen2.so.
Falls back gracefully if the shared library is not found.

Build the shared library:
    cd c && mkdir -p build && cd build && cmake -DBUILD_SHARED_LIBS=ON .. && make
"""

import ctypes
import ctypes.util
import os
import sys
import struct as _struct
from ctypes import (
    c_uint8, c_int, c_int64, c_uint64, c_double, c_size_t, c_char_p, c_void_p,
    POINTER, Structure, byref, cast, memmove,
)

import numpy as np

# DType mapping mirrors COWRIEDType enum; canonical map lives in _wire_constants.
from ._wire_constants import (
    NP_TO_WIRE_DTYPE as _NP_TO_COWRIE_DTYPE,
    WIRE_FLOAT32 as COWRIE_DTYPE_FLOAT32,
    WIRE_FLOAT16 as COWRIE_DTYPE_FLOAT16,
    WIRE_BFLOAT16 as COWRIE_DTYPE_BFLOAT16,
    WIRE_INT8 as COWRIE_DTYPE_INT8,
    WIRE_INT16 as COWRIE_DTYPE_INT16,
    WIRE_INT32 as COWRIE_DTYPE_INT32,
    WIRE_INT64 as COWRIE_DTYPE_INT64,
    WIRE_UINT8 as COWRIE_DTYPE_UINT8,
    WIRE_UINT16 as COWRIE_DTYPE_UINT16,
    WIRE_UINT32 as COWRIE_DTYPE_UINT32,
    WIRE_UINT64 as COWRIE_DTYPE_UINT64,
    WIRE_FLOAT64 as COWRIE_DTYPE_FLOAT64,
    WIRE_BOOL as COWRIE_DTYPE_BOOL,
)

# COWRIEType enum values (from cowrie_gen2.h — matches C enum order)
COWRIE_NULL = 0
COWRIE_BOOL = 1
COWRIE_INT64 = 2
COWRIE_UINT64 = 3
COWRIE_FLOAT64 = 4
COWRIE_DECIMAL128 = 5
COWRIE_STRING = 6
COWRIE_BYTES = 7
COWRIE_DATETIME64 = 8
COWRIE_UUID128 = 9
COWRIE_BIGINT = 10
COWRIE_EXT = 11
COWRIE_ARRAY = 12
COWRIE_OBJECT = 13
COWRIE_TENSOR = 14


# ── C Structs ─────────────────────────────────────────────────────

class COWRIEBuf(Structure):
    _fields_ = [
        ("data", POINTER(c_uint8)),
        ("len", c_size_t),
        ("cap", c_size_t),
    ]


class _StrData(Structure):
    _fields_ = [("data", c_char_p), ("len", c_size_t)]

class _BytesData(Structure):
    _fields_ = [("data", POINTER(c_uint8)), ("len", c_size_t)]

class _ArrayData(Structure):
    _fields_ = [("items", c_void_p), ("len", c_size_t)]  # COWRIEValue** items

class _ObjectData(Structure):
    _fields_ = [("members", c_void_p), ("len", c_size_t)]

class _TensorData(Structure):
    _fields_ = [
        ("dtype", c_uint8),
        ("rank", c_uint8),
        ("dims", POINTER(c_size_t)),
        ("data", POINTER(c_uint8)),
        ("data_len", c_size_t),
    ]

class _ValueUnion(ctypes.Union):
    _fields_ = [
        ("boolean", c_int),
        ("i64", c_int64),
        ("u64", c_uint64),
        ("f64", c_double),
        ("str", _StrData),
        ("bytes", _BytesData),
        ("datetime64", c_int64),
        ("uuid", c_uint8 * 16),
        ("array", _ArrayData),
        ("object", _ObjectData),
        ("tensor", _TensorData),
    ]

class COWRIEValue(Structure):
    _fields_ = [
        ("type", c_int),  # COWRIEType enum
        ("as_", _ValueUnion),
    ]

class COWRIEMember(Structure):
    _fields_ = [
        ("key", c_char_p),
        ("key_len", c_size_t),
        ("value", POINTER(COWRIEValue)),
    ]


# ── Library Loading ───────────────────────────────────────────────

def _find_lib():
    """Find libcowrie_gen2.so in known locations."""
    candidates = [
        # Relative to this file (development layout)
        os.path.join(os.path.dirname(__file__), '..', '..', 'c', 'build', 'libcowrie_gen2.so'),
        # System paths
        'libcowrie_gen2.so',
    ]
    # Check LD_LIBRARY_PATH via ctypes.util
    found = ctypes.util.find_library('cowrie_gen2')
    if found:
        candidates.insert(0, found)

    for path in candidates:
        path = os.path.abspath(path) if not path.startswith('/') and '/' in path else path
        try:
            return ctypes.CDLL(path)
        except OSError:
            continue
    return None


_lib = _find_lib()
if _lib is None:
    raise ImportError("libcowrie_gen2.so not found. Build with: cd c && mkdir -p build && cd build && cmake -DBUILD_SHARED_LIBS=ON .. && make")


# ── Function Signatures ──────────────────────────────────────────

# Buffer management
_lib.cowrie_buf_init.argtypes = [POINTER(COWRIEBuf)]
_lib.cowrie_buf_init.restype = None

_lib.cowrie_buf_free.argtypes = [POINTER(COWRIEBuf)]
_lib.cowrie_buf_free.restype = None

# Direct tensor encode (zero-malloc fast path)
_lib.cowrie_direct_encode_tensor.argtypes = [
    POINTER(COWRIEBuf), c_uint8, c_uint8,
    POINTER(c_size_t), POINTER(c_uint8), c_size_t,
]
_lib.cowrie_direct_encode_tensor.restype = c_int

# Value constructors
_lib.cowrie_new_null.argtypes = []
_lib.cowrie_new_null.restype = POINTER(COWRIEValue)

_lib.cowrie_new_bool.argtypes = [c_int]
_lib.cowrie_new_bool.restype = POINTER(COWRIEValue)

_lib.cowrie_new_int64.argtypes = [c_int64]
_lib.cowrie_new_int64.restype = POINTER(COWRIEValue)

_lib.cowrie_new_uint64.argtypes = [c_uint64]
_lib.cowrie_new_uint64.restype = POINTER(COWRIEValue)

_lib.cowrie_new_float64.argtypes = [c_double]
_lib.cowrie_new_float64.restype = POINTER(COWRIEValue)

_lib.cowrie_new_string.argtypes = [c_char_p, c_size_t]
_lib.cowrie_new_string.restype = POINTER(COWRIEValue)

_lib.cowrie_new_bytes.argtypes = [POINTER(c_uint8), c_size_t]
_lib.cowrie_new_bytes.restype = POINTER(COWRIEValue)

_lib.cowrie_new_array.argtypes = []
_lib.cowrie_new_array.restype = POINTER(COWRIEValue)

_lib.cowrie_new_object.argtypes = []
_lib.cowrie_new_object.restype = POINTER(COWRIEValue)

_lib.cowrie_new_tensor.argtypes = [
    c_uint8, c_uint8, POINTER(c_size_t), POINTER(c_uint8), c_size_t,
]
_lib.cowrie_new_tensor.restype = POINTER(COWRIEValue)

_lib.cowrie_array_append.argtypes = [POINTER(COWRIEValue), POINTER(COWRIEValue)]
_lib.cowrie_array_append.restype = c_int

_lib.cowrie_object_set.argtypes = [POINTER(COWRIEValue), c_char_p, c_size_t, POINTER(COWRIEValue)]
_lib.cowrie_object_set.restype = c_int

# Encode/decode
_lib.cowrie_encode.argtypes = [POINTER(COWRIEValue), POINTER(COWRIEBuf)]
_lib.cowrie_encode.restype = c_int

_lib.cowrie_decode.argtypes = [POINTER(c_uint8), c_size_t, POINTER(POINTER(COWRIEValue))]
_lib.cowrie_decode.restype = c_int

_lib.cowrie_free.argtypes = [POINTER(COWRIEValue)]
_lib.cowrie_free.restype = None

# Array/object accessors
_lib.cowrie_array_len.argtypes = [POINTER(COWRIEValue)]
_lib.cowrie_array_len.restype = c_size_t

_lib.cowrie_array_get.argtypes = [POINTER(COWRIEValue), c_size_t]
_lib.cowrie_array_get.restype = POINTER(COWRIEValue)

_lib.cowrie_object_len.argtypes = [POINTER(COWRIEValue)]
_lib.cowrie_object_len.restype = c_size_t


