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

# ── DType mapping (mirrors COWRIEDType enum) ──────────────────────

COWRIE_DTYPE_FLOAT32  = 0x01
COWRIE_DTYPE_FLOAT16  = 0x02
COWRIE_DTYPE_BFLOAT16 = 0x03
COWRIE_DTYPE_INT8     = 0x04
COWRIE_DTYPE_INT16    = 0x05
COWRIE_DTYPE_INT32    = 0x06
COWRIE_DTYPE_INT64    = 0x07
COWRIE_DTYPE_UINT8    = 0x08
COWRIE_DTYPE_UINT16   = 0x09
COWRIE_DTYPE_UINT32   = 0x0A
COWRIE_DTYPE_UINT64   = 0x0B
COWRIE_DTYPE_FLOAT64  = 0x0C
COWRIE_DTYPE_BOOL     = 0x0D

_NP_TO_COWRIE_DTYPE = {
    np.dtype('float32'):  COWRIE_DTYPE_FLOAT32,
    np.dtype('float64'):  COWRIE_DTYPE_FLOAT64,
    np.dtype('float16'):  COWRIE_DTYPE_FLOAT16,
    np.dtype('int8'):     COWRIE_DTYPE_INT8,
    np.dtype('int16'):    COWRIE_DTYPE_INT16,
    np.dtype('int32'):    COWRIE_DTYPE_INT32,
    np.dtype('int64'):    COWRIE_DTYPE_INT64,
    np.dtype('uint8'):    COWRIE_DTYPE_UINT8,
    np.dtype('uint16'):   COWRIE_DTYPE_UINT16,
    np.dtype('uint32'):   COWRIE_DTYPE_UINT32,
    np.dtype('uint64'):   COWRIE_DTYPE_UINT64,
    np.dtype('bool'):     COWRIE_DTYPE_BOOL,
}

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


# ── Public API ────────────────────────────────────────────────────

def native_encode_tensor(arr: np.ndarray) -> bytes:
    """Encode a numpy array as a cowrie tensor. Zero-copy from numpy buffer."""
    arr = np.ascontiguousarray(arr)
    dtype = _NP_TO_COWRIE_DTYPE.get(arr.dtype)
    if dtype is None:
        raise ValueError(f"Unsupported numpy dtype: {arr.dtype}")

    rank = len(arr.shape)
    dims = (c_size_t * rank)(*arr.shape)
    data_ptr = arr.ctypes.data_as(POINTER(c_uint8))

    buf = COWRIEBuf()
    _lib.cowrie_buf_init(byref(buf))

    rc = _lib.cowrie_direct_encode_tensor(byref(buf), dtype, rank, dims, data_ptr, arr.nbytes)
    if rc != 0:
        _lib.cowrie_buf_free(byref(buf))
        raise RuntimeError("cowrie_direct_encode_tensor failed")

    result = bytes(ctypes.string_at(buf.data, buf.len))
    _lib.cowrie_buf_free(byref(buf))
    return result


def _python_value_to_c(obj) -> POINTER(COWRIEValue):
    """Convert a Python object to a COWRIEValue pointer. Caller must cowrie_free()."""
    # Import here to avoid circular dependency
    from .gen2 import Value, Type, DType, TensorData

    if isinstance(obj, Value):
        t = obj.type
        if t == Type.NULL:
            return _lib.cowrie_new_null()
        elif t == Type.BOOL:
            return _lib.cowrie_new_bool(1 if obj.data else 0)
        elif t == Type.INT64:
            return _lib.cowrie_new_int64(obj.data)
        elif t == Type.UINT64:
            return _lib.cowrie_new_uint64(obj.data)
        elif t == Type.FLOAT64:
            return _lib.cowrie_new_float64(obj.data)
        elif t == Type.STRING:
            s = obj.data.encode('utf-8')
            return _lib.cowrie_new_string(s, len(s))
        elif t == Type.BYTES:
            data = obj.data
            return _lib.cowrie_new_bytes(
                cast(ctypes.create_string_buffer(data, len(data)), POINTER(c_uint8)),
                len(data),
            )
        elif t == Type.ARRAY:
            c_arr = _lib.cowrie_new_array()
            for item in obj.data:
                c_item = _python_value_to_c(item)
                _lib.cowrie_array_append(c_arr, c_item)
            return c_arr
        elif t == Type.OBJECT:
            c_obj = _lib.cowrie_new_object()
            for k, v in obj.data.items():
                key_bytes = k.encode('utf-8')
                c_val = _python_value_to_c(v)
                _lib.cowrie_object_set(c_obj, key_bytes, len(key_bytes), c_val)
            return c_obj
        elif t == Type.TENSOR:
            td = obj.data  # TensorData
            arr = td.to_numpy() if hasattr(td, 'to_numpy') else None
            if arr is not None:
                arr = np.ascontiguousarray(arr)
                dtype = _NP_TO_COWRIE_DTYPE.get(arr.dtype, COWRIE_DTYPE_FLOAT32)
                rank = len(arr.shape)
                dims = (c_size_t * rank)(*arr.shape)
                data_ptr = arr.ctypes.data_as(POINTER(c_uint8))
                return _lib.cowrie_new_tensor(dtype, rank, dims, data_ptr, arr.nbytes)
            # Fallback: use raw tensor data
            dtype_map = {
                DType.FLOAT32: COWRIE_DTYPE_FLOAT32,
                DType.FLOAT64: COWRIE_DTYPE_FLOAT64,
                DType.FLOAT16: COWRIE_DTYPE_FLOAT16,
                DType.INT8: COWRIE_DTYPE_INT8,
                DType.INT32: COWRIE_DTYPE_INT32,
                DType.INT64: COWRIE_DTYPE_INT64,
                DType.UINT8: COWRIE_DTYPE_UINT8,
            }
            c_dtype = dtype_map.get(td.dtype, COWRIE_DTYPE_FLOAT32)
            rank = len(td.shape)
            dims = (c_size_t * rank)(*td.shape)
            raw = td.data
            return _lib.cowrie_new_tensor(
                c_dtype, rank, dims,
                cast(ctypes.create_string_buffer(raw, len(raw)), POINTER(c_uint8)),
                len(raw),
            )
        # Fallback for other types — let pure Python handle it
        raise TypeError(f"Cannot convert Value type {t} to C")

    # Raw Python objects
    if obj is None:
        return _lib.cowrie_new_null()
    if isinstance(obj, bool):
        return _lib.cowrie_new_bool(1 if obj else 0)
    if isinstance(obj, int):
        if obj < 0 or obj > (2**63 - 1):
            return _lib.cowrie_new_int64(obj)
        return _lib.cowrie_new_int64(obj)
    if isinstance(obj, float):
        return _lib.cowrie_new_float64(obj)
    if isinstance(obj, str):
        s = obj.encode('utf-8')
        return _lib.cowrie_new_string(s, len(s))
    if isinstance(obj, bytes):
        return _lib.cowrie_new_bytes(
            cast(ctypes.create_string_buffer(obj, len(obj)), POINTER(c_uint8)),
            len(obj),
        )
    if isinstance(obj, np.ndarray):
        arr = np.ascontiguousarray(obj)
        dtype = _NP_TO_COWRIE_DTYPE.get(arr.dtype, COWRIE_DTYPE_FLOAT32)
        rank = len(arr.shape)
        dims = (c_size_t * rank)(*arr.shape)
        data_ptr = arr.ctypes.data_as(POINTER(c_uint8))
        return _lib.cowrie_new_tensor(dtype, rank, dims, data_ptr, arr.nbytes)
    if isinstance(obj, dict):
        c_obj = _lib.cowrie_new_object()
        for k, v in obj.items():
            key_bytes = str(k).encode('utf-8')
            c_val = _python_value_to_c(v)
            _lib.cowrie_object_set(c_obj, key_bytes, len(key_bytes), c_val)
        return c_obj
    if isinstance(obj, (list, tuple)):
        c_arr = _lib.cowrie_new_array()
        for item in obj:
            c_item = _python_value_to_c(item)
            _lib.cowrie_array_append(c_arr, c_item)
        return c_arr
    raise TypeError(f"Cannot convert {type(obj)} to COWRIEValue")


def _c_value_to_python(ptr: POINTER(COWRIEValue)):
    """Convert a COWRIEValue pointer to Python objects."""
    from .gen2 import Value, Type, DType, TensorData

    v = ptr.contents
    t = v.type

    if t == COWRIE_NULL:
        return Value.null()
    elif t == COWRIE_BOOL:
        return Value.bool_(bool(v.as_.boolean))
    elif t == COWRIE_INT64:
        return Value.int64(v.as_.i64)
    elif t == COWRIE_UINT64:
        return Value.uint64(v.as_.u64)
    elif t == COWRIE_FLOAT64:
        return Value.float64(v.as_.f64)
    elif t == COWRIE_STRING:
        s = v.as_.str
        return Value.string(ctypes.string_at(s.data, s.len).decode('utf-8'))
    elif t == COWRIE_BYTES:
        b = v.as_.bytes
        return Value.bytes_(bytes(ctypes.string_at(b.data, b.len)))
    elif t == COWRIE_ARRAY:
        n = _lib.cowrie_array_len(ptr)
        items = []
        for i in range(n):
            child = _lib.cowrie_array_get(ptr, i)
            items.append(_c_value_to_python(child))
        return Value.array(items)
    elif t == COWRIE_OBJECT:
        n = _lib.cowrie_object_len(ptr)
        d = {}
        obj_data = v.as_.object
        member_arr = cast(obj_data.members, POINTER(COWRIEMember))
        for i in range(n):
            m = member_arr[i]
            key = ctypes.string_at(m.key, m.key_len).decode('utf-8')
            val = _c_value_to_python(m.value)
            d[key] = val
        return Value.object(d)
    elif t == COWRIE_TENSOR:
        td = v.as_.tensor
        dtype_map = {
            COWRIE_DTYPE_FLOAT32: DType.FLOAT32,
            COWRIE_DTYPE_FLOAT64: DType.FLOAT64,
            COWRIE_DTYPE_FLOAT16: DType.FLOAT16,
            COWRIE_DTYPE_INT8: DType.INT8,
            COWRIE_DTYPE_INT32: DType.INT32,
            COWRIE_DTYPE_INT64: DType.INT64,
            COWRIE_DTYPE_UINT8: DType.UINT8,
        }
        shape = tuple(td.dims[i] for i in range(td.rank))
        raw = bytes(ctypes.string_at(td.data, td.data_len))
        cow_dtype = dtype_map.get(td.dtype, DType.FLOAT32)
        return Value.tensor(cow_dtype, list(shape), raw)
    else:
        # Fallback: return null for unsupported types
        return Value.null()


def native_encode(value) -> bytes:
    """Encode a Python Value (or dict/list/scalar) to cowrie bytes via C."""
    c_val = _python_value_to_c(value)
    try:
        buf = COWRIEBuf()
        _lib.cowrie_buf_init(byref(buf))
        rc = _lib.cowrie_encode(c_val, byref(buf))
        if rc != 0:
            _lib.cowrie_buf_free(byref(buf))
            raise RuntimeError("cowrie_encode failed")
        result = bytes(ctypes.string_at(buf.data, buf.len))
        _lib.cowrie_buf_free(byref(buf))
        return result
    finally:
        _lib.cowrie_free(c_val)


def native_decode(data: bytes):
    """Decode cowrie bytes to a Python Value via C."""
    out = POINTER(COWRIEValue)()
    buf = (c_uint8 * len(data)).from_buffer_copy(data)
    rc = _lib.cowrie_decode(buf, len(data), byref(out))
    if rc != 0:
        raise RuntimeError("cowrie_decode failed")
    try:
        return _c_value_to_python(out)
    finally:
        _lib.cowrie_free(out)
