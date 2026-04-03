# cython: language_level=3, boundscheck=False, wraparound=False
"""
Cython extension for C-speed cowrie encode/decode.
Uses CPython C API for zero-overhead type dispatch and string handling.
"""

from libc.stdint cimport uint8_t, uint16_t, uint32_t, int64_t, uint64_t
from libc.stddef cimport size_t
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_CheckExact
from cpython.unicode cimport PyUnicode_CheckExact, PyUnicode_AsUTF8AndSize
from cpython.long cimport PyLong_CheckExact
from cpython.float cimport PyFloat_CheckExact
from cpython.dict cimport PyDict_CheckExact
from cpython.list cimport PyList_CheckExact
from cpython.tuple cimport PyTuple_CheckExact
from cpython.ref cimport PyObject

import numpy as np
cimport numpy as cnp

cnp.import_array()

# ── C declarations ────────────────────────────────────────────────

cdef extern from "cowrie_gen2.h":
    ctypedef struct COWRIEBuf:
        uint8_t *data
        size_t len
        size_t cap

    ctypedef struct COWRIEValue:
        pass

    void cowrie_buf_init(COWRIEBuf *buf)
    void cowrie_buf_free(COWRIEBuf *buf)

    COWRIEValue *cowrie_new_null()
    COWRIEValue *cowrie_new_bool(int b)
    COWRIEValue *cowrie_new_int64(int64_t i)
    COWRIEValue *cowrie_new_uint64(uint64_t u)
    COWRIEValue *cowrie_new_float64(double f)
    COWRIEValue *cowrie_new_string(const char *s, size_t length)
    COWRIEValue *cowrie_new_bytes(const uint8_t *data, size_t length)
    COWRIEValue *cowrie_new_array()
    COWRIEValue *cowrie_new_object()
    COWRIEValue *cowrie_new_tensor(uint8_t dtype, uint8_t rank,
                                   const size_t *dims,
                                   const uint8_t *data, size_t data_len)

    int cowrie_array_append(COWRIEValue *arr, COWRIEValue *item)
    int cowrie_object_set(COWRIEValue *obj, const char *key, size_t key_len,
                          COWRIEValue *value)

    int cowrie_encode(const COWRIEValue *root, COWRIEBuf *buf)
    int cowrie_encode_with_dict(const COWRIEValue *root,
                                const char **keys, const size_t *key_lens, size_t key_count,
                                COWRIEBuf *buf)
    void cowrie_free(COWRIEValue *v)

    int cowrie_direct_encode_tensor(COWRIEBuf *buf, uint8_t dtype, uint8_t rank,
                                    const size_t *dims, const uint8_t *data,
                                    size_t data_len)


# ── DType constants ───────────────────────────────────────────────

cdef uint8_t DTYPE_FLOAT32  = 0x01
cdef uint8_t DTYPE_FLOAT16  = 0x02
cdef uint8_t DTYPE_INT8     = 0x04
cdef uint8_t DTYPE_INT16    = 0x05
cdef uint8_t DTYPE_INT32    = 0x06
cdef uint8_t DTYPE_INT64    = 0x07
cdef uint8_t DTYPE_UINT8    = 0x08
cdef uint8_t DTYPE_UINT16   = 0x09
cdef uint8_t DTYPE_UINT32   = 0x0A
cdef uint8_t DTYPE_UINT64   = 0x0B
cdef uint8_t DTYPE_FLOAT64  = 0x0C
cdef uint8_t DTYPE_BOOL     = 0x0D

_NP_DTYPE_MAP = {
    np.dtype('float32'):  DTYPE_FLOAT32,
    np.dtype('float64'):  DTYPE_FLOAT64,
    np.dtype('float16'):  DTYPE_FLOAT16,
    np.dtype('int8'):     DTYPE_INT8,
    np.dtype('int16'):    DTYPE_INT16,
    np.dtype('int32'):    DTYPE_INT32,
    np.dtype('int64'):    DTYPE_INT64,
    np.dtype('uint8'):    DTYPE_UINT8,
    np.dtype('uint16'):   DTYPE_UINT16,
    np.dtype('uint32'):   DTYPE_UINT32,
    np.dtype('uint64'):   DTYPE_UINT64,
    np.dtype('bool'):     DTYPE_BOOL,
}


# ── Module-level cached imports (avoid per-call import overhead) ──

cdef object _Value = None
cdef object _Type = None
cdef object _DType = None
cdef object _TensorData = None
# Cached type enum values
cdef int _T_NULL = -1
cdef int _T_BOOL = -1
cdef int _T_INT64 = -1
cdef int _T_UINT64 = -1
cdef int _T_FLOAT64 = -1
cdef int _T_STRING = -1
cdef int _T_BYTES = -1
cdef int _T_ARRAY = -1
cdef int _T_OBJECT = -1
cdef int _T_TENSOR = -1

cdef bint _imports_cached = False

cdef void _ensure_imports():
    global _Value, _Type, _DType, _TensorData, _imports_cached
    global _T_NULL, _T_BOOL, _T_INT64, _T_UINT64, _T_FLOAT64
    global _T_STRING, _T_BYTES, _T_ARRAY, _T_OBJECT, _T_TENSOR
    if _imports_cached:
        return
    from cowrie.gen2 import Value, Type, DType, TensorData
    _Value = Value
    _Type = Type
    _DType = DType
    _TensorData = TensorData
    _T_NULL = Type.NULL
    _T_BOOL = Type.BOOL
    _T_INT64 = Type.INT64
    _T_UINT64 = Type.UINT64
    _T_FLOAT64 = Type.FLOAT64
    _T_STRING = Type.STRING
    _T_BYTES = Type.BYTES
    _T_ARRAY = Type.ARRAY
    _T_OBJECT = Type.OBJECT
    _T_TENSOR = Type.TENSOR
    _imports_cached = True


# ── Value tree builder (C-speed, msgpack-style dispatch) ──────────

cdef COWRIEValue* _to_c(object obj, list key_collector) except NULL:
    """Convert Python object to COWRIEValue*. Collects object keys for single-pass encode."""
    cdef COWRIEValue *result
    cdef COWRIEValue *child
    cdef const char *utf8_ptr
    cdef Py_ssize_t utf8_len
    cdef cnp.ndarray arr
    cdef cnp.npy_intp *arr_shape
    cdef size_t dims[32]
    cdef uint8_t dtype_code, rank
    cdef Py_ssize_t i
    cdef int t

    _ensure_imports()

    # Check for Value type first
    if isinstance(obj, _Value):
        t = obj.type
        d = obj.data

        if t == _T_NULL:
            return cowrie_new_null()
        elif t == _T_BOOL:
            return cowrie_new_bool(1 if d else 0)
        elif t == _T_INT64:
            return cowrie_new_int64(<int64_t>d)
        elif t == _T_UINT64:
            return cowrie_new_uint64(<uint64_t>d)
        elif t == _T_FLOAT64:
            return cowrie_new_float64(<double>d)
        elif t == _T_STRING:
            # Zero-copy: PyUnicode_AsUTF8AndSize returns pointer to internal cache
            utf8_ptr = PyUnicode_AsUTF8AndSize(d, &utf8_len)
            return cowrie_new_string(utf8_ptr, <size_t>utf8_len)
        elif t == _T_BYTES:
            return cowrie_new_bytes(<const uint8_t*><const char*>d, len(d))
        elif t == _T_ARRAY:
            result = cowrie_new_array()
            for item in d:
                child = _to_c(item, key_collector)
                cowrie_array_append(result, child)
            return result
        elif t == _T_OBJECT:
            result = cowrie_new_object()
            for k, v in d.items():
                utf8_ptr = PyUnicode_AsUTF8AndSize(k, &utf8_len)
                key_collector.append(k)  # collect for pre-built dictionary
                child = _to_c(v, key_collector)
                cowrie_object_set(result, utf8_ptr, <size_t>utf8_len, child)
            return result
        elif t == _T_TENSOR:
            td = d  # TensorData
            if hasattr(td, 'to_numpy'):
                arr = np.ascontiguousarray(td.to_numpy())
                dtype_code = _NP_DTYPE_MAP.get(arr.dtype, DTYPE_FLOAT32)
                rank = <uint8_t>cnp.PyArray_NDIM(arr)
                arr_shape = cnp.PyArray_DIMS(arr)
                for i in range(rank):
                    dims[i] = <size_t>arr_shape[i]
                return cowrie_new_tensor(dtype_code, rank, dims,
                                         <const uint8_t*>cnp.PyArray_DATA(arr),
                                         <size_t>arr.nbytes)
            else:
                raw = td.data
                shape = td.shape
                rank = <uint8_t>len(shape)
                for i in range(rank):
                    dims[i] = shape[i]
                _dt_map = {
                    _DType.FLOAT32: DTYPE_FLOAT32, _DType.FLOAT64: DTYPE_FLOAT64,
                    _DType.FLOAT16: DTYPE_FLOAT16, _DType.INT8: DTYPE_INT8,
                    _DType.INT32: DTYPE_INT32, _DType.INT64: DTYPE_INT64,
                    _DType.UINT8: DTYPE_UINT8,
                }
                dtype_code = _dt_map.get(td.dtype, DTYPE_FLOAT32)
                return cowrie_new_tensor(dtype_code, rank, dims,
                                         <const uint8_t*><const char*>raw, len(raw))
        else:
            raise TypeError(f"Unsupported Value type: {t}")

    # ── Raw Python objects (msgpack-style fast dispatch) ──────────

    # Identity checks first (fastest — pointer comparison)
    if obj is None:
        return cowrie_new_null()
    if obj is True:
        return cowrie_new_bool(1)
    if obj is False:
        return cowrie_new_bool(0)

    # CPython C API exact type checks (no isinstance overhead)
    if PyLong_CheckExact(obj):
        return cowrie_new_int64(<int64_t>obj)
    if PyFloat_CheckExact(obj):
        return cowrie_new_float64(<double>obj)
    if PyUnicode_CheckExact(obj):
        utf8_ptr = PyUnicode_AsUTF8AndSize(obj, &utf8_len)
        return cowrie_new_string(utf8_ptr, <size_t>utf8_len)
    if PyBytes_CheckExact(obj):
        return cowrie_new_bytes(<const uint8_t*><const char*>obj, len(obj))
    if PyDict_CheckExact(obj):
        result = cowrie_new_object()
        for k, v in (<dict>obj).items():
            if PyUnicode_CheckExact(k):
                utf8_ptr = PyUnicode_AsUTF8AndSize(k, &utf8_len)
            else:
                k = str(k)
                utf8_ptr = PyUnicode_AsUTF8AndSize(k, &utf8_len)
            key_collector.append(k)
            child = _to_c(v, key_collector)
            cowrie_object_set(result, utf8_ptr, <size_t>utf8_len, child)
        return result
    if PyList_CheckExact(obj) or PyTuple_CheckExact(obj):
        result = cowrie_new_array()
        for item in obj:
            child = _to_c(item, key_collector)
            cowrie_array_append(result, child)
        return result

    # numpy array (less common in hot path, OK to use isinstance)
    if isinstance(obj, np.ndarray):
        arr = np.ascontiguousarray(obj)
        dtype_code = _NP_DTYPE_MAP.get(arr.dtype, DTYPE_FLOAT32)
        rank = <uint8_t>cnp.PyArray_NDIM(arr)
        arr_shape = cnp.PyArray_DIMS(arr)
        for i in range(rank):
            dims[i] = <size_t>arr_shape[i]
        return cowrie_new_tensor(dtype_code, rank, dims,
                                 <const uint8_t*>cnp.PyArray_DATA(arr),
                                 <size_t>arr.nbytes)

    raise TypeError(f"Cannot encode {type(obj)}")


# ── Public API ────────────────────────────────────────────────────

def cython_encode(obj) -> bytes:
    """Encode a Python Value/dict/list to cowrie bytes via Cython+C."""
    cdef list key_collector = []
    cdef COWRIEValue *c_val = _to_c(obj, key_collector)
    cdef COWRIEBuf buf

    cowrie_buf_init(&buf)
    cdef int rc = cowrie_encode(c_val, &buf)
    cowrie_free(c_val)

    if rc != 0:
        cowrie_buf_free(&buf)
        raise RuntimeError("cowrie_encode failed")

    cdef bytes result = PyBytes_FromStringAndSize(<const char*>buf.data, buf.len)
    cowrie_buf_free(&buf)
    return result


def cython_encode_tensor(object arr_in) -> bytes:
    """Encode a numpy array as cowrie tensor bytes."""
    cdef cnp.ndarray arr = np.ascontiguousarray(arr_in)
    cdef uint8_t dtype_code = _NP_DTYPE_MAP.get(arr.dtype, DTYPE_FLOAT32)
    cdef uint8_t rank = <uint8_t>cnp.PyArray_NDIM(arr)
    cdef size_t dims[32]
    cdef Py_ssize_t i
    cdef cnp.npy_intp *shape = cnp.PyArray_DIMS(arr)

    for i in range(rank):
        dims[i] = <size_t>shape[i]

    cdef COWRIEBuf buf
    cowrie_buf_init(&buf)

    cdef int rc = cowrie_direct_encode_tensor(
        &buf, dtype_code, rank, dims,
        <const uint8_t*>cnp.PyArray_DATA(arr), <size_t>arr.nbytes)

    if rc != 0:
        cowrie_buf_free(&buf)
        raise RuntimeError("cowrie_direct_encode_tensor failed")

    cdef bytes result = PyBytes_FromStringAndSize(<const char*>buf.data, buf.len)
    cowrie_buf_free(&buf)
    return result
