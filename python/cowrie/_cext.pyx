# cython: language_level=3, boundscheck=False, wraparound=False
"""
Cython direct wire format writer for cowrie.
Writes cowrie bytes directly from Python objects — no intermediate C value tree.
"""

from libc.stdint cimport uint8_t, uint16_t, uint32_t, int64_t, uint64_t
from libc.stddef cimport size_t
from libc.stdlib cimport free, malloc, realloc
from libc.string cimport memcpy
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_CheckExact, PyBytes_AS_STRING, PyBytes_GET_SIZE
from cpython.unicode cimport PyUnicode_CheckExact, PyUnicode_AsUTF8AndSize
from cpython.long cimport PyLong_CheckExact
from cpython.float cimport PyFloat_CheckExact, PyFloat_AS_DOUBLE
from cpython.dict cimport PyDict_CheckExact
from cpython.list cimport PyList_CheckExact, PyList_GET_SIZE, PyList_GET_ITEM
from cpython.tuple cimport PyTuple_CheckExact
from cpython.ref cimport PyObject

import numpy as np
cimport numpy as cnp

cnp.import_array()

# ── Wire format constants ────────────────────────────────────────

cdef uint8_t TAG_NULL       = 0x00
cdef uint8_t TAG_FALSE      = 0x01
cdef uint8_t TAG_TRUE       = 0x02
cdef uint8_t TAG_INT64      = 0x03
cdef uint8_t TAG_FLOAT64    = 0x04
cdef uint8_t TAG_STRING     = 0x05
cdef uint8_t TAG_ARRAY      = 0x06
cdef uint8_t TAG_OBJECT     = 0x07
cdef uint8_t TAG_BYTES      = 0x08
cdef uint8_t TAG_UINT64     = 0x09
cdef uint8_t TAG_TENSOR     = 0x20
cdef uint8_t FIXINT_BASE    = 0x40
cdef uint8_t FIXARRAY_BASE  = 0xC0
cdef uint8_t FIXMAP_BASE    = 0xD0
cdef uint8_t FIXNEG_BASE    = 0xE0

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
    np.dtype('float32'): DTYPE_FLOAT32, np.dtype('float64'): DTYPE_FLOAT64,
    np.dtype('float16'): DTYPE_FLOAT16, np.dtype('int8'): DTYPE_INT8,
    np.dtype('int16'): DTYPE_INT16, np.dtype('int32'): DTYPE_INT32,
    np.dtype('int64'): DTYPE_INT64, np.dtype('uint8'): DTYPE_UINT8,
    np.dtype('uint16'): DTYPE_UINT16, np.dtype('uint32'): DTYPE_UINT32,
    np.dtype('uint64'): DTYPE_UINT64, np.dtype('bool'): DTYPE_BOOL,
}

# ── Cached imports ────────────────────────────────────────────────

cdef object _Value = None
cdef int _T_NULL = -1, _T_BOOL = -1, _T_INT64 = -1, _T_UINT64 = -1
cdef int _T_FLOAT64 = -1, _T_STRING = -1, _T_BYTES = -1
cdef int _T_ARRAY = -1, _T_OBJECT = -1, _T_TENSOR = -1
cdef object _DType = None
cdef bint _cached = False

cdef void _cache():
    global _Value, _DType, _cached
    global _T_NULL, _T_BOOL, _T_INT64, _T_UINT64, _T_FLOAT64
    global _T_STRING, _T_BYTES, _T_ARRAY, _T_OBJECT, _T_TENSOR
    if _cached: return
    from cowrie.gen2 import Value, Type, DType
    _Value = Value; _DType = DType
    _T_NULL = Type.NULL; _T_BOOL = Type.BOOL; _T_INT64 = Type.INT64
    _T_UINT64 = Type.UINT64; _T_FLOAT64 = Type.FLOAT64
    _T_STRING = Type.STRING; _T_BYTES = Type.BYTES
    _T_ARRAY = Type.ARRAY; _T_OBJECT = Type.OBJECT; _T_TENSOR = Type.TENSOR
    _cached = True


# ── Write buffer (inline, no Python overhead) ────────────────────

cdef struct WBuf:
    uint8_t *data
    size_t pos
    size_t cap

cdef inline int wb_grow(WBuf *b, size_t need) noexcept nogil:
    cdef size_t new_cap = b.cap
    cdef uint8_t *new_data
    while b.pos + need > new_cap:
        new_cap = new_cap * 2
    new_data = <uint8_t*>realloc(b.data, new_cap)
    if new_data == NULL: return -1
    b.data = new_data
    b.cap = new_cap
    return 0

cdef inline int wb_byte(WBuf *b, uint8_t v) noexcept nogil:
    if b.pos >= b.cap:
        if wb_grow(b, 1) != 0: return -1
    b.data[b.pos] = v
    b.pos += 1
    return 0

cdef inline int wb_raw(WBuf *b, const void *src, size_t n) noexcept nogil:
    if b.pos + n > b.cap:
        if wb_grow(b, n) != 0: return -1
    memcpy(b.data + b.pos, src, n)
    b.pos += n
    return 0

cdef inline int wb_uvarint(WBuf *b, uint64_t v) noexcept nogil:
    cdef uint8_t tmp[10]
    cdef int i = 0
    while v >= 0x80:
        tmp[i] = <uint8_t>((v & 0x7F) | 0x80)
        i += 1; v >>= 7
    tmp[i] = <uint8_t>v
    i += 1
    return wb_raw(b, tmp, i)

cdef inline int wb_zigzag(WBuf *b, int64_t v) noexcept nogil:
    return wb_uvarint(b, <uint64_t>((v << 1) ^ (v >> 63)))


# ── Pass 1: Collect unique keys ──────────────────────────────────

cdef void _collect(object obj, dict seen, list order):
    """Walk Python tree, collect unique object keys in insertion order."""
    cdef int t
    cdef const char *kp
    cdef Py_ssize_t kl

    if isinstance(obj, _Value):
        t = obj.type
        d = obj.data
        if t == _T_ARRAY:
            for item in d:
                _collect(item, seen, order)
        elif t == _T_OBJECT:
            for k in d:
                if k not in seen:
                    seen[k] = len(order)
                    order.append(k)
                _collect(d[k], seen, order)
        return

    if PyDict_CheckExact(obj):
        for k, v in (<dict>obj).items():
            ks = k if PyUnicode_CheckExact(k) else str(k)
            if ks not in seen:
                seen[ks] = len(order)
                order.append(ks)
            _collect(v, seen, order)
    elif PyList_CheckExact(obj) or PyTuple_CheckExact(obj):
        for item in obj:
            _collect(item, seen, order)


# ── Pass 2: Write wire format directly ───────────────────────────

cdef int _write(object obj, WBuf *b, dict key_idx) except -1:
    """Write cowrie wire bytes for obj into buffer. Returns 0 on success."""
    cdef const char *utf8
    cdef Py_ssize_t utf8_len
    cdef int64_t ival
    cdef uint64_t uval
    cdef double fval
    cdef Py_ssize_t n, i
    cdef int t, kidx
    cdef cnp.ndarray arr
    cdef cnp.npy_intp *ashape
    cdef uint8_t dtype_code, rank

    # ── Value objects ──
    if isinstance(obj, _Value):
        t = obj.type
        d = obj.data

        if t == _T_NULL:
            return wb_byte(b, TAG_NULL)
        elif t == _T_BOOL:
            return wb_byte(b, TAG_TRUE if d else TAG_FALSE)
        elif t == _T_INT64:
            ival = <int64_t>d
            if 0 <= ival <= 127:
                return wb_byte(b, <uint8_t>(FIXINT_BASE + ival))
            if -16 <= ival <= -1:
                return wb_byte(b, <uint8_t>(FIXNEG_BASE + (-1 - ival)))
            if wb_byte(b, TAG_INT64) != 0: return -1
            return wb_zigzag(b, ival)
        elif t == _T_UINT64:
            uval = <uint64_t>d
            if wb_byte(b, TAG_UINT64) != 0: return -1
            return wb_uvarint(b, uval)
        elif t == _T_FLOAT64:
            fval = <double>d
            if wb_byte(b, TAG_FLOAT64) != 0: return -1
            return wb_raw(b, &fval, 8)
        elif t == _T_STRING:
            utf8 = PyUnicode_AsUTF8AndSize(d, &utf8_len)
            if wb_byte(b, TAG_STRING) != 0: return -1
            if wb_uvarint(b, <uint64_t>utf8_len) != 0: return -1
            return wb_raw(b, utf8, utf8_len)
        elif t == _T_BYTES:
            n = len(d)
            if wb_byte(b, TAG_BYTES) != 0: return -1
            if wb_uvarint(b, <uint64_t>n) != 0: return -1
            return wb_raw(b, <const char*>d, n)
        elif t == _T_ARRAY:
            n = len(d)
            if n <= 15:
                if wb_byte(b, <uint8_t>(FIXARRAY_BASE + n)) != 0: return -1
            else:
                if wb_byte(b, TAG_ARRAY) != 0: return -1
                if wb_uvarint(b, <uint64_t>n) != 0: return -1
            for item in d:
                if _write(item, b, key_idx) != 0: return -1
            return 0
        elif t == _T_OBJECT:
            n = len(d)
            if n <= 15:
                if wb_byte(b, <uint8_t>(FIXMAP_BASE + n)) != 0: return -1
            else:
                if wb_byte(b, TAG_OBJECT) != 0: return -1
                if wb_uvarint(b, <uint64_t>n) != 0: return -1
            for k, v in d.items():
                kidx = key_idx[k]
                if wb_uvarint(b, <uint64_t>kidx) != 0: return -1
                if _write(v, b, key_idx) != 0: return -1
            return 0
        elif t == _T_TENSOR:
            td = d
            if hasattr(td, 'to_numpy'):
                arr = np.ascontiguousarray(td.to_numpy())
            else:
                # Raw tensor — write from raw bytes
                raw = td.data
                shape = td.shape
                _dt = {
                    _DType.FLOAT32: DTYPE_FLOAT32, _DType.FLOAT64: DTYPE_FLOAT64,
                    _DType.FLOAT16: DTYPE_FLOAT16, _DType.INT8: DTYPE_INT8,
                    _DType.INT32: DTYPE_INT32, _DType.INT64: DTYPE_INT64,
                    _DType.UINT8: DTYPE_UINT8,
                }
                dtype_code = _dt.get(td.dtype, DTYPE_FLOAT32)
                rank = <uint8_t>len(shape)
                if wb_byte(b, TAG_TENSOR) != 0: return -1
                if wb_byte(b, dtype_code) != 0: return -1
                if wb_byte(b, rank) != 0: return -1
                for i in range(rank):
                    if wb_uvarint(b, <uint64_t>shape[i]) != 0: return -1
                if wb_uvarint(b, <uint64_t>len(raw)) != 0: return -1
                return wb_raw(b, <const char*>raw, len(raw))

            # numpy tensor path
            dtype_code = _NP_DTYPE_MAP.get(arr.dtype, DTYPE_FLOAT32)
            rank = <uint8_t>cnp.PyArray_NDIM(arr)
            ashape = cnp.PyArray_DIMS(arr)
            if wb_byte(b, TAG_TENSOR) != 0: return -1
            if wb_byte(b, dtype_code) != 0: return -1
            if wb_byte(b, rank) != 0: return -1
            for i in range(rank):
                if wb_uvarint(b, <uint64_t>ashape[i]) != 0: return -1
            if wb_uvarint(b, <uint64_t>arr.nbytes) != 0: return -1
            return wb_raw(b, <const uint8_t*>cnp.PyArray_DATA(arr), <size_t>arr.nbytes)
        else:
            raise TypeError(f"Unsupported Value type: {t}")

    # ── Raw Python objects ──

    if obj is None:
        return wb_byte(b, TAG_NULL)
    if obj is True:
        return wb_byte(b, TAG_TRUE)
    if obj is False:
        return wb_byte(b, TAG_FALSE)
    if PyLong_CheckExact(obj):
        ival = <int64_t>obj
        if 0 <= ival <= 127:
            return wb_byte(b, <uint8_t>(FIXINT_BASE + ival))
        if -16 <= ival <= -1:
            return wb_byte(b, <uint8_t>(FIXNEG_BASE + (-1 - ival)))
        if wb_byte(b, TAG_INT64) != 0: return -1
        return wb_zigzag(b, ival)
    if PyFloat_CheckExact(obj):
        fval = PyFloat_AS_DOUBLE(obj)
        if wb_byte(b, TAG_FLOAT64) != 0: return -1
        return wb_raw(b, &fval, 8)
    if PyUnicode_CheckExact(obj):
        utf8 = PyUnicode_AsUTF8AndSize(obj, &utf8_len)
        if wb_byte(b, TAG_STRING) != 0: return -1
        if wb_uvarint(b, <uint64_t>utf8_len) != 0: return -1
        return wb_raw(b, utf8, utf8_len)
    if PyBytes_CheckExact(obj):
        n = PyBytes_GET_SIZE(obj)
        if wb_byte(b, TAG_BYTES) != 0: return -1
        if wb_uvarint(b, <uint64_t>n) != 0: return -1
        return wb_raw(b, PyBytes_AS_STRING(obj), n)
    if PyDict_CheckExact(obj):
        n = len(obj)
        if n <= 15:
            if wb_byte(b, <uint8_t>(FIXMAP_BASE + n)) != 0: return -1
        else:
            if wb_byte(b, TAG_OBJECT) != 0: return -1
            if wb_uvarint(b, <uint64_t>n) != 0: return -1
        for k, v in (<dict>obj).items():
            ks = k if PyUnicode_CheckExact(k) else str(k)
            kidx = key_idx[ks]
            if wb_uvarint(b, <uint64_t>kidx) != 0: return -1
            if _write(v, b, key_idx) != 0: return -1
        return 0
    if PyList_CheckExact(obj) or PyTuple_CheckExact(obj):
        n = len(obj)
        if n <= 15:
            if wb_byte(b, <uint8_t>(FIXARRAY_BASE + n)) != 0: return -1
        else:
            if wb_byte(b, TAG_ARRAY) != 0: return -1
            if wb_uvarint(b, <uint64_t>n) != 0: return -1
        for item in obj:
            if _write(item, b, key_idx) != 0: return -1
        return 0
    if isinstance(obj, np.ndarray):
        arr = np.ascontiguousarray(obj)
        dtype_code = _NP_DTYPE_MAP.get(arr.dtype, DTYPE_FLOAT32)
        rank = <uint8_t>cnp.PyArray_NDIM(arr)
        ashape = cnp.PyArray_DIMS(arr)
        if wb_byte(b, TAG_TENSOR) != 0: return -1
        if wb_byte(b, dtype_code) != 0: return -1
        if wb_byte(b, rank) != 0: return -1
        for i in range(rank):
            if wb_uvarint(b, <uint64_t>ashape[i]) != 0: return -1
        if wb_uvarint(b, <uint64_t>arr.nbytes) != 0: return -1
        return wb_raw(b, <const uint8_t*>cnp.PyArray_DATA(arr), <size_t>arr.nbytes)

    raise TypeError(f"Cannot direct-encode {type(obj)}")


# ── Public API ────────────────────────────────────────────────────

# Keep old _to_c + cowrie_encode for fallback
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
    void cowrie_free(COWRIEValue *v)
    int cowrie_direct_encode_tensor(COWRIEBuf *buf, uint8_t dtype, uint8_t rank,
                                    const size_t *dims, const uint8_t *data,
                                    size_t data_len)

cdef COWRIEValue* _to_c_fallback(object obj) except NULL:
    """Fallback: build COWRIEValue tree for types direct writer can't handle."""
    _cache()
    cdef COWRIEValue *result
    cdef COWRIEValue *child
    cdef const char *utf8
    cdef Py_ssize_t utf8_len
    cdef cnp.ndarray arr
    cdef cnp.npy_intp *ashape
    cdef size_t dims[32]
    cdef uint8_t dtype_code, rank
    cdef Py_ssize_t i
    cdef int t

    if isinstance(obj, _Value):
        t = obj.type; d = obj.data
        if t == _T_NULL: return cowrie_new_null()
        elif t == _T_BOOL: return cowrie_new_bool(1 if d else 0)
        elif t == _T_INT64: return cowrie_new_int64(<int64_t>d)
        elif t == _T_UINT64: return cowrie_new_uint64(<uint64_t>d)
        elif t == _T_FLOAT64: return cowrie_new_float64(<double>d)
        elif t == _T_STRING:
            utf8 = PyUnicode_AsUTF8AndSize(d, &utf8_len)
            return cowrie_new_string(utf8, <size_t>utf8_len)
        elif t == _T_BYTES:
            return cowrie_new_bytes(<const uint8_t*><const char*>d, len(d))
        elif t == _T_ARRAY:
            result = cowrie_new_array()
            for item in d:
                child = _to_c_fallback(item)
                cowrie_array_append(result, child)
            return result
        elif t == _T_OBJECT:
            result = cowrie_new_object()
            for k, v in d.items():
                utf8 = PyUnicode_AsUTF8AndSize(k, &utf8_len)
                child = _to_c_fallback(v)
                cowrie_object_set(result, utf8, <size_t>utf8_len, child)
            return result
        elif t == _T_TENSOR:
            td = d
            if hasattr(td, 'to_numpy'):
                arr = np.ascontiguousarray(td.to_numpy())
                dtype_code = _NP_DTYPE_MAP.get(arr.dtype, DTYPE_FLOAT32)
                rank = <uint8_t>cnp.PyArray_NDIM(arr)
                ashape = cnp.PyArray_DIMS(arr)
                for i in range(rank): dims[i] = <size_t>ashape[i]
                return cowrie_new_tensor(dtype_code, rank, dims,
                    <const uint8_t*>cnp.PyArray_DATA(arr), <size_t>arr.nbytes)
    # For anything else, let it fail — gen2.py pure Python fallback handles it
    raise TypeError(f"Fallback cannot handle type: {type(obj)}")


def cython_encode(obj) -> bytes:
    """Encode Python Value/dict/list to cowrie bytes. Direct wire writer, no C tree."""
    _cache()

    # Pass 1: collect unique keys
    cdef dict key_idx = {}
    cdef list key_order = []
    _collect(obj, key_idx, key_order)

    # Allocate write buffer (4KB initial — covers most payloads)
    cdef WBuf b
    b.cap = 4096
    b.pos = 0
    b.data = <uint8_t*>malloc(b.cap)
    if b.data == NULL:
        raise MemoryError()

    cdef const char *kp
    cdef Py_ssize_t kl
    cdef COWRIEValue *c_val
    cdef COWRIEBuf cbuf
    cdef int rc

    try:
        # Write header: 'S' 'J' version=2 flags=0
        wb_byte(&b, 0x53)  # 'S'
        wb_byte(&b, 0x4A)  # 'J'
        wb_byte(&b, 0x02)  # version
        wb_byte(&b, 0x00)  # flags

        # Write dictionary
        wb_uvarint(&b, <uint64_t>len(key_order))
        for k in key_order:
            kp = PyUnicode_AsUTF8AndSize(k, &kl)
            wb_uvarint(&b, <uint64_t>kl)
            wb_raw(&b, kp, kl)

        # Pass 2: write values
        _write(obj, &b, key_idx)

        return PyBytes_FromStringAndSize(<const char*>b.data, b.pos)
    except TypeError:
        # Unsupported type — fall back to C tree builder
        free(b.data)
        b.data = NULL
        c_val = _to_c_fallback(obj)
        cowrie_buf_init(&cbuf)
        rc = cowrie_encode(c_val, &cbuf)
        cowrie_free(c_val)
        if rc != 0:
            cowrie_buf_free(&cbuf)
            raise RuntimeError("cowrie_encode fallback failed")
        result = PyBytes_FromStringAndSize(<const char*>cbuf.data, cbuf.len)
        cowrie_buf_free(&cbuf)
        return result
    finally:
        if b.data != NULL:
            free(b.data)


def cython_encode_tensor(object arr_in) -> bytes:
    """Encode a numpy array as cowrie tensor bytes."""
    cdef cnp.ndarray arr = np.ascontiguousarray(arr_in)
    cdef uint8_t dtype_code = _NP_DTYPE_MAP.get(arr.dtype, DTYPE_FLOAT32)
    cdef uint8_t rank = <uint8_t>cnp.PyArray_NDIM(arr)
    cdef size_t dims[32]
    cdef Py_ssize_t i
    cdef cnp.npy_intp *shape = cnp.PyArray_DIMS(arr)
    for i in range(rank): dims[i] = <size_t>shape[i]
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


# ══════════════════════════════════════════════════════════════════
# DECODE — Direct wire reader
# ══════════════════════════════════════════════════════════════════

cdef struct RBuf:
    const uint8_t *data
    size_t pos
    size_t len

cdef inline int rb_byte(RBuf *r, uint8_t *out) noexcept nogil:
    if r.pos >= r.len: return -1
    out[0] = r.data[r.pos]; r.pos += 1
    return 0

cdef inline int rb_raw(RBuf *r, size_t n, const uint8_t **out) noexcept nogil:
    if r.pos + n > r.len: return -1
    out[0] = r.data + r.pos; r.pos += n
    return 0

cdef inline int rb_uvarint(RBuf *r, uint64_t *out) noexcept nogil:
    cdef uint64_t val = 0
    cdef int shift = 0
    cdef uint8_t b
    while True:
        if r.pos >= r.len: return -1
        b = r.data[r.pos]; r.pos += 1
        val |= (<uint64_t>(b & 0x7F)) << shift
        if (b & 0x80) == 0:
            out[0] = val
            return 0
        shift += 7
        if shift >= 64: return -1

cdef inline int64_t zigzag_dec(uint64_t v) noexcept nogil:
    return <int64_t>((v >> 1) ^ -<int64_t>(v & 1))


cdef object _read_value(RBuf *r, list dictionary, int depth):
    """Decode one cowrie value from the read buffer."""
    cdef uint8_t tag, dt, rk, flags_byte
    cdef uint64_t uv, count, field_id, data_len
    cdef int64_t iv
    cdef double fval
    cdef float f32val
    cdef const uint8_t *raw
    cdef Py_ssize_t i, n
    cdef list items
    cdef dict members

    if depth > 1000:
        raise ValueError("Maximum nesting depth exceeded")

    if rb_byte(r, &tag) != 0:
        raise ValueError("Unexpected end of data")

    # ── Inline types (most common first) ──
    if FIXINT_BASE <= tag <= 0xBF:
        return _Value(Type_INT64, <int64_t>(tag - FIXINT_BASE))

    if FIXMAP_BASE <= tag <= 0xDF:
        count = tag - FIXMAP_BASE
        members = {}
        for i in range(<Py_ssize_t>count):
            if rb_uvarint(r, &field_id) != 0: raise ValueError("truncated")
            if field_id >= <uint64_t>len(dictionary): raise ValueError("bad dict idx")
            key = dictionary[<Py_ssize_t>field_id]
            members[key] = _read_value(r, dictionary, depth + 1)
        return _Value(Type_OBJECT, members)

    if FIXARRAY_BASE <= tag <= 0xCF:
        count = tag - FIXARRAY_BASE
        items = []
        for i in range(<Py_ssize_t>count):
            items.append(_read_value(r, dictionary, depth + 1))
        return _Value(Type_ARRAY, items)

    if FIXNEG_BASE <= tag <= 0xEF:
        return _Value(Type_INT64, <int64_t>(-1 - (tag - FIXNEG_BASE)))

    # ── Tag-based types ──
    if tag == TAG_NULL:
        return _Value(Type_NULL, None)
    elif tag == TAG_FALSE:
        return _Value(Type_BOOL, False)
    elif tag == TAG_TRUE:
        return _Value(Type_BOOL, True)
    elif tag == TAG_INT64:
        if rb_uvarint(r, &uv) != 0: raise ValueError("truncated")
        return _Value(Type_INT64, zigzag_dec(uv))
    elif tag == TAG_UINT64:
        if rb_uvarint(r, &uv) != 0: raise ValueError("truncated")
        return _Value(Type_UINT64, <object>uv)
    elif tag == TAG_FLOAT64:
        if rb_raw(r, 8, &raw) != 0: raise ValueError("truncated")
        memcpy(&fval, raw, 8)
        return _Value(Type_FLOAT64, fval)
    elif tag == 0x0F:  # FLOAT32
        if rb_raw(r, 4, &raw) != 0: raise ValueError("truncated")
        memcpy(&f32val, raw, 4)
        return _Value(Type_FLOAT64, <double>f32val)
    elif tag == TAG_STRING:
        if rb_uvarint(r, &uv) != 0: raise ValueError("truncated")
        if rb_raw(r, <size_t>uv, &raw) != 0: raise ValueError("truncated")
        return _Value(Type_STRING, (<const char*>raw)[:uv].decode('utf-8'))
    elif tag == TAG_BYTES:
        if rb_uvarint(r, &uv) != 0: raise ValueError("truncated")
        if rb_raw(r, <size_t>uv, &raw) != 0: raise ValueError("truncated")
        return _Value(Type_BYTES, (<const char*>raw)[:uv])
    elif tag == TAG_ARRAY:
        if rb_uvarint(r, &count) != 0: raise ValueError("truncated")
        items = []
        for i in range(<Py_ssize_t>count):
            items.append(_read_value(r, dictionary, depth + 1))
        return _Value(Type_ARRAY, items)
    elif tag == TAG_OBJECT:
        if rb_uvarint(r, &count) != 0: raise ValueError("truncated")
        members = {}
        for i in range(<Py_ssize_t>count):
            if rb_uvarint(r, &field_id) != 0: raise ValueError("truncated")
            if field_id >= <uint64_t>len(dictionary): raise ValueError("bad dict idx")
            key = dictionary[<Py_ssize_t>field_id]
            members[key] = _read_value(r, dictionary, depth + 1)
        return _Value(Type_OBJECT, members)
    elif tag == TAG_TENSOR:
        if rb_byte(r, &dt) != 0: raise ValueError("truncated")
        if rb_byte(r, &rk) != 0: raise ValueError("truncated")
        shape = []
        for i in range(rk):
            if rb_uvarint(r, &uv) != 0: raise ValueError("truncated")
            shape.append(<int>uv)
        if rb_uvarint(r, &data_len) != 0: raise ValueError("truncated")
        if rb_raw(r, <size_t>data_len, &raw) != 0: raise ValueError("truncated")
        tensor_bytes = (<const char*>raw)[:data_len]
        from cowrie.gen2 import DType as DT_enum
        return _Value(Type_TENSOR, TensorData_cls(DT_enum(dt), shape, tensor_bytes))
    else:
        # Unsupported tag — fall back to pure Python
        raise TypeError(f"Unsupported tag: 0x{tag:02x}")


# Cached type constants for decode (avoid attribute lookups)
cdef object Type_NULL, Type_BOOL, Type_INT64, Type_UINT64, Type_FLOAT64
cdef object Type_STRING, Type_BYTES, Type_ARRAY, Type_OBJECT, Type_TENSOR
cdef object TensorData_cls
cdef bint _decode_cached = False

cdef void _cache_decode():
    global Type_NULL, Type_BOOL, Type_INT64, Type_UINT64, Type_FLOAT64
    global Type_STRING, Type_BYTES, Type_ARRAY, Type_OBJECT, Type_TENSOR
    global TensorData_cls, _decode_cached
    if _decode_cached: return
    from cowrie.gen2 import Type, TensorData
    Type_NULL = Type.NULL; Type_BOOL = Type.BOOL; Type_INT64 = Type.INT64
    Type_UINT64 = Type.UINT64; Type_FLOAT64 = Type.FLOAT64
    Type_STRING = Type.STRING; Type_BYTES = Type.BYTES
    Type_ARRAY = Type.ARRAY; Type_OBJECT = Type.OBJECT; Type_TENSOR = Type.TENSOR
    TensorData_cls = TensorData
    _decode_cached = True


def cython_decode(bytes data):
    """Decode cowrie bytes to a Python Value. Direct wire reader."""
    _cache_decode()

    cdef RBuf r
    r.data = <const uint8_t*>data
    r.pos = 0
    r.len = len(data)

    # Read header
    cdef uint8_t m0, m1, ver, flags
    if rb_byte(&r, &m0) != 0 or rb_byte(&r, &m1) != 0: raise ValueError("truncated header")
    if m0 != 0x53 or m1 != 0x4A: raise ValueError("invalid magic")
    if rb_byte(&r, &ver) != 0: raise ValueError("truncated header")
    if ver != 0x02: raise ValueError(f"unsupported version: {ver}")
    if rb_byte(&r, &flags) != 0: raise ValueError("truncated header")

    # Skip column hints if present
    cdef uint64_t uv, hint_count, shape_len
    if flags & 0x08:
        if rb_uvarint(&r, &hint_count) != 0: raise ValueError("truncated hints")
        for i in range(<Py_ssize_t>hint_count):
            if rb_uvarint(&r, &uv) != 0: raise ValueError("truncated")
            r.pos += <size_t>uv  # skip field name
            r.pos += 1  # skip type byte
            if rb_uvarint(&r, &shape_len) != 0: raise ValueError("truncated")
            for j in range(<Py_ssize_t>shape_len):
                if rb_uvarint(&r, &uv) != 0: raise ValueError("truncated")
            r.pos += 1  # skip flags

    # Read dictionary
    cdef uint64_t dict_len
    if rb_uvarint(&r, &dict_len) != 0: raise ValueError("truncated dict")
    cdef list dictionary = []
    cdef const uint8_t *sraw
    for i in range(<Py_ssize_t>dict_len):
        if rb_uvarint(&r, &uv) != 0: raise ValueError("truncated dict entry")
        if rb_raw(&r, <size_t>uv, &sraw) != 0: raise ValueError("truncated dict entry")
        dictionary.append((<const char*>sraw)[:uv].decode('utf-8'))

    # Decode root value
    result = _read_value(&r, dictionary, 0)

    # Verify all consumed
    if r.pos < r.len:
        raise ValueError(f"trailing data: {r.len - r.pos} bytes")

    return result
