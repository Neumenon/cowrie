"""
Descriptor ring encoder — io_uring-style one-call Python→C bridge.

Python packs a flat descriptor buffer (struct.pack, C-accelerated),
then a single ctypes call to C does dictionary coding + varint encoding.
"""

import struct
import ctypes
from ctypes import c_uint8, c_size_t, POINTER, byref

import numpy as np

from ._native import _lib, COWRIEBuf, _NP_TO_COWRIE_DTYPE

# Descriptor tags (must match DESC_* in gen2.c)
_NULL    = 0x00
_FALSE   = 0x01
_TRUE    = 0x02
_INT64   = 0x03
_FLOAT64 = 0x04
_STRING  = 0x05
_BYTES   = 0x06
_UINT64  = 0x07
_ARRAY   = 0x10
_OBJECT  = 0x11
_TENSOR  = 0x20

# Pre-compute struct packers (avoid repeated format string parsing)
_pack_tag = struct.Struct('<B').pack
_pack_i64 = struct.Struct('<Bq').pack
_pack_u64 = struct.Struct('<BQ').pack
_pack_f64 = struct.Struct('<Bd').pack
_pack_u32 = struct.Struct('<I').pack
_pack_u16 = struct.Struct('<H').pack
_pack_tensor_hdr = struct.Struct('<BBB').pack  # tag + dtype + rank

# Set up the C function signature
_lib.cowrie_encode_from_descriptor.argtypes = [
    POINTER(c_uint8), c_size_t,           # desc, desc_len
    POINTER(POINTER(c_uint8)), POINTER(c_size_t), c_size_t,  # tensor_ptrs, tensor_lens, tensor_count
    POINTER(COWRIEBuf),                    # out
]
_lib.cowrie_encode_from_descriptor.restype = ctypes.c_int


def _pack_value(obj, parts, tensors):
    """Pack a Python object into descriptor parts. Collects tensor pointers separately."""
    from .gen2 import Value, Type, DType

    if isinstance(obj, Value):
        t = obj.type
        d = obj.data
        if t == Type.NULL:
            parts.append(_pack_tag(_NULL))
        elif t == Type.BOOL:
            parts.append(_pack_tag(_TRUE if d else _FALSE))
        elif t == Type.INT64:
            parts.append(_pack_i64(_INT64, d))
        elif t == Type.UINT64:
            parts.append(_pack_u64(_UINT64, d))
        elif t == Type.FLOAT64:
            parts.append(_pack_f64(_FLOAT64, d))
        elif t == Type.STRING:
            s = d.encode('utf-8')
            parts.append(_pack_tag(_STRING))
            parts.append(_pack_u32(len(s)))
            parts.append(s)
        elif t == Type.BYTES:
            parts.append(_pack_tag(_BYTES))
            parts.append(_pack_u32(len(d)))
            parts.append(d)
        elif t == Type.ARRAY:
            parts.append(_pack_tag(_ARRAY))
            parts.append(_pack_u32(len(d)))
            for item in d:
                _pack_value(item, parts, tensors)
        elif t == Type.OBJECT:
            parts.append(_pack_tag(_OBJECT))
            parts.append(_pack_u32(len(d)))
            for k, v in d.items():
                kb = k.encode('utf-8')
                parts.append(_pack_u16(len(kb)))
                parts.append(kb)
                _pack_value(v, parts, tensors)
        elif t == Type.TENSOR:
            td = d  # TensorData
            arr = td.to_numpy() if hasattr(td, 'to_numpy') else None
            if arr is not None:
                arr = np.ascontiguousarray(arr)
                dtype_code = _NP_TO_COWRIE_DTYPE.get(arr.dtype, 0x01)
            else:
                # DType is an IntEnum whose value is the wire code.
                dtype_code = int(td.dtype)
                arr = None

            shape = td.shape if arr is None else arr.shape
            rank = len(shape)
            ptr_index = len(tensors)

            parts.append(_pack_tensor_hdr(_TENSOR, dtype_code, rank))
            for dim in shape:
                parts.append(_pack_u32(dim))
            parts.append(_pack_u32(ptr_index))

            if arr is not None:
                tensors.append((arr.ctypes.data_as(POINTER(c_uint8)), arr.nbytes, arr))
            else:
                raw = td.data
                buf = (c_uint8 * len(raw)).from_buffer_copy(raw)
                tensors.append((ctypes.cast(buf, POINTER(c_uint8)), len(raw), buf))
        else:
            raise TypeError(f"Unsupported Value type for fast encode: {t}")
        return

    # Raw Python objects
    if obj is None:
        parts.append(_pack_tag(_NULL))
    elif isinstance(obj, bool):
        parts.append(_pack_tag(_TRUE if obj else _FALSE))
    elif isinstance(obj, int):
        if obj < 0:
            parts.append(_pack_i64(_INT64, obj))
        else:
            parts.append(_pack_u64(_UINT64, obj))
    elif isinstance(obj, float):
        parts.append(_pack_f64(_FLOAT64, obj))
    elif isinstance(obj, str):
        s = obj.encode('utf-8')
        parts.append(_pack_tag(_STRING))
        parts.append(_pack_u32(len(s)))
        parts.append(s)
    elif isinstance(obj, bytes):
        parts.append(_pack_tag(_BYTES))
        parts.append(_pack_u32(len(obj)))
        parts.append(obj)
    elif isinstance(obj, np.ndarray):
        arr = np.ascontiguousarray(obj)
        dtype_code = _NP_TO_COWRIE_DTYPE.get(arr.dtype, 0x01)
        rank = len(arr.shape)
        ptr_index = len(tensors)
        parts.append(_pack_tensor_hdr(_TENSOR, dtype_code, rank))
        for dim in arr.shape:
            parts.append(_pack_u32(dim))
        parts.append(_pack_u32(ptr_index))
        tensors.append((arr.ctypes.data_as(POINTER(c_uint8)), arr.nbytes, arr))
    elif isinstance(obj, dict):
        parts.append(_pack_tag(_OBJECT))
        parts.append(_pack_u32(len(obj)))
        for k, v in obj.items():
            kb = str(k).encode('utf-8')
            parts.append(_pack_u16(len(kb)))
            parts.append(kb)
            _pack_value(v, parts, tensors)
    elif isinstance(obj, (list, tuple)):
        parts.append(_pack_tag(_ARRAY))
        parts.append(_pack_u32(len(obj)))
        for item in obj:
            _pack_value(item, parts, tensors)
    else:
        raise TypeError(f"Cannot fast-encode {type(obj)}")


def fast_encode(obj) -> bytes:
    """Encode a Python Value/dict/list to cowrie bytes via descriptor ring."""
    parts = []
    tensors = []  # list of (ctypes_ptr, nbytes, ref_holder)
    _pack_value(obj, parts, tensors)

    desc = b''.join(parts)
    desc_buf = (c_uint8 * len(desc)).from_buffer_copy(desc)

    tc = len(tensors)
    if tc > 0:
        PtrArray = POINTER(c_uint8) * tc
        LenArray = c_size_t * tc
        tp = PtrArray(*[t[0] for t in tensors])
        tl = LenArray(*[t[1] for t in tensors])
    else:
        tp = None
        tl = None

    out = COWRIEBuf()
    rc = _lib.cowrie_encode_from_descriptor(
        desc_buf, len(desc),
        tp, tl, tc,
        byref(out),
    )
    if rc != 0:
        raise RuntimeError("cowrie_encode_from_descriptor failed")

    result = bytes(ctypes.string_at(out.data, out.len))
    _lib.cowrie_buf_free(byref(out))
    return result
