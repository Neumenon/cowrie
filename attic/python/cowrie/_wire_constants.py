"""Wire-format constants shared across Python codec backends.

Kept in a single module so _native.py, _fast.py, and _cext.pyx cannot
drift. DType enum values in gen2.py are the canonical wire codes; this
file mirrors them for numpy-dtype lookups.
"""

import numpy as np

# Dtype wire codes — match gen2.DType IntEnum values.
WIRE_FLOAT32 = 0x01
WIRE_FLOAT16 = 0x02
WIRE_BFLOAT16 = 0x03
WIRE_INT8 = 0x04
WIRE_INT16 = 0x05
WIRE_INT32 = 0x06
WIRE_INT64 = 0x07
WIRE_UINT8 = 0x08
WIRE_UINT16 = 0x09
WIRE_UINT32 = 0x0A
WIRE_UINT64 = 0x0B
WIRE_FLOAT64 = 0x0C
WIRE_BOOL = 0x0D

NP_TO_WIRE_DTYPE = {
    np.dtype('float32'): WIRE_FLOAT32,
    np.dtype('float64'): WIRE_FLOAT64,
    np.dtype('float16'): WIRE_FLOAT16,
    np.dtype('int8'):    WIRE_INT8,
    np.dtype('int16'):   WIRE_INT16,
    np.dtype('int32'):   WIRE_INT32,
    np.dtype('int64'):   WIRE_INT64,
    np.dtype('uint8'):   WIRE_UINT8,
    np.dtype('uint16'):  WIRE_UINT16,
    np.dtype('uint32'):  WIRE_UINT32,
    np.dtype('uint64'):  WIRE_UINT64,
    np.dtype('bool'):    WIRE_BOOL,
}
