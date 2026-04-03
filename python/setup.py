#!/usr/bin/env python
"""Build script for cowrie-py with optional C extension.

The C extension provides 10x faster encode/decode for common types.
If compilation fails (no C compiler, PyPy, etc.), cowrie falls back
to pure Python automatically.

To regenerate _cext.c from _cext.pyx:
    pip install cython numpy
    cython cowrie/_cext.pyx
"""
import os
import sys

from setuptools import Extension, setup

PYPY = hasattr(sys, "pypy_version_info")
ext_modules = []

if not PYPY and not os.environ.get("COWRIE_PUREPYTHON"):
    # Find numpy include dir (needed for cimport numpy)
    try:
        import numpy as np
        numpy_include = [np.get_include()]
    except ImportError:
        numpy_include = []

    # C sources are in ../c/ relative to this setup.py
    c_src = os.path.join("..", "c", "src")
    c_inc = os.path.join("..", "c", "include")

    # Check if C sources exist (they won't in a PyPI sdist without them)
    gen2_c = os.path.join(c_src, "gen2.c")
    if os.path.exists(gen2_c):
        ext_modules.append(
            Extension(
                "cowrie._cext",
                sources=[
                    os.path.join("cowrie", "_cext.c"),
                    os.path.join(c_src, "gen2.c"),
                    os.path.join(c_src, "json.c"),
                ],
                include_dirs=[c_inc] + numpy_include,
                libraries=["z"],
                extra_compile_args=["-O2", "-std=c11", "-D_POSIX_C_SOURCE=200809L"],
            )
        )
    else:
        print("NOTE: C sources not found (../c/src/gen2.c). "
              "Building without C extension — pure Python only.")

setup(
    ext_modules=ext_modules,
    packages=["cowrie"],
)
