#!/usr/bin/env python
"""Build script for cowrie-py with optional C extension.

The C extension provides 10x faster encode/decode for common types.
If compilation fails (no C compiler, missing zlib, etc.), cowrie falls
back to pure Python automatically — no functionality is lost.

C sources are bundled in csrc/ for sdist builds. In development,
they're also found via ../c/.

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
    # Find C sources: csrc/ (sdist) or ../c/ (dev checkout)
    # csrc/ mirrors ../c/ layout so #include "../include/..." works
    if os.path.exists("csrc/src/gen2.c"):
        c_sources = ["csrc/src/gen2.c", "csrc/src/json.c"]
        c_include = ["csrc/include"]
    elif os.path.exists("../c/src/gen2.c"):
        c_sources = ["../c/src/gen2.c", "../c/src/json.c"]
        c_include = ["../c/include"]
    else:
        c_sources = None
        print("NOTE: C sources not found. Installing pure Python only.")

    if c_sources and os.path.exists("cowrie/_cext.c"):
        # numpy include must go through -I flag (absolute paths in
        # include_dirs are rejected by setuptools)
        extra_cflags = ["-O2", "-std=c11", "-D_POSIX_C_SOURCE=200809L"]
        try:
            import numpy as np
            extra_cflags.append(f"-I{np.get_include()}")
        except ImportError:
            pass

        ext_modules.append(
            Extension(
                "cowrie._cext",
                sources=["cowrie/_cext.c"] + c_sources,
                include_dirs=c_include,
                libraries=["z"],
                extra_compile_args=extra_cflags,
            )
        )

setup(
    ext_modules=ext_modules,
    packages=["cowrie"],
)
