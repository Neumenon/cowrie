#!/usr/bin/env python
"""Build script for cowrie-py with an optional Cython extension.

cowrie-py is pure Python by default. The optional Cython extension can
accelerate encode/decode where it builds, but it is best-effort: if
compilation fails (no compiler, missing zlib, or a NumPy C-ABI mismatch),
cowrie falls back to pure Python automatically — no functionality is lost.

C sources are bundled in csrc/ (self-contained — the standalone c/
implementation was removed; csrc/ is now the canonical Cython backend).

To regenerate _cext.c from _cext.pyx:
    pip install cython numpy
    cython cowrie/_cext.pyx
"""
import os
import sys

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext as _build_ext

try:
    from distutils.errors import (
        CCompilerError, DistutilsExecError, DistutilsPlatformError,
    )
except ImportError:  # distutils dropped from stdlib in 3.12+; setuptools vendors it
    from setuptools._distutils.errors import (  # type: ignore[no-redef]
        CCompilerError, DistutilsExecError, DistutilsPlatformError,
    )


class _OptionalBuildExt(_build_ext):
    """Build the optional Cython extension, falling back to pure Python if it fails.

    A numpy C-ABI mismatch or a missing compiler can break the extension build;
    cowrie's runtime imports degrade to the pure-Python path, so a failed extension
    build must never fail the whole install (this is what the module docstring
    promised but was not previously implemented).
    """

    def run(self):
        try:
            super().run()
        except Exception as exc:  # noqa: BLE001 — any build failure -> pure Python
            print(f"WARNING: C extension build skipped ({exc}); using pure Python.")

    def build_extension(self, ext):
        try:
            super().build_extension(ext)
        except (CCompilerError, DistutilsExecError, DistutilsPlatformError, OSError, ValueError) as exc:
            print(f"WARNING: C extension '{ext.name}' build failed ({exc}); using pure Python.")


PYPY = hasattr(sys, "pypy_version_info")
ext_modules = []

if not PYPY and not os.environ.get("COWRIE_PUREPYTHON"):
    # C sources for the Cython extension are bundled in csrc/ (self-contained;
    # the standalone c/ implementation was removed).
    if os.path.exists("csrc/src/gen2.c"):
        c_sources = ["csrc/src/gen2.c", "csrc/src/json.c"]
        c_include = ["csrc/include"]
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
    cmdclass={"build_ext": _OptionalBuildExt},
)
