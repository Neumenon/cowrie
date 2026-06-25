"""Build the Cython extension for cowrie._cext."""
import os
import sys
import numpy as np
from Cython.Build import cythonize
from setuptools import setup, Extension

# Paths relative to this script
here = os.path.dirname(os.path.abspath(__file__))
c_dir = os.path.join(here, '..', 'c')
c_src = os.path.join(c_dir, 'src')
c_inc = os.path.join(c_dir, 'include')

ext = Extension(
    "cowrie._cext",
    sources=[
        os.path.join(here, "cowrie", "_cext.pyx"),
        os.path.join(c_src, "gen2.c"),
        os.path.join(c_src, "json.c"),
    ],
    include_dirs=[c_inc, np.get_include()],
    libraries=["z"],  # zlib required by gen2.c
    extra_compile_args=["-O2", "-std=c11", "-D_POSIX_C_SOURCE=200809L"],
)

setup(
    name="cowrie-cext",
    ext_modules=cythonize([ext], language_level="3"),
    script_args=["build_ext", "--inplace"],
)
