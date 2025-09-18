"""
Build script for opencis.util.fast_logger Cython extension.
"""

import os
from setuptools import setup, Extension
from Cython.Build import cythonize

here = os.path.abspath(os.path.dirname(__file__))

common_cflags = [
    "-O3",
    "-fno-omit-frame-pointer",
    "-march=native",
    "-DNDEBUG",
]

ext_modules = cythonize(
    [
        Extension(
            "opencis.util.fast_logger",
            [os.path.join(here, "fast_logger.pyx")],
            extra_compile_args=common_cflags,
            extra_link_args=["-lpthread"],
        ),
    ],
    gdb_debug=False,
    compiler_directives={
        "boundscheck": False,
        "wraparound": False,
        "nonecheck": False,
        "initializedcheck": False,
        "language_level": 3,
    },
)

setup(
    name="fast-logger",
    ext_modules=ext_modules,
    packages=[],  # avoid package discovery
)
