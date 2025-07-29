"""
Fast transport setup with high optimization
"""

import os
from setuptools import setup, Extension
from Cython.Build import cythonize

here = os.path.abspath(os.path.dirname(__file__))

# High optimization flags
common_cflags = [
    "-O3",
    "-march=native", 
    "-mtune=native",
    "-ffast-math",
    "-funroll-loops",
    "-DNDEBUG",
]

ext_modules = cythonize(
    [
        Extension(
            "fast_transport",
            [os.path.join(here, "fast_transport.pyx")],
            extra_compile_args=common_cflags,
            extra_link_args=["-O3"],
        ),
    ],
    compiler_directives={
        "boundscheck": False,
        "wraparound": False,
        "nonecheck": False,
        "cdivision": True,
        "language_level": 3,
    },
)

setup(
    name="fast-transport",
    ext_modules=ext_modules,
) 