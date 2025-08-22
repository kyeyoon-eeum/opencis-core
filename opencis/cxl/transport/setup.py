"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import os
from setuptools import setup, Extension
from Cython.Build import cythonize

here = os.path.abspath(os.path.dirname(__file__))


common_cflags = [
    "-O3",
    "-fno-omit-frame-pointer",
    "-march=native",
]

ext_modules = cythonize(
    [
        Extension(
            "packet_structs",
            [os.path.join(here, "packet_structs.pyx")],
            extra_compile_args=common_cflags,
            extra_link_args=[],
        ),
        Extension(
            "shm_ring",
            [os.path.join(here, "shm_ring.pyx")],
            extra_compile_args=common_cflags,
            extra_link_args=[],
        ),
        Extension(
            "c_mmap",
            [os.path.join(here, "c_mmap.pyx")],
            extra_compile_args=common_cflags,
            extra_link_args=[],
        ),
        Extension(
            "packet_reader_c",
            [os.path.join(here, "packet_reader_c.pyx")],
            extra_compile_args=common_cflags,
            extra_link_args=[],
        ),
        Extension(
            "shm_stream_c",
            [os.path.join(here, "shm_stream_c.pyx")],
            extra_compile_args=common_cflags,
            extra_link_args=[],
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
    name="my-packet-lib",
    ext_modules=ext_modules,
)
