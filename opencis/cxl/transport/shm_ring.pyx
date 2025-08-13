# cython: language_level=3
# cython: boundscheck=False, wraparound=False, nonecheck=False, initializedcheck=False

import os
import mmap
from libc.stddef cimport size_t

cdef class ShmRing:
    cdef object mm
    cdef size_t capacity
    cdef size_t elem_size
    cdef size_t region_size
    cdef str path

    def __cinit__(self):
        self.mm = None
        self.capacity = 0
        self.elem_size = 0
        self.region_size = 0
        self.path = ""

    def create(self, str path, size_t capacity, size_t elem_size):
        """Create or truncate a shared ring buffer file and mmap it."""
        self.path = path
        self.capacity = capacity
        self.elem_size = elem_size
        cdef size_t header_size = 32
        self.region_size = header_size + capacity * elem_size
        cdef int fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            os.ftruncate(fd, self.region_size)
            self.mm = mmap.mmap(fd, self.region_size, access=mmap.ACCESS_WRITE)
        finally:
            os.close(fd)
        # Initialize header: [cap(8)][elem(8)][head(8)][tail(8)]
        self._write_u64(0, capacity)
        self._write_u64(8, elem_size)
        self._write_u64(16, 0)
        self._write_u64(24, 0)

    def open(self, str path):
        """Open an existing shared ring buffer file and mmap it."""
        self.path = path
        cdef Py_ssize_t sz = os.path.getsize(path)
        cdef int fd = os.open(path, os.O_RDWR)
        try:
            self.mm = mmap.mmap(fd, sz, access=mmap.ACCESS_WRITE)
        finally:
            os.close(fd)
        self.capacity = <size_t> self._read_u64(0)
        self.elem_size = <size_t> self._read_u64(8)
        self.region_size = <size_t> os.path.getsize(path)

    cdef inline unsigned long long _read_u64(self, size_t off):
        cdef bytes b = self.mm[off:off + 8]
        return int.from_bytes(b, "little")

    cdef inline void _write_u64(self, size_t off, unsigned long long val):
        self.mm[off:off + 8] = int(val).to_bytes(8, "little")

    def try_push(self, bytes data):
        """Non-blocking push. Returns True on success, False if full."""
        if self.mm is None:
            raise RuntimeError("ShmRing not initialized")
        cdef Py_ssize_t data_len = len(data)
        if data_len > self.elem_size:
            raise ValueError("Invalid element size")
        cdef unsigned long long head = self._read_u64(16)
        cdef unsigned long long tail = self._read_u64(24)
        if head - tail >= self.capacity:
            return False
        cdef unsigned long long idx = head % self.capacity
        cdef size_t off = 32 + idx * self.elem_size
        # Write payload then pad residue with zeros
        self.mm[off:off + data_len] = data
        if data_len < self.elem_size:
            self.mm[off + data_len:off + self.elem_size] = b"\x00" * (self.elem_size - data_len)
        self._write_u64(16, head + 1)
        return True

    def try_pop(self):
        """Non-blocking pop. Returns bytes on success, or None if empty."""
        if self.mm is None:
            raise RuntimeError("ShmRing not initialized")
        cdef unsigned long long head = self._read_u64(16)
        cdef unsigned long long tail = self._read_u64(24)
        if tail >= head:
            return None
        cdef unsigned long long idx = tail % self.capacity
        cdef size_t off = 32 + idx * self.elem_size
        data = self.mm[off:off + self.elem_size]
        self._write_u64(24, tail + 1)
        return data

    def empty(self):
        cdef unsigned long long head = self._read_u64(16)
        cdef unsigned long long tail = self._read_u64(24)
        return tail >= head

    def is_full(self):
        cdef unsigned long long head = self._read_u64(16)
        cdef unsigned long long tail = self._read_u64(24)
        return head - tail >= self.capacity

    def qsize(self):
        cdef unsigned long long head = self._read_u64(16)
        cdef unsigned long long tail = self._read_u64(24)
        return head - tail

    def close(self):
        if self.mm is not None:
            try:
                self.mm.flush()
            finally:
                self.mm.close()
                self.mm = None 