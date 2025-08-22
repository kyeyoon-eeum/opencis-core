# cython: language_level=3
# cython: boundscheck=False, wraparound=False, nonecheck=False, initializedcheck=False

import os
import mmap
from libc.stddef cimport size_t
from libc.stdint cimport uint8_t
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.string cimport memcpy

cdef extern from "time.h":
    cdef struct timespec:
        long tv_sec
        long tv_nsec
    int nanosleep(timespec* req, timespec* rem) nogil

cdef class ShmRing:
    cdef object mm
    cdef uint8_t[:] buf
    cdef size_t capacity
    cdef size_t elem_size
    cdef size_t region_size
    cdef str path

    def __cinit__(self):
        self.mm = None
        self.buf = None
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
        cdef int fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o666)
        try:
            os.ftruncate(fd, self.region_size)
            self.mm = mmap.mmap(fd, self.region_size, access=mmap.ACCESS_WRITE)
            self.buf = self.mm
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
            self.buf = self.mm
        finally:
            os.close(fd)
        self.capacity = <size_t> self._read_u64(0)
        self.elem_size = <size_t> self._read_u64(8)
        self.region_size = <size_t> os.path.getsize(path)

    cdef inline unsigned long long _read_u64(self, size_t off):
        return (<unsigned long long*> (&self.buf[0] + off))[0]

    cdef inline void _write_u64(self, size_t off, unsigned long long v):
        (<unsigned long long*> (&self.buf[0] + off))[0] = v

    cdef inline unsigned int _read_u32(self, size_t off):
        cdef unsigned int result = 0
        result = (self.buf[off] | (self.buf[off + 1] << 8) | (self.buf[off + 2] << 16) | (self.buf[off + 3] << 24))
        return result

    cdef inline void _write_u32(self, size_t off, unsigned int val):
        self.buf[off] = <uint8_t> (val & 0xFF)
        self.buf[off + 1] = <uint8_t> ((val >> 8) & 0xFF)
        self.buf[off + 2] = <uint8_t> ((val >> 16) & 0xFF)
        self.buf[off + 3] = <uint8_t> ((val >> 24) & 0xFF)

    def try_push(self, bytes data):
        """Non-blocking push. Returns True on success, False if full."""
        if self.mm is None:
            raise RuntimeError("ShmRing not initialized")
        cdef Py_ssize_t data_len = len(data)
        if <size_t>data_len > self.elem_size:
            raise ValueError("Invalid element size")
        cdef unsigned long long head = self._read_u64(16)
        cdef unsigned long long tail = self._read_u64(24)
        if head - tail >= self.capacity:
            return False
        cdef unsigned long long idx = head % self.capacity
        cdef size_t off = 32 + idx * self.elem_size
        # Write payload only (no zero-padding)
        self.mm[off:off + data_len] = data
        self._write_u64(16, head + 1)
        return True

    def try_pop(self):
        """Non-blocking pop. Returns memoryview on success, or None if empty."""
        if self.mm is None:
            raise RuntimeError("ShmRing not initialized")
        cdef unsigned long long head = self._read_u64(16)
        cdef unsigned long long tail = self._read_u64(24)
        if tail >= head:
            return None
        cdef unsigned long long idx = tail % self.capacity
        cdef size_t off = 32 + idx * self.elem_size
        mv = self.buf[off:off + self.elem_size]
        self._write_u64(24, tail + 1)
        return mv

    def try_push_frame(self, bytes payload):
        """Non-blocking push of a length-prefixed frame (4-byte LE header + payload)."""
        if self.mm is None:
            raise RuntimeError("ShmRing not initialized")
        cdef Py_ssize_t payload_len = len(payload)
        if <size_t>(payload_len + 4) > self.elem_size:
            raise ValueError("Frame too large for element")
        cdef unsigned long long head = self._read_u64(16)
        cdef unsigned long long tail = self._read_u64(24)
        if head - tail >= self.capacity:
            return False
        cdef unsigned long long idx = head % self.capacity
        cdef size_t off = 32 + idx * self.elem_size
        # write header and payload
        self._write_u32(off, <unsigned int> payload_len)
        off += 4
        self.mm[off:off + payload_len] = payload
        self._write_u64(16, head + 1)
        return True

    def try_pop_frame(self):
        """Non-blocking pop of a length-prefixed frame, returns memoryview payload or None."""
        if self.mm is None:
            raise RuntimeError("ShmRing not initialized")
        cdef unsigned long long head = self._read_u64(16)
        cdef unsigned long long tail = self._read_u64(24)
        if tail >= head:
            return None
        cdef unsigned long long idx = tail % self.capacity
        cdef size_t off = 32 + idx * self.elem_size
        cdef unsigned int payload_len = self._read_u32(off)
        if payload_len > self.elem_size - 4:
            # corrupted frame; drop
            self._write_u64(24, tail + 1)
            return memoryview(b"")
        off += 4
        mv = self.buf[off:off + payload_len]
        self._write_u64(24, tail + 1)
        return mv

    cpdef bytes read_up_to(self, size_t max_bytes):
        """Copy whole frames up to max_bytes into a bytes object; returns b"" if none."""
        if self.mm is None:
            raise RuntimeError("ShmRing not initialized")
        cdef unsigned long long head = self._read_u64(16)
        cdef unsigned long long tail = self._read_u64(24)
        if tail >= head or max_bytes == 0:
            return b""
        cdef unsigned long long t = tail
        cdef size_t will_copy = 0
        cdef size_t off
        cdef unsigned int payload_len
        # First pass: compute total bytes to copy in whole frames
        while t < head and will_copy < max_bytes:
            off = 32 + (t % self.capacity) * self.elem_size
            payload_len = self._read_u32(off)
            if payload_len > self.elem_size - 4:
                # drop corrupted frame
                t += 1
                continue
            if will_copy + payload_len > max_bytes:
                break
            will_copy += payload_len
            t += 1
        if will_copy == 0:
            return b""
        # Allocate output bytes
        cdef bytes out = PyBytes_FromStringAndSize(NULL, will_copy)
        cdef char* dst = <char*> out
        # Second pass: copy and advance tail
        cdef size_t copied = 0
        while tail < head and copied < will_copy:
            off = 32 + (tail % self.capacity) * self.elem_size
            payload_len = self._read_u32(off)
            if payload_len > self.elem_size - 4:
                self._write_u64(24, tail + 1)
                tail += 1
                continue
            off += 4
            memcpy(dst + copied, &self.buf[0] + off, payload_len)
            copied += payload_len
            tail += 1
            self._write_u64(24, tail)
        return out

    cpdef object pop_frame_wait(self, unsigned int max_sleep_ns=1000):
        """
        Wait for up to max_sleep_ns nanoseconds (cumulative, capped at ~1ms) for a frame.
        Returns memoryview payload if available, None otherwise.
        """
        if self.mm is None:
            raise RuntimeError("ShmRing not initialized")
        cdef unsigned long long head
        cdef unsigned long long tail
        cdef unsigned long long idx
        cdef size_t off
        cdef unsigned int slept_ns = 0
        cdef unsigned int step = 100  # 100 ns minimal backoff
        cdef timespec ts
        cdef unsigned int payload_len
        while True:
            head = self._read_u64(16)
            tail = self._read_u64(24)
            if tail < head:
                idx = tail % self.capacity
                off = 32 + idx * self.elem_size
                payload_len = self._read_u32(off)
                if payload_len > self.elem_size - 4:
                    self._write_u64(24, tail + 1)
                    return memoryview(b"")
                off += 4
                mv = self.buf[off:off + payload_len]
                self._write_u64(24, tail + 1)
                return mv
            if slept_ns >= max_sleep_ns:
                return None
            ts.tv_sec = 0
            ts.tv_nsec = step
            with nogil:
                nanosleep(&ts, <timespec*>0)
            slept_ns += step
            if step < 1000000:
                step <<= 1

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
                self.buf = None 