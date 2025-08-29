# cython: language_level=3
# cython: boundscheck=False, wraparound=False, nonecheck=False, initializedcheck=False

import asyncio
from collections import deque
from libc.stddef cimport size_t
from libc.string cimport memcpy
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AsString
from cpython.bytearray cimport PyByteArray_AS_STRING

from opencis.cxl.transport import shm_ring as _shm
cimport opencis.cxl.transport.shm_ring as _shm_c


# Constants mirror shm_stream.py (compile-time sizes used in C arrays)
DEFAULT_ELEM_SIZE = 256
HEADER_SIZE = 4
MAX_PAYLOAD = DEFAULT_ELEM_SIZE - HEADER_SIZE


cdef class ShmStreamReader:
    def __cinit__(self, object endpoint):
        self._ep = endpoint
        self._in_ring = <_shm_c.ShmRing> getattr(endpoint, "_in_ring")
        self._left_off = 0
        self._left_len = 0

    cdef void _readinto_exactly_blocking(self, unsigned char* dst, Py_ssize_t n):
        if n <= 0:
            return
        cdef Py_ssize_t off = 0
        cdef Py_ssize_t take
        cdef Py_ssize_t plen
        # Consume leftover first
        if self._left_len > 0:
            take = n if n <= self._left_len else self._left_len
            memcpy(<void*>dst, <const void*>(self._left_buf + self._left_off), <size_t>take)
            self._left_off += take
            self._left_len -= take
            if self._left_len == 0:
                self._left_off = 0
            off += take
        # Pull frames until filled (fallback to bytes payload for correctness)
        cdef Py_ssize_t got
        while off < n:
            got = self._in_ring.pop_frame_wait_into(self._tmp_buf, 256, 100)
            if got <= 0:
                continue
            plen = got
            take = n - off
            if plen <= take:
                memcpy(<void*>(dst + off), <const void*>self._tmp_buf, <size_t>plen)
                off += plen
            else:
                memcpy(<void*>(dst + off), <const void*>self._tmp_buf, <size_t>take)
                memcpy(<void*>self._left_buf, <const void*>(self._tmp_buf + take), <size_t>(plen - take))
                self._left_off = 0
                self._left_len = plen - take
                off = n

    def readexactly_blocking(self, int n):
        """Blocking read of exactly n bytes using ring's blocking pop path."""
        cdef object out_bytes
        cdef unsigned char* dst
        if n <= 0:
            return b""
        out_bytes = PyBytes_FromStringAndSize(NULL, n)
        dst = <unsigned char*> PyBytes_AsString(out_bytes)
        self._readinto_exactly_blocking(dst, n)
        return out_bytes


cdef class ShmStreamWriter:
    def __cinit__(self, object endpoint):
        self._ep = endpoint
        self._out_ring = <_shm_c.ShmRing> getattr(endpoint, "_out_ring")
        self._pending = deque()

    def write(self, data):
        if not isinstance(data, (bytes, bytearray)):
            data = bytes(data)
        cdef const unsigned char* data_ptr
        cdef Py_ssize_t total
        cdef Py_ssize_t offset = 0
        cdef Py_ssize_t take
        # get pointer
        if isinstance(data, bytes):
            data_ptr = <const unsigned char*> PyBytes_AsString(data)
            total = (<object>data).__len__()
        else:
            # bytearray
            data_ptr = <const unsigned char*> PyByteArray_AS_STRING(data)
            total = (<object>data).__len__()
        while offset < total:
            take = MAX_PAYLOAD if MAX_PAYLOAD <= (total - offset) else (total - offset)
            if not self._out_ring.try_push_frame_from(data_ptr + offset, <size_t>take):
                # keep reference without slicing
                self._pending.append((data, offset, take))
            offset += take

    async def drain(self):
        cdef object item
        cdef object data
        cdef Py_ssize_t offset
        cdef Py_ssize_t length
        cdef const unsigned char* ptr
        while self._pending:
            item = self._pending[0]
            data, offset, length = item
            if isinstance(data, bytes):
                ptr = <const unsigned char*> PyBytes_AsString(data)
            else:
                ptr = <const unsigned char*> PyByteArray_AS_STRING(data)
            if self._out_ring.try_push_frame_from(ptr + offset, <size_t>length):
                self._pending.popleft()
            else:
                await asyncio.sleep(0)
                return
        return None

    def close(self):
        self._ep.close()

    def drain_blocking(self):
        """Blocking drain of pending frames using ring's wait-for-space helper."""
        cdef object item
        cdef object data
        cdef Py_ssize_t offset
        cdef Py_ssize_t length
        cdef const unsigned char* ptr
        while self._pending:
            item = self._pending[0]
            data, offset, length = item
            if isinstance(data, bytes):
                ptr = <const unsigned char*> PyBytes_AsString(data)
            else:
                ptr = <const unsigned char*> PyByteArray_AS_STRING(data)
            if self._out_ring.push_frame_wait_from(ptr + offset, <size_t>length, 1000000):
                self._pending.popleft()
            else:
                # Timed out; give up for now
                break