# cython: language_level=3
# cython: boundscheck=False, wraparound=False, nonecheck=False, initializedcheck=False

from libc.stddef cimport size_t
from libc.string cimport memcpy
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AsString, PyBytes_Check
from cpython.bytearray cimport PyByteArray_AS_STRING, PyByteArray_Check

from opencis.cxl.transport import shm_ring as _shm
cimport opencis.cxl.transport.shm_ring as _shm_c


# Constants mirror shm_stream.py (compile-time sizes used in C arrays)
DEFAULT_ELEM_SIZE = 256
HEADER_SIZE = 4
MAX_PAYLOAD = DEFAULT_ELEM_SIZE - HEADER_SIZE


cdef class ShmStreamReader:
    def __cinit__(self, object in_ring):
        self._in_ring = in_ring
        self._left_off = 0
        self._left_len = 0

    cdef void read_into_buf(self, unsigned char* dst, Py_ssize_t n):
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
            got = self._in_ring.pop_frame_wait_into(self._tmp_buf, 256, 100000)
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
        self.read_into_buf(dst, n)
        return out_bytes


cdef class ShmStreamWriter:
    def __cinit__(self, object out_ring):
        self._out_ring = out_ring
        self._last_obj = None
        self._last_ptr = <const unsigned char*> 0
        self._last_len = 0

    def write(self, data):
        """Write bytes-like data to the stream without batching.
        Uses a single frame when possible, otherwise splits respecting MAX_PAYLOAD.
        """
        cdef const unsigned char* data_ptr
        cdef Py_ssize_t total
        cdef Py_ssize_t offset
        cdef Py_ssize_t take

        # Reuse cached pointer for identical object to avoid repeated pointer lookups
        if data is self._last_obj:
            data_ptr = self._last_ptr
            total = self._last_len
        else:
            # Accept bytes/bytearray; fallback to bytes(data)
            if PyBytes_Check(data):
                data_ptr = <const unsigned char*> PyBytes_AsString(data)
                total = (<object>data).__len__()
            elif PyByteArray_Check(data):
                data_ptr = <const unsigned char*> PyByteArray_AS_STRING(data)
                total = (<object>data).__len__()
            else:
                data = bytes(data)
                data_ptr = <const unsigned char*> PyBytes_AsString(data)
                total = (<object>data).__len__()
            self._last_obj = data
            self._last_ptr = data_ptr
            self._last_len = total

        # Single-frame fast path
        if total <= MAX_PAYLOAD:
            # First attempt without releasing GIL to avoid per-call nogil overhead
            if self._out_ring.try_push_frame_from(data_ptr, <size_t>total):
                return
            # If ring is full, wait and push with GIL released
            with nogil:
                while not self._out_ring.wait_for_space(<unsigned int>1000):
                    pass
                while not self._out_ring.try_push_frame_from(data_ptr, <size_t>total):
                    pass
            return

        # General path
        offset = 0
        while offset < total:
            take = MAX_PAYLOAD if MAX_PAYLOAD <= (total - offset) else (total - offset)
            # Try once without releasing GIL
            if not self._out_ring.try_push_frame_from(data_ptr + offset, <size_t>take):
                with nogil:
                    while not self._out_ring.wait_for_space(<unsigned int>1000):
                        pass
                    while not self._out_ring.try_push_frame_from(data_ptr + offset, <size_t>take):
                        pass
            offset += take

