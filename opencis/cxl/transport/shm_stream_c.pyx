# cython: language_level=3
# cython: boundscheck=False, wraparound=False, nonecheck=False, initializedcheck=False

import asyncio
from collections import deque
from libc.stddef cimport size_t
from libc.string cimport memcpy

from opencis.cxl.transport import shm_ring as _shm
from opencis.util.logger import logger

# Constants mirror shm_stream.py
DEFAULT_ELEM_SIZE = 256
HEADER_SIZE = 4
MAX_PAYLOAD = DEFAULT_ELEM_SIZE - HEADER_SIZE


cdef class ShmStreamReader:
    def __cinit__(self, object endpoint):
        self._ep = endpoint
        self._in_ring = getattr(endpoint, "_in_ring")
        self._buf = bytearray()
        self._debug_reads = 0
        self._left_payload = None
        self._left_off = 0
        self._left_len = 0

    async def read(self, int n):
        if n <= 0:
            return b""
        if not self._buf:
            delay = 1e-6
            while True:
                payload = self._in_ring.try_pop_frame()
                if payload is None or not payload:
                    await asyncio.sleep(delay)
                    delay = delay * 2.0 if delay < 0.001 else 0.001
                    continue
                self._buf.extend(payload)
                self._debug_reads += 1
                if self._debug_reads <= 3:
                    logger.debug(f"[ShmStreamReader] read frame bytes={len(payload)}")
                break
        out_len = n if n <= len(self._buf) else len(self._buf)
        out = self._buf[:out_len]
        del self._buf[:out_len]
        return bytes(out)

    async def readexactly(self, int n):
        while len(self._buf) < n:
            payload = self._in_ring.try_pop_frame()
            if payload is None or not payload:
                await asyncio.sleep(1e-6)
                continue
            self._buf.extend(payload)
            self._debug_reads += 1
            if self._debug_reads <= 3:
                logger.debug(f"[ShmStreamReader] readexactly frame bytes={len(payload)}")
        out = self._buf[:n]
        del self._buf[:n]
        return bytes(out)

    async def readinto_exactly(self, buf, int n):
        if n <= 0:
            return None
        cdef Py_ssize_t offset = 0
        cdef Py_ssize_t take, plen, need
        # Consume from internal buffer first
        if self._buf:
            take = n if n <= len(self._buf) else len(self._buf)
            buf[0:take] = self._buf[:take]
            del self._buf[:take]
            offset += take
        while offset < n:
            payload = self._in_ring.try_pop_frame()
            if payload is None or not payload:
                await asyncio.sleep(0)
                continue
            plen = len(payload)
            need = n - offset
            if plen <= need:
                buf[offset : offset + plen] = payload
                offset += plen
            else:
                buf[offset:n] = payload[:need]
                self._buf.extend(payload[need:])
                offset = n
        return None

    cdef void _readinto_exactly_blocking(self, unsigned char* dst, Py_ssize_t n):
        if n <= 0:
            return
        cdef Py_ssize_t off = 0
        cdef Py_ssize_t take, plen, i
        # Consume leftover slice first
        if self._left_payload is not None and self._left_len > 0:
            take = n if n <= self._left_len else self._left_len
            for i in range(take):
                dst[i] = self._left_payload[self._left_off + i]
            self._left_off += take
            self._left_len -= take
            if self._left_len == 0:
                self._left_payload = None
                self._left_off = 0
            off += take
        # Pull frames until filled
        while off < n:
            payload = self._in_ring.pop_frame_wait(100)
            if payload is None:
                continue
            plen = <Py_ssize_t> len(payload)
            take = n - off
            if plen <= take:
                for i in range(plen):
                    dst[off + i] = payload[i]
                off += plen
            else:
                for i in range(take):
                    dst[off + i] = payload[i]
                self._left_payload = payload
                self._left_off = take
                self._left_len = plen - take
                off = n


cdef class ShmStreamWriter:
    def __cinit__(self, object endpoint):
        self._ep = endpoint
        self._out_ring = getattr(endpoint, "_out_ring")
        self._debug_writes = 0
        self._pending = deque()

    def write(self, data):
        if not isinstance(data, (bytes, bytearray)):
            data = bytes(data)
        cdef Py_ssize_t offset = 0
        cdef Py_ssize_t total = len(data)
        while offset < total:
            chunk = data[offset : offset + MAX_PAYLOAD]
            if not isinstance(chunk, (bytes, bytearray)):
                chunk = bytes(chunk)
            if not self._out_ring.try_push_frame(chunk):
                self._pending.append(chunk)
            else:
                self._debug_writes += 1
                if self._debug_writes <= 3:
                    logger.debug(f"[ShmStreamWriter] wrote frame bytes={len(chunk)} (inline)")
            offset += len(chunk)

    async def drain(self):
        while self._pending:
            payload = self._pending[0]
            if not isinstance(payload, (bytes, bytearray)):
                payload = bytes(payload)
            if self._out_ring.try_push_frame(payload):
                self._pending.popleft()
                self._debug_writes += 1
                if self._debug_writes <= 3:
                    logger.debug(f"[ShmStreamWriter] wrote frame bytes={len(payload)}")
            else:
                await asyncio.sleep(0)
                return
        return None

    def close(self):
        self._ep.close()