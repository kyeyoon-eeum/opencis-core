"""
Shared-memory stream emulation over fixed-size SPSC rings.
Creates two rings per logical connection: client->server (c2s) and server->client (s2c).
Each ring element is a frame: [len:4][payload:<=max_payload][padding].
"""

import asyncio
from collections import deque

from opencis.cxl.transport import shm_ring as _shm
from opencis.util.logger import logger


DEFAULT_ELEM_SIZE = 256
DEFAULT_CAPACITY = 8192
HEADER_SIZE = 4
MAX_PAYLOAD = DEFAULT_ELEM_SIZE - HEADER_SIZE


def _paths_for_port(port_index: int, namespace: str):
    base = f"/dev/shm/opencis_shm_{namespace}_port{port_index}"
    return base + ".c2s", base + ".s2c"


class ShmEndpoint:
    def __init__(self, port_index: int, is_server: bool, namespace: str = "switch"):
        c2s, s2c = _paths_for_port(port_index, namespace)
        logger.info(
            f"[ShmEndpoint] init port={port_index} server={is_server} ns={namespace} c2s={c2s} s2c={s2c}"
        )
        self._in_ring = _shm.ShmRing()
        self._out_ring = _shm.ShmRing()
        if is_server:
            # Server creates rings
            self._in_ring.create(c2s, DEFAULT_CAPACITY, DEFAULT_ELEM_SIZE)
            self._out_ring.create(s2c, DEFAULT_CAPACITY, DEFAULT_ELEM_SIZE)
            logger.debug(f"[ShmEndpoint] created rings: in={c2s}, out={s2c}")
        else:
            # Client opens existing rings (single try; caller should retry asynchronously if needed)
            logger.debug(f"[ShmEndpoint] opening rings: in={s2c}, out={c2s}")
            try:
                self._in_ring.open(s2c)
                self._out_ring.open(c2s)
                logger.debug("[ShmEndpoint] opened rings successfully")
            except Exception as e:
                logger.debug("[ShmEndpoint] rings not ready yet")
                raise e

    def close(self):
        try:
            self._in_ring.close()
        except Exception:
            pass
        try:
            self._out_ring.close()
        except Exception:
            pass


class ShmStreamReader:
    def __init__(self, endpoint: ShmEndpoint):
        self._ep = endpoint
        self._buf = bytearray()
        self._debug_reads = 0

    async def read(self, n: int) -> bytes:
        if n <= 0:
            return b""
        if not self._buf:
            delay = 1e-6
            while True:
                payload = self._ep._in_ring.try_pop_frame()
                if payload is None or not payload:
                    await asyncio.sleep(delay)
                    delay = delay * 2 if delay < 0.001 else 0.001
                    continue
                self._buf.extend(payload)
                self._debug_reads += 1
                if self._debug_reads <= 3:
                    logger.debug(f"[ShmStreamReader] read frame bytes={len(payload)}")
                break
        out_len = min(n, len(self._buf))
        out = self._buf[:out_len]
        del self._buf[:out_len]
        return bytes(out)

    async def readexactly(self, n: int) -> bytes:
        spins = 0
        while len(self._buf) < n:
            payload = self._ep._in_ring.try_pop_frame()
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

    async def readinto_exactly(self, buf, n: int) -> None:
        """Fill the provided writable buffer with exactly n bytes, minimizing allocations.

        Copies any pending bytes from the internal buffer first, then pulls SHM frames and
        copies directly into the destination. Any excess from the last frame is kept in the
        internal buffer for subsequent reads.
        """
        if n <= 0:
            return
        mv = memoryview(buf)
        if mv.readonly:
            raise TypeError("destination buffer must be writable")
        offset = 0
        # Use any pending buffered bytes
        if self._buf:
            take = n if n <= len(self._buf) else len(self._buf)
            mv[:take] = self._buf[:take]
            del self._buf[:take]
            offset += take
        while offset < n:
            payload = self._ep._in_ring.try_pop_frame()
            if payload is None or not payload:
                await asyncio.sleep(0)
                continue
            plen = len(payload)
            need = n - offset
            if plen <= need:
                mv[offset : offset + plen] = payload
                offset += plen
            else:
                mv[offset:n] = payload[:need]
                self._buf.extend(payload[need:])
                offset = n


class ShmStreamWriter:
    def __init__(self, endpoint: ShmEndpoint):
        self._ep = endpoint
        self._debug_writes = 0
        self._pending = deque()

    def write(self, data: bytes):
        if not isinstance(data, (bytes, bytearray)):
            # Coerce once to avoid repeated conversions
            data = memoryview(data).tobytes()
        offset = 0
        total = len(data)
        while offset < total:
            chunk = data[offset : offset + MAX_PAYLOAD]
            # Try immediate push to avoid extra drain call
            if not isinstance(chunk, (bytes, bytearray)):
                chunk = bytes(chunk)
            if not self._ep._out_ring.try_push_frame(chunk):
                self._pending.append(chunk)
            else:
                self._debug_writes += 1
                if self._debug_writes <= 3:
                    logger.debug(f"[ShmStreamWriter] wrote frame bytes={len(chunk)} (inline)")
            offset += len(chunk)

    async def drain(self):
        # Flush as many pending frames as the ring can accept
        while self._pending:
            payload = self._pending[0]
            if not isinstance(payload, (bytes, bytearray)):
                payload = bytes(payload)
            if self._ep._out_ring.try_push_frame(payload):
                self._pending.popleft()
                self._debug_writes += 1
                if self._debug_writes <= 3:
                    logger.debug(f"[ShmStreamWriter] wrote frame bytes={len(payload)}")
            else:
                # Ring is full; yield once and return
                await asyncio.sleep(0)
                return

    def close(self):
        self._ep.close()


class ShmStreamPair:
    def __init__(self, port_index: int, is_server: bool, namespace: str = "switch"):
        self._endpoint = ShmEndpoint(port_index, is_server, namespace)
        self.reader = ShmStreamReader(self._endpoint)
        self.writer = ShmStreamWriter(self._endpoint)
        logger.debug(
            f"[ShmStreamPair] created for port={port_index} server={is_server} ns={namespace}"
        )

    def close(self):
        self._endpoint.close()
