"""
Shared-memory stream emulation over fixed-size SPSC rings.
Creates two rings per logical connection: client->server (c2s) and server->client (s2c).
Each ring element is a frame: [len:4][payload:<=max_payload][padding].
"""

import asyncio
import os
import struct
from typing import Optional

from opencis.cxl.transport import shm_ring as _shm
from opencis.util.logger import logger


DEFAULT_ELEM_SIZE = 256
DEFAULT_CAPACITY = 4096
HEADER_SIZE = 4
MAX_PAYLOAD = DEFAULT_ELEM_SIZE - HEADER_SIZE


def _paths_for_port(port_index: int, namespace: str):
    base = f"/tmp/opencis_shm_{namespace}_port{port_index}"
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
            logger.info(f"[ShmEndpoint] created rings: in={c2s}, out={s2c}")
        else:
            # Client opens existing rings (may need to wait until server creates)
            logger.info(f"[ShmEndpoint] opening rings: in={s2c}, out={c2s}")
            while True:
                try:
                    self._in_ring.open(s2c)
                    self._out_ring.open(c2s)
                    logger.info("[ShmEndpoint] opened rings successfully")
                    break
                except Exception:
                    # Rings not ready yet; sleep a bit
                    import time

                    time.sleep(0.01)

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
        # Ensure there is at least some data; if buffer empty, try to pull a frame
        if not self._buf:
            while True:
                frame = self._ep._in_ring.try_pop()
                if frame is None:
                    await asyncio.sleep(0)
                    continue
                if len(frame) != DEFAULT_ELEM_SIZE:
                    continue
                (length,) = struct.unpack_from("<I", frame, 0)
                if length:
                    payload = frame[HEADER_SIZE : HEADER_SIZE + min(length, MAX_PAYLOAD)]
                    self._buf.extend(payload)
                self._debug_reads += 1
                if self._debug_reads <= 3:
                    logger.info(f"[ShmStreamReader] read frame bytes={length}")
                break
        # Return up to n bytes
        out_len = min(n, len(self._buf))
        out = self._buf[:out_len]
        del self._buf[:out_len]
        return bytes(out)

    async def readexactly(self, n: int) -> bytes:
        while len(self._buf) < n:
            frame = self._ep._in_ring.try_pop()
            if frame is None:
                await asyncio.sleep(0)
                continue
            if len(frame) != DEFAULT_ELEM_SIZE:
                # Skip malformed frames
                continue
            (length,) = struct.unpack_from("<I", frame, 0)
            if length:
                payload = frame[HEADER_SIZE : HEADER_SIZE + min(length, MAX_PAYLOAD)]
                self._buf.extend(payload)
            self._debug_reads += 1
            if self._debug_reads <= 3:
                logger.info(f"[ShmStreamReader] readexactly frame bytes={length}")
        out = self._buf[:n]
        del self._buf[:n]
        return bytes(out)


class ShmStreamWriter:
    def __init__(self, endpoint: ShmEndpoint):
        self._ep = endpoint
        self._debug_writes = 0

    def write(self, data: bytes):
        # Break into fixed-size frames
        offset = 0
        total = len(data)
        while offset < total:
            chunk = data[offset : offset + MAX_PAYLOAD]
            length = len(chunk)
            head = struct.pack("<I", length)
            pad_len = DEFAULT_ELEM_SIZE - HEADER_SIZE - length
            frame = head + chunk + (b"\x00" * pad_len)
            if len(frame) != DEFAULT_ELEM_SIZE:
                logger.error(
                    f"[ShmStreamWriter] Invalid frame size: {len(frame)} (expected {DEFAULT_ELEM_SIZE}), chunk={length}"
                )
                raise RuntimeError("ShmStreamWriter frame size mismatch")
            while not self._ep._out_ring.try_push(frame):
                # very light backoff to avoid tight spins
                import time

                time.sleep(0.0005)
            offset += length
            self._debug_writes += 1
            if self._debug_writes <= 3:
                logger.info(f"[ShmStreamWriter] wrote frame bytes={length}")

    async def drain(self):
        # No-op for now
        await asyncio.sleep(0)

    def close(self):
        self._ep.close()


class ShmStreamPair:
    def __init__(self, port_index: int, is_server: bool, namespace: str = "switch"):
        self._endpoint = ShmEndpoint(port_index, is_server, namespace)
        self.reader = ShmStreamReader(self._endpoint)
        self.writer = ShmStreamWriter(self._endpoint)
        logger.info(
            f"[ShmStreamPair] created for port={port_index} server={is_server} ns={namespace}"
        )

    def close(self):
        self._endpoint.close()
