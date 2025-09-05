"""
Shared-memory stream emulation over fixed-size SPSC rings.
Creates two rings per logical connection: client->server (c2s) and server->client (s2c).
Each ring element is a frame: [len:4][payload:<=max_payload][padding].
"""

from opencis.cxl.transport import shm_ring as _shm
from opencis.cxl.transport.shm_stream_c import ShmStreamReader, ShmStreamWriter
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
            # Server binds notify socket for inbound ring
            self._in_ring.setup_unix_notify(True)
            logger.debug(f"[ShmEndpoint] created rings: in={c2s}, out={s2c}")
        else:
            # Client opens existing rings (single try; caller should retry asynchronously if needed)
            logger.debug(f"[ShmEndpoint] opening rings: in={s2c}, out={c2s}")
            try:
                self._in_ring.open(s2c)
                self._out_ring.open(c2s)
                # Client configures TX notify to server's inbound ring path
                self._out_ring.setup_unix_notify(False)
                logger.debug("[ShmEndpoint] opened rings successfully")
            except Exception as e:
                logger.debug("[ShmEndpoint] rings not ready yet")
                raise e

    def close(self):
        try:
            try:
                self._in_ring.teardown_unix_notify()
            except Exception:
                pass
            self._in_ring.close()
        except Exception:
            pass
        try:
            try:
                self._out_ring.teardown_unix_notify()
            except Exception:
                pass
            self._out_ring.close()
        except Exception:
            pass


class ShmStreamPair:
    def __init__(self, port_index: int, is_server: bool, namespace: str = "switch"):
        self._endpoint = ShmEndpoint(port_index, is_server, namespace)
        self.reader = ShmStreamReader(self._endpoint._in_ring)
        self.writer = ShmStreamWriter(self._endpoint._out_ring)
        logger.debug(
            f"[ShmStreamPair] created for port={port_index} server={is_server} ns={namespace}"
        )

    def close(self):
        self._endpoint.close()
