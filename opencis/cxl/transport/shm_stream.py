"""
Shared-memory stream emulation over fixed-size SPSC rings.
Creates two rings per logical connection: client->server (c2s) and server->client (s2c).
Each ring element is a frame: [len:4][payload:<=max_payload][padding].
"""

import os
import fcntl
import threading
from opencis.cxl.transport import shm_ring as _shm
from opencis.cxl.transport.shm_stream_c import ShmStreamReader, ShmStreamWriter
from opencis.util.logger import logger


DEFAULT_ELEM_SIZE = 128
DEFAULT_CAPACITY = 65536
HEADER_SIZE = 4
MAX_PAYLOAD = DEFAULT_ELEM_SIZE - HEADER_SIZE

# Thread-local lock for within-process synchronization
_shm_operation_lock = threading.Lock()

# Inter-process lock file for cross-process synchronization
_GLOBAL_LOCK_PATH = "/dev/shm/opencis_shm_global.lock"


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
        
        # Use both thread lock (for same process) and file lock (for cross-process)
        # This prevents race conditions when multiple pytest workers (processes) run concurrently
        with _shm_operation_lock:
            # Open/create the global lock file for inter-process synchronization
            lockfile = open(_GLOBAL_LOCK_PATH, 'a')
            try:
                fcntl.flock(lockfile.fileno(), fcntl.LOCK_EX)
                
                if is_server:
                    # Server creates rings
                    try:
                        self._in_ring.create(c2s, DEFAULT_CAPACITY, DEFAULT_ELEM_SIZE)
                        self._out_ring.create(s2c, DEFAULT_CAPACITY, DEFAULT_ELEM_SIZE)
                    except Exception as create_error:
                        # If create fails (file already exists), try to open instead
                        logger.debug(f"[ShmEndpoint] Create failed ({create_error}), opening existing rings")
                        try:
                            self._in_ring.open(c2s)
                            self._out_ring.open(s2c)
                        except Exception as open_error:
                            logger.error(f"[ShmEndpoint] Failed to create or open rings. Create: {create_error}, Open: {open_error}")
                            raise
                    # Server binds notify socket for inbound ring
                    self._in_ring.setup_unix_notify(True)
                    logger.debug(f"[ShmEndpoint] created rings: in={c2s}, out={s2c}")
                else:
                    # Client opens existing rings
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
            finally:
                fcntl.flock(lockfile.fileno(), fcntl.LOCK_UN)
                lockfile.close()

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
        # Store endpoint reference in reader for PacketReader compatibility
        # Some C-extension types are "no dict" and cannot receive attributes; guard with try
        try:
            self.reader._ep = self._endpoint  # type: ignore[attr-defined]
        except Exception:
            pass
        try:
            self.writer._ep = self._endpoint  # type: ignore[attr-defined]
        except Exception:
            pass
        logger.debug(
            f"[ShmStreamPair] created for port={port_index} server={is_server} ns={namespace}"
        )

    def close(self):
        self._endpoint.close()
