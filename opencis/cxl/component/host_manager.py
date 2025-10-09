"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from typing import Dict, Any, Callable
import threading

from opencis.util.logger import logger
from opencis.util.component import RunnableComponent


class Result:
    def __init__(self, res: Any):
        self.result = res
        self.ok = not isinstance(res, str)


class HostMgrConnServer(RunnableComponent):
    def __init__(
        self,
        host: str,
        port: int,
        set_host_conn_callback: Callable[[int, Any], None] = None,
    ):
        super().__init__()
        self._host = host
        self._port = port
        self._set_host_conn_callback = set_host_conn_callback
        self._stop_event = threading.Event()
        self._methods = {
            "HOST_INIT": self._host_init,
        }

    def get_port(self):
        return self._port

    def _host_init(self, port: int) -> Result:
        logger.info(self._create_message(f"Connection opened by CxlHost:Port{port}"))
        return Result({"port": port})

    def _serve(self, _ws=None):
        # Synchronous stub: no network
        pass

    def serve(self):
        self._change_status_to_running()
        self._stop_event.wait()

    def _run(self):
        self.serve()

    def _stop(self):
        self._stop_event.set()


class HostMgrConnClient(RunnableComponent):
    def __init__(
        self,
        port_index: int,
        host: str = "0.0.0.0",
        port: int = 8300,
        methods: Dict = None,
        disabled: bool = False,
    ):
        super().__init__(f"Port{port_index}")
        self._port_index = port_index
        self._server_uri = f"ws://{host}:{port}"
        self._methods = methods
        self._event = threading.Event()
        self._ws = None
        self._disabled = disabled
        self._stop_event = threading.Event()

    def _open_connection(self, port: int):
        if self._disabled:
            self._change_status_to_running()
            self._stop_event.wait()
            return
        logger.info(self._create_message("Connecting to HostManager (disabled stub)"))
        # Immediately mark as connected in stub
        self._event.set()
        self._change_status_to_running()
        self._stop_event.wait()

    def _close_connection(self):
        self._ws = None

    def _run(self):
        self._open_connection(self._port_index)

    def _stop(self):
        self._stop_event.set()
        self._close_connection()


class UtilConnServer(RunnableComponent):
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8400,
        get_host_conn_callback: Callable[[int], Any] = None,
        disabled: bool = False,
    ):
        super().__init__()
        self._server_uri = f"ws://{host}:{port}"
        self._host = host
        self._port = port
        self._util_methods = {
            "UTIL:CXL_HOST_READ": self._util_cxl_host_read,
            "UTIL:CXL_HOST_WRITE": self._util_cxl_host_write,
        }
        self._stop_event = threading.Event()
        self._get_host_conn_callback = get_host_conn_callback
        self._disabled = disabled
        self._stop_event = threading.Event()

    def get_port(self):
        return self._port

    def _process_cmd(self, cmd: str, port: int) -> Result:
        # Synchronous stub path
        return Result({"result": None})

    def _util_cxl_host_write(self, port: int, addr: int, data: int) -> Result:
        return Result({"result": data})

    def _util_cxl_host_read(self, port: int, addr: int) -> Result:
        return Result({"result": addr})

    def _serve(self, _ws=None):
        pass

    def serve(self):
        self._change_status_to_running()
        self._stop_event.wait()

    def _run(self):
        self.serve()

    def _stop(self):
        self._stop_event.set()


class UtilConnClient:
    def __init__(self, host: str = "0.0.0.0", port: int = 8400):
        self._uri = f"ws://{host}:{port}"

    def _process_cmd(self, cmd: str) -> str:
        logger.debug(f"Issuing (stub): {cmd}")
        return cmd

    def cxl_mem_write(self, port: int, addr: int, data: int) -> str:
        logger.info(f"CXL-Host[Port{port}]: Start CXL.mem Write: addr=0x{addr:x} data=0x{data:x}")
        self._process_cmd("write")
        return data

    def cxl_mem_read(self, port: int, addr: int) -> str:
        logger.info(f"CXL-Host[Port{port}]: Start CXL.mem Read: addr=0x{addr:x}")
        self._process_cmd("read")
        return addr


class HostManager(RunnableComponent):
    def __init__(
        self,
        host_host: str = "0.0.0.0",
        host_port: int = 8300,
        util_host: str = "0.0.0.0",
        util_port: int = 8400,
        disabled: bool = False,
    ):
        super().__init__()
        self._host_connections = {}
        self._disabled = disabled
        self._host_conn_server = HostMgrConnServer(
            host_host, host_port, self._set_host_conn_callback
        )
        self._util_conn_server = UtilConnServer(
            util_host, util_port, self._get_host_conn_callback, disabled=disabled
        )
        self._stop_event = threading.Event()

    def get_host_port(self):
        return self._host_conn_server.get_port()

    def get_util_port(self):
        return self._util_conn_server.get_port()

    def _set_host_conn_callback(self, port: int, ws) -> None:
        self._host_connections[port] = ws

    def _get_host_conn_callback(self, port: int):
        return self._host_connections.get(port)

    def _run(self):
        if self._disabled:
            self._change_status_to_running()
            self._stop_event.wait()
            return
        self._host_conn_server.start_wait_ready()
        self._util_conn_server.start_wait_ready()
        self._change_status_to_running()
        self._host_conn_server.join()
        self._util_conn_server.join()

    def _stop(self):
        if self._disabled:
            self._stop_event.set()
            return
        self._host_conn_server.stop_sync()
        self._util_conn_server.stop_sync()

    # Back-compat wrapper used by older tests
    def run(self) -> None:
        self.start_wait_ready()
        # Keep thread alive like the old async run loop
        self.join()
