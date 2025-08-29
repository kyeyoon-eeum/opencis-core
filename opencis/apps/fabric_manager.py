"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import traceback
from opencis.cxl.component.mctp.mctp_connection_manager import (
    MctpConnectionManager,
)
from opencis.cxl.component.mctp.mctp_cci_api_client import (
    MctpCciApiClient,
    GetPhysicalPortStateRequestPayload,
    GetVirtualCxlSwitchInfoRequestPayload,
    BindVppbRequestPayload,
    UnbindVppbRequestPayload,
)
from opencis.cxl.component.fabric_manager.socketio_server import (
    FabricManagerSocketIoServer,
    HostFMConnManager,
    HostFMMsg,
)
from opencis.cxl.component.short_msg_conn import ShortMsgConn
from opencis.util.component import RunnableComponent
from opencis.util.logger import logger


class CxlFabricManager(RunnableComponent):
    def __init__(
        self,
        mctp_host: str = "0.0.0.0",
        mctp_port: int = 8100,
        socketio_host: str = "0.0.0.0",
        socketio_port: int = 8200,
        host_fm_conn_port: int = 8700,
        use_test_runner: bool = False,
    ):
        super().__init__()
        self._connection_manager = MctpConnectionManager(mctp_host, mctp_port)

        self._api_client = MctpCciApiClient(self._connection_manager.get_mctp_connection())
        self._host_fm_conn_server = ShortMsgConn(
            "FM_Server", port=host_fm_conn_port, server=True, msg_width=16, msg_type=HostFMMsg
        )
        self._host_fm_conn_manager = HostFMConnManager(self._api_client, self._host_fm_conn_server)
        self._socketio_server = FabricManagerSocketIoServer(
            self._api_client, self._host_fm_conn_manager, socketio_host, socketio_port
        )

        self._host_fm_conn_server.register_general_handler(HostFMMsg.CONFIRM, self._host_callback())
        self._use_test_runner = use_test_runner

    def get_host_fm_port(self):
        return self._host_fm_conn_server.get_port()

    def _host_callback(self):
        def _func(_: int, data: HostFMMsg):
            print(f"Received {data.readable} from host (root port={data.root_port})")

        return _func

    def _run_test(self):
        try:
            # Disabled in sync test path
            pass
        except Exception as e:
            logger.error(
                self._create_message(
                    f"{self.__class__.__name__} error: {str(e)}, {traceback.format_exc()}"
                )
            )

    def _run(self):
        # Start only minimal components needed for the test path
        components = [
            self._socketio_server,
            self._host_fm_conn_server,
        ]
        for comp in components:
            comp.start_wait_ready()
        if self._use_test_runner:
            self._run_test()
        self._change_status_to_running()
        for comp in components:
            comp.join()

    def _stop(self):
        self._host_fm_conn_server.stop_sync()
        self._socketio_server.stop_sync()
