"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from typing import TypedDict, Any

from opencis.util.logger import logger
from opencis.cxl.component.short_msg_conn import ShortMsgBase, ShortMsgConn
from opencis.util.component import RunnableComponent
from opencis.cxl.component.mctp.mctp_cci_api_client import MctpCciApiClient


class CommandResponse(TypedDict):
    error: str
    result: Any


class HostFMMsg(ShortMsgBase):
    UNBIND = 0x00
    BIND = 0x01
    CONFIRM = 0x02
    EXTRA = 0x03

    def __init__(self, arg):
        super().__init__(self, arg)
        self.data = 0x00

    @property
    def real_val(self):
        return self.data

    @classmethod
    def _missing_(cls, value):
        inst = cls.parse(value)
        inst.data = value
        return inst

    @classmethod
    def create(cls, vppb: int, root_port: int, confirmation: bool, bind: bool):
        data = (root_port << 8) | (vppb << 4) | (int(confirmation) << 1) | int(bind)
        inst = cls(data)
        inst.data = data
        return inst

    @classmethod
    def parse(cls, data):
        bind = data & 0b1
        confirmation = data & 0b10
        if confirmation:
            new_cls = cls(confirmation)
        else:
            new_cls = cls(bind)
        return new_cls

    @property
    def is_confirmation(self) -> bool:
        return bool(self.data & 0b10)

    @property
    def is_bind(self) -> bool:
        return bool(self.data & 0b1)

    @property
    def root_port(self) -> int:
        return self.data >> 8

    @property
    def vppb(self) -> bool:
        return (self.data >> 4) & 0xF

    @property
    def readable(self):
        data = ""
        if self.is_confirmation:
            data = "Host confirmation for "
        if self.is_bind:
            data += "Binding "
        else:
            data += "Unbinding "
        return data + f"Root Port: {self.root_port}, vPPB: {self.vppb}"


class HostFMConnManager:
    def __init__(self, api_client: MctpCciApiClient, host_fm_conn_server: ShortMsgConn):
        self._api_client = api_client
        self._host_fm_conn_server = host_fm_conn_server

    def notify_host_bind(self, device_vppb: int, vcs_id: int):
        # Synchronous no-op for test path
        try:
            root_port = 0
            req = HostFMMsg.create(device_vppb, root_port, False, True)
            logger.info(
                f"Host bind notification root_port {root_port}, "
                f"device_vppb {device_vppb}, val {req.real_val}"
            )
            self._host_fm_conn_server.send_irq_request(req, root_port)
        except Exception:
            pass

    def notify_host_unbind(self, device_vppb: int, vcs_id: int):
        # Synchronous no-op for test path
        try:
            root_port = 0
            req = HostFMMsg.create(device_vppb, root_port, False, False)
            logger.info(
                f"Host unbind notification root_port {root_port}, "
                f"device_vppb {device_vppb}, val {req.real_val}"
            )
            self._host_fm_conn_server.send_irq_request(req, root_port)
        except Exception:
            pass

    def get_usp_by_vcs_id(self, vcs_id: int):
        # Synchronous stub: assume root port 0
        return 0


class FabricManagerSocketIoServer(RunnableComponent):
    def __init__(
        self,
        mctp_client: MctpCciApiClient,
        host_fm_conn_manager: HostFMConnManager,
        host: str = "0.0.0.0",
        port: int = 8200,
    ):
        super().__init__()
        self._mctp_client = mctp_client
        self._host_fm_conn_manager = host_fm_conn_manager
        self._host = host
        self._port = port

    def _run(self):
        logger.info(self._create_message("SocketIO server disabled for sync test path"))
        self._change_status_to_running()
        # Block until stopped
        self._running_event.wait()

    def _stop(self):
        # No resources to release
        pass
