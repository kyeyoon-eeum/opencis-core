"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import threading
from typing import Optional, cast, List

from opencis.util.component import RunnableComponent
from opencis.cxl.component.mctp.mctp_connection import MctpConnection
from opencis.cxl.component.cci_executor import (
    CciExecutor,
    CciRequest,
    CciResponse,
    CciCommand,
)
from opencis.cxl.component.cxl_connection import CxlConnection
from opencis.cxl.component.switch_connection_manager import SwitchConnectionManager
from opencis.cxl.component.cxl_component import (
    PORT_TYPE,
    PortConfig,
)
from opencis.cxl.transport.cci_packets import (
    CciMessagePacket,
    CciPayloadPacket,
    GetLdInfoRequestPacket,
    GetLdAllocationsRequestPacket,
    SetLdAllocationsRequestPacket,
)
from opencis.cxl.transport.packet_constants import CCI_MCTP_MESSAGE_CATEGORY

from opencis.cxl.cci.common import CCI_FM_API_COMMAND_OPCODE, get_opcode_string
from opencis.util.logger import logger


class MctpCciExecutor(RunnableComponent):
    def __init__(
        self,
        mctp_connection: MctpConnection,
        switch_connection_manager: SwitchConnectionManager,
        port_configs: List[PortConfig],
        label: Optional[str] = None,
    ):
        super().__init__(label)
        self._message_tag_list: dict[int, int] = {}
        self._mctp_connection = mctp_connection
        self._cci_executor = CciExecutor(label="MCTP")
        self._switch_connection_manager = switch_connection_manager
        self._downstream_port_connections: dict[int, CxlConnection] = {}
        self._incoming_thread: threading.Thread | None = None
        self._incoming_stop = threading.Event()
        self._outgoing_threads: list[threading.Thread] = []
        self._outgoing_stop = threading.Event()

        for port_index, port_config in enumerate(port_configs):
            if port_config.type == PORT_TYPE.DSP:
                self._downstream_port_connections[port_index] = (
                    self._switch_connection_manager.get_cxl_connection(port_index)
                )

    def register_cci_commands(self, commands: List[CciCommand]):
        for command in commands:
            self._cci_executor.register_command(command.get_opcode(), command)

    def _packet_to_request(self, packet: CciMessagePacket) -> CciRequest:
        return CciRequest(opcode=packet.cci_msg_header.command_opcode, payload=packet.get_payload())

    def _send_response(self, response: CciResponse, message_tag: int):
        response_packet = CciMessagePacket.create(
            message_category=CCI_MCTP_MESSAGE_CATEGORY.RESPONSE,
            opcode=0,
            data=response.payload,
            message_tag=message_tag,
            vendor_specific_extended_status=response.vendor_specific_status,
            return_code=response.return_code,
            background_operation=int(response.bo_flag),
        )
        response_packet_tmc = CciPayloadPacket.create(response_packet)
        self._mctp_connection.ep_to_controller.put(response_packet_tmc)

    def _incoming_worker(self) -> None:
        logger.debug(self._create_message("Started processing incoming request (thread)"))
        while not self._incoming_stop.is_set():
            packet = self._mctp_connection.controller_to_ep.get()
            if packet is None:
                break
            cci_packet_tmc = cast(CciPayloadPacket, packet)
            port_index = cci_packet_tmc.cci_header.port_index
            cci_message = cci_packet_tmc.get_cci_message()
            command_opcode = cci_message.cci_msg_header.command_opcode
            opcodes_for_ld = [
                CCI_FM_API_COMMAND_OPCODE.GET_LD_INFO,
                CCI_FM_API_COMMAND_OPCODE.GET_LD_ALLOCATIONS,
                CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS,
            ]
            if command_opcode in opcodes_for_ld:
                message_tag = cci_message.cci_msg_header.message_tag
                self._message_tag_list[message_tag] = port_index
                out_packet = None
                if command_opcode == CCI_FM_API_COMMAND_OPCODE.GET_LD_INFO:
                    out_packet = GetLdInfoRequestPacket.create_from_cci_message(cci_message)
                elif command_opcode == CCI_FM_API_COMMAND_OPCODE.GET_LD_ALLOCATIONS:
                    out_packet = GetLdAllocationsRequestPacket.create_from_cci_message(cci_message)
                elif command_opcode == CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS:
                    out_packet = SetLdAllocationsRequestPacket.create_from_cci_message(cci_message)
                self._downstream_port_connections[port_index].cci_fifo.host_to_target.put(
                    out_packet
                )
            else:
                request = self._packet_to_request(cci_message)
                response = self._cci_executor.execute_command(request)
                self._send_response(response, cci_message.cci_msg_header.message_tag)
        logger.debug(self._create_message("Stopped processing incoming request (thread)"))

    def _outgoing_worker(self, downstream_connection: CxlConnection) -> None:
        logger.debug(self._create_message("Started processing outcoming request (thread)"))
        while not self._outgoing_stop.is_set():
            packet = downstream_connection.cci_fifo.target_to_host.get()
            if packet is None:
                break
            opcode = packet.get_command_opcode()
            if opcode == CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS:
                logger.info(self._create_message("switch received SetLdAllocationsResponsePacket"))
                port_index = self._message_tag_list.get(packet.cci_msg_header.message_tag, None)
                if port_index is None:
                    raise ValueError("Invalid message tag")
            self._message_tag_list.pop(packet.cci_msg_header.message_tag, None)
            cci_packet = packet.get_cci_message()
            cci_packet_tmc = CciPayloadPacket.create(cci_packet)
            self._mctp_connection.ep_to_controller.put(cci_packet_tmc)
        logger.debug(self._create_message("Stopped processing outcoming request (thread)"))

    def _run(self):
        # Start executor component
        self._cci_executor.start_wait_ready()
        # Start incoming worker thread
        self._incoming_stop.clear()
        self._incoming_thread = threading.Thread(
            target=self._incoming_worker, name=f"{self.get_message_label()}-in", daemon=True
        )
        self._incoming_thread.start()
        # Start outgoing worker threads
        self._outgoing_stop.clear()
        self._outgoing_threads = []
        for downstream_connection in self._downstream_port_connections.values():
            t = threading.Thread(
                target=self._outgoing_worker,
                args=(downstream_connection,),
                name=f"{self.get_message_label()}-out",
                daemon=True,
            )
            t.start()
            self._outgoing_threads.append(t)
        self._change_status_to_running()
        # Keep alive until stop
        threading.Event().wait()

    def _stop(self):
        # Stop worker threads
        self._incoming_stop.set()
        self._outgoing_stop.set()
        try:
            self._mctp_connection.controller_to_ep.put(None)
        except Exception:
            pass
        for downstream_connection in self._downstream_port_connections.values():
            try:
                downstream_connection.cci_fifo.target_to_host.put(None)
            except Exception:
                pass
        try:
            if self._incoming_thread is not None:
                self._incoming_thread.join(timeout=1.0)
        except Exception:
            pass
        for t in self._outgoing_threads:
            try:
                t.join(timeout=1.0)
            except Exception:
                pass
        # Stop the executor
        self._cci_executor.stop_sync()
