"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from asyncio import create_task, gather
import asyncio
import threading
from typing import Optional, cast, List
from opencis.util.component import RunnableComponent
from opencis.cxl.component.mctp.mctp_connection import MctpConnection
from opencis.cxl.component.cci_executor import (
    CciExecutor,
    CciRequest,
    CciResponse,
    CciCommand,
    CciBackgroundStatus,
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
        self._message_tag_list = {}
        self._mctp_connection = mctp_connection
        self._cci_executor = CciExecutor(label="MCTP")
        self._switch_connection_manager = switch_connection_manager
        self._downstream_port_connections = {}
        self._loop: asyncio.AbstractEventLoop | None = None
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

    async def _send_response(self, response: CciResponse, message_tag: int):
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

        await self._mctp_connection.ep_to_controller.put(response_packet_tmc)

    async def _process_incoming_requests(self):
        # Async fallback (unused in thread mode)
        logger.debug(self._create_message("Started processing incoming request (async)"))
        while True:
            packet = await self._mctp_connection.controller_to_ep.get()
            if packet is None:
                logger.debug(self._create_message("Stopped processing incoming request (async)"))
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
                packet = None
                match command_opcode:
                    case CCI_FM_API_COMMAND_OPCODE.GET_LD_INFO:
                        packet = GetLdInfoRequestPacket.create_from_cci_message(cci_message)
                    case CCI_FM_API_COMMAND_OPCODE.GET_LD_ALLOCATIONS:
                        packet = GetLdAllocationsRequestPacket.create_from_cci_message(cci_message)
                    case CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS:
                        packet = SetLdAllocationsRequestPacket.create_from_cci_message(cci_message)
                    case _:
                        break
                await self._downstream_port_connections[port_index].cci_fifo.host_to_target.put(
                    packet
                )
            else:
                request = self._packet_to_request(cci_message)
                response = await self._cci_executor.execute_command(request)
                await self._send_response(response, cci_message.cci_msg_header.message_tag)

    async def _process_outcoming_responses(self, downstream_connection: CxlConnection):
        # Async fallback (unused in thread mode)
        logger.debug(self._create_message("Started processing outcoming request (async)"))
        while True:
            packet = await downstream_connection.cci_fifo.target_to_host.get()
            if packet is None:
                logger.debug(self._create_message("Stopped processing outcoming request (async)"))
                break
            opcode = packet.get_command_opcode()
            if opcode == CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS:
                logger.info(self._create_message("switch received SetLdAllocationsResponsePacket"))
                port_index = self._message_tag_list.get(packet.cci_msg_header.message_tag, None)
                if port_index is None:
                    raise ValueError("Invalid message tag")
            self._message_tag_list.pop(packet.cci_msg_header.message_tag)
            cci_packet = packet.get_cci_message()
            cci_packet_tmc = CciPayloadPacket.create(cci_packet)
            await self._mctp_connection.ep_to_controller.put(cci_packet_tmc)

    def _incoming_worker(self) -> None:
        assert self._loop is not None
        logger.debug(self._create_message("Started processing incoming request (thread)"))
        while not self._incoming_stop.is_set():
            packet = asyncio.run_coroutine_threadsafe(
                self._mctp_connection.controller_to_ep.get(), self._loop
            ).result()
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
                asyncio.run_coroutine_threadsafe(
                    self._downstream_port_connections[port_index].cci_fifo.host_to_target.put(
                        out_packet
                    ),
                    self._loop,
                ).result()
            else:
                request = self._packet_to_request(cci_message)
                response = asyncio.run_coroutine_threadsafe(
                    self._cci_executor.execute_command(request), self._loop
                ).result()
                asyncio.run_coroutine_threadsafe(
                    self._send_response(response, cci_message.cci_msg_header.message_tag),
                    self._loop,
                ).result()
        logger.debug(self._create_message("Stopped processing incoming request (thread)"))

    def _outgoing_worker(self, downstream_connection: CxlConnection) -> None:
        assert self._loop is not None
        logger.debug(self._create_message("Started processing outcoming request (thread)"))
        while not self._outgoing_stop.is_set():
            packet = asyncio.run_coroutine_threadsafe(
                downstream_connection.cci_fifo.target_to_host.get(), self._loop
            ).result()
            if packet is None:
                break
            opcode = packet.get_command_opcode()
            if opcode == CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS:
                logger.info(self._create_message("switch received SetLdAllocationsResponsePacket"))
                port_index = self._message_tag_list.get(packet.cci_msg_header.message_tag, None)
                if port_index is None:
                    raise ValueError("Invalid message tag")
            self._message_tag_list.pop(packet.cci_msg_header.message_tag)
            cci_packet = packet.get_cci_message()
            cci_packet_tmc = CciPayloadPacket.create(cci_packet)
            asyncio.run_coroutine_threadsafe(
                self._mctp_connection.ep_to_controller.put(cci_packet_tmc), self._loop
            ).result()
        logger.debug(self._create_message("Stopped processing outcoming request (thread)"))

    async def _run(self):
        self._loop = asyncio.get_running_loop()
        # Start executor task
        tasks = [create_task(self._cci_executor.run())]
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
        await self._change_status_to_running()
        await gather(*tasks)

    async def _stop(self):
        # Stop worker threads
        self._incoming_stop.set()
        self._outgoing_stop.set()
        try:
            await self._mctp_connection.controller_to_ep.put(None)
        except Exception:
            pass
        for downstream_connection in self._downstream_port_connections.values():
            try:
                await downstream_connection.cci_fifo.target_to_host.put(None)
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
        await self._cci_executor.stop()

    async def get_background_command_status(self) -> CciBackgroundStatus:
        status = await self._cci_executor.get_background_command_status()
        return status

    async def send_notification(self, request: CciRequest):
        message_packet = CciMessagePacket.create(
            data=request.payload,
            message_category=CCI_MCTP_MESSAGE_CATEGORY.REQUEST,
            opcode=request.opcode,
        )
        opcode_str = get_opcode_string(request.opcode)
        message_packet_tmc = CciPayloadPacket.create(message_packet)
        logger.debug(self._create_message(f"Sending {opcode_str}"))
        await self._mctp_connection.ep_to_controller.put(message_packet_tmc)
