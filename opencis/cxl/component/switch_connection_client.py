"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import asyncio
from typing import cast, Tuple, Optional
from enum import Enum, auto

from opencis.util.logger import logger
from opencis.cxl.transport.packet_constants import SIDEBAND_TYPES
from opencis.cxl.transport.sideband_packets import (
    BaseSidebandPacket,
    SidebandConnectionRequestPacket,
)
from opencis.cxl.transport.cxl_io_packets import CxlIoCfgRdPacket
from opencis.cxl.component.common import CXL_COMPONENT_TYPE
from opencis.cxl.component.cxl_connection import CxlConnection
from opencis.cxl.component.cxl_packet_processor import CxlPacketProcessor
from opencis.util.component import RunnableComponent
from opencis.util.pci import create_bdf
from opencis.cxl.transport.shm_stream import ShmStreamPair, ShmStreamReader
import os
from opencis.cxl.transport.packet_constants import SYSTEM_PAYLOAD_TYPE
from opencis.cxl.transport.stream_types import StreamReaderLike, StreamWriterLike

try:
    from opencis.cxl.transport import packet_reader_c as _prc
except Exception as e:  # pragma: no cover
    raise


class INJECTED_ERRORS(Enum):
    NON_SIDEBAND = auto()
    NON_CONNNECTION_REQUEST = auto()


class SwitchConnectionClient(RunnableComponent):
    def __init__(
        self,
        port_index: int,
        component_type: CXL_COMPONENT_TYPE,
        ld_count: int = 0,
        host: str = "0.0.0.0",
        port: int = 8000,
        retry: bool = True,
        parent_name: Optional[str] = None,
    ):
        label_prefix = parent_name + ":" if parent_name else ""
        super().__init__(lambda class_name: f"{label_prefix}{class_name}:Port{port_index}")
        self._host = host
        self._port = port
        self._port_index = port_index
        self._component_type = component_type
        if ld_count != 0:
            self._cxl_connection = [CxlConnection() for _ in range(ld_count)]
        else:
            self._cxl_connection = CxlConnection()
        self._packet_processor = None
        self._injected_error = None
        self._retry = retry
        self._stop_signal = False

    async def _connect(self) -> Tuple[ShmStreamReader, StreamWriterLike]:
        # Connect using selected transport with async retry for server readiness
        retry_deadline = asyncio.get_running_loop().time() + 2.0
        last_error = None
        transport = "shm"
        while True:
            try:
                shm_pair = ShmStreamPair(port_index=self._port_index, is_server=False, namespace="switch")
                reader = shm_pair.reader
                writer = shm_pair.writer
                logger.debug(self._create_message(f"Client Connected ({transport})"))
                return (reader, writer)
            except Exception as e:
                last_error = e
                if asyncio.get_running_loop().time() >= retry_deadline:
                    raise e
                await asyncio.sleep(0.01)

    def inject_error(self, injected_error: INJECTED_ERRORS):
        self._injected_error = injected_error

    def get_cxl_connection(self):
        return self._cxl_connection

    def get_port_index(self):
        return self._port_index

    def set_port(self, port: int):
        self._port = port

    async def _run(self):
        (reader, writer) = await self._connect()
        logger.info(self._create_message("Client connected"))
        # Send sideband connection request and wait for accept
        logger.info(self._create_message("Sending CONNECTION_REQUEST"))
        sb_req = SidebandConnectionRequestPacket.create(self._port_index)
        writer.write(bytes(sb_req))
        await writer.drain()
        logger.info(self._create_message("Sent CONNECTION_REQUEST; waiting for ACCEPT"))
        pr = _prc.ShmPacketReader(reader)
        packet = await asyncio.to_thread(pr.get_packet)
        if packet.system_header.payload_type != SYSTEM_PAYLOAD_TYPE.SIDEBAND:
            raise Exception(self._create_message("Handshake Error: non-sideband"))
        base_sideband_packet = cast(BaseSidebandPacket, packet)
        if base_sideband_packet.sideband_header.type != SIDEBAND_TYPES.CONNECTION_ACCEPT:
            raise Exception(self._create_message("Handshake Error: not accepted"))
        logger.info(self._create_message("Handshake accepted by server"))

        logger.info(self._create_message("Connected to switch using shm"))

        self._packet_processor = CxlPacketProcessor(
            reader,
            writer,
            self._cxl_connection,
            self._component_type,
            label=f"ClientPort{self._port_index}",
        )
        logger.info(self._create_message("Starting client PacketProcessor"))
        tasks = [asyncio.create_task(self._packet_processor.run())]
        await self._packet_processor.wait_for_ready()
        logger.info(self._create_message("Client PacketProcessor RUNNING"))
        logger.info(self._create_message("SwitchConnectionClient READY"))
        await self._change_status_to_running()
        await asyncio.gather(*tasks)

    async def _stop(self):
        self._stop_signal = True
        await self._packet_processor.stop()
