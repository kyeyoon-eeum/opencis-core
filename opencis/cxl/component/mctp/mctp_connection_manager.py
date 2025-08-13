"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Optional

from opencis.util.logger import logger
from opencis.cxl.component.mctp.mctp_connection import MctpConnection
from opencis.cxl.component.mctp.mctp_packet_processor import (
    MctpPacketProcessor,
    MCTP_PACKET_PROCESSOR_TYPE,
)
from opencis.util.component import RunnableComponent
from opencis.util.server import ServerComponent
from opencis.cxl.transport.shm_stream import ShmStreamPair

# pylint: disable=duplicate-code


@dataclass
class MctpPort:
    connected: bool = False
    mctp_connection: MctpConnection = field(default_factory=MctpConnection)
    packet_processor: Optional[MctpPacketProcessor] = None


class MctpConnectionManager(RunnableComponent):
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8100,
        connection_timeout_ms: int = 5000,
    ):
        super().__init__()
        self._host = host
        self._port = port
        self._connection_timeout_ms = connection_timeout_ms
        self._switch_port = MctpPort()
        # No TCP server; use shared memory stream pair at logical port 0
        self._shm_pair = ShmStreamPair(port_index=0, is_server=True, namespace="mctp")
        self._server_component = None

    async def _run(self):
        logger.info(self._create_message("MCTP SHM server initializing"))
        reader = self._shm_pair.reader
        writer = self._shm_pair.writer
        self._switch_port.connected = True
        await self._change_status_to_running()
        logger.info(self._create_message("MCTP SHM server RUNNING; starting processor"))
        await self._start_packet_processor(reader, writer)

    async def _stop_callback(self):
        if self._switch_port.packet_processor is not None:
            logger.info(self._create_message("Stopping PacketProcessor for Switch Port"))
            await self._switch_port.packet_processor.stop()
            logger.info(self._create_message("Stopped PacketProcessor for Switch Port"))

    async def _stop(self):
        if self._switch_port.packet_processor is not None:
            await self._switch_port.packet_processor.stop()
        if self._shm_pair:
            self._shm_pair.close()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # Unused in shm mode
        pass

    async def _start_packet_processor(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        logger.info(self._create_message("Starting PacketProcessor for Switch Port"))
        packet_processor = MctpPacketProcessor(
            reader,
            writer,
            self._switch_port.mctp_connection,
            MCTP_PACKET_PROCESSOR_TYPE.CONTROLLER,
            self._label,
            parent_name=self.get_message_label(),
        )
        self._switch_port.packet_processor = packet_processor
        await packet_processor.run()
        self._switch_port.packet_processor = None

    def get_mctp_connection(self) -> MctpConnection:
        return self._switch_port.mctp_connection
