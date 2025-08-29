"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import threading
from dataclasses import dataclass, field
from typing import Optional

from opencis.util.logger import logger
from opencis.cxl.component.mctp.mctp_connection import MctpConnection
from opencis.cxl.component.mctp.mctp_packet_processor import (
    MctpPacketProcessor,
    MCTP_PACKET_PROCESSOR_TYPE,
)
from opencis.util.component import RunnableComponent
from opencis.cxl.transport.shm_stream import ShmStreamPair


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
        self._shm_pair: ShmStreamPair | None = None

    def get_mctp_connection(self) -> MctpConnection:
        return self._switch_port.mctp_connection

    def _run(self):
        # Force SHM for MCTP to ensure stable connection
        logger.info(self._create_message("MCTP shm server initializing"))
        self._shm_pair = ShmStreamPair(port_index=0, is_server=True, namespace="mctp")
        reader = self._shm_pair.reader
        writer = self._shm_pair.writer
        self._switch_port.connected = True
        logger.info(self._create_message("MCTP server RUNNING; starting processor"))
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
        packet_processor.start_wait_ready()
        self._change_status_to_running()
        # Block until stopped; keep thread alive
        threading.Event().wait()

    def _stop(self):
        if self._switch_port.packet_processor is not None:
            logger.info(self._create_message("Stopping PacketProcessor for Switch Port"))
            try:
                self._switch_port.packet_processor.stop()
            finally:
                self._switch_port.packet_processor = None
        if self._shm_pair:
            self._shm_pair.close()
            self._shm_pair = None
