"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import threading
from typing import Optional

from opencis.cxl.component.mctp.mctp_connection import MctpConnection
from opencis.cxl.component.mctp.mctp_packet_processor import (
    MctpPacketProcessor,
    MCTP_PACKET_PROCESSOR_TYPE,
)
from opencis.util.component import RunnableComponent
from opencis.util.logger import logger
from opencis.cxl.transport.shm_stream import ShmStreamPair


class MctpConnectionClient(RunnableComponent):
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8100,
        auto_reconnect: bool = True,
        reconnect_delay: float = 0.1,
    ):
        super().__init__()
        self._host = host
        self._port = port
        self._auto_reconnect = auto_reconnect
        self._reconnect_delay = reconnect_delay
        self._mctp_connection = MctpConnection()
        self._packet_processor: MctpPacketProcessor | None = None
        self._running = False

    def get_mctp_connection(self):
        return self._mctp_connection

    def _connect(self):
        # Force SHM for MCTP to ensure stable connection
        shm = ShmStreamPair(port_index=0, is_server=False, namespace="mctp")
        return (shm.reader, shm.writer)

    def _run(self):
        self._running = True
        if self._auto_reconnect:
            logger.debug(self._create_message("Enabled auto-reconnect"))

        while self._running:
            try:
                (reader, writer) = self._connect()
                logger.info(self._create_message("MCTP client connected"))
                self._packet_processor = MctpPacketProcessor(
                    reader,
                    writer,
                    self._mctp_connection,
                    MCTP_PACKET_PROCESSOR_TYPE.ENDPOINT,
                    label=self._label,
                    parent_name=self.get_message_label(),
                )
                self._change_status_to_running()
                logger.info(self._create_message("MCTP client PacketProcessor RUNNING"))
                self._packet_processor.start_wait_ready()
                # Block until stop
                threading.Event().wait()
                self._packet_processor = None
            except Exception as e:
                if not self._auto_reconnect:
                    logger.warning(self._create_message(str(e)))
                    break

            if self._packet_processor is not None:
                break  # Normal termination

            if not self._auto_reconnect:
                logger.error(self._create_message("Connection attempt failed"))
                break

            logger.warning(self._create_message("Attempting to reconnect"))
            threading.Event().wait(self._reconnect_delay)

    def _stop(self):
        self._running = False
        if self._packet_processor:
            try:
                self._packet_processor.stop()
            finally:
                self._packet_processor = None
