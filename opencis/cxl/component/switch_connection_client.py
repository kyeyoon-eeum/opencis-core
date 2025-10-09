"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import threading
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
        self._loop = None
        self._worker_thread: threading.Thread | None = None
        self._worker_stop = threading.Event()

    def _connect_sync(self) -> Tuple[ShmStreamReader, StreamWriterLike]:
        # Connect using selected transport with async retry for server readiness
        import time
        retry_deadline = time.monotonic() + 2.0
        last_error = None
        transport = "shm"
        while True:
            try:
                # Use port as unique namespace - must match server namespace
                shm_pair = ShmStreamPair(
                    port_index=self._port_index, is_server=False, namespace=f"sw_{self._port}"
                )
                reader = shm_pair.reader
                writer = shm_pair.writer
                logger.debug(self._create_message(f"Client Connected ({transport})"))
                return (reader, writer)
            except Exception as e:
                last_error = e
                if time.monotonic() >= retry_deadline:
                    raise e
                time.sleep(0.01)

    def inject_error(self, injected_error: INJECTED_ERRORS):
        self._injected_error = injected_error

    def get_cxl_connection(self):
        return self._cxl_connection

    def get_port_index(self):
        return self._port_index

    def set_port(self, port: int):
        self._port = port

    def _client_worker(self) -> None:
        reader, writer = self._connect_sync()
        logger.info(self._create_message("Client connected"))
        # Sideband handshake (blocking read/write in this thread)
        logger.info(self._create_message("Sending CONNECTION_REQUEST"))
        sb_req = SidebandConnectionRequestPacket.create(self._port_index)
        writer.write(bytes(sb_req))
        logger.info(self._create_message("Sent CONNECTION_REQUEST; waiting for ACCEPT"))
        pr = _prc.ShmPacketReader(reader)
        while not self._worker_stop.is_set():
            packet = pr.get_packet()
            if packet.system_header.payload_type != SYSTEM_PAYLOAD_TYPE.SIDEBAND:
                continue
            base_sideband_packet = cast(BaseSidebandPacket, packet)
            if base_sideband_packet.sideband_header.type == SIDEBAND_TYPES.CONNECTION_ACCEPT:
                break
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

        # Start processor in loop and wait until ready
        self._packet_processor.start_wait_ready()
        logger.info(self._create_message("Client PacketProcessor RUNNING"))
        logger.info(self._create_message("SwitchConnectionClient READY"))

    def _run(self):
        self._worker_stop.clear()
        self._worker_thread = threading.Thread(
            target=self._client_worker,
            name=f"{self.get_message_label()}-client",
            daemon=True,
        )
        self._worker_thread.start()
        self._change_status_to_running()
        # Block until worker stops
        self._worker_thread.join()

    def _stop(self):
        self._stop_signal = True
        self._worker_stop.set()
        try:
            if self._packet_processor is not None:
                self._packet_processor.stop_sync()
        except Exception:
            pass
        if self._worker_thread is not None:
            self._worker_thread.join(timeout=1.0)
