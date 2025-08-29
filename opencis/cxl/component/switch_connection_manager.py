"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from typing import Optional, List, Callable, Coroutine, Any, cast
import threading
from dataclasses import dataclass, field
from enum import Enum, auto

import traceback

from opencis.cxl.component.cxl_connection import CxlConnection
from opencis.cxl.transport.packet_constants import (
    SYSTEM_PAYLOAD_TYPE,
    SIDEBAND_TYPES,
)
from opencis.cxl.transport.sideband_packets import BaseSidebandPacket
from opencis.cxl.component.cxl_packet_processor import CxlPacketProcessor
from opencis.cxl.component.cxl_component import (
    PortConfig,
    PORT_TYPE,
)
from opencis.cxl.component.common import CXL_COMPONENT_TYPE
from opencis.util.component import RunnableComponent
from opencis.util.logger import logger
from opencis.util.server import ServerComponent
from opencis.cxl.transport.shm_stream import ShmStreamPair, ShmStreamReader
import os

try:
    from opencis.cxl.transport import packet_reader_c as _prc
except Exception as e:  # pragma: no cover
    raise


@dataclass
class SwitchPort:
    port_config: PortConfig
    connected: bool = False
    cxl_connection: CxlConnection = field(default_factory=CxlConnection)
    packet_processor: Optional[CxlPacketProcessor] = None
    shm_stream: Optional[ShmStreamPair] = None


@dataclass
class PortUpdateEvent:
    port_id: int
    connected: bool


AsyncEventHandlerType = Callable[[PortUpdateEvent], Coroutine[Any, Any, None]]


class CONNECTION_STATUS(Enum):
    OK = auto()
    DISCONNECTED = auto()
    HANDSHAKE_ERROR = auto()


class SwitchConnectionManager(RunnableComponent):
    def __init__(
        self,
        port_configs: List[PortConfig],
        host: str = "0.0.0.0",
        port: int = 8000,
        connection_timeout_ms: int = 5000,
    ):
        super().__init__()
        self._port_configs = port_configs
        self._host = host
        self._port = port
        self._connection_timeout_ms = connection_timeout_ms
        self._ports = [SwitchPort(port_config=port_config) for port_config in port_configs]
        self._event_handler = None
        self._use_shm = True

    def _wait_for_connection_request_sync(self, reader: ShmStreamReader) -> int:
        logger.debug(self._create_message("Waiting for a connection request (shm)"))
        pr = _prc.ShmPacketReader(reader)
        packet = pr.get_packet()
        logger.debug(self._create_message("Received a packet"))
        if packet.system_header.payload_type != SYSTEM_PAYLOAD_TYPE.SIDEBAND:
            message = "Handshake Error"
            logger.debug(self._create_message(message))
            logger.debug(self._create_message(packet.get_pretty_string()))
            raise Exception(message)

        base_sideband_packet = cast(BaseSidebandPacket, packet)
        if base_sideband_packet.sideband_header.type != SIDEBAND_TYPES.CONNECTION_REQUEST:
            message = "Handshake Error"
            logger.debug(self._create_message(message))
            logger.debug(self._create_message(packet.get_pretty_string()))
            raise Exception(message)

        connection_request = packet
        port_index = connection_request.get_data_as_int()

        # TODO: CE-32, ensure incoming device is connected to a correct port.
        logger.debug(
            self._create_message("Checking if the connection request had a valid port index")
        )
        if port_index < 0 or port_index >= len(self._ports):
            raise Exception(f"Invalid port number: {port_index}")
        if self._ports[port_index].connected:
            raise Exception(f"Connection already exists for port {port_index}")

        return port_index

    def _start_packet_processor_sync(self, reader: ShmStreamReader, writer, port_index: int):
        logger.info(self._create_message(f"Starting PacketProcessor for port {port_index}"))
        cxl_connection = self._ports[port_index].cxl_connection
        port_config = self._ports[port_index].port_config
        component_type = (
            CXL_COMPONENT_TYPE.USP if port_config.type == PORT_TYPE.USP else CXL_COMPONENT_TYPE.DSP
        )
        packet_processor = CxlPacketProcessor(
            reader,
            writer,
            cxl_connection,
            component_type,
            label=f"SwitchPort{port_index}",
        )
        self._ports[port_index].packet_processor = packet_processor
        packet_processor.start_wait_ready()
        packet_processor.join()
        self._ports[port_index].packet_processor = None

    def _run(self):
        transport = "shm"
        logger.info(self._create_message("Transport mode: shm"))
        # Accept and run per-port concurrently
        threads: list[threading.Thread] = []
        for idx, _ in enumerate(self._ports):

            def accept_and_run(idx_local: int = idx):
                logger.info(
                    self._create_message(f"Starting PacketProcessor for port {idx_local} (shm)")
                )
                cxl_connection = self._ports[idx_local].cxl_connection
                port_config = self._ports[idx_local].port_config
                component_type = (
                    CXL_COMPONENT_TYPE.USP
                    if port_config.type == PORT_TYPE.USP
                    else CXL_COMPONENT_TYPE.DSP
                )
                shm_pair = ShmStreamPair(port_index=idx_local, is_server=True, namespace="switch")
                self._ports[idx_local].shm_stream = shm_pair
                logger.info(self._create_message(f"SHM server ready for port {idx_local}"))
                reader = shm_pair.reader
                writer = shm_pair.writer
                # Wait for CONNECTION_REQUEST then send ACCEPT
                try:
                    req_port = self._wait_for_connection_request_sync(reader)
                    logger.info(
                        self._create_message(f"Received CONNECTION_REQUEST for port {req_port}")
                    )
                except Exception as e:
                    logger.error(self._create_message(f"Handshake error on port {idx_local}: {e}"))
                    return
                accept = BaseSidebandPacket.create(SIDEBAND_TYPES.CONNECTION_ACCEPT)
                writer.write(bytes(accept))
                logger.info(self._create_message(f"Sent ACCEPT for port {idx_local}"))
                self._update_connection_status_sync(idx_local, connected=True)
                packet_processor = CxlPacketProcessor(
                    reader,
                    writer,
                    cxl_connection,
                    component_type,
                    label=f"SwitchPort{idx_local}",
                )
                self._ports[idx_local].packet_processor = packet_processor
                logger.info(self._create_message(f"Starting PacketProcessor for port {idx_local}"))
                packet_processor.start_wait_ready()
                packet_processor.join()

            t = threading.Thread(target=accept_and_run, name=f"switch-accept-{idx}", daemon=True)
            t.start()
            threads.append(t)
        self._change_status_to_running()
        for t in threads:
            t.join()

    def _stop(self):
        # Notify processors via disconnection (not strictly needed); close shm streams
        for idx, port in enumerate(self._ports):
            try:
                if port.packet_processor:
                    port.packet_processor.stop_sync()
            except Exception:
                pass
            if port.shm_stream:
                try:
                    port.shm_stream.close()
                except Exception:
                    pass

    # Compatibility helpers for existing components
    def get_cxl_connection(self, port: int) -> CxlConnection:
        if port >= len(self._ports):
            raise Exception(f"Port {port} is unsupported.")
        return self._ports[port].cxl_connection

    def get_switch_ports(self) -> List[SwitchPort]:
        return self._ports

    def register_event_handler(self, event_handler: AsyncEventHandlerType):
        self._event_handler = event_handler

    def get_port(self):
        return self._port

    def _update_connection_status_sync(self, port_id: int, connected: bool):
        self._ports[port_id].connected = connected
        if not self._event_handler:
            return
        # If an event handler expects async, skip invoking here in sync mode
