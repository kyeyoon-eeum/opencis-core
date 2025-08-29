"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from dataclasses import dataclass
from typing import cast
import threading
from queue import Queue

from opencis.util.logger import logger
from opencis.util.component import RunnableComponent

from opencis.cxl.component.cxl_connection import CxlConnection
from opencis.cxl.transport.cxl_mem_packets import (
    CxlMemBasePacket,
    CxlMemS2MNDRPacket,
    CxlMemS2MDRSPacket,
    CxlMemS2MBISnpPacket,
)
from opencis.cxl.transport.cxl_io_packets import CxlIoBasePacket


@dataclass
class BindPair:
    source: Queue
    destination: Queue


class PpbDownRouting(RunnableComponent):
    def __init__(
        self,
        downsteam_connection: CxlConnection,
        upstream_connection: CxlConnection,
        ld_id: int = 0,
    ):
        super().__init__()
        self._dsc = downsteam_connection
        self._usc = upstream_connection
        self._ld_id = ld_id

        self._pairs = [
            BindPair(self._usc.cfg_fifo.host_to_target, self._dsc.cfg_fifo.host_to_target),
            BindPair(self._usc.mmio_fifo.host_to_target, self._dsc.mmio_fifo.host_to_target),
            BindPair(self._usc.cxl_mem_fifo.host_to_target, self._dsc.cxl_mem_fifo.host_to_target),
            BindPair(
                self._usc.cxl_cache_fifo.host_to_target, self._dsc.cxl_cache_fifo.host_to_target
            ),
        ]

    def _cfg_worker(
        self,
        source: Queue,
        destination: Queue,
        stop_evt: threading.Event,
    ):
        while not stop_evt.is_set():
            packet = source.get()
            if packet is None:
                break
            packet = cast(CxlIoBasePacket, packet)
            packet.tlp_prefix.ld_id = self._ld_id
            destination.put(packet)

    def _mmio_worker(
        self,
        source: Queue,
        destination: Queue,
        stop_evt: threading.Event,
    ):
        while not stop_evt.is_set():
            packet = source.get()
            if packet is None:
                break
            packet = cast(CxlIoBasePacket, packet)
            packet.tlp_prefix.ld_id = self._ld_id
            destination.put(packet)

    def _mem_worker(
        self,
        source: Queue,
        destination: Queue,
        stop_evt: threading.Event,
    ):
        while not stop_evt.is_set():
            packet = source.get()
            if packet is None:
                break
            packet = cast(CxlMemBasePacket, packet)
            if packet.is_m2sreq():
                packet.m2sreq_header.ld_id = self._ld_id
            elif packet.is_m2srwd():
                packet.m2srwd_header.ld_id = self._ld_id
            elif packet.is_m2sbirsp():
                pass
            else:
                logger.warning(self._create_message("Unexpected CXL.mem packet"))
            destination.put(packet)

    def _cache_worker(
        self,
        source: Queue,
        destination: Queue,
        stop_evt: threading.Event,
    ):
        while not stop_evt.is_set():
            packet = source.get()
            if packet is None:
                break
            destination.put(packet)

    def _run(self):
        stop_evt = threading.Event()
        self._stop_evt = stop_evt
        self._threads = [
            threading.Thread(
                target=self._cfg_worker,
                args=(self._pairs[0].source, self._pairs[0].destination, stop_evt),
                name=f"{self.__class__.__name__}-cfg",
                daemon=True,
            ),
            threading.Thread(
                target=self._mmio_worker,
                args=(self._pairs[1].source, self._pairs[1].destination, stop_evt),
                name=f"{self.__class__.__name__}-mmio",
                daemon=True,
            ),
            threading.Thread(
                target=self._mem_worker,
                args=(self._pairs[2].source, self._pairs[2].destination, stop_evt),
                name=f"{self.__class__.__name__}-mem",
                daemon=True,
            ),
            threading.Thread(
                target=self._cache_worker,
                args=(self._pairs[3].source, self._pairs[3].destination, stop_evt),
                name=f"{self.__class__.__name__}-cache",
                daemon=True,
            ),
        ]
        for t in self._threads:
            t.start()
        self._change_status_to_running()
        # Block until stop
        self._running_event.wait()

    def _stop(self):
        self._stop_evt.set()
        for pair in self._pairs:
            pair.source.put(None)
        for t in getattr(self, "_threads", []):
            t.join(timeout=2)


class PpbUpRouting(RunnableComponent):
    def __init__(
        self,
        downsteam_connection: CxlConnection,
        upstream_connections: CxlConnection,
    ):
        super().__init__()
        self._dsc = downsteam_connection
        self._usc = upstream_connections

        self._sources = [
            self._dsc.cfg_fifo.target_to_host,
            self._dsc.mmio_fifo.target_to_host,
            self._dsc.cxl_mem_fifo.target_to_host,
            self._dsc.cxl_cache_fifo.target_to_host,
        ]

    def _cfg_worker(self, stop_evt: threading.Event):
        source = self._dsc.cfg_fifo.target_to_host
        while not stop_evt.is_set():
            packet = source.get()
            if packet is None:
                break
            cxl_io_packet = cast(CxlIoBasePacket, packet)
            ld_id = cxl_io_packet.tlp_prefix.ld_id
            self._usc[ld_id].cfg_fifo.target_to_host.put(packet)

    def _mmio_worker(self, stop_evt: threading.Event):
        source = self._dsc.mmio_fifo.target_to_host
        while not stop_evt.is_set():
            packet = source.get()
            if packet is None:
                break
            cxl_io_packet = cast(CxlIoBasePacket, packet)
            ld_id = cxl_io_packet.tlp_prefix.ld_id
            self._usc[ld_id].mmio_fifo.target_to_host.put(packet)

    def _mem_worker(self, stop_evt: threading.Event):
        source = self._dsc.cxl_mem_fifo.target_to_host
        while not stop_evt.is_set():
            packet = source.get()
            if packet is None:
                break
            cxl_mem_base_packet = cast(CxlMemBasePacket, packet)
            if cxl_mem_base_packet.is_s2mndr():
                cxl_mem_packet = cast(CxlMemS2MNDRPacket, packet)
                ld_id = cxl_mem_packet.s2mndr_header.ld_id
            elif cxl_mem_base_packet.is_s2mdrs():
                cxl_mem_packet = cast(CxlMemS2MDRSPacket, packet)
                ld_id = cxl_mem_packet.s2mdrs_header.ld_id
            elif cxl_mem_base_packet.is_s2mbisnp():
                cxl_mem_packet = cast(CxlMemS2MBISnpPacket, packet)
                ld_id = 0
            else:
                raise Exception("No packet type!!!!")
            self._usc[ld_id].cxl_mem_fifo.target_to_host.put(packet)

    def _cache_worker(self, stop_evt: threading.Event):
        source = self._dsc.cxl_cache_fifo.target_to_host
        while not stop_evt.is_set():
            packet = source.get()
            if packet is None:
                break
            self._usc[0].cxl_cache_fifo.target_to_host.put(packet)

    def _run(self):
        stop_evt = threading.Event()
        self._stop_evt = stop_evt
        self._threads = [
            threading.Thread(
                target=self._cfg_worker,
                args=(stop_evt,),
                name=f"{self.__class__.__name__}-cfg",
                daemon=True,
            ),
            threading.Thread(
                target=self._mmio_worker,
                args=(stop_evt,),
                name=f"{self.__class__.__name__}-mmio",
                daemon=True,
            ),
            threading.Thread(
                target=self._mem_worker,
                args=(stop_evt,),
                name=f"{self.__class__.__name__}-mem",
                daemon=True,
            ),
            threading.Thread(
                target=self._cache_worker,
                args=(stop_evt,),
                name=f"{self.__class__.__name__}-cache",
                daemon=True,
            ),
        ]
        for t in self._threads:
            t.start()
        self._change_status_to_running()
        # Block until stop
        self._running_event.wait()

    def _stop(self):
        self._stop_evt.set()
        for source in self._sources:
            source.put(None)
        for t in getattr(self, "_threads", []):
            t.join(timeout=2)


@dataclass
class EnumerationInfo:
    secondary_bus: int
    subordinate_bus: int
    memory_base: int
    memory_limit: int


class PpbDevice(RunnableComponent):
    def __init__(
        self,
        port_index: int = 0,
    ):
        super().__init__()
        self._port_index = port_index

        self._downstream_connection = CxlConnection()
        self._upstream_connections: dict[int, CxlConnection] = {}

        self._up_routing = PpbUpRouting(self._downstream_connection, self._upstream_connections)
        self._down_routings: dict[int, PpbDownRouting] = {}

    def _get_label(self) -> str:
        return f"PPB{self._port_index}"

    def _create_message(self, message: str) -> str:
        message = f"[{self.__class__.__name__}:{self._get_label()}] {message}"
        return message

    def get_upstream_connection(self):
        return self._upstream_connections

    def get_downstream_connection(self) -> CxlConnection:
        return self._downstream_connection

    def bind(self, ld_id: int):
        self._upstream_connections[ld_id] = CxlConnection()
        self._down_routings[ld_id] = PpbDownRouting(
            self._downstream_connection, self._upstream_connections[ld_id], ld_id
        )
        self._down_routings[ld_id].start_wait_ready()

    def unbind(self, ld_id: int):
        self._upstream_connections.pop(ld_id)
        task = self._down_routings.pop(ld_id)
        task.stop_sync()

    def freeze(self, ld_id: int):
        task = self._down_routings[ld_id]
        task.stop_sync()

    def unfreeze(self, ld_id: int):
        task = self._down_routings[ld_id]
        task.start_wait_ready()

    def _run(self):
        logger.info(self._create_message("Starting"))
        self._up_routing.start_wait_ready()
        self._change_status_to_running()
        self._up_routing.join()
        logger.info(self._create_message("Stopped"))

    def _stop(self):
        logger.info(self._create_message("Stopping"))
        try:
            self._up_routing.stop_sync()
        except Exception:
            pass
        for task in self._down_routings.values():
            try:
                task.stop_sync()
            except Exception:
                pass
