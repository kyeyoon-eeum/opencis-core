"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from dataclasses import dataclass
from typing import cast
import asyncio
import threading
from opencis.util.async_queue import AsyncQueue as Queue
from asyncio import create_task, gather

from opencis.util.logger import logger
from opencis.util.component import RunnableComponent
from opencis.util.async_gatherer import AsyncGatherer

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
        loop: asyncio.AbstractEventLoop,
        stop_evt: threading.Event,
    ):
        while not stop_evt.is_set():
            packet = asyncio.run_coroutine_threadsafe(source.get(), loop).result()
            if packet is None:
                break
            packet = cast(CxlIoBasePacket, packet)
            packet.tlp_prefix.ld_id = self._ld_id
            asyncio.run_coroutine_threadsafe(destination.put(packet), loop).result()

    def _mmio_worker(
        self,
        source: Queue,
        destination: Queue,
        loop: asyncio.AbstractEventLoop,
        stop_evt: threading.Event,
    ):
        while not stop_evt.is_set():
            packet = asyncio.run_coroutine_threadsafe(source.get(), loop).result()
            if packet is None:
                break
            packet = cast(CxlIoBasePacket, packet)
            packet.tlp_prefix.ld_id = self._ld_id
            asyncio.run_coroutine_threadsafe(destination.put(packet), loop).result()

    def _mem_worker(
        self,
        source: Queue,
        destination: Queue,
        loop: asyncio.AbstractEventLoop,
        stop_evt: threading.Event,
    ):
        while not stop_evt.is_set():
            packet = asyncio.run_coroutine_threadsafe(source.get(), loop).result()
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
            asyncio.run_coroutine_threadsafe(destination.put(packet), loop).result()

    def _cache_worker(
        self,
        source: Queue,
        destination: Queue,
        loop: asyncio.AbstractEventLoop,
        stop_evt: threading.Event,
    ):
        while not stop_evt.is_set():
            packet = asyncio.run_coroutine_threadsafe(source.get(), loop).result()
            if packet is None:
                break
            asyncio.run_coroutine_threadsafe(destination.put(packet), loop).result()

    async def _run(self):
        loop = asyncio.get_running_loop()
        stop_evt = threading.Event()
        self._stop_evt = stop_evt
        self._threads = [
            threading.Thread(
                target=self._cfg_worker,
                args=(self._pairs[0].source, self._pairs[0].destination, loop, stop_evt),
                name=f"{self.__class__.__name__}-cfg",
                daemon=True,
            ),
            threading.Thread(
                target=self._mmio_worker,
                args=(self._pairs[1].source, self._pairs[1].destination, loop, stop_evt),
                name=f"{self.__class__.__name__}-mmio",
                daemon=True,
            ),
            threading.Thread(
                target=self._mem_worker,
                args=(self._pairs[2].source, self._pairs[2].destination, loop, stop_evt),
                name=f"{self.__class__.__name__}-mem",
                daemon=True,
            ),
            threading.Thread(
                target=self._cache_worker,
                args=(self._pairs[3].source, self._pairs[3].destination, loop, stop_evt),
                name=f"{self.__class__.__name__}-cache",
                daemon=True,
            ),
        ]
        for t in self._threads:
            t.start()
        self._keepalive_evt = asyncio.Event()
        await self._change_status_to_running()
        await self._keepalive_evt.wait()

    async def _stop(self):
        self._stop_evt.set()
        for pair in self._pairs:
            await pair.source.put(None)
        for t in getattr(self, "_threads", []):
            t.join(timeout=2)
        if hasattr(self, "_keepalive_evt"):
            self._keepalive_evt.set()


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

    def _cfg_worker(self, loop: asyncio.AbstractEventLoop, stop_evt: threading.Event):
        source = self._dsc.cfg_fifo.target_to_host
        while not stop_evt.is_set():
            packet = asyncio.run_coroutine_threadsafe(source.get(), loop).result()
            if packet is None:
                break
            cxl_io_packet = cast(CxlIoBasePacket, packet)
            ld_id = cxl_io_packet.tlp_prefix.ld_id
            asyncio.run_coroutine_threadsafe(
                self._usc[ld_id].cfg_fifo.target_to_host.put(packet), loop
            ).result()

    def _mmio_worker(self, loop: asyncio.AbstractEventLoop, stop_evt: threading.Event):
        source = self._dsc.mmio_fifo.target_to_host
        while not stop_evt.is_set():
            packet = asyncio.run_coroutine_threadsafe(source.get(), loop).result()
            if packet is None:
                break
            cxl_io_packet = cast(CxlIoBasePacket, packet)
            ld_id = cxl_io_packet.tlp_prefix.ld_id
            asyncio.run_coroutine_threadsafe(
                self._usc[ld_id].mmio_fifo.target_to_host.put(packet), loop
            ).result()

    def _mem_worker(self, loop: asyncio.AbstractEventLoop, stop_evt: threading.Event):
        source = self._dsc.cxl_mem_fifo.target_to_host
        while not stop_evt.is_set():
            packet = asyncio.run_coroutine_threadsafe(source.get(), loop).result()
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
            asyncio.run_coroutine_threadsafe(
                self._usc[ld_id].cxl_mem_fifo.target_to_host.put(packet), loop
            ).result()

    def _cache_worker(self, loop: asyncio.AbstractEventLoop, stop_evt: threading.Event):
        source = self._dsc.cxl_cache_fifo.target_to_host
        while not stop_evt.is_set():
            packet = asyncio.run_coroutine_threadsafe(source.get(), loop).result()
            if packet is None:
                break
            asyncio.run_coroutine_threadsafe(
                self._usc[0].cxl_cache_fifo.target_to_host.put(packet), loop
            ).result()

    async def _run(self):
        loop = asyncio.get_running_loop()
        stop_evt = threading.Event()
        self._stop_evt = stop_evt
        self._threads = [
            threading.Thread(
                target=self._cfg_worker,
                args=(loop, stop_evt),
                name=f"{self.__class__.__name__}-cfg",
                daemon=True,
            ),
            threading.Thread(
                target=self._mmio_worker,
                args=(loop, stop_evt),
                name=f"{self.__class__.__name__}-mmio",
                daemon=True,
            ),
            threading.Thread(
                target=self._mem_worker,
                args=(loop, stop_evt),
                name=f"{self.__class__.__name__}-mem",
                daemon=True,
            ),
            threading.Thread(
                target=self._cache_worker,
                args=(loop, stop_evt),
                name=f"{self.__class__.__name__}-cache",
                daemon=True,
            ),
        ]
        for t in self._threads:
            t.start()
        self._keepalive_evt = asyncio.Event()
        await self._change_status_to_running()
        await self._keepalive_evt.wait()

    async def _stop(self):
        self._stop_evt.set()
        for source in self._sources:
            await source.put(None)
        for t in getattr(self, "_threads", []):
            t.join(timeout=2)
        if hasattr(self, "_keepalive_evt"):
            self._keepalive_evt.set()


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
        self._routing_tasks = AsyncGatherer()

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

    async def bind(self, ld_id: int):
        self._upstream_connections[ld_id] = CxlConnection()
        self._down_routings[ld_id] = PpbDownRouting(
            self._downstream_connection, self._upstream_connections[ld_id], ld_id
        )
        self._routing_tasks.add_task(self._down_routings[ld_id].run())
        await self._down_routings[ld_id].wait_for_ready()

    async def unbind(self, ld_id: int):
        self._upstream_connections.pop(ld_id)
        task = self._down_routings.pop(ld_id)
        await task.stop()

    async def freeze(self, ld_id: int):
        task = self._down_routings[ld_id]
        await task.stop()

    async def unfreeze(self, ld_id: int):
        task = self._down_routings[ld_id]
        await task.run_wait_ready()

    async def _run(self):
        logger.info(self._create_message("Starting"))
        self._routing_tasks.add_task(self._up_routing.run())
        await self._up_routing.wait_for_ready()

        await self._change_status_to_running()
        await self._routing_tasks.wait_for_completion()
        logger.info(self._create_message("Stopped"))

    async def _stop(self):
        logger.info(self._create_message("Stopping"))
        tasks = [
            create_task(self._up_routing.stop()),
        ]
        for task in self._down_routings.values():
            tasks.append(create_task(task.stop()))
        await gather(*tasks)
