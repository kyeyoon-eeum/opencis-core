"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import asyncio
import threading
from opencis.util.async_queue import AsyncQueue as Queue
from dataclasses import dataclass

from opencis.cxl.component.cxl_connection import CxlConnection
from opencis.util.component import RunnableComponent


@dataclass
class BindPair:
    source: Queue
    destination: Queue


class GenericBindProcessor(RunnableComponent):
    def __init__(
        self,
        downsteam_connection: CxlConnection,
        upstream_connection: CxlConnection,
    ):
        super().__init__()
        self._dsc = downsteam_connection
        self._usc = upstream_connection

        self._pairs = [
            BindPair(self._dsc.cfg_fifo.host_to_target, self._usc.cfg_fifo.host_to_target),
            BindPair(self._usc.cfg_fifo.target_to_host, self._dsc.cfg_fifo.target_to_host),
            BindPair(self._dsc.mmio_fifo.host_to_target, self._usc.mmio_fifo.host_to_target),
            BindPair(self._usc.mmio_fifo.target_to_host, self._dsc.mmio_fifo.target_to_host),
            BindPair(
                self._dsc.cxl_mem_fifo.host_to_target,
                self._usc.cxl_mem_fifo.host_to_target,
            ),
            BindPair(
                self._usc.cxl_mem_fifo.target_to_host,
                self._dsc.cxl_mem_fifo.target_to_host,
            ),
            BindPair(
                self._dsc.cxl_cache_fifo.host_to_target,
                self._usc.cxl_cache_fifo.host_to_target,
            ),
            BindPair(
                self._usc.cxl_cache_fifo.target_to_host,
                self._dsc.cxl_cache_fifo.target_to_host,
            ),
        ]
        self._loop: asyncio.AbstractEventLoop | None = None
        self._threads: list[threading.Thread] = []
        self._stop_evt = threading.Event()
        self._keepalive_evt: asyncio.Event | None = None

    def _create_message(self, message):
        message = f"[{self.__class__.__name__}] {message}"
        return message

    # Similar logic to pci_to_pci_bridge_device.py but with thread bridging
    def _process_worker(self, source: Queue, destination: Queue) -> None:
        assert self._loop is not None
        while not self._stop_evt.is_set():
            packet = asyncio.run_coroutine_threadsafe(source.get(), self._loop).result()
            if packet is None:
                break
            asyncio.run_coroutine_threadsafe(destination.put(packet), self._loop).result()

    async def _run(self):
        self._loop = asyncio.get_running_loop()
        self._threads = []
        self._stop_evt.clear()
        for pair in self._pairs:
            t = threading.Thread(
                target=self._process_worker,
                args=(pair.source, pair.destination),
                name=f"{self.__class__.__name__}-bind",
                daemon=True,
            )
            t.start()
            self._threads.append(t)
        self._keepalive_evt = asyncio.Event()
        await self._change_status_to_running()
        await self._keepalive_evt.wait()

    async def _stop(self):
        self._stop_evt.set()
        for pair in self._pairs:
            await pair.source.put(None)
        for t in self._threads:
            t.join(timeout=2)
        if self._keepalive_evt is not None:
            self._keepalive_evt.set()


class PpbDspBindProcessor(GenericBindProcessor):
    # vcs_id and vppb_id are not needed in PPB-DSP relation
    def _create_message(self, message):
        message = f"[{self.__class__.__name__}] {message}"
        return message


class VppbPpbBindProcessor(GenericBindProcessor):
    def __init__(
        self,
        vcs_id: int,
        vppb_id: int,
        downsteam_connection: CxlConnection,
        upstream_connection: CxlConnection,
    ):
        super().__init__(downsteam_connection, upstream_connection)
        self._vcs_id = vcs_id
        self._vppb_id = vppb_id

    def _create_message(self, message):
        message = f"[{self.__class__.__name__}:VCS{self._vcs_id}:vPPB{self._vppb_id}] {message}"
        return message
