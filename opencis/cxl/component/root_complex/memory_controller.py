"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from dataclasses import dataclass
from asyncio import create_task, gather
import asyncio
import threading
from opencis.util.component import RunnableComponent
from opencis.cxl.transport.memory_fifo import (
    MemoryFifoPair,
    MEMORY_REQUEST_TYPE,
    MemoryResponse,
    MEMORY_RESPONSE_STATUS,
)
from opencis.util.logger import logger
from opencis.util.accessor import FileAccessor


@dataclass
class MemoryControllerConfig:
    memory_size: int
    memory_filename: str
    host_name: str
    memory_consumer_fifos: MemoryFifoPair


class MemoryController(RunnableComponent):
    def __init__(self, config: MemoryControllerConfig):
        super().__init__(lambda class_name: f"{config.host_name}:{class_name}")
        self._memory_size = config.memory_size
        self._memory_consumer_fifos = config.memory_consumer_fifos
        self._file_accessor = FileAccessor(config.memory_filename, config.memory_size)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._worker_thread: threading.Thread | None = None
        self._stop_evt = threading.Event()

    def get_mem_size(self) -> int:
        return self._memory_size

    async def _process_memory_requests(self):
        # Async fallback if thread is not used
        while True:
            packet = await self._memory_consumer_fifos.request.get()
            if packet is None:
                logger.debug(self._create_message("Stopped processing memory access requests"))
                break

            addr = packet.addr
            if packet.type == MEMORY_REQUEST_TYPE.WRITE:
                await self._file_accessor.write(addr, packet.data, packet.size)
                response = MemoryResponse(MEMORY_RESPONSE_STATUS.OK)
            elif packet.type == MEMORY_REQUEST_TYPE.READ:
                data = await self._file_accessor.read(addr, packet.size)
                response = MemoryResponse(MEMORY_RESPONSE_STATUS.OK, data)
            await self._memory_consumer_fifos.response.put(response)

    def _worker(self) -> None:
        assert self._loop is not None
        while not self._stop_evt.is_set():
            packet = asyncio.run_coroutine_threadsafe(
                self._memory_consumer_fifos.request.get(), self._loop
            ).result()
            if packet is None:
                break
            addr = packet.addr
            if packet.type == MEMORY_REQUEST_TYPE.WRITE:
                asyncio.run_coroutine_threadsafe(
                    self._file_accessor.write(addr, packet.data, packet.size), self._loop
                ).result()
                response = MemoryResponse(MEMORY_RESPONSE_STATUS.OK)
            elif packet.type == MEMORY_REQUEST_TYPE.READ:
                data = asyncio.run_coroutine_threadsafe(
                    self._file_accessor.read(addr, packet.size), self._loop
                ).result()
                response = MemoryResponse(MEMORY_RESPONSE_STATUS.OK, data)
            else:
                response = MemoryResponse(MEMORY_RESPONSE_STATUS.OK)
            asyncio.run_coroutine_threadsafe(
                self._memory_consumer_fifos.response.put(response), self._loop
            ).result()

    async def _run(self):
        self._loop = asyncio.get_running_loop()
        self._stop_evt.clear()
        self._worker_thread = threading.Thread(
            target=self._worker, name=f"{self.get_message_label()}-memctlr", daemon=True
        )
        self._worker_thread.start()
        await self._change_status_to_running()
        # Keep alive loop until stop requested
        stopper = asyncio.Event()
        while not self._stop_evt.is_set():
            try:
                await asyncio.wait_for(stopper.wait(), timeout=0.1)
            except asyncio.TimeoutError:
                pass

    async def _stop(self):
        self._stop_evt.set()
        await self._memory_consumer_fifos.request.put(None)
        try:
            if self._worker_thread is not None:
                self._worker_thread.join(timeout=1.0)
        finally:
            await self._memory_consumer_fifos.response.put(None)
