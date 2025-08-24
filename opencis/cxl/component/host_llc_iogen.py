"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

# pylint: disable=duplicate-code
from asyncio import create_task, gather
import asyncio
import threading
import time
from dataclasses import dataclass
from random import randrange

from opencis.util.logger import logger
from opencis.util.component import RunnableComponent
from opencis.cxl.transport.memory_fifo import (
    MemoryFifoPair,
    MemoryRequest,
    MemoryResponse,
    MEMORY_REQUEST_TYPE,
    MEMORY_RESPONSE_STATUS,
)


@dataclass
class HostLlcIoGenConfig:
    host_name: str
    processor_to_cache_fifo: MemoryFifoPair
    memory_size: int


class HostLlcIoGen(RunnableComponent):
    def __init__(self, config: HostLlcIoGenConfig):
        super().__init__(lambda class_name: f"{config.host_name}:{class_name}")
        self._host_name = config.host_name
        self._processor_to_cache_fifo = config.processor_to_cache_fifo
        self._memory_line = config.memory_size // 0x40

        self._internal_iogen = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._worker_thread: threading.Thread | None = None
        self._worker_stop = threading.Event()

    # pylint: disable=duplicate-code
    async def load(self, address: int, size: int) -> MemoryResponse:
        packet = MemoryRequest(MEMORY_REQUEST_TYPE.READ, address, size)
        await self._processor_to_cache_fifo.request.put(packet)
        packet = await self._processor_to_cache_fifo.response.get()
        return packet

    async def store(self, address: int, size: int, value: int) -> MemoryResponse:
        packet = MemoryRequest(MEMORY_REQUEST_TYPE.WRITE, address, size, value)
        await self._processor_to_cache_fifo.request.put(packet)
        packet = await self._processor_to_cache_fifo.response.get()
        return packet

    def _host_process_llc_iogen_worker(self) -> None:
        assert self._loop is not None
        time.sleep(5)
        while not self._worker_stop.is_set():
            if self._internal_iogen is True:
                valid_addr = set()
                for _ in range(10000):
                    addr = randrange(0, self._memory_line) * 0x40
                    written_data = addr
                    valid_addr.add(addr)

                    logger.debug(f"[{self._host_name}] Write 0x{written_data:X} at 0x{addr:x}")
                    packet = asyncio.run_coroutine_threadsafe(
                        self.store(addr, 0x40, written_data), self._loop
                    ).result()
                    if packet is None:
                        logger.debug(self._create_message("Stop processing host llc iogen"))
                        self._worker_stop.set()
                        break
                    assert packet.status == MEMORY_RESPONSE_STATUS.OK

                logger.info(f"[{self._host_name}] Written Counts {len(valid_addr)}")

                for _, addr in enumerate(valid_addr):
                    packet = asyncio.run_coroutine_threadsafe(
                        self.load(addr, 0x40), self._loop
                    ).result()
                    if packet is None:
                        logger.debug(self._create_message("Stop processing host llc iogen"))
                        self._worker_stop.set()
                        break
                    assert packet.status == MEMORY_RESPONSE_STATUS.OK

                    read_data = packet.get_data_as_int()
                    logger.debug(f"[{self._host_name}] Read 0x{read_data:X} from 0x{addr:x}")
                    assert addr == read_data, f"addr={hex(addr)}:data={hex(read_data)}"

                logger.info(f"[{self._host_name}] Simple Test Done")

            else:
                packet = asyncio.run_coroutine_threadsafe(
                    self._processor_to_cache_fifo.response.get(), self._loop
                ).result()
                if packet is None:
                    logger.debug(self._create_message("Stop processing host llc iogen"))
                    self._worker_stop.set()
                    break

    async def _run(self):
        self._loop = asyncio.get_running_loop()
        self._worker_stop.clear()
        self._worker_thread = threading.Thread(
            target=self._host_process_llc_iogen_worker,
            name=f"{self.get_message_label()}-host-llc-iogen",
            daemon=True,
        )
        self._worker_thread.start()
        await self._change_status_to_running()
        stopper = asyncio.Event()
        try:
            await stopper.wait()
        except asyncio.CancelledError:
            pass

    async def _stop(self):
        self._worker_stop.set()
        await self._processor_to_cache_fifo.response.put(None)
        if self._worker_thread is not None:
            self._worker_thread.join(timeout=1.0)
