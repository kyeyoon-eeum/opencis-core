"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from asyncio import create_task, gather
from typing import Optional, Union, Callable
import asyncio
import threading

from opencis.util.logger import logger
from opencis.pci.component.fifo_pair import FifoPair
from opencis.util.component import RunnableComponent


# PacketProcessor can be used a relay between two FifoPairs when it is used as is.
# PacketProcessor can be inherited by another class when customized processing logics are needed.


class PacketProcessor(RunnableComponent):
    def __init__(
        self,
        upstream_fifo: FifoPair,
        downstream_fifo: Optional[FifoPair] = None,
        label: Optional[Union[str, Callable]] = None,
    ):
        super().__init__(label)
        self._upstream_fifo = upstream_fifo
        self._downstream_fifo = downstream_fifo
        # Threaded runtime state
        self._loop: asyncio.AbstractEventLoop | None = None
        self._h2t_thread: threading.Thread | None = None
        self._t2h_thread: threading.Thread | None = None
        self._h2t_stop = threading.Event()
        self._t2h_stop = threading.Event()

    async def _process_host_to_target(self):
        # Async fallback (unused in threaded runtime)
        if self._downstream_fifo is None:
            logger.debug(self._create_message("Skipped processing host to target packets"))
            return
        logger.info(self._create_message("Started processing host->target packets"))
        while True:
            packet = await self._upstream_fifo.host_to_target.get()
            if packet is None:
                logger.info(self._create_message("Stopped host->target packets"))
                break
            logger.info(self._create_message("Forwarding host->target packet"))
            await self._downstream_fifo.host_to_target.put(packet)

    async def _process_target_to_host(self):
        # Async fallback (unused in threaded runtime)
        if self._downstream_fifo is None:
            logger.debug(self._create_message("Skipped processing target to host packets"))
            return
        logger.info(self._create_message("Started processing target->host packets"))
        while True:
            packet = await self._downstream_fifo.target_to_host.get()
            if packet is None:
                logger.info(self._create_message("Stopped target->host packets"))
                break
            logger.info(self._create_message("Forwarding target->host packet"))
            await self._upstream_fifo.target_to_host.put(packet)

    # Thread workers (transport-only change)
    def _h2t_worker(self) -> None:
        if self._downstream_fifo is None:
            return
        assert self._loop is not None
        while not self._h2t_stop.is_set():
            packet = asyncio.run_coroutine_threadsafe(
                self._upstream_fifo.host_to_target.get(), self._loop
            ).result()
            if packet is None:
                break
            asyncio.run_coroutine_threadsafe(
                self._downstream_fifo.host_to_target.put(packet), self._loop
            ).result()

    def _t2h_worker(self) -> None:
        if self._downstream_fifo is None:
            return
        assert self._loop is not None
        while not self._t2h_stop.is_set():
            packet = asyncio.run_coroutine_threadsafe(
                self._downstream_fifo.target_to_host.get(), self._loop
            ).result()
            if packet is None:
                break
            asyncio.run_coroutine_threadsafe(
                self._upstream_fifo.target_to_host.put(packet), self._loop
            ).result()

    async def _run(self):
        self._loop = asyncio.get_running_loop()
        # Start thread workers
        if self._downstream_fifo is not None:
            self._h2t_stop.clear()
            self._t2h_stop.clear()
            self._h2t_thread = threading.Thread(
                target=self._h2t_worker, name=f"{self.get_message_label()}-h2t", daemon=True
            )
            self._t2h_thread = threading.Thread(
                target=self._t2h_worker, name=f"{self.get_message_label()}-t2h", daemon=True
            )
            self._h2t_thread.start()
            self._t2h_thread.start()
        await self._change_status_to_running()
        # Keep alive until stop
        stopper = asyncio.Event()
        while True:
            try:
                await asyncio.wait_for(stopper.wait(), timeout=0.1)
            except asyncio.TimeoutError:
                # poll for stop by checking flags
                if self._downstream_fifo is None:
                    # nothing to do
                    continue
                if self._h2t_stop.is_set() and self._t2h_stop.is_set():
                    break

    async def _stop(self):
        if self._downstream_fifo is not None:
            # signal and join threads
            self._h2t_stop.set()
            self._t2h_stop.set()
            try:
                await self._downstream_fifo.target_to_host.put(None)
            except Exception:
                pass
            try:
                await self._upstream_fifo.host_to_target.put(None)
            except Exception:
                pass
            try:
                if self._h2t_thread is not None:
                    self._h2t_thread.join(timeout=1.0)
                if self._t2h_thread is not None:
                    self._t2h_thread.join(timeout=1.0)
            except Exception:
                pass
        else:
            # relay-less: just nudge upstream to exit any waiters
            try:
                await self._upstream_fifo.host_to_target.put(None)
            except Exception:
                pass
