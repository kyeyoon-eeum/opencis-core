"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import asyncio
from asyncio import create_task, gather
import threading
from enum import Enum, auto
from typing import Optional

from opencis.util.logger import logger
from opencis.cxl.component.mctp.mctp_connection import MctpConnection
from opencis.cxl.component.mctp.mctp_packet_reader import MctpPacketReader
from opencis.util.component import RunnableComponent
from opencis.cxl.transport.stream_types import StreamReaderLike, StreamWriterLike


class MCTP_PACKET_PROCESSOR_TYPE(Enum):
    CONTROLLER = auto()
    ENDPOINT = auto()


class MctpPacketProcessor(RunnableComponent):
    def __init__(
        self,
        reader: StreamReaderLike,
        writer: StreamWriterLike,
        mctp_connection: MctpConnection,
        processor_type: MCTP_PACKET_PROCESSOR_TYPE,
        label: Optional[str] = None,
        parent_name: Optional[str] = None,
    ):
        label_prefix = parent_name + ":" if parent_name else ""
        super().__init__(lambda class_name: f"{label_prefix}{class_name}")
        self._reader = MctpPacketReader(reader, label=label, parent_name=self.get_message_label())
        self._writer = writer
        self._mctp_connection = mctp_connection
        if processor_type == MCTP_PACKET_PROCESSOR_TYPE.CONTROLLER:
            self._incoming = self._mctp_connection.ep_to_controller
            self._outgoing = self._mctp_connection.controller_to_ep
        else:
            self._incoming = self._mctp_connection.controller_to_ep
            self._outgoing = self._mctp_connection.ep_to_controller
        self._loop = None
        self._writer_thread = None
        self._writer_stop = threading.Event()
        self._reader_thread = None
        self._reader_stop = threading.Event()

    async def _process_incoming_packets(self):
        # Fallback async path; main path uses reader thread
        logger.debug(self._create_message("Starting incoming packet processor (async)"))
        while True:
            try:
                packet = await self._reader.get_packet()
                await self._incoming.put(packet)
            except Exception as e:
                logger.debug(self._create_message(str(e)))
                await self._stop_outgoing_processor()
                break
        logger.debug(self._create_message("Stopped incoming packet processor (async)"))

    async def _stop_outgoing_processor(self):
        await self._outgoing.put(None)

    async def _process_outgoing_packets(self):
        # Async fallback; main path uses writer thread
        logger.debug(self._create_message("Starting outgoing packet processor (async)"))
        while True:
            packet = await self._outgoing.get()
            if packet is None:
                break
            try:
                self._writer.write(bytes(packet))
                await asyncio.to_thread(self._writer.drain_blocking)
            except Exception as e:
                logger.debug(self._create_message(str(e)))
                break
        logger.debug(self._create_message("Stopped outgoing packet processor (async)"))

    async def _run(self):
        self._loop = asyncio.get_running_loop()
        # Start writer thread
        self._writer_stop.clear()
        self._writer_thread = threading.Thread(
            target=self._writer_worker, name=f"{self.get_message_label()}-writer", daemon=True
        )
        self._writer_thread.start()
        # Start reader thread (blocking read -> enqueue into incoming queue)
        self._reader_stop.clear()
        self._reader_thread = threading.Thread(
            target=self._reader_worker, name=f"{self.get_message_label()}-reader", daemon=True
        )
        self._reader_thread.start()
        await self._change_status_to_running()
        # Keep alive until stop is requested
        stopper = asyncio.Event()
        while not (self._reader_stop.is_set() and self._writer_stop.is_set()):
            try:
                await asyncio.wait_for(stopper.wait(), timeout=0.1)
            except asyncio.TimeoutError:
                pass

    async def _stop(self):
        # Stop reader thread
        try:
            self._reader_stop.set()
            self._reader.abort()
            if self._reader_thread is not None:
                self._reader_thread.join(timeout=1.0)
        except Exception:
            pass
        # Stop writer thread
        try:
            self._writer_stop.set()
            await self._outgoing.put(None)
            if self._writer_thread is not None:
                self._writer_thread.join(timeout=1.0)
        except Exception:
            pass

    def _writer_worker(self) -> None:
        assert self._loop is not None
        logger.debug(self._create_message("Starting writer worker (thread)"))
        while not self._writer_stop.is_set():
            try:
                packet = asyncio.run_coroutine_threadsafe(self._outgoing.get(), self._loop).result()
            except Exception:
                break
            if packet is None:
                break
            try:
                self._writer.write(bytes(packet))
                self._writer.drain_blocking()
            except Exception as e:
                logger.debug(self._create_message(str(e)))
                break
        logger.debug(self._create_message("Stopped writer worker (thread)"))

    def _reader_worker(self) -> None:
        assert self._loop is not None
        logger.debug(self._create_message("Starting reader worker (thread)"))
        while not self._reader_stop.is_set():
            try:
                packet = self._reader.get_packet_blocking()
            except ValueError as e:
                # Non-CCI frame or benign parse issue – skip and continue
                logger.debug(self._create_message(str(e)))
                continue
            except Exception as e:
                logger.debug(self._create_message(str(e)))
                # Nudge outgoing to exit
                try:
                    asyncio.run_coroutine_threadsafe(self._stop_outgoing_processor(), self._loop)
                except Exception:
                    pass
                break
            try:
                asyncio.run_coroutine_threadsafe(self._incoming.put(packet), self._loop).result()
            except Exception:
                break
        logger.debug(self._create_message("Stopped reader worker (thread)"))
