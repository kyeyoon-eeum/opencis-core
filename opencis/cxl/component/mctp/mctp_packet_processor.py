"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

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
        self._writer_thread: threading.Thread | None = None
        self._writer_stop = threading.Event()
        self._reader_thread: threading.Thread | None = None
        self._reader_stop = threading.Event()

    def _run(self):
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
        self._change_status_to_running()
        # Keep alive until stop is requested
        while not (self._reader_stop.is_set() and self._writer_stop.is_set()):
            # short sleep to yield
            threading.Event().wait(0.05)

    def _stop(self):
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
            self._outgoing.put(None)
            if self._writer_thread is not None:
                self._writer_thread.join(timeout=1.0)
        except Exception:
            pass

    def _writer_worker(self) -> None:
        logger.debug(self._create_message("Starting writer worker (thread)"))
        while not self._writer_stop.is_set():
            try:
                packet = self._outgoing.get()
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
                    self._writer_stop.set()
                    self._outgoing.put(None)
                except Exception:
                    pass
                break
            try:
                self._incoming.put(packet)
            except Exception:
                break
        logger.debug(self._create_message("Stopped reader worker (thread)"))
