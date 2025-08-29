"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from typing import Optional, Union, Callable
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
        self._h2t_thread: threading.Thread | None = None
        self._t2h_thread: threading.Thread | None = None
        self._h2t_stop = threading.Event()
        self._t2h_stop = threading.Event()

    def _process_host_to_target_sync(self):
        if self._downstream_fifo is None:
            logger.debug(self._create_message("Skipped processing host to target packets"))
            return
        logger.info(self._create_message("Started processing host->target packets"))
        while True:
            packet = self._upstream_fifo.host_to_target.get()
            if packet is None:
                logger.info(self._create_message("Stopped host->target packets"))
                break
            logger.info(self._create_message("Forwarding host->target packet"))
            self._downstream_fifo.host_to_target.put(packet)

    def _process_target_to_host_sync(self):
        if self._downstream_fifo is None:
            logger.debug(self._create_message("Skipped processing target to host packets"))
            return
        logger.info(self._create_message("Started processing target->host packets"))
        while True:
            packet = self._downstream_fifo.target_to_host.get()
            if packet is None:
                logger.info(self._create_message("Stopped target->host packets"))
                break
            logger.info(self._create_message("Forwarding target->host packet"))
            self._upstream_fifo.target_to_host.put(packet)

    # Thread workers (transport-only change)
    def _h2t_worker(self) -> None:
        if self._downstream_fifo is None:
            return
        while not self._h2t_stop.is_set():
            packet = self._upstream_fifo.host_to_target.get()
            if packet is None:
                break
            self._downstream_fifo.host_to_target.put(packet)

    def _t2h_worker(self) -> None:
        if self._downstream_fifo is None:
            return
        while not self._t2h_stop.is_set():
            packet = self._downstream_fifo.target_to_host.get()
            if packet is None:
                break
            self._upstream_fifo.target_to_host.put(packet)

    def _run(self):
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
        self._change_status_to_running()
        # Block until both workers are signaled to stop
        if self._downstream_fifo is not None:
            self._h2t_thread.join()
            self._t2h_thread.join()

    def _stop(self):
        if self._downstream_fifo is not None:
            # signal and join threads
            self._h2t_stop.set()
            self._t2h_stop.set()
            try:
                self._downstream_fifo.target_to_host.put(None)
            except Exception:
                pass
            try:
                self._upstream_fifo.host_to_target.put(None)
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
                self._upstream_fifo.host_to_target.put(None)
            except Exception:
                pass
