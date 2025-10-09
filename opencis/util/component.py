"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from abc import abstractmethod
from enum import Enum, auto
import threading
import traceback
from typing import Optional, Union, Callable, TypeAlias

from opencis.util.logger import logger

Label: TypeAlias = Union[str, Callable[[str], str]]


class COMPONENT_STATUS(Enum):
    STOPPED = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()


class LabeledComponent:
    def __init__(self, label: Optional[Label] = None):
        self._label = label

    def get_message_label(self) -> str:
        class_name = self.__class__.__name__
        if self._label:
            if callable(self._label):
                return self._label(class_name)
            return f"{class_name}:{self._label}"
        return class_name

    def _create_message(self, message: str) -> str:
        return f"[{self.get_message_label()}] {message}"


class RunnableComponent(LabeledComponent):
    def __init__(self, label: Optional[Label] = None):
        super().__init__(label)
        self._condition = threading.Condition()
        self._status = COMPONENT_STATUS.STOPPED
        self._thread: Optional[threading.Thread] = None
        self._running_event = threading.Event()

    def start_wait_ready(self) -> None:
        with self._condition:
            if self._status != COMPONENT_STATUS.STOPPED:
                # For test predictability, raise instead of silently returning
                raise RuntimeError(self._create_message("Cannot start when not STOPPED"))
            self._status = COMPONENT_STATUS.STARTING
            logger.debug(self._create_message("Starting"))
            logger.info(self._create_message("Lifecycle: STARTING"))

        def _thread_target():
            try:
                self._run()
            except Exception as e:  # pragma: no cover
                logger.error(self._create_message(f"Unexpected Exception: {str(e)}"))
                logger.error(traceback.format_exc())
                with self._condition:
                    self._status = COMPONENT_STATUS.STOPPED
                    self._condition.notify_all()

        self._thread = threading.Thread(
            target=_thread_target, name=self.get_message_label(), daemon=True
        )
        self._thread.start()
        self.wait_for_ready()

    def stop_sync(self, timeout: float | None = None) -> None:
        with self._condition:
            if self._status not in (COMPONENT_STATUS.RUNNING, COMPONENT_STATUS.STARTING):
                return
            logger.debug(self._create_message("Stopping"))
            self._status = COMPONENT_STATUS.STOPPING
        try:
            self._stop()
        finally:
            with self._condition:
                self._status = COMPONENT_STATUS.STOPPED
                self._condition.notify_all()
        self.join(timeout)

    def join(self, timeout: float | None = None) -> None:
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def wait_for_ready(self) -> None:
        with self._condition:
            logger.info(self._create_message("wait_for_ready(): waiting for RUNNING"))
            while self._status != COMPONENT_STATUS.RUNNING:
                self._condition.wait(timeout=0.1)
            logger.info(self._create_message("wait_for_ready(): READY"))

    @abstractmethod
    def _run(self) -> None:
        """must be implemented by a child class"""

    def _change_status_to_running(self) -> None:
        with self._condition:
            self._status = COMPONENT_STATUS.RUNNING
            self._condition.notify_all()
        logger.info(self._create_message("Lifecycle: RUNNING"))

    @abstractmethod
    def _stop(self) -> None:
        """must be implemented by a child class"""

    def run(self) -> None:
        """Backward compatibility method for tests that use .run in threads"""
        self.start_wait_ready()
        # Don't block on join - let the component run in background
        import time
        time.sleep(0.1)
