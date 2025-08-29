from __future__ import annotations

import threading
from collections import deque
from typing import Deque, Generic, TypeVar


T = TypeVar("T")


class AsyncQueue(Generic[T]):
    """
    Thread-safe blocking queue (synchronous replacement for the old async queue).

    Note: The class name is kept for import compatibility, but all operations are synchronous.
    """

    def __init__(self) -> None:
        self._items: Deque[T] = deque()
        self._cond: threading.Condition = threading.Condition()

    def put(self, item: T) -> None:
        with self._cond:
            self._items.append(item)
            self._cond.notify()

    def get(self) -> T:
        with self._cond:
            while not self._items:
                self._cond.wait()
            return self._items.popleft()

    def empty(self) -> bool:
        with self._cond:
            return not self._items

    def qsize(self) -> int:
        with self._cond:
            return len(self._items)
