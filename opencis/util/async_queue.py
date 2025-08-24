from __future__ import annotations

import asyncio
from collections import deque
from typing import Deque, Generic, TypeVar, Optional


T = TypeVar("T")


class AsyncQueue(Generic[T]):
    def __init__(self) -> None:
        self._items: Deque[T] = deque()
        self._cond: Optional[asyncio.Condition] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _ensure_cond(self) -> asyncio.Condition:
        if self._cond is None:
            # Bind the condition to the currently running loop lazily
            self._loop = asyncio.get_running_loop()
            self._cond = asyncio.Condition()
        return self._cond

    async def put(self, item: T) -> None:
        cond = self._ensure_cond()
        async with cond:
            self._items.append(item)
            cond.notify()

    async def get(self) -> T:
        cond = self._ensure_cond()
        async with cond:
            while not self._items:
                await cond.wait()
            return self._items.popleft()

    def empty(self) -> bool:
        return not self._items

    def qsize(self) -> int:
        return len(self._items)
