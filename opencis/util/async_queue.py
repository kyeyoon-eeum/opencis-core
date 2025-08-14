from __future__ import annotations

import asyncio
from collections import deque
from typing import Deque, Generic, Optional, TypeVar


T = TypeVar("T")


class AsyncQueue(Generic[T]):
    def __init__(self):
        self._items: Deque[T] = deque()
        self._cond = asyncio.Condition()

    async def put(self, item: T) -> None:
        async with self._cond:
            self._items.append(item)
            self._cond.notify()

    async def get(self) -> T:
        async with self._cond:
            while not self._items:
                await self._cond.wait()
            return self._items.popleft()

    def empty(self) -> bool:
        return not self._items

    def qsize(self) -> int:
        return len(self._items)
