"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import asyncio
from typing import Final


class FileAccessor:
    def __init__(self, filename: str, size: int):
        self.filename: Final[str] = filename
        with open(filename, "wb") as file:
            file.write(b"\x00" * size)
            file.flush()

    def _write_blocking(self, offset: int, data: int, size: int) -> None:
        with open(self.filename, "r+b") as file:
            file.seek(offset)
            file.write(data.to_bytes(size, byteorder="little"))

    def _read_blocking(self, offset: int, size: int) -> int:
        with open(self.filename, "rb") as file:
            file.seek(offset)
            data = file.read(size)
            return int.from_bytes(data, byteorder="little")

    async def write(self, offset: int, data: int, size: int) -> None:
        await asyncio.to_thread(self._write_blocking, offset, data, size)

    async def read(self, offset: int, size: int) -> int:
        return await asyncio.to_thread(self._read_blocking, offset, size)
