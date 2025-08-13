"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import mmap
import os
from typing import Optional

try:
    from opencis.cxl.transport import c_mmap as _c_mmap
except Exception:  # pragma: no cover
    _c_mmap = None


class FileAccessor:
    def __init__(self, filename: str, size: int):
        self.filename = filename
        self._size = size
        # Ensure directory exists
        directory: Optional[str] = os.path.dirname(os.path.abspath(filename))
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

        flags = os.O_RDWR | os.O_CREAT
        mode = 0o644
        fd = os.open(filename, flags, mode)
        try:
            os.ftruncate(fd, size)
            self._file = os.fdopen(fd, "r+b", buffering=0)
            self._mmap = mmap.mmap(self._file.fileno(), size, access=mmap.ACCESS_WRITE)
        except Exception:
            try:
                os.close(fd)
            except Exception:
                pass
            raise

    async def write(self, offset: int, data: int, size: int):
        if offset < 0 or offset + size > self._size:
            raise ValueError("FileAccessor.write: out-of-bounds access")
        if _c_mmap is not None:
            _c_mmap.write_int_to_mmap(self._mmap, offset, data, size)
        else:
            self._mmap[offset : offset + size] = data.to_bytes(
                size, byteorder="little", signed=False
            )

    async def write_bytes(self, offset: int, data: bytes):
        size = len(data)
        if offset < 0 or offset + size > self._size:
            raise ValueError("FileAccessor.write_bytes: out-of-bounds access")
        self._mmap[offset : offset + size] = data

    async def read(self, offset: int, size: int) -> int:
        if offset < 0 or offset + size > self._size:
            raise ValueError("FileAccessor.read: out-of-bounds access")
        if _c_mmap is not None:
            return int(_c_mmap.read_int_from_mmap(self._mmap, offset, size))
        data = self._mmap[offset : offset + size]
        return int.from_bytes(data, byteorder="little", signed=False)

    def close(self) -> None:
        try:
            if hasattr(self, "_mmap") and self._mmap is not None:
                self._mmap.flush()
                self._mmap.close()
        finally:
            if hasattr(self, "_file") and self._file is not None:
                try:
                    self._file.close()
                finally:
                    self._file = None
                    self._mmap = None
