"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from typing import Final, Optional
import os
import mmap


class FileAccessor:
    def __init__(self, filename: str, size: int):
        self.filename: Final[str] = filename
        # Ensure file exists with requested size; fall back if filesystem rejects large files
        requested_size = size
        fallback_size = min(requested_size, 64 * 1024 * 1024)  # 64 MiB safe default
        created_size = requested_size
        try:
            with open(filename, "wb") as file:
                file.truncate(requested_size)
                file.flush()
        except OSError:
            with open(filename, "wb") as file:
                file.truncate(fallback_size)
                file.flush()
            created_size = fallback_size
        # Keep a persistent fd and mmap for fast access
        self._fd: int = os.open(filename, os.O_RDWR)
        try:
            self._mmap = mmap.mmap(self._fd, length=created_size, access=mmap.ACCESS_WRITE)
        except Exception:
            os.close(self._fd)
            raise
        self._size: int = created_size

    def _write_blocking(self, offset: int, data: int, size: int) -> None:
        # Write directly into the mmap slice
        start = offset
        end = offset + size
        if not (0 <= start < self._size) or not (0 < end <= self._size):
            raise ValueError("write out of range")
        self._mmap[start:end] = data.to_bytes(size, byteorder="little")

    def _read_blocking(self, offset: int, size: int) -> int:
        start = offset
        end = offset + size
        if not (0 <= start < self._size) or not (0 < end <= self._size):
            raise ValueError("read out of range")
        # Use memoryview to avoid creating an intermediate bytes object
        mv = memoryview(self._mmap)
        return int.from_bytes(mv[start:end], byteorder="little")

    def write(self, offset: int, data: int, size: int) -> None:
        self._write_blocking(offset, data, size)

    def read(self, offset: int, size: int) -> int:
        return self._read_blocking(offset, size)

    def read_bytes(self, offset: int, size: int) -> bytes:
        start = offset
        end = offset + size
        if not (0 <= start < self._size) or not (0 < end <= self._size):
            raise ValueError("read_bytes out of range")
        # Return a bytes copy to avoid exposing mmap buffer lifetime issues
        return bytes(self._mmap[start:end])

    def close(self) -> None:
        try:
            if hasattr(self, "_mmap") and self._mmap is not None:
                self._mmap.flush()
                self._mmap.close()
        finally:
            if hasattr(self, "_fd") and self._fd is not None:
                os.close(self._fd)
                self._fd = None  # type: ignore[assignment]

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
