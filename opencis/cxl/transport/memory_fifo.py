"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from queue import SimpleQueue
from dataclasses import dataclass, field
from enum import Enum, auto


class MEMORY_REQUEST_TYPE(Enum):
    READ = auto()
    WRITE = auto()
    UNCACHED_READ = auto()
    UNCACHED_WRITE = auto()


@dataclass(slots=True)
class MemoryRequest:
    type: MEMORY_REQUEST_TYPE
    addr: int
    size: int
    data: int = 0


class MEMORY_RESPONSE_STATUS(Enum):
    OK = auto()
    FAILED = auto()


@dataclass(slots=True)
class MemoryResponse:
    status: MEMORY_RESPONSE_STATUS
    data: int = 0


@dataclass(slots=True)
class MemoryFifoPair:
    request: SimpleQueue[MemoryRequest] = field(default_factory=SimpleQueue)
    response: SimpleQueue[MemoryResponse] = field(default_factory=SimpleQueue)
