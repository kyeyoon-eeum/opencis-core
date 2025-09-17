"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from queue import SimpleQueue
from dataclasses import dataclass, field


@dataclass(slots=True)
class FifoPair:
    host_to_target: SimpleQueue = field(default_factory=SimpleQueue)
    target_to_host: SimpleQueue = field(default_factory=SimpleQueue)
