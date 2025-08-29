"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import threading


class AsyncGatherer:
    def __init__(self):
        # Synchronous thread-based gatherer only
        self._threads: list[threading.Thread] = []

    # --- Synchronous thread helpers ---
    def clear(self) -> None:
        self._threads = []

    def add_thread(self, thread: threading.Thread) -> None:
        self._threads.append(thread)

    def join_all(self) -> None:
        for t in self._threads:
            t.join()
