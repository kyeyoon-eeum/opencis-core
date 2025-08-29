"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import threading


class BoundEvent:
    def __init__(self):
        self.ev = threading.Event()
        self._lock = threading.Lock()
        self.res = None

    # def __await__(self):
    #     return self.ev.wait().__await__()

    def set_result(self, result):
        with self._lock:
            self.res = result
            self.ev.set()

    def result(self):
        # change to "claim_result"
        with self._lock:
            ret = self.res
            self.ev.clear()
        return ret
