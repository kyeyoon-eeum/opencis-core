"""
Copyright (c) 2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import atexit
from functools import partial
import os
import sys


def get_memory_bin_name(index_primary: int = 0, index_secondary: int = -1) -> str:
    def cleanup(path):
        try:
            os.remove(path)
        except Exception:
            pass

    # Get caller function name
    # pylint: disable=protected-access
    func_name = sys._getframe(1).f_code.co_name

    base_dir = "/tmp"
    # Add PID and a unique ID to prevent collisions in parallel test execution
    import uuid
    unique_id = str(uuid.uuid4())[:8]
    pid = os.getpid()
    
    if index_secondary != -1:
        bin_name_only = f"mem_{func_name}_{pid}_{unique_id}_{index_primary}-{index_secondary}.bin"
    else:
        bin_name_only = f"mem_{func_name}_{pid}_{unique_id}_{index_primary}.bin"
    bin_name = os.path.join(base_dir, bin_name_only)

    # Make sure we remove it upon exit
    atexit.register(partial(cleanup, bin_name))
    return bin_name
