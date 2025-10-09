"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import pytest
import socket
import tempfile
import os


@pytest.fixture
def get_gold_std_reg_vals():
    def _get_gold_std_reg_vals(device_type: str):
        with open("tests/regvals.txt") as f:
            for line in f:
                (k, v) = line.strip().split(":")
                if k == device_type:
                    return v
        return None

    return _get_gold_std_reg_vals


def _find_free_port():
    """Find a free port by binding to port 0 and getting the assigned port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port


@pytest.fixture
def unique_ports():
    """Fixture that provides a set of unique ports for testing.

    Returns a dict with ports for different services to avoid conflicts
    when running tests in parallel.
    
    NOTE: We allocate 4 separate ports rather than base+offset to avoid
    race conditions where sequential ports might be assigned to other processes.
    """
    ports = {
        'switch': _find_free_port(),
        'fabric': _find_free_port(),
        'host': _find_free_port(),
        'util': _find_free_port(),
    }
    
    yield ports
    
    # Let OS handle SHM file cleanup naturally to avoid race conditions
    # Deleting SHM files while other threads may be accessing them causes segfaults
    # The unique port numbers ensure tests don't interfere with each other


@pytest.fixture
def temp_shm_namespace(tmp_path, worker_id):
    """Fixture that provides a unique shared memory namespace for testing.

    This ensures that parallel tests don't conflict on shared memory files.
    Uses worker_id to ensure uniqueness across pytest-xdist workers and
    tmp_path to ensure uniqueness within each worker.
    """
    # Use a unique namespace based on worker ID and temp path hash
    # worker_id is 'master' for non-xdist runs, or 'gw0', 'gw1', etc. for xdist
    namespace = f"test_{worker_id}_{hash(str(tmp_path)) % 100000}"
    return namespace


@pytest.fixture
def unique_test_id(tmp_path, worker_id):
    """Generate a truly unique test ID for complete test isolation.
    
    Uses pytest-xdist worker_id and tmp_path to ensure uniqueness across
    all parallel test runs.
    """
    # worker_id is 'master' for non-xdist runs, or 'gw0', 'gw1', etc. for xdist
    # tmp_path is unique per test
    import uuid
    test_uuid = str(uuid.uuid4())[:8]
    return f"{worker_id}_{test_uuid}"


@pytest.fixture
def get_memory_bin_name(unique_test_id, tmp_path):
    """Generate unique memory file names for each test.
    
    Returns a function that generates unique memory file paths to prevent
    conflicts between parallel tests.
    """
    def _get_memory_bin_name(index: int = 0, prefix: str = "mem") -> str:
        # Use tmp_path to ensure files are in test-specific directory
        return str(tmp_path / f"{prefix}_{unique_test_id}_{index}.bin")
    return _get_memory_bin_name
