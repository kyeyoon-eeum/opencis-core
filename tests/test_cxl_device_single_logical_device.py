"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import pytest

from opencis.apps.single_logical_device import SingleLogicalDevice
from opencis.cxl.device.root_port_device import CxlRootPortDevice
from opencis.cxl.component.cxl_connection import CxlConnection
from opencis.util.memory import get_memory_bin_name
from opencis.util.number_const import MB

# pylint: disable=duplicate-code


def test_single_logical_device():
    memory_size = 256 * MB
    memory_file = get_memory_bin_name()
    transport_connection = CxlConnection()
    SingleLogicalDevice(
        memory_size=memory_size,
        memory_file=memory_file,
        serial_number="BBBBBBBBBBBBBBBB",
        test_mode=True,
        cxl_connection=transport_connection,
    )


def test_single_logical_device_run_stop(get_gold_std_reg_vals):
    memory_size = 256 * MB
    memory_file = get_memory_bin_name()
    transport_connection = CxlConnection()
    device = SingleLogicalDevice(
        memory_size=memory_size,
        memory_file=memory_file,
        serial_number="AAAAAAAAAAAAAAAA",
        test_mode=True,
        cxl_connection=transport_connection,
    )

    # check register values after initialization
    reg_vals = str(device.get_reg_vals())
    reg_vals_expected = get_gold_std_reg_vals("SLD")
    assert reg_vals == reg_vals_expected

    device.start_wait_ready()
    device.stop_sync()


def test_single_logical_device_enumeration():
    memory_size = 256 * MB
    memory_file = get_memory_bin_name()
    transport_connection = CxlConnection()
    root_port_device = CxlRootPortDevice(downstream_connection=transport_connection, label="Port0")
    device = SingleLogicalDevice(
        memory_size=memory_size,
        memory_file=memory_file,
        serial_number="BBBBBBBBBBBBBBBB",
        test_mode=True,
        cxl_connection=transport_connection,
    )
    memory_base_address = 0xFE000000

    device.start_wait_ready()
    root_port_device.enumerate(memory_base_address)
    device.stop_sync()
