"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import pytest

from opencis.cxl.device.cxl_type2_device import (
    CxlType2Device,
    CxlType2DeviceConfig,
)
from opencis.cxl.device.root_port_device import CxlRootPortDevice
from opencis.cxl.component.cxl_connection import CxlConnection


def test_type2_device():
    device_config = CxlType2DeviceConfig(
        device_name="CXLType2Device",
        transport_connection=CxlConnection(),
        memory_size=0x100000,
        memory_file="/tmp/test.bin",
    )
    CxlType2Device(device_config)


def test_type2_device_run_stop():
    device_config = CxlType2DeviceConfig(
        device_name="CXLType2Device",
        transport_connection=CxlConnection(),
        memory_size=0x100000,
        memory_file="/tmp/test.bin",
    )
    device = CxlType2Device(device_config)
    device.start_wait_ready()
    device.stop_sync()


def test_type2_device_enumeration():
    transport_connection = CxlConnection()
    device_config = CxlType2DeviceConfig(
        device_name="CXLType2Device",
        transport_connection=transport_connection,
        memory_size=0x100000,
        memory_file="/tmp/test.bin",
    )
    root_port_device = CxlRootPortDevice(downstream_connection=transport_connection, label="Port0")
    _ = CxlType2Device(device_config)
    memory_base_address = 0xFE000000
    root_port_device.enumerate(memory_base_address)
    _ = root_port_device.scan_devices()
