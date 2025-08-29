"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

# pylint: disable=duplicate-code
import pytest

from opencis.cxl.device.cxl_type1_device import (
    CxlType1Device,
    CxlType1DeviceConfig,
)
from opencis.cxl.device.root_port_device import CxlRootPortDevice
from opencis.cxl.component.cxl_connection import CxlConnection


def test_type1_device():
    device_config = CxlType1DeviceConfig(
        device_name="CXLType1Device",
        transport_connection=CxlConnection(),
    )
    CxlType1Device(device_config)


def test_type1_device_run_stop(get_gold_std_reg_vals):
    device_config = CxlType1DeviceConfig(
        device_name="CXLType1Device",
        transport_connection=CxlConnection(),
    )
    device = CxlType1Device(device_config)

    # check register values after initialization
    reg_vals = str(device.get_reg_vals())
    reg_vals_expected = get_gold_std_reg_vals("ACCEL_TYPE_1")
    assert reg_vals == reg_vals_expected

    # run and stop synchronously
    device.start_wait_ready()
    device.stop_sync()


def test_type1_device_enumeration():
    transport_connection = CxlConnection()
    device_config = CxlType1DeviceConfig(
        device_name="CXLType1Device",
        transport_connection=transport_connection,
    )
    root_port_device = CxlRootPortDevice(downstream_connection=transport_connection, label="Port0")
    _ = CxlType1Device(device_config)
    memory_base_address = 0xFE000000

    # enumerate synchronously; shim provides sync API
    root_port_device.enumerate(memory_base_address)
    _ = root_port_device.scan_devices()  # minimal shim returns enumeration info
