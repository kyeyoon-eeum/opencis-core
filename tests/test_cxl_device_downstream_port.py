"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import pytest

from opencis.cxl.device.downstream_port_device import DownstreamPortDevice
from opencis.cxl.component.cxl_connection import CxlConnection


def test_downstream_port_device():
    device = DownstreamPortDevice(transport_connection=CxlConnection(), port_index=1)
    device.start_wait_ready()
    device.stop_sync()
