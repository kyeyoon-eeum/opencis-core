"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import pytest

from opencis.cxl.device.upstream_port_device import UpstreamPortDevice
from opencis.cxl.component.cxl_connection import CxlConnection


def test_upstream_port_device():
    device = UpstreamPortDevice(transport_connection=CxlConnection(), port_index=0)
    device.start_wait_ready()
    device.stop_sync()
