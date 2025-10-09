"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import pytest

from opencis.cxl.component.physical_port_manager import (
    PhysicalPortManager,
    PortConfig,
    PORT_TYPE,
    UpstreamPortDevice,
    DownstreamPortDevice,
)
from opencis.cxl.component.switch_connection_manager import SwitchConnectionManager


def test_physical_port_manager_init(get_gold_std_reg_vals, unique_ports):
    # pylint: disable=duplicate-code
    # CE-94
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    switch_connection_manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    physical_port_manager = PhysicalPortManager(
        switch_connection_manager=switch_connection_manager, port_configs=port_configs
    )
    for port_index, port_config in enumerate(port_configs):
        port_device = physical_port_manager.get_port_device(port_index)
        if port_config.type == PORT_TYPE.USP:
            reg_vals = str(port_device.get_reg_vals())
            assert isinstance(port_device, UpstreamPortDevice)
            reg_vals_expected = get_gold_std_reg_vals("USP")
            assert reg_vals == reg_vals_expected
        else:  # vPPB binding required for DSP
            port_device.bind_to_vppb(0)
            reg_vals = str(port_device.get_reg_vals())
            assert isinstance(port_device, DownstreamPortDevice)
            reg_vals_expected = get_gold_std_reg_vals("DSP")
            assert reg_vals == reg_vals_expected
            port_device.unbind_from_vppb(0)

    with pytest.raises(Exception):
        physical_port_manager.get_port_device(len(port_configs))
    assert physical_port_manager.get_port_counts() == len(port_configs)


@pytest.mark.timeout(60)  # Increase timeout for high parallelism scenarios
def test_physical_port_manager_run_and_stop(unique_ports):
    # pylint: disable=duplicate-code
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    switch_connection_manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    physical_port_manager = PhysicalPortManager(
        switch_connection_manager=switch_connection_manager, port_configs=port_configs
    )

    physical_port_manager.start_wait_ready()
    physical_port_manager.stop_sync()
    physical_port_manager.join(timeout=5.0)  # Ensure thread fully exits
    switch_connection_manager.stop_sync()
    switch_connection_manager.join(timeout=5.0)


@pytest.mark.timeout(60)  # Increase timeout for high parallelism scenarios
def test_physical_port_manager_run_after_run(unique_ports):
    # pylint: disable=duplicate-code
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    switch_connection_manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    physical_port_manager = PhysicalPortManager(
        switch_connection_manager=switch_connection_manager, port_configs=port_configs
    )

    physical_port_manager.start_wait_ready()
    with pytest.raises(Exception):
        physical_port_manager.start_wait_ready()
    physical_port_manager.stop_sync()
    physical_port_manager.join(timeout=5.0)  # Ensure thread fully exits
    switch_connection_manager.stop_sync()
    switch_connection_manager.join(timeout=5.0)
