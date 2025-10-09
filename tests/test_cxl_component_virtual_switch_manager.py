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
)
from opencis.cxl.component.switch_connection_manager import (
    SwitchConnectionManager,
)
from opencis.cxl.component.virtual_switch_manager import (
    VirtualSwitchManager,
    VirtualSwitchConfig,
    CxlVirtualSwitch,
)
from opencis.util.unaligned_bit_structure import UnalignedBitStructure


def test_virtual_switch_manager_init(unique_ports):
    UnalignedBitStructure.make_quiet()
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    switch_connection_manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    physical_port_manager = PhysicalPortManager(
        switch_connection_manager=switch_connection_manager, port_configs=port_configs
    )
    vppb_counts = 3
    initial_bounds = [1, 2, 3]
    switch_configs = [
        VirtualSwitchConfig(
            upstream_port_index=0,
            vppb_counts=vppb_counts,
            initial_bounds=initial_bounds,
            irq_host="127.0.0.1",
            irq_port=unique_ports['fabric'],
        )
    ]
    allocated_ld = {}
    for index in range(vppb_counts):
        allocated_ld[index + 1] = [index]
    virtual_switch_manager = VirtualSwitchManager(
        switch_configs=switch_configs,
        physical_port_manager=physical_port_manager,
        allocated_ld=allocated_ld,
    )
    for switch_index in range(len(switch_configs)):
        virtual_switch = virtual_switch_manager.get_virtual_switch(switch_index)
        assert isinstance(virtual_switch, CxlVirtualSwitch)
    with pytest.raises(Exception):
        virtual_switch_manager.get_virtual_switch(len(switch_configs))
    assert virtual_switch_manager.get_virtual_switch_counts() == len(switch_configs)


def test_virtual_switch_manager_run_and_stop(unique_ports):
    UnalignedBitStructure.make_quiet()
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    switch_connection_manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    physical_port_manager = PhysicalPortManager(
        switch_connection_manager=switch_connection_manager, port_configs=port_configs
    )
    vppb_counts = 3
    initial_bounds = [1, 2, 3]
    switch_configs = [
        VirtualSwitchConfig(
            upstream_port_index=0,
            vppb_counts=vppb_counts,
            initial_bounds=initial_bounds,
            irq_host="127.0.0.1",
            irq_port=unique_ports['fabric'],
        )
    ]
    allocated_ld = {}
    for index in range(vppb_counts):
        allocated_ld[index + 1] = [0]
    virtual_switch_manager = VirtualSwitchManager(
        switch_configs=switch_configs,
        physical_port_manager=physical_port_manager,
        allocated_ld=allocated_ld,
    )

    # Start all components
    switch_connection_manager.start_wait_ready()
    physical_port_manager.start_wait_ready()
    virtual_switch_manager.start_wait_ready()

    # Stop all components
    virtual_switch_manager.stop_sync()
    physical_port_manager.stop_sync()
    switch_connection_manager.stop_sync()
