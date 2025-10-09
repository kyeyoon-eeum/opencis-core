"""
Copyright (c) 2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import threading
import time

from opencis.apps.single_logical_device import SingleLogicalDevice
from opencis.apps.multi_logical_device import MultiLogicalDevice
from opencis.cxl.component.bind_processor import PpbDspBindProcessor
from opencis.cxl.component.cxl_connection import CxlConnection
from opencis.cxl.device.downstream_port_device import DownstreamPortDevice
from opencis.cxl.device.upstream_port_device import UpstreamPortDevice
from opencis.cxl.device.pci_to_pci_bridge_device import PpbDevice
from opencis.cxl.component.virtual_switch_manager import CxlVirtualSwitch
from opencis.util.memory import get_memory_bin_name
from opencis.util.number_const import MB
from opencis.util.unaligned_bit_structure import UnalignedBitStructure


# test_single_logical_device_bind_unbind() and test_multi_logical_device_bind_unbind() are similar
# pylint: disable=duplicate-code
def test_single_logical_device_bind_unbind(unique_ports):
    # Connection between switch and device
    transport_connection = CxlConnection()

    UnalignedBitStructure.make_quiet()

    vcs_id = 0
    upstream_port_index = 0
    vppb_counts = 3
    initial_bounds = [1, 2, 3]
    physical_ports = [
        UpstreamPortDevice(transport_connection=CxlConnection(), port_index=0),
        DownstreamPortDevice(transport_connection=transport_connection, port_index=1),
        DownstreamPortDevice(transport_connection=CxlConnection(), port_index=2),
        DownstreamPortDevice(transport_connection=CxlConnection(), port_index=3),
    ]
    allocated_ld = {}
    for index in range(vppb_counts):
        allocated_ld[index + 1] = [0]

    # Add PPB relation
    ppb_devices = []
    ppb_bind_processors = []

    ppb = PpbDevice(1)
    bind = PpbDspBindProcessor(
        ppb.get_downstream_connection(), physical_ports[1].get_transport_connection()
    )
    physical_ports[1].set_ppb(ppb, bind)
    ppb_devices.append(ppb)
    ppb_bind_processors.append(bind)

    ppb = PpbDevice(2)
    bind = PpbDspBindProcessor(CxlConnection(), CxlConnection())
    physical_ports[2].set_ppb(ppb, bind)
    ppb_devices.append(ppb)
    ppb_bind_processors.append(bind)

    ppb = PpbDevice(3)
    bind = PpbDspBindProcessor(CxlConnection(), CxlConnection())
    physical_ports[3].set_ppb(ppb, bind)
    ppb_devices.append(ppb)
    ppb_bind_processors.append(bind)

    vcs = CxlVirtualSwitch(
        id=vcs_id,
        upstream_port_index=upstream_port_index,
        vppb_counts=vppb_counts,
        initial_bounds=initial_bounds,
        physical_ports=physical_ports,
        irq_port=unique_ports['fabric'],
        allocated_ld=allocated_ld,
    )

    memory_size = 256 * MB
    memory_file = get_memory_bin_name()
    device = SingleLogicalDevice(
        memory_size=memory_size,
        memory_file=memory_file,
        serial_number="BBBBBBBBBBBBBBBB",
        test_mode=True,
        cxl_connection=transport_connection,
    )

    # Start all components and wait for ready
    device.start_wait_ready()
    vcs.start_wait_ready()
    for port in physical_ports:
        port.start_wait_ready()
    for ppb_bind_processor in ppb_bind_processors:
        ppb_bind_processor.start_wait_ready()
    for ppb_device in ppb_devices:
        ppb_device.start_wait_ready()

    # Give components time to fully initialize
    time.sleep(0.5)

    # Test bind/unbind operations
    vcs.unbind_vppb(0)
    vcs.bind_vppb(1, 0, 0)

    # Stop all components
    vcs.stop_sync()
    for port in physical_ports:
        port.stop_sync()
    for ppb_bind_processor in ppb_bind_processors:
        ppb_bind_processor.stop_sync()
    for ppb_device in ppb_devices:
        ppb_device.stop_sync()
    device.stop_sync()


# test_single_logical_device_bind_unbind() and test_multi_logical_device_bind_unbind() are similar
# pylint: disable=duplicate-code
def test_multi_logical_device_bind_unbind(unique_ports):
    # Connection between switch and device
    transport_connection = CxlConnection()

    UnalignedBitStructure.make_quiet()

    vcs_id = 0
    upstream_port_index = 0
    vppb_counts = 3
    initial_bounds = [1, 2, 3]
    # 2 ports are unbound
    physical_ports = [
        UpstreamPortDevice(transport_connection=CxlConnection(), port_index=0),
        DownstreamPortDevice(transport_connection=transport_connection, port_index=1),
        DownstreamPortDevice(transport_connection=CxlConnection(), port_index=2),
        DownstreamPortDevice(transport_connection=CxlConnection(), port_index=3),
    ]
    allocated_ld = {}
    for index in range(vppb_counts):
        allocated_ld[index + 1] = [0]

    # Add PPB relation
    ppb_devices = []
    ppb_bind_processors = []

    ppb = PpbDevice(1)
    bind = PpbDspBindProcessor(
        ppb.get_downstream_connection(), physical_ports[1].get_transport_connection()
    )
    physical_ports[1].set_ppb(ppb, bind)
    ppb_devices.append(ppb)
    ppb_bind_processors.append(bind)

    ppb = PpbDevice(2)
    bind = PpbDspBindProcessor(CxlConnection(), CxlConnection())
    physical_ports[2].set_ppb(ppb, bind)
    ppb_devices.append(ppb)
    ppb_bind_processors.append(bind)

    ppb = PpbDevice(3)
    bind = PpbDspBindProcessor(CxlConnection(), CxlConnection())
    physical_ports[3].set_ppb(ppb, bind)
    ppb_devices.append(ppb)
    ppb_bind_processors.append(bind)

    vcs = CxlVirtualSwitch(
        id=vcs_id,
        upstream_port_index=upstream_port_index,
        vppb_counts=vppb_counts,
        initial_bounds=initial_bounds,
        physical_ports=physical_ports,
        irq_port=unique_ports['fabric'],
        allocated_ld=allocated_ld,
    )

    memory_sizes = [256 * MB, 256 * MB, 256 * MB, 256 * MB]
    memory_files = [
        get_memory_bin_name(1),
        get_memory_bin_name(2),
        get_memory_bin_name(3),
        get_memory_bin_name(4),
    ]
    device = MultiLogicalDevice(
        memory_sizes=memory_sizes,
        memory_files=memory_files,
        serial_numbers=[
            "BBBBBBBBBBBBBBBB",
            "BBBBBBBBBBBBBBBB",
            "BBBBBBBBBBBBBBBB",
            "BBBBBBBBBBBBBBBB",
        ],
        test_mode=True,
        cxl_connections=[transport_connection, CxlConnection(), CxlConnection(), CxlConnection()],
        port_index=0,
    )

    # Start all components and wait for ready
    device.start_wait_ready()
    vcs.start_wait_ready()
    for port in physical_ports:
        port.start_wait_ready()
    for ppb_bind_processor in ppb_bind_processors:
        ppb_bind_processor.start_wait_ready()
    for ppb_device in ppb_devices:
        ppb_device.start_wait_ready()

    # Give components time to fully initialize
    time.sleep(0.5)

    # Test bind/unbind operations
    vcs.unbind_vppb(0)
    vcs.bind_vppb(1, 0, 0)

    # Stop all components
    vcs.stop_sync()
    for port in physical_ports:
        port.stop_sync()
    for ppb_bind_processor in ppb_bind_processors:
        ppb_bind_processor.stop_sync()
    for ppb_device in ppb_devices:
        ppb_device.stop_sync()
    device.stop_sync()
