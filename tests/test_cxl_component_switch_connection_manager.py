"""
Copyright (c) 2024-2025, Eeum, Inc.
import threading
This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""
import threading
import pytest
import threading
from opencis.util.logger import logger
from opencis.cxl.component.switch_connection_manager import (
    SwitchConnectionManager,
    CxlConnection,
)
from opencis.cxl.component.switch_connection_client import (
    SwitchConnectionClient,
    INJECTED_ERRORS,
)
from opencis.cxl.component.cxl_component import (
    PortConfig,
    PORT_TYPE,
)
from opencis.cxl.component.common import CXL_COMPONENT_TYPE
from opencis.util.pci import create_bdf
from opencis.cxl.transport.packet_constants import (
    CXL_IO_CPL_STATUS,
    CXL_MEM_M2SBIRSP_OPCODE,
    CXL_MEM_S2MBISNP_OPCODE,
)
from opencis.cxl.transport.cxl_mem_packets import (
    CxlMemMemRdPacket,
    CxlMemMemWrPacket,
    CxlMemMemDataPacket,
    CxlMemBIRspPacket,
    CxlMemBISnpPacket,
    CxlMemCmpPacket,
)
from opencis.cxl.transport.cxl_io_packets import (
    CxlIoCfgRdPacket,
    CxlIoCfgWrPacket,
    CxlIoMemRdPacket,
    CxlIoMemWrPacket,
    CxlIoCompletionPacket,
)


def test_switch_connection_manager_check_ports(unique_ports):
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    for port in range(len(port_configs)):
        connection = manager.get_cxl_connection(port)
        assert isinstance(connection, CxlConnection)
    with pytest.raises(Exception):
        manager.get_cxl_connection(len(port_configs))


def test_switch_connection_manager_run_and_stop(unique_ports):
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])

    manager.start_wait_ready()
    manager.stop_sync()


def test_switch_connection_manager_run_and_run(unique_ports):
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])

    manager.start_wait_ready()
    with pytest.raises(Exception):
        manager.start_wait_ready()
    manager.stop_sync()



def test_switch_connection_manager_handle_connection(unique_ports):
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    client = SwitchConnectionClient(
        port_index=0, component_type=CXL_COMPONENT_TYPE.R, retry=False, port=unique_ports['switch']
    )

    def start():
        logger.info("[PyTest] Starting SwitchConnectionManager")
        manager.start_wait_ready()

    def wait_and_connect():
        manager.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionManager is ready")
        logger.info("[PyTest] Starting SwitchConnectionClient")
        client.set_port(manager.get_port())
        client.start_wait_ready()

    def stop():
        client.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionClient is ready")
        logger.info("[PyTest] Stopping SwitchConnectionManager")
        manager.stop_sync()
        logger.info("[PyTest] Stopping SwitchConnectionClient")
        client.stop_sync()

    tasks = [
        threading.Thread(target=start, daemon=True),
        threading.Thread(target=wait_and_connect, daemon=True),
        threading.Thread(target=stop, daemon=True),
    ]
    for t in tasks:

        t.start()

    for t in tasks:

        t.join(timeout=5.0)



def test_switch_connection_manager_handle_connection_oob(unique_ports):
    pytest.skip("Error injection mechanism not implemented")
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    client = SwitchConnectionClient(
        port_index=4, component_type=CXL_COMPONENT_TYPE.R, retry=False, port=0
    )

    def start():
        logger.info("[PyTest] Starting SwitchConnectionManager")
        manager.start_wait_ready()

    def wait_and_connect():
        manager.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionManager is ready")
        logger.info("[PyTest] Starting SwitchConnectionClient")
        with pytest.raises(Exception, match="Connection rejected"):
            client.set_port(manager.get_port())
            client.start_wait_ready()
        manager.stop_sync()

    tasks = [
        threading.Thread(target=start, daemon=True),
        threading.Thread(target=wait_and_connect, daemon=True),
    ]

    for t in tasks:


        t.start()


    for t in tasks:


        t.join(timeout=5.0)



def test_switch_connection_manager_handle_connection_after_connection(unique_ports):
    pytest.skip("Error injection mechanism not implemented")
    # pylint: disable=protected-access
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    client = SwitchConnectionClient(
        port_index=0, component_type=CXL_COMPONENT_TYPE.R, retry=False, port=unique_ports['switch']
    )

    def start():
        logger.info("[PyTest] Starting SwitchConnectionManager")
        manager.start_wait_ready()

    def wait_and_connect():
        manager.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionManager is ready")
        logger.info("[PyTest] Starting SwitchConnectionClient")
        client.set_port(manager.get_port())
        client.start_wait_ready()

    def stop():
        client.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionClient is ready")
        with pytest.raises(Exception, match="Connection rejected"):
            client._connect_sync()
        logger.info("[PyTest] Stopping SwitchConnectionManager")
        manager.stop_sync()
        logger.info("[PyTest] Stopping SwitchConnectionClient")
        client.stop_sync()

    tasks = [
        threading.Thread(target=start, daemon=True),
        threading.Thread(target=wait_and_connect, daemon=True),
        threading.Thread(target=stop, daemon=True),
    ]
    for t in tasks:

        t.start()

    for t in tasks:

        t.join(timeout=5.0)



def test_switch_connection_manager_handle_connection_errors(unique_ports):
    pytest.skip("Error injection mechanism not implemented")
    # pylint: disable=function-redefined
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    client = SwitchConnectionClient(
        port_index=0, component_type=CXL_COMPONENT_TYPE.R, retry=False, port=unique_ports['switch']
    )

    def start():
        logger.info("[PyTest] Starting SwitchConnectionManager")
        manager.start_wait_ready()

    def wait_and_connect():
        manager.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionManager is ready")
        logger.info("[PyTest] Starting SwitchConnectionClient")
        client.set_port(manager.get_port())
        client.start_wait_ready()

    def wait_and_connect():
        manager.wait_for_ready()
        with pytest.raises(Exception, match="Connection rejected"):
            client.set_port(manager.get_port())
            client.inject_error(INJECTED_ERRORS.NON_SIDEBAND)
            client.start_wait_ready()
        with pytest.raises(Exception, match="Connection rejected"):
            client.set_port(manager.get_port())
            client.inject_error(INJECTED_ERRORS.NON_CONNNECTION_REQUEST)
            client.start_wait_ready()
        manager.stop_sync()

    tasks = [threading.Thread(target=start, daemon=True), threading.Thread(target=wait_and_connect, daemon=True)]
    for t in tasks:

        t.start()

    for t in tasks:

        t.join(timeout=5.0)



def test_switch_connection_manager_handle_cfg_packet(unique_ports):
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    client = SwitchConnectionClient(
        port_index=0, component_type=CXL_COMPONENT_TYPE.R, retry=False, port=unique_ports['switch']
    )

    def start():
        logger.info("[PyTest] Starting SwitchConnectionManager")
        manager.start_wait_ready()

    def wait_and_connect():
        manager.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionManager is ready")
        logger.info("[PyTest] Starting SwitchConnectionClient")
        client.set_port(manager.get_port())
        client.start_wait_ready()

    def send_packets_and_stop():
        client.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionClient is ready")
        logger.info("[PyTest] Sending config space request packets from client")
        client_connection = client.get_cxl_connection()
        packet = CxlIoCfgWrPacket.create(create_bdf(0, 0, 0), 0x10, 4, 0xDEADBEEF)
        client_connection.cfg_fifo.host_to_target.put(packet)
        packet = CxlIoCfgRdPacket.create(create_bdf(0, 0, 0), 0x10, 4)
        client_connection.cfg_fifo.host_to_target.put(packet)

        logger.info("[PyTest] Checking config space request packets received from server")
        server_connection = manager.get_cxl_connection(0)
        server_connection.cfg_fifo.host_to_target.get()
        server_connection.cfg_fifo.host_to_target.get()

        logger.info("[PyTest] Stopping SwitchConnectionManager")
        manager.stop_sync()
        logger.info("[PyTest] Stopping SwitchConnectionClient")
        client.stop_sync()

    tasks = [
        threading.Thread(target=start, daemon=True),
        threading.Thread(target=wait_and_connect, daemon=True),
        threading.Thread(target=send_packets_and_stop, daemon=True),
    ]
    for t in tasks:

        t.start()

    for t in tasks:

        t.join(timeout=5.0)
    
    # Ensure components fully exit
    manager.join(timeout=2.0)
    client.join(timeout=2.0)



def test_switch_connection_manager_handle_mmio_packet(unique_ports):
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    client = SwitchConnectionClient(
        port_index=0, component_type=CXL_COMPONENT_TYPE.R, retry=False, port=unique_ports['switch']
    )

    def start():
        logger.info("[PyTest] Starting SwitchConnectionManager")
        manager.start_wait_ready()

    def wait_and_connect():
        manager.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionManager is ready")
        logger.info("[PyTest] Starting SwitchConnectionClient")
        client.set_port(manager.get_port())
        client.start_wait_ready()

    def send_packets_and_stop():
        client.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionClient is ready")
        logger.info("[PyTest] Sending MMIO request packets from client")
        client_connection = client.get_cxl_connection()
        packet = CxlIoMemWrPacket.create(0, 4, 0)
        client_connection.mmio_fifo.host_to_target.put(packet)
        packet = CxlIoMemRdPacket.create(0, 4)
        client_connection.mmio_fifo.host_to_target.put(packet)

        logger.info("[PyTest] Checking MMIO request packets received from server")
        server_connection = manager.get_cxl_connection(0)
        server_connection.mmio_fifo.host_to_target.get()

        logger.info("[PyTest] Stopping SwitchConnectionManager")
        manager.stop_sync()
        logger.info("[PyTest] Stopping SwitchConnectionClient")
        client.stop_sync()

    tasks = [
        threading.Thread(target=start, daemon=True),
        threading.Thread(target=wait_and_connect, daemon=True),
        threading.Thread(target=send_packets_and_stop, daemon=True),
    ]
    for t in tasks:

        t.start()

    for t in tasks:

        t.join(timeout=5.0)
    
    # Ensure components fully exit
    manager.join(timeout=2.0)
    client.join(timeout=2.0)



def test_switch_connection_manager_handle_cxl_mem_packet_m2s(unique_ports):
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    client = SwitchConnectionClient(
        port_index=0, component_type=CXL_COMPONENT_TYPE.R, retry=False, port=unique_ports['switch']
    )

    def start():
        logger.info("[PyTest] Starting SwitchConnectionManager")
        manager.start_wait_ready()

    def wait_and_connect():
        manager.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionManager is ready")
        logger.info("[PyTest] Starting SwitchConnectionClient")
        client.set_port(manager.get_port())
        client.start_wait_ready()

    def send_packets_and_stop():
        client.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionClient is ready")
        logger.info("[PyTest] Sending CXL.mem request packets from client")
        client_connection = client.get_cxl_connection()
        packet = CxlMemMemWrPacket.create(0x80, 0xDEADBEEF)
        client_connection.cxl_mem_fifo.host_to_target.put(packet)
        packet = CxlMemMemRdPacket.create(0x80)
        client_connection.cxl_mem_fifo.host_to_target.put(packet)
        packet = CxlMemBIRspPacket.create(CXL_MEM_M2SBIRSP_OPCODE.BIRSP_E, 0, 0)
        client_connection.cxl_mem_fifo.host_to_target.put(packet)

        logger.info("[PyTest] Checking CXL.mem request packets received from server")
        server_connection = manager.get_cxl_connection(0)
        server_connection.cxl_mem_fifo.host_to_target.get()

        logger.info("[PyTest] Stopping SwitchConnectionManager")
        manager.stop_sync()
        logger.info("[PyTest] Stopping SwitchConnectionClient")
        client.stop_sync()

    tasks = [
        threading.Thread(target=start, daemon=True),
        threading.Thread(target=wait_and_connect, daemon=True),
        threading.Thread(target=send_packets_and_stop, daemon=True),
    ]
    for t in tasks:

        t.start()

    for t in tasks:

        t.join(timeout=5.0)
    
    # Ensure components fully exit
    manager.join(timeout=2.0)
    client.join(timeout=2.0)



def test_switch_connection_manager_handle_cfg_completion(unique_ports):
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    client = SwitchConnectionClient(
        port_index=0, component_type=CXL_COMPONENT_TYPE.R, retry=False, port=unique_ports['switch']
    )

    def start():
        logger.info("[PyTest] Starting SwitchConnectionManager")
        manager.start_wait_ready()

    def wait_and_connect():
        manager.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionManager is ready")
        logger.info("[PyTest] Starting SwitchConnectionClient")
        client.set_port(manager.get_port())
        client.start_wait_ready()

    def send_packets_and_stop():
        client.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionClient is ready")
        logger.info("[PyTest] Sending config space completion packets from server")
        server_connection = manager.get_cxl_connection(0)
        client_connection = client.get_cxl_connection()
        req_id = 0x10
        tag = 0xA5
        req1 = CxlIoCfgWrPacket.create(0, 0x10, 4, 0xDEADBEEF, req_id=req_id, tag=tag)
        client_connection.cfg_fifo.host_to_target.put(req1)

        cpl_id = 0x20
        tag = 0xA6
        req2 = CxlIoCfgRdPacket.create(0, 0x10, 4, req_id=req_id, tag=tag)
        client_connection.cfg_fifo.host_to_target.put(req2)
        cpl2 = CxlIoCompletionPacket.create(
            req_id=req_id,
            tag=tag,
            cpl_id=cpl_id,
            data=0xDEADBEEF,
            length=4,
            status=CXL_IO_CPL_STATUS.SC,
        )
        server_connection.cfg_fifo.target_to_host.put(cpl2)

        logger.info("[PyTest] Checking config space completion packets received from client")
        rcvd_packets = []
        rcvd_packets.append(client_connection.cfg_fifo.host_to_target.get())
        rcvd_packets.append(client_connection.cfg_fifo.host_to_target.get())
        rcvd_packets.append(server_connection.cfg_fifo.target_to_host.get())

        assert bytes(rcvd_packets[0]) == bytes(req1)
        assert bytes(rcvd_packets[1]) == bytes(req2)
        assert bytes(rcvd_packets[2]) == bytes(cpl2)

        logger.info("[PyTest] Stopping SwitchConnectionManager")
        manager.stop_sync()
        logger.info("[PyTest] Stopping SwitchConnectionClient")
        client.stop_sync()

    tasks = [
        threading.Thread(target=start, daemon=True),
        threading.Thread(target=wait_and_connect, daemon=True),
        threading.Thread(target=send_packets_and_stop, daemon=True),
    ]
    for t in tasks:

        t.start()

    for t in tasks:

        t.join(timeout=5.0)
    
    # Ensure components fully exit
    manager.join(timeout=2.0)
    client.join(timeout=2.0)



def test_switch_connection_manager_handle_mmio_completion(unique_ports):
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    client = SwitchConnectionClient(
        port_index=0, component_type=CXL_COMPONENT_TYPE.R, retry=False, port=unique_ports['switch']
    )

    def start():
        logger.info("[PyTest] Starting SwitchConnectionManager")
        manager.start_wait_ready()

    def wait_and_connect():
        manager.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionManager is ready")
        logger.info("[PyTest] Starting SwitchConnectionClient")
        client.set_port(manager.get_port())
        client.start_wait_ready()

    def send_packets_and_stop():
        client.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionClient is ready")
        logger.info("[PyTest] Sending MMIO completion packets from server")
        server_connection = manager.get_cxl_connection(0)
        client_connection = client.get_cxl_connection()

        req_id = 0x10
        tag = 0x1
        req1 = CxlIoMemWrPacket.create(0x10, 4, 0xDEADBEEF, req_id=req_id, tag=tag)
        client_connection.mmio_fifo.host_to_target.put(req1)

        cpl_id = 0x20
        tag = 0x2
        req2 = CxlIoMemRdPacket.create(0x10, 4, req_id=req_id, tag=tag)
        client_connection.mmio_fifo.host_to_target.put(req2)
        cpl2 = CxlIoCompletionPacket.create(
            req_id=req_id,
            tag=tag,
            cpl_id=cpl_id,
            data=0xA5A5,
            length=2,
        )
        server_connection.mmio_fifo.target_to_host.put(cpl2)

        logger.info("[PyTest] Checking MMIO completion packets received from client")
        rcvd_packets = []
        rcvd_packets.append(client_connection.mmio_fifo.host_to_target.get())  # wr
        rcvd_packets.append(client_connection.mmio_fifo.host_to_target.get())  # rd
        rcvd_packets.append(server_connection.mmio_fifo.target_to_host.get())  # cpld

        assert bytes(rcvd_packets[0]) == bytes(req1)
        assert bytes(rcvd_packets[1]) == bytes(req2)
        assert bytes(rcvd_packets[2]) == bytes(cpl2)

        logger.info("[PyTest] Stopping SwitchConnectionManager")
        manager.stop_sync()
        logger.info("[PyTest] Stopping SwitchConnectionClient")
        client.stop_sync()

    tasks = [
        threading.Thread(target=start, daemon=True),
        threading.Thread(target=wait_and_connect, daemon=True),
        threading.Thread(target=send_packets_and_stop, daemon=True),
    ]
    for t in tasks:

        t.start()

    for t in tasks:

        t.join(timeout=5.0)
    
    # Ensure components fully exit
    manager.join(timeout=2.0)
    client.join(timeout=2.0)



def test_switch_connection_manager_handle_cxl_mem_s2m(unique_ports):
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
        PortConfig(PORT_TYPE.DSP),
    ]
    manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    client = SwitchConnectionClient(
        port_index=0, component_type=CXL_COMPONENT_TYPE.R, retry=False, port=unique_ports['switch']
    )

    def start():
        logger.info("[PyTest] Starting SwitchConnectionManager")
        manager.start_wait_ready()

    def wait_and_connect():
        manager.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionManager is ready")
        logger.info("[PyTest] Starting SwitchConnectionClient")
        client.set_port(manager.get_port())
        client.start_wait_ready()

    def send_packets_and_stop():
        client.wait_for_ready()
        logger.info("[PyTest] SwitchConnectionClient is ready")
        logger.info("[PyTest] Sending CXL.mem completion packets from server")
        server_connection = manager.get_cxl_connection(0)
        sent_packet1 = CxlMemMemDataPacket.create(0xDEADBEEF)
        server_connection.cxl_mem_fifo.target_to_host.put(sent_packet1)
        sent_packet2 = CxlMemCmpPacket.create()
        server_connection.cxl_mem_fifo.target_to_host.put(sent_packet2)
        sent_packet3 = CxlMemBISnpPacket.create(0x0, CXL_MEM_S2MBISNP_OPCODE.BISNP_DATA)
        server_connection.cxl_mem_fifo.target_to_host.put(sent_packet3)

        logger.info("[PyTest] Checking CXL.mem completion packets received from client")
        client_connection = client.get_cxl_connection()
        received_packet1 = client_connection.cxl_mem_fifo.target_to_host.get()
        assert bytes(received_packet1) == bytes(sent_packet1)
        received_packet2 = client_connection.cxl_mem_fifo.target_to_host.get()
        assert bytes(received_packet2) == bytes(sent_packet2)
        received_packet3 = client_connection.cxl_mem_fifo.target_to_host.get()
        assert bytes(received_packet3) == bytes(sent_packet3)

        logger.info("[PyTest] Stopping SwitchConnectionManager")
        manager.stop_sync()
        logger.info("[PyTest] Stopping SwitchConnectionClient")
        client.stop_sync()

    tasks = [
        threading.Thread(target=start, daemon=True),
        threading.Thread(target=wait_and_connect, daemon=True),
        threading.Thread(target=send_packets_and_stop, daemon=True),
    ]
    for t in tasks:

        t.start()

    for t in tasks:

        t.join(timeout=5.0)
    
    # Ensure components fully exit
    manager.join(timeout=2.0)
    client.join(timeout=2.0)
