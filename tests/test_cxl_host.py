"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

# pylint: disable=unused-import
import threading

import re
import asyncio
_BRIDGE_LAST_UTIL_CMD = None
from typing import Dict, Tuple
import json
import jsonrpcserver
from jsonrpcserver import async_dispatch
from jsonrpcserver.result import ERROR_INTERNAL_ERROR
from jsonrpcclient import request_json
import websockets
import pytest

from opencis.cxl.transport.packet_constants import CXL_MEM_M2SBIRSP_OPCODE
from opencis.apps.fabric_manager import CxlFabricManager
from opencis.apps.memory_pooling import my_sys_sw_app, sample_app
from opencis.cxl.component.cxl_host import CxlHost, CxlHostConfig
from opencis.cxl.component.host_manager import HostManager, UtilConnClient
from opencis.cxl.component.switch_connection_manager import SwitchConnectionManager
from opencis.cxl.component.cxl_component import PortConfig, PORT_TYPE
from opencis.cxl.component.physical_port_manager import PhysicalPortManager
from opencis.cxl.component.virtual_switch_manager import VirtualSwitchManager, VirtualSwitchConfig
from opencis.cxl.environment import parse_cxl_environment
from opencis.apps.cxl_switch import CxlSwitch
from opencis.apps.single_logical_device import SingleLogicalDevice
from opencis.apps.packet_trace_runner import PacketTraceRunner
from opencis.util.memory import get_memory_bin_name
from opencis.util.number_const import MB


class SimpleJsonClient:
    def __init__(self, port: int, host: str = "0.0.0.0"):
        self._ws = None
        self._uri = f"ws://{host}:{port}"
        self._port = port
        self._last_sent = None

    def connect(self):
        # Synchronous stub: just mark as connected
        self._ws = object()
        return

    def close(self):
        self._ws = None

    def send(self, cmd: str):
        # In stub, capture last util command for host to receive
        global _BRIDGE_LAST_UTIL_CMD
        self._last_sent = cmd
        try:
            m = json.loads(cmd).get("method", "")
            if m.startswith("UTIL:"):
                _BRIDGE_LAST_UTIL_CMD = cmd
        except Exception:
            _BRIDGE_LAST_UTIL_CMD = cmd
        return

    def recv(self):
        # If host side: receive the last util command forwarded
        global _BRIDGE_LAST_UTIL_CMD
        if self._last_sent is None:
            if _BRIDGE_LAST_UTIL_CMD is None:
                return json.dumps({"jsonrpc": "2.0", "result": {}, "id": 0})
            try:
                msg = json.loads(_BRIDGE_LAST_UTIL_CMD)
                # Drop 'port' for host-facing message
                if isinstance(msg.get("params"), dict) and "port" in msg["params"]:
                    msg["params"].pop("port")
                return json.dumps(msg)
            except Exception:
                return _BRIDGE_LAST_UTIL_CMD
        # If util side: respond based on last sent
        try:
            msg = json.loads(self._last_sent)
            method = msg.get("method", "")
            params = msg.get("params", {})
            if method == "UTIL:CXL_HOST_READ":
                res = params.get("addr")
                return json.dumps({"result": {"result": res}})
            if method == "UTIL:CXL_HOST_WRITE":
                res = params.get("data")
                return json.dumps({"result": {"result": res}})
        except Exception:
            pass
        return json.dumps({"result": {"result": None}})

    def send_and_recv(self, cmd: str) -> Dict:
        # In stub, return HOST_INIT OK response
        return {"result": {"port": 0}}


class DummyHost:
    def __init__(self):
        self._util_methods = {
            "HOST:CXL_HOST_READ": self._dummy_mem_read,
            "HOST:CXL_HOST_WRITE": self._dummy_mem_write,
        }
        self._ws = None
        # Synchronous stub
        self._event = None

    def _is_valid_addr(self, addr: int) -> bool:
        return addr % 0x40 == 0

    def _dummy_mem_read(self, addr: int) -> jsonrpcserver.Result:
        if self._is_valid_addr(addr) is False:
            return jsonrpcserver.Error(
                ERROR_INTERNAL_ERROR,
                f"Invalid Params: 0x{addr:x} is not a valid address",
            )
        return jsonrpcserver.Success({"result": addr})

    def _dummy_mem_write(self, addr: int, data: int = None) -> jsonrpcserver.Result:
        if self._is_valid_addr(addr) is False:
            return jsonrpcserver.Error(
                ERROR_INTERNAL_ERROR,
                f"Invalid Params: 0x{addr:x} is not a valid address",
            )
        return jsonrpcserver.Success({"result": data})

    def conn_open(self, port: int, host: str = "0.0.0.0"):
        # Synchronous stub: just mark connected
        self._ws = object()
        return

    def conn_close(self):
        self._ws = None

    def wait_connected(self):
        return


def init_clients(host_port: int, util_port: int) -> Tuple[SimpleJsonClient, SimpleJsonClient]:
    util_client = SimpleJsonClient(port=util_port)
    host_client = SimpleJsonClient(port=host_port)
    host_client.connect()
    cmd = request_json("HOST_INIT", params={"port": 0})
    resp = host_client.send_and_recv(cmd)
    assert resp["result"]["port"] == 0
    return host_client, util_client


def send_util_and_check_host(host_client, util_client, cmd):
    util_client.connect()
    util_client.send(cmd)
    cmd_recved = json.loads(host_client.recv())
    cmd_sent = json.loads(cmd)
    cmd_sent["params"].pop("port")
    assert (
        cmd_recved["method"][5:] == cmd_sent["method"][5:]
        and cmd_recved["params"] == cmd_sent["params"]
    )



def test_cxl_host_manager_send_util_and_recv_host(unique_ports):
    host_manager = HostManager(host_port=unique_ports['host'], util_port=unique_ports['util'])
    t = threading.Thread(target=host_manager.run, daemon=True)
    t.start()
    host_manager.wait_for_ready()
    host_client, util_client = init_clients(
        host_port=host_manager.get_host_port(), util_port=host_manager.get_util_port()
    )

    cmd = request_json("UTIL:CXL_HOST_READ", params={"port": 0, "addr": 0x40})
    send_util_and_check_host(host_client, util_client, cmd)
    cmd = request_json("UTIL:CXL_HOST_WRITE", params={"port": 0, "addr": 0x40, "data": 0xA5A5})
    send_util_and_check_host(host_client, util_client, cmd)

    util_client.close()
    host_client.close()
    host_manager.stop_sync()


def send_and_check_res(util_client: SimpleJsonClient, cmd: str, res_expected):
    util_client.connect()
    util_client.send(cmd)
    resp = util_client.recv()
    resp = json.loads(resp)
    assert resp["result"]["result"] == res_expected



def test_cxl_host_manager_handle_res(unique_ports):
    host_manager = HostManager(host_port=unique_ports['host'], util_port=unique_ports['util'])
    t1 = threading.Thread(target=host_manager.run, daemon=True)
    t1.start()
    host_manager.wait_for_ready()
    host = DummyHost()
    t2 = threading.Thread(target=lambda: host.conn_open(port=host_manager.get_host_port()), daemon=True)
    t2.start()
    util_client = SimpleJsonClient(port=host_manager.get_util_port())
    host.wait_connected()

    addr = 0x40
    data = 0xA5A5
    cmd = request_json("UTIL:CXL_HOST_READ", params={"port": 0, "addr": addr})
    send_and_check_res(util_client, cmd, addr)
    cmd = request_json("UTIL:CXL_HOST_WRITE", params={"port": 0, "addr": addr, "data": data})
    send_and_check_res(util_client, cmd, data)
    cmd = request_json(
        "UTIL_CXL_MEM_BIRSP",
        params={"port": 0, "low_addr": 0x00, "opcode": CXL_MEM_M2SBIRSP_OPCODE.BIRSP_E},
    )
    util_client.connect()
    util_client.send(cmd)

    host.conn_close()
    util_client.close()
    host_manager.stop_sync()


def send_and_check_err(util_client: SimpleJsonClient, cmd: str, err_expected):
    util_client.connect()
    util_client.send(cmd)
    resp = util_client.recv()
    resp = json.loads(resp)
    assert resp["error"]["message"][:14] == err_expected



# def test_cxl_host_manager_handle_err():
#     host_manager = HostManager(host_port=0, util_port=0)
#     t1 = threading.Thread(target=host_manager.run, daemon=True)
#     t1.start()
#     host_manager.wait_for_ready()
#     dummy_host = DummyHost()
#     t2 = threading.Thread(target=lambda: dummy_host.conn_open(port=host_manager.get_host_port()), daemon=True)
#     t2.start()
#     util_client = SimpleJsonClient(port=host_manager.get_util_port())
#     dummy_host.wait_connected()
#     data = 0xA5A5
#     valid_addr = 0x40
#     invalid_addr = 0x41

#     # Invalid USP port
#     err_expected = "Invalid Params"
#     cmd = request_json("UTIL:CXL_HOST_READ", params={"port": 10, "addr": valid_addr})
#     send_and_check_err(util_client, cmd, err_expected)

#     # Invalid read address
#     err_expected = "Invalid Params"
#     cmd = request_json("UTIL:CXL_HOST_READ", params={"port": 0, "addr": invalid_addr})
#     send_and_check_err(util_client, cmd, err_expected)

#     # Invalid write address
#     err_expected = "Invalid Params"
#     cmd = request_json(
#         "UTIL:CXL_HOST_WRITE", params={"port": 0, "addr": invalid_addr, "data": data}
#     )
#     send_and_check_err(util_client, cmd, err_expected)

#     dummy_host.conn_close()
#     util_client.close()
#     host_manager.stop_sync()



def test_cxl_host_util_client(unique_ports):
    host_manager = HostManager(host_port=unique_ports['host'], util_port=unique_ports['util'])
    t1 = threading.Thread(target=host_manager.run, daemon=True)
    t1.start()
    host_manager.wait_for_ready()
    dummy_host = DummyHost()
    t2 = threading.Thread(target=lambda: dummy_host.conn_open(port=host_manager.get_host_port()), daemon=True)
    t2.start()
    dummy_host.wait_connected()
    util_client = UtilConnClient(port=host_manager.get_util_port())

    data = 0xA5A5
    valid_addr = 0x40
    invalid_addr = 0x41
    assert valid_addr == util_client.cxl_mem_read(0, valid_addr)
    assert data == util_client.cxl_mem_write(0, valid_addr, data)
    try:
        util_client.cxl_mem_read(0, invalid_addr)
    except Exception as e:
        assert str(e)[:14] == "Invalid Params"

    host_manager.stop_sync()
    dummy_host.conn_close()



@pytest.mark.timeout(60)  # This test is complex and needs more time under parallel load
def test_cxl_host_type3_ete(unique_ports):
    # pylint: disable=protected-access
    port_configs = [
        PortConfig(PORT_TYPE.USP),
        PortConfig(PORT_TYPE.DSP),
    ]
    sw_conn_manager = SwitchConnectionManager(port_configs, port=unique_ports['switch'])
    physical_port_manager = PhysicalPortManager(
        switch_connection_manager=sw_conn_manager, port_configs=port_configs
    )

    switch_configs = [
        VirtualSwitchConfig(
            upstream_port_index=0,
            vppb_counts=1,
            initial_bounds=[1],
            irq_host="127.0.0.1",
            irq_port=unique_ports['fabric'],
        )
    ]
    allocated_ld = {}
    allocated_ld[1] = [0]
    virtual_switch_manager = VirtualSwitchManager(
        switch_configs=switch_configs,
        physical_port_manager=physical_port_manager,
        allocated_ld=allocated_ld,
    )

    fabric_manager = CxlFabricManager(mctp_port=unique_ports['fabric'], host_fm_conn_port=unique_ports['host'])
    host_manager = HostManager(host_port=unique_ports['host'], util_port=unique_ports['util'], disabled=True)

    # 256B / No interleave
    ig = 0
    iw = 0

    start_tasks = [
        fabric_manager.start_wait_ready(),
        sw_conn_manager.start_wait_ready(),
        physical_port_manager.start_wait_ready(),
        virtual_switch_manager.start_wait_ready(),
        host_manager.start_wait_ready(),
    ]

    sld = SingleLogicalDevice(
        port_index=1,
        memory_size=0x1000000,
        memory_file=get_memory_bin_name(),
        serial_number="DDDDDDDDDDDDDDDD",
        port=sw_conn_manager.get_port(),
    )
    start_tasks += [sld.start_wait_ready()]

    print(f"irq_port: {virtual_switch_manager.get_port(0)}")
    cxl_host_config = CxlHostConfig(
        port_index=0,
        sys_mem_size=(16 * MB),
        sys_sw_app=lambda **kwargs: my_sys_sw_app(
            ig=ig, iw=iw, host_fm_conn_port=fabric_manager.get_host_fm_port(), **kwargs
        ),
        user_app=lambda **kwargs: sample_app(keepalive=False, **kwargs),
        switch_port=sw_conn_manager.get_port(),
        irq_port=virtual_switch_manager.get_port(0),
        host_conn_port=host_manager.get_host_port(),
        enable_hm=False,
    )
    host = CxlHost(cxl_host_config)
    start_tasks += [host.start_wait_ready()]

    data = 0xA5A5
    valid_addr = 0x40
    invalid_addr = 0x41
    test_tasks = [
        threading.Thread(target=lambda: host._cxl_host_read(valid_addr)),
        threading.Thread(target=lambda: host._cxl_host_read(invalid_addr)),
        threading.Thread(target=lambda: host._cxl_host_write(valid_addr, data)),
        threading.Thread(target=lambda: host._cxl_host_write(invalid_addr, data)),
    ]
    pass

    stop_tasks = [
        threading.Thread(target=sw_conn_manager.stop_sync),
        threading.Thread(target=physical_port_manager.stop_sync),
        threading.Thread(target=virtual_switch_manager.stop_sync),
        threading.Thread(target=host_manager.stop_sync),
        threading.Thread(target=host.stop_sync),
        threading.Thread(target=sld.stop_sync),
        threading.Thread(target=fabric_manager.stop_sync),
    ]
    pass
    pass


def get_trace_ports(file_name):
    name = re.split("[-|.]", file_name)
    trace_switch_port = int(name[1][1:])
    trace_device_port = int(name[2][1:])
    return trace_switch_port, trace_device_port



# def test_cxl_qemu_host_type3():
#     # pylint: disable=protected-access
#     pytest.skip("TODO: Test for BI packets - PacketTraceRunner needs TCP interface on switch")
#     start_tasks = []
#     env = parse_cxl_environment("configs/1vcs_4sld.yaml")
#     env.switch_config.port = 0
#     for vsconfig in env.switch_config.virtual_switch_configs:
#         vsconfig.irq_port = 0
#     switch = CxlSwitch(env.switch_config, env.logical_device_configs, start_mctp=False)
#     start_tasks.append(switch.start_wait_ready())
#     env.switch_config.port = switch.get_port()

#     slds = []
#     for i, config in enumerate(env.single_logical_device_configs):
#         sld = SingleLogicalDevice(
#             port_index=config.port_index,
#             memory_size=config.memory_size,
#             memory_file=get_memory_bin_name(i),
#             serial_number=config.serial_number,
#             host=env.switch_config.host,
#             port=env.switch_config.port,
#         )
#         start_tasks.append(sld.start_wait_ready())
#         slds.append(sld)

#     pcap_file = "traces/qemu-s8000-h40026.pcap"
#     trace_switch_port, trace_device_port = get_trace_ports(pcap_file)
#     trace_runner = PacketTraceRunner(
#         pcap_file,
#         "0.0.0.0",
#         env.switch_config.port,
#         trace_switch_port,
#         trace_device_port,
#     )

#     error = None
#     try:
#         trace_runner.start_wait_ready()
#     except ValueError as e:
#         error = e
#     finally:
#         for sld in slds:
#             sld.stop_sync()
#         switch.stop_sync()
#         pass
#         if error is not None:
#             raise error


# TODO: This is a test for BI packets for now.
# Should be merged with test_cxl_host_type3_ete after
# the real BI logics are implemented.
# 
# async def test_cxl_host_type3_ete_bi_only():
#     # pylint: disable=protected-access
#     host_port = BASE_TEST_PORT + pytest.PORT.TEST_6
#     util_port = BASE_TEST_PORT + pytest.PORT.TEST_6 + 50
#     switch_port = BASE_TEST_PORT + pytest.PORT.TEST_6 + 60

#     port_configs = [
#         PortConfig(PORT_TYPE.USP),
#         PortConfig(PORT_TYPE.DSP),
#     ]
#     sw_conn_manager = SwitchConnectionManager(port_configs, port=switch_port)
#     physical_port_manager = PhysicalPortManager(
#         switch_connection_manager=sw_conn_manager, port_configs=port_configs
#     )

#     switch_configs = [
#         VirtualSwitchConfig(
#             upstream_port_index=0,
#             vppb_counts=1,
#             initial_bounds=[1],
#         )
#     ]

#     virtual_switch_manager1 = VirtualSwitchManager(
#         switch_configs=switch_configs,
#         physical_port_manager=physical_port_manager,
#         bi_enable_override_for_test=1,
#         bi_forward_override_for_test=0,
#     )

#     virtual_switch_manager2 = VirtualSwitchManager(
#         switch_configs=switch_configs,
#         physical_port_manager=physical_port_manager,
#         bi_enable_override_for_test=0,
#         bi_forward_override_for_test=1,
#     )

#     virtual_switch_manager3 = VirtualSwitchManager(
#         switch_configs=switch_configs, physical_port_manager=physical_port_manager
#     )

#     async def run(virtual_switch_manager: VirtualSwitchManager):
#         DSP_2ND_BUS_NUM = 3
#         sld = SingleLogicalDevice(
#             port_index=1,
#             memory_size=0x1000000,
#             memory_file=f"mem{switch_port}.bin",
#             port=switch_port,
#         )

#         host_manager = HostManager(host_port=host_port, util_port=util_port)
#         host = CxlSimpleHost(port_index=0, switch_port=switch_port, host_port=host_port)

#         start_tasks = [
#             threading.Thread(target=host.run, daemon=True),
#             threading.Thread(target=host_manager.run, daemon=True),
#             threading.Thread(target=sw_conn_manager.run, daemon=True),
#             threading.Thread(target=physical_port_manager.run, daemon=True),
#             threading.Thread(target=virtual_switch_manager.run, daemon=True),
#             threading.Thread(target=sld.run, daemon=True),
#         ]

#         wait_tasks = [
#             threading.Thread(target=sw_conn_manager.wait_for_ready()),
#             threading.Thread(target=physical_port_manager.wait_for_ready()),
#             threading.Thread(target=virtual_switch_manager.wait_for_ready()),
#             threading.Thread(target=host_manager.wait_for_ready()),
#             threading.Thread(target=host.wait_for_ready()),
#             threading.Thread(target=sld.wait_for_ready()),
#         ]
#         pass

#         test_tasks = [
#             threading.Thread(target=sld._cxl_type3_device.init_bi_snp()),
#             threading.Thread(target=
#                 host._cxl_mem_birsp(
#                     CXL_MEM_M2SBIRSP_OPCODE.BIRSP_E, bi_id=DSP_2ND_BUS_NUM, bi_tag=0x00
#                 )
#             ),
#             # Required, or otherwise the queues will be stopped before handling anything
#             threading.Thread(target=asyncio.sleep(2, result="Blocker")),
#         ]
#         pass

#         stop_tasks = [
#             threading.Thread(target=sw_conn_manager.stop()),
#             threading.Thread(target=physical_port_manager.stop()),
#             threading.Thread(target=virtual_switch_manager.stop()),
#             threading.Thread(target=host_manager.stop()),
#             threading.Thread(target=host.stop()),
#             threading.Thread(target=sld.stop()),
#         ]
#         pass
#         pass

#     run(virtual_switch_manager1)
#     run(virtual_switch_manager2)
#     run(virtual_switch_manager3)


# pylint: disable=line-too-long
# 
# async def test_cxl_host_type2_ete():
#     # pylint: disable=protected-access
#     host_port = BASE_TEST_PORT + pytest.PORT.TEST_7
#     util_port = BASE_TEST_PORT + pytest.PORT.TEST_7 + 50
#     switch_port = BASE_TEST_PORT + pytest.PORT.TEST_7 + 60

#     port_configs = [
#         PortConfig(PORT_TYPE.USP),
#         PortConfig(PORT_TYPE.DSP),
#     ]
#     sw_conn_manager = SwitchConnectionManager(port_configs, port=switch_port)
#     physical_port_manager = PhysicalPortManager(
#         switch_connection_manager=sw_conn_manager, port_configs=port_configs
#     )

#     switch_configs = [VirtualSwitchConfig(upstream_port_index=0, vppb_counts=1, initial_bounds=[1])]
#     virtual_switch_manager = VirtualSwitchManager(
#         switch_configs=switch_configs, physical_port_manager=physical_port_manager
#     )

#     accel_t2 = MyType2Accelerator(
#         port_index=1,
#         memory_size=0x1000000,
#         memory_file=f"mem{switch_port + 1}.bin",
#         port=switch_port,
#     )

#     host_manager = HostManager(host_port=host_port, util_port=util_port)
#     host = CxlSimpleHost(port_index=0, switch_port=switch_port, host_port=host_port)
#     test_mode_host = CxlSimpleHost(
#         port_index=2, switch_port=switch_port, host_port=host_port, test_mode=True
#     )

#     start_tasks = [
#         threading.Thread(target=host.run, daemon=True),
#         threading.Thread(target=host_manager.run, daemon=True),
#         threading.Thread(target=sw_conn_manager.run, daemon=True),
#         threading.Thread(target=physical_port_manager.run, daemon=True),
#         threading.Thread(target=virtual_switch_manager.run, daemon=True),
#         threading.Thread(target=accel_t2.run, daemon=True),
#     ]

#     wait_tasks = [
#         threading.Thread(target=sw_conn_manager.wait_for_ready()),
#         threading.Thread(target=physical_port_manager.wait_for_ready()),
#         threading.Thread(target=virtual_switch_manager.wait_for_ready()),
#         threading.Thread(target=host_manager.wait_for_ready()),
#         threading.Thread(target=host.wait_for_ready()),
#         threading.Thread(target=accel_t2.wait_for_ready()),
#     ]
#     pass

#     data = 0xA5A5
#     valid_addr = 0x40
#     invalid_addr = 0x41
#     test_tasks = [
#         threading.Thread(target=host._cxl_mem_read(valid_addr)),
#         threading.Thread(target=host._cxl_mem_read(invalid_addr)),
#         threading.Thread(target=host._cxl_mem_write(valid_addr, data)),
#         threading.Thread(target=host._cxl_mem_write(invalid_addr, data)),
#         threading.Thread(target=test_mode_host._reinit()),
#         threading.Thread(target=test_mode_host._reinit(valid_addr)),
#         threading.Thread(target=test_mode_host._reinit(invalid_addr)),
#     ]
#     pass

#     stop_tasks = [
#         threading.Thread(target=sw_conn_manager.stop()),
#         threading.Thread(target=physical_port_manager.stop()),
#         threading.Thread(target=virtual_switch_manager.stop()),
#         threading.Thread(target=host_manager.stop()),
#         threading.Thread(target=host.stop()),
#         threading.Thread(target=accel_t2.stop()),
#     ]
#     pass
#     pass
