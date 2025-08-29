"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List
from enum import Enum, auto
from opencis.util.component import RunnableComponent
from opencis.cxl.component.cxl_connection import CxlConnection
from opencis.pci.component.packet_processor import PacketProcessor


class ROOT_PORT_SWITCH_TYPE(Enum):
    PASS_THROUGH = auto()
    PCIE_SWITCH = auto()


@dataclass
class CxlRootPortConfig:
    port_index: int
    upstream_connection: CxlConnection
    downstream_connection: CxlConnection
    is_pass_through: bool
    host_name: str


class CxlRootPort(RunnableComponent):
    def __init__(self, config: CxlRootPortConfig):
        super().__init__(
            lambda class_name: f"{config.host_name}:{class_name}:RootPort{config.port_index}"
        )
        if not config.is_pass_through:
            raise Exception("Only pass-through mode is supported")

        self._is_pass_through = config.is_pass_through
        self._upstream_connection = config.upstream_connection
        self._downstream_connection = config.downstream_connection

        self._cxl_io_cfg_processor = PacketProcessor(
            self._upstream_connection.cfg_fifo,
            self._downstream_connection.cfg_fifo,
            lambda _: f"{self.get_message_label()}:FifoRelay:CXL.io CFG",
        )
        self._cxl_io_mmio_processor = PacketProcessor(
            self._upstream_connection.mmio_fifo,
            self._downstream_connection.mmio_fifo,
            lambda _: f"{self.get_message_label()}:FifoRelay:CXL.io MMIO",
        )
        self._cxl_mem_processor = PacketProcessor(
            self._upstream_connection.cxl_mem_fifo,
            self._downstream_connection.cxl_mem_fifo,
            lambda _: f"{self.get_message_label()}:FifoRelay:CXL.mem",
        )
        self._cxl_cache_processor = PacketProcessor(
            self._upstream_connection.cxl_cache_fifo,
            self._downstream_connection.cxl_cache_fifo,
            lambda _: f"{self.get_message_label()}:FifoRelay:CXL.cache",
        )

    def _run(self):
        self._cxl_io_cfg_processor.start_wait_ready()
        self._cxl_io_mmio_processor.start_wait_ready()
        self._cxl_mem_processor.start_wait_ready()
        self._cxl_cache_processor.start_wait_ready()
        self._change_status_to_running()
        from opencis.util.logger import logger

        logger.info(self._create_message("RootPort FIFO relays RUNNING"))
        self._cxl_io_cfg_processor.join()
        self._cxl_io_mmio_processor.join()
        self._cxl_mem_processor.join()
        self._cxl_cache_processor.join()

    def _stop(self):
        self._cxl_io_cfg_processor.stop_sync()
        self._cxl_io_mmio_processor.stop_sync()
        self._cxl_mem_processor.stop_sync()
        self._cxl_cache_processor.stop_sync()


@dataclass
class RootPortSwitchPortConfig:
    port_index: int
    downstream_connection: CxlConnection


@dataclass
class RootPortSwitchConfig:
    upstream_connection: CxlConnection
    host_name: str
    root_bus: int
    root_ports: List[RootPortSwitchPortConfig] = field(default_factory=list)


class RootPortSwitchBase(RunnableComponent, ABC):
    @abstractmethod
    def get_root_bus(self) -> int:
        raise Exception("get_root_bus must be implemented by the child class")


class SimpleRootPortSwitch(RootPortSwitchBase):
    def __init__(self, config: RootPortSwitchConfig):
        super().__init__(lambda class_name: f"{config.host_name}:{class_name}")
        if len(config.root_ports) != 1:
            raise Exception("the length of config.root_ports must be 1 for SimpleRootPortSwitch")
        cxl_root_port_config = CxlRootPortConfig(
            port_index=config.root_ports[0].port_index,
            upstream_connection=config.upstream_connection,
            downstream_connection=config.root_ports[0].downstream_connection,
            is_pass_through=True,
            host_name=config.host_name,
        )
        self._root_port_device_client = CxlRootPort(cxl_root_port_config)
        self._root_bus_num = config.root_bus + 1

    def get_root_bus(self) -> int:
        return self._root_bus_num

    def _run(self):
        self._root_port_device_client.start_wait_ready()
        self._change_status_to_running()
        self._root_port_device_client.join()

    def _stop(self):
        self._root_port_device_client.stop_sync()
