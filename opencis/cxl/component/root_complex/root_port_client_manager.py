"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from dataclasses import dataclass, field
from typing import List
from opencis.util.component import RunnableComponent
from opencis.cxl.component.cxl_connection import CxlConnection
from opencis.cxl.component.switch_connection_client import SwitchConnectionClient
from opencis.cxl.component.common import CXL_COMPONENT_TYPE


@dataclass
class RootPortClientConfig:
    port_index: int
    switch_host: str
    switch_port: int


@dataclass
class RootPortClientManagerConfig:
    host_name: str
    client_configs: List[RootPortClientConfig] = field(default_factory=list)


@dataclass
class RootPortConnection:
    connection: CxlConnection
    port_index: int


class RootPortClientManager(RunnableComponent):
    def __init__(self, config: RootPortClientManagerConfig):
        super().__init__(lambda class_name: f"{config.host_name}:{class_name}:")

        self._sw_conn_clients: List[SwitchConnectionClient] = []
        for client_config in config.client_configs:
            connection_client = SwitchConnectionClient(
                client_config.port_index,
                CXL_COMPONENT_TYPE.R,
                host=client_config.switch_host,
                port=client_config.switch_port,
                parent_name=self.get_message_label(),
            )
            self._sw_conn_clients.append(connection_client)
            from opencis.util.logger import logger

            logger.info(
                self._create_message(
                    f"Initialized RootPortClient for port {client_config.port_index}"
                )
            )

    def get_cxl_connections(self) -> List[RootPortConnection]:
        connections = []
        for client in self._sw_conn_clients:
            connections.append(
                RootPortConnection(
                    connection=client.get_cxl_connection(), port_index=client.get_port_index()
                )
            )
        return connections

    def _run(self):
        from opencis.util.logger import logger

        logger.info(self._create_message("RootPortClientManager starting"))
        for client in self._sw_conn_clients:
            client.start_wait_ready()
        logger.info(self._create_message("All clients READY"))
        self._change_status_to_running()
        logger.info(self._create_message("RootPortClientManager RUNNING"))
        for client in self._sw_conn_clients:
            client.join()

    def _stop(self):
        for client in self._sw_conn_clients:
            client.stop_sync()
