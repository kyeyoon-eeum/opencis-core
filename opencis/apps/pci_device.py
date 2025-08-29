"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import threading
from opencis.util.component import RunnableComponent
from opencis.pci.device.pci_device import PciDevice as PciDeviceInternal
from opencis.pci.component.pci import (
    PciComponentIdentity,
    EEUM_VID,
    SW_EP_DID,
    PCI_CLASS,
    PCI_SYSTEM_PERIPHERAL_SUBCLASS,
)
from opencis.cxl.component.switch_connection_client import SwitchConnectionClient
from opencis.cxl.component.common import CXL_COMPONENT_TYPE


class PciDevice(RunnableComponent):
    def __init__(
        self,
        port_index: int,
        bar_size: int,
        host: str = "0.0.0.0",
        port: int = 8000,
    ):
        label = f"Port{port_index}"
        super().__init__(label)
        self._sw_conn_client = SwitchConnectionClient(
            port_index,
            CXL_COMPONENT_TYPE.P,
            host=host,
            port=port,
            parent_name=f"PciDevice{port_index}",
        )
        self._pci_device = PciDeviceInternal(
            transport_connection=self._sw_conn_client.get_cxl_connection(),
            identity=PciComponentIdentity(
                EEUM_VID,
                SW_EP_DID,
                PCI_CLASS.SYSTEM_PERIPHERAL,
                PCI_SYSTEM_PERIPHERAL_SUBCLASS.OTHER,
            ),
            bar_size=bar_size,
            label=f"PCIDevice{port_index}",
        )

    def _run(self):
        self._sw_conn_client.start_wait_ready()
        self._pci_device.start_wait_ready()
        self._change_status_to_running()
        self._sw_conn_client.join()
        self._pci_device.join()

    def _stop(self):
        self._sw_conn_client.stop_sync()
        self._pci_device.stop_sync()
