"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from typing import Optional, Callable

from opencis.cxl.component.cxl_io_callback_data import CxlIoCallbackData
from opencis.pci.component.mmio_manager import MmioManager
from opencis.pci.component.config_space_manager import ConfigSpaceManager, PCI_DEVICE_TYPE
from opencis.pci.component.fifo_pair import FifoPair
from opencis.util.component import RunnableComponent


class CxlIoManager(RunnableComponent):
    def __init__(
        self,
        mmio_upstream_fifo: FifoPair,
        mmio_downstream_fifo: Optional[FifoPair],
        cfg_upstream_fifo: FifoPair,
        cfg_downstream_fifo: Optional[FifoPair],
        device_type: PCI_DEVICE_TYPE,
        init_callback: Callable[[CxlIoCallbackData], None],
        label: Optional[str] = None,
        ld_id: int = 0,
    ):
        super().__init__(label)
        self._mmio_manager = MmioManager(
            mmio_upstream_fifo,
            mmio_downstream_fifo,
            label=label,
        )
        self._config_space_manager = ConfigSpaceManager(
            cfg_upstream_fifo,
            cfg_downstream_fifo,
            device_type=device_type,
            label=label,
        )
        init_callback(CxlIoCallbackData(self._mmio_manager, self._config_space_manager, ld_id))

    def get_cfg_reg_vals(self):
        return self._config_space_manager.get_register()

    def _run(self):
        # Start subcomponents synchronously via threads
        self._mmio_manager.start_wait_ready()
        self._config_space_manager.start_wait_ready()
        self._change_status_to_running()
        # Join subcomponents until stop
        self._mmio_manager.join()
        self._config_space_manager.join()

    def _stop(self):
        self._mmio_manager.stop_sync()
        self._config_space_manager.stop_sync()
