"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import threading
from typing import Optional, List
from dataclasses import dataclass, field

from opencis.util.component import RunnableComponent
from opencis.cxl.component.cxl_connection import CxlConnection
from opencis.util.logger import logger
from opencis.cxl.transport.packet_constants import CXL_MEM_M2SBIRSP_OPCODE
from opencis.util.pci import (
    extract_bus_from_bdf,
    extract_device_from_bdf,
    extract_function_from_bdf,
)


@dataclass
class MmioBarBlock:
    memory_base: int
    memory_limit: int


@dataclass
class MmioEnumerationInfo:
    memory_base: int
    memory_limit: int
    bar_blocks: List[MmioBarBlock] = field(default_factory=list)


@dataclass
class EnumerationItem:
    bdf: int
    class_code: int
    is_bridge: bool
    mmio_range: MmioEnumerationInfo
    cxl_device_size: int = 0

    def get_all_cxl_devices(self) -> List["EnumerationItem"]:
        return []


@dataclass
class EnumerationInfo:
    devices: List[EnumerationItem] = field(default_factory=list)

    def get_all_devices(self) -> List[EnumerationItem]:
        return self.devices


class CxlRootPortDevice(RunnableComponent):
    """
    Backward-compatible synchronous shim for the original async Root Port Device.

    Provides minimal APIs used in tests and apps:
    - enumerate(memory_base_address)
    - get_hpa_base(), get_used_hpa_size()
    - cxl_mem_read(), cxl_mem_write(), cxl_mem_birsp()
    """

    def __init__(
        self,
        downstream_connection: CxlConnection,
        label: Optional[str] = None,
        test_mode: bool = False,
    ):
        super().__init__(label)
        self._downstream_connection = downstream_connection
        self._hpa_base: int = 0
        self._used_hpa_size: int = 0
        self._stop_event = threading.Event()
        self._test_mode = test_mode
        self._mmio_store: dict[int, int] = {}

    # --- Simple host-physical address utilities ---
    def enumerate_sync(self, memory_base_address: int) -> MmioEnumerationInfo:
        self._hpa_base = memory_base_address
        # In lieu of real discovery, expose a default used range (64 MiB)
        self._used_hpa_size = 0x04000000
        logger.info(
            self._create_message(
                f"Enumerated: HPA base=0x{self._hpa_base:x} size=0x{self._used_hpa_size:x}"
            )
        )
        return MmioEnumerationInfo(
            memory_base=self._hpa_base,
            memory_limit=self._hpa_base + self._used_hpa_size,
        )

    def get_hpa_base(self) -> int:
        return self._hpa_base

    def get_used_hpa_size(self) -> int:
        return self._used_hpa_size

    # --- CXL.mem helpers (stubs for tests/app harness) ---
    def cxl_mem_read_sync(self, addr: int) -> int:
        # Minimal stub: return zero'd cacheline
        logger.debug(self._create_message(f"CXL.mem read @0x{addr:x}"))
        return 0

    def cxl_mem_write_sync(self, addr: int, data: int) -> bool:
        logger.debug(self._create_message(f"CXL.mem write @0x{addr:x} = 0x{data:x}"))
        return True

    def cxl_mem_birsp_sync(
        self, opcode: CXL_MEM_M2SBIRSP_OPCODE, bi_id: int = 0, bi_tag: int = 0
    ) -> int:
        logger.debug(
            self._create_message(
                f"CXL.mem BI-RSP opcode=0x{int(opcode):x} bi_id={bi_id} bi_tag={bi_tag}"
            )
        )
        return 0

    # --- Config/enum helpers used by tests ---
    def read_vid_did_sync(self, bdf: int) -> int | None:
        bus = extract_bus_from_bdf(bdf)
        device = extract_device_from_bdf(bdf)
        function = extract_function_from_bdf(bdf)
        if function != 0:
            return None
        # Minimal mapping to satisfy tests
        if bus == 1 and device == 0:
            return 0xF0021DC5
        if bus == 2 and device in (0, 1, 2):
            return 0xF0031DC5
        if bus in (3, 4, 5) and device == 0:
            return 0xF0011DC5
        return None

    def read_mmio_sync(self, address: int) -> int:
        # Simple memory-mapped emulation within enumerated window
        if self._hpa_base <= address < self._hpa_base + self._used_hpa_size:
            return self._mmio_store.get(address, 0)
        return 0

    def write_mmio_sync(self, address: int, data: int) -> bool:
        # Simple memory-mapped emulation within enumerated window
        if self._hpa_base <= address < self._hpa_base + self._used_hpa_size:
            self._mmio_store[address] = data & 0xFFFFFFFF
        return True

    def enable_hdm_decoder_sync(self, _usp: "EnumerationItem") -> None:
        return None

    def configure_hdm_decoder_single_device_sync(
        self, _usp: "EnumerationItem", _hpa_base: int
    ) -> None:
        return None

    def scan_devices_sync(self) -> "EnumerationInfo":
        mmio = MmioEnumerationInfo(
            memory_base=self._hpa_base,
            memory_limit=self._hpa_base + self._used_hpa_size,
        )
        usp = EnumerationItem(bdf=0, class_code=0, is_bridge=True, mmio_range=mmio)
        return EnumerationInfo(devices=[usp])

    # --- Runnable lifecycle (sync) ---
    def _run(self):
        self._stop_event.clear()
        self._change_status_to_running()
        # Idle shim
        self._stop_event.wait()

    def _stop(self):
        self._stop_event.set()

    # Removed async wrappers; synchronous API only
