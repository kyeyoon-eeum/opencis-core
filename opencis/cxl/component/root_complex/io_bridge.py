"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from dataclasses import dataclass
import asyncio
import threading
from typing import cast
from opencis.util.async_queue import AsyncQueue as Queue
from asyncio import create_task, gather, timeout, exceptions
from opencis.util.component import RunnableComponent
from opencis.pci.component.fifo_pair import FifoPair
from opencis.cxl.transport.memory_fifo import MemoryFifoPair
from opencis.util.logger import logger
from opencis.util.pci import (
    extract_bus_from_bdf,
    extract_device_from_bdf,
    bdf_to_string,
)
from opencis.cxl.transport.cxl_io_packets import (
    CxlIoCfgRdPacket,
    CxlIoCfgWrPacket,
    CxlIoCompletionPacket,
    CxlIoMemRdPacket,
    CxlIoMemWrPacket,
    is_cxl_io_completion_status_sc,
)


@dataclass
class IoBridgeConfig:
    root_bus: int
    cxl_io_cfg_fifos: FifoPair
    cxl_io_mmio_fifos: FifoPair
    memory_producer_fifos: MemoryFifoPair
    host_name: str


class IoBridge(RunnableComponent):
    def __init__(self, config: IoBridgeConfig):
        super().__init__(lambda class_name: f"{config.host_name}:{class_name}")
        self._root_bus = config.root_bus
        self._cxl_io_cfg_fifos = config.cxl_io_cfg_fifos
        self._cxl_io_mmio_fifos = config.cxl_io_mmio_fifos
        self._memory_producer_fifos = config.memory_producer_fifos
        self._next_tag = 0

        self._internal_io_fifo = Queue()
        self._internal_cfg_fifo = Queue()
        self._loop = None
        self._mmio_thread = None
        self._cfg_thread = None
        self._stop_evt = threading.Event()

    # pylint: disable=unused-argument
    async def _get_mmio_response(self, tag: int):
        # TODO: get packet based on tag
        packet = await self._internal_io_fifo.get()

        assert is_cxl_io_completion_status_sc(packet)
        return packet

    def _get_secondary_bus(self) -> int:
        return self._root_bus + 1

    # pylint: disable=duplicate-code

    async def write_config(self, bdf: int, offset: int, size: int, value: int):
        # TODO: Move pass-through handling to Root Port Switch
        bus = extract_bus_from_bdf(bdf)
        if self._root_bus == bus:
            raise Exception("Accessing Root Port isn't supported under pass-through mode")

        # TODO: Set CfgRd/CfgWr type from RootPortSwitch
        bdf_string = bdf_to_string(bdf)
        is_type0 = bus == self._get_secondary_bus()
        if is_type0:
            # NOTE: For non-ARI component, only allow device 0
            device_num = extract_device_from_bdf(bdf)
            if device_num != 0:
                return

        packet = CxlIoCfgWrPacket.create(
            bdf, offset, size, value, is_type0, req_id=0, tag=self._next_tag
        )
        self._next_tag = (self._next_tag + 1) % 256

        await self._cxl_io_cfg_fifos.host_to_target.put(packet)
        logger.debug(self._create_message("Enqueued CFG WR to host_to_target"))

        # Wait for completion from internal cfg fifo (fed by thread)
        try:
            async with timeout(10):
                packet = await self._internal_cfg_fifo.get()
        except exceptions.TimeoutError:
            logger.error(self._create_message("CXL.io cfg WR: Timed-out"))
            return
        logger.debug(self._create_message("Dequeued CFG completion from target_to_host"))

        tpl_type_str = "CFG WR0" if is_type0 else "CFG WR1"

        if not is_cxl_io_completion_status_sc(packet):
            cpl_packet = cast(CxlIoCompletionPacket, packet)
            logger.debug(
                self._create_message(
                    f"[{bdf_string}] {tpl_type_str} @ 0x{offset:x}[{size}B] : "
                    + f"Unsuccessful, Status: 0x{cpl_packet.cpl_header.status:x}"
                )
            )
            return

        logger.debug(
            self._create_message(
                f"[{bdf_string}] {tpl_type_str} @ 0x{offset:x}[{size}B] : 0x{value:x}"
            )
        )

    async def read_config(self, bdf: int, offset: int, size: int) -> int:
        logger.debug(self._create_message("Reading config from IO Bridge"))
        if offset + size > ((offset // 4) + 1) * 4:
            raise Exception("offset + size out of DWORD boundary")

        bit_mask = (1 << size * 8) - 1

        bus = extract_bus_from_bdf(bdf)
        # TODO: Move pass-through handling to Root Port Switch
        if self._root_bus == bus:
            raise Exception("Accessing Root Port isn't supported under pass-through mode")

        bdf_string = bdf_to_string(bdf)
        # TODO: Set CfgRd/CfgWr type from RootPortSwitch
        is_type0 = bus == self._get_secondary_bus()
        if is_type0:
            # NOTE: For non-ARI component, only allow device 0
            device_num = extract_device_from_bdf(bdf)
            if device_num != 0:
                return 0xFFFFFFFF & bit_mask

        packet = CxlIoCfgRdPacket.create(bdf, offset, size, is_type0, req_id=0, tag=self._next_tag)
        self._next_tag = (self._next_tag + 1) % 256
        await self._cxl_io_cfg_fifos.host_to_target.put(packet)
        logger.debug(self._create_message("Enqueued CFG RD to host_to_target"))

        # Wait for completion from internal cfg fifo (fed by thread)
        logger.debug(self._create_message("Putting Read Config packet to FIFO"))
        try:
            async with timeout(10):
                packet = await self._internal_cfg_fifo.get()
        except exceptions.TimeoutError:
            logger.error(self._create_message("CXL.io cfg RD: Timed-out"))
            return None
        logger.debug(self._create_message("Dequeued CFG completion from target_to_host"))

        bit_offset = (offset % 4) * 8

        tpl_type_str = "CFG RD0" if is_type0 else "CFG RD1"
        if not is_cxl_io_completion_status_sc(packet):
            cpl_packet = cast(CxlIoCompletionPacket, packet)
            logger.debug(
                self._create_message(
                    f"[{bdf_string}] {tpl_type_str} @ 0x{offset:x}[{size}B] : "
                    + f"Unsuccessful, Status: 0x{cpl_packet.cpl_header.status:x}"
                )
            )
            return 0xFFFFFFFF & bit_mask

        cpld_packet = cast(CxlIoCompletionPacket, packet)
        data = (cpld_packet.get_data_as_int() >> bit_offset) & bit_mask

        logger.debug(
            self._create_message(
                f"[{bdf_string}] {tpl_type_str} @ 0x{offset:x}[{size}B] : 0x{data:x}"
            )
        )
        return data

    async def write_mmio(self, address: int, size: int, value: int):
        message = self._create_message(f"MMIO: Writing 0x{value:08x} to 0x{address:08x}")
        logger.debug(message)
        packet = CxlIoMemWrPacket.create(address, size, value)
        await self._cxl_io_mmio_fifos.host_to_target.put(packet)
        logger.debug(self._create_message("Enqueued MMIO WR to host_to_target"))

    async def read_mmio(self, address: int, size: int) -> int:
        message = self._create_message(f"MMIO: Reading data from 0x{address:08x}")
        logger.debug(message)
        packet = CxlIoMemRdPacket.create(address, size)
        await self._cxl_io_mmio_fifos.host_to_target.put(packet)
        logger.debug(self._create_message("Enqueued MMIO RD to host_to_target"))

        try:
            async with timeout(1):
                packet = await self._get_mmio_response(packet.mreq_header.tag)

        except exceptions.TimeoutError:
            logger.error(self._create_message("CXL.io mmio RD: Timed-out"))
            return None

        cpld_packet = cast(CxlIoCompletionPacket, packet)
        logger.debug(self._create_message("Received MMIO completion"))
        return cpld_packet.get_data_as_int()

    # pylint: enable=duplicate-code

    async def process_target_to_host_mmio_packets(self):
        # Async fallback if thread not used
        while True:
            packet = await self._cxl_io_mmio_fifos.target_to_host.get()
            if packet is None:
                logger.debug(self._create_message("Stopped processing target to host MMIO packets"))
                break
            await self._internal_io_fifo.put(packet)

    async def _run(self):
        self._loop = asyncio.get_running_loop()
        # Start thread to forward MMIO completions into internal FIFO
        self._stop_evt.clear()
        self._mmio_thread = threading.Thread(
            target=self._mmio_resp_worker, name=f"{self.get_message_label()}-mmio", daemon=True
        )
        self._mmio_thread.start()
        # Start thread to forward CFG completions into internal FIFO
        self._cfg_thread = threading.Thread(
            target=self._cfg_resp_worker, name=f"{self.get_message_label()}-cfg", daemon=True
        )
        self._cfg_thread.start()
        await self._change_status_to_running()
        # Keep alive until stop
        stopper = asyncio.Event()
        try:
            while not self._stop_evt.is_set():
                try:
                    await asyncio.wait_for(stopper.wait(), timeout=0.1)
                except asyncio.TimeoutError:
                    pass
        finally:
            pass

    async def _stop(self):
        self._stop_evt.set()
        try:
            await self._cxl_io_mmio_fifos.host_to_target.put(None)
            await self._cxl_io_mmio_fifos.target_to_host.put(None)
            await self._cxl_io_cfg_fifos.host_to_target.put(None)
            await self._cxl_io_cfg_fifos.target_to_host.put(None)
        except Exception:
            pass
        try:
            if self._mmio_thread is not None:
                self._mmio_thread.join(timeout=1.0)
            if self._cfg_thread is not None:
                self._cfg_thread.join(timeout=1.0)
        except Exception:
            pass

    def _mmio_resp_worker(self) -> None:
        assert self._loop is not None
        while not self._stop_evt.is_set():
            try:
                packet = asyncio.run_coroutine_threadsafe(
                    self._cxl_io_mmio_fifos.target_to_host.get(), self._loop
                ).result()
            except Exception:
                break
            if packet is None:
                break
            try:
                asyncio.run_coroutine_threadsafe(
                    self._internal_io_fifo.put(packet), self._loop
                ).result()
            except Exception:
                break

    def _cfg_resp_worker(self) -> None:
        assert self._loop is not None
        while not self._stop_evt.is_set():
            try:
                packet = asyncio.run_coroutine_threadsafe(
                    self._cxl_io_cfg_fifos.target_to_host.get(), self._loop
                ).result()
            except Exception:
                break
            if packet is None:
                break
            try:
                asyncio.run_coroutine_threadsafe(
                    self._internal_cfg_fifo.put(packet), self._loop
                ).result()
            except Exception:
                break
