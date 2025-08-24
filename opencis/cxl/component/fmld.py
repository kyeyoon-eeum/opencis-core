"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from asyncio import create_task, gather
import asyncio
import threading
from typing import Optional, cast
from opencis.cxl.cci.common import CCI_FM_API_COMMAND_OPCODE
from opencis.util.component import RunnableComponent
from opencis.util.logger import logger
from opencis.pci.component.fifo_pair import FifoPair
from opencis.cxl.device.cxl_type3_device import CXL_T3_DEV_TYPE
from opencis.cxl.transport.cci_packets import (
    CciRequestPacket,
    GetLdInfoRequestPacket,
    GetLdInfoResponsePacket,
    GetLdAllocationsRequestPacket,
    GetLdAllocationsResponsePacket,
    SetLdAllocationsRequestPacket,
    SetLdAllocationsResponsePacket,
)


class FMLD(RunnableComponent):
    def __init__(
        self,
        upstream_fifo: FifoPair,
        ld_count: int,
        dev_type: CXL_T3_DEV_TYPE,
        # TODO: to-LD fifo should be implemented during FM-API implementation
        downstream_fifo: Optional[FifoPair] = None,
        label: Optional[str] = None,
    ):
        super().__init__(label)
        self.downstream_fifo = downstream_fifo
        self.upstream_fifo = upstream_fifo
        self._ld_count = ld_count
        self._dev_type = dev_type
        self._memory_granularity = 256

        # Key: LD ID, value: remaining number of memory block(s)
        # e.g., {0:1, 1:3, 2:2}
        # ld_id of 0 has 256M of memory
        # ld_id of 1 has 768M of memory
        # ld_id of 2 has 512M of memory
        self._ld_allocations = {i: 1 for i in range(ld_count)}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._t2f_thread: threading.Thread | None = None
        self._t2f_stop = threading.Event()
        self._f2t_thread: threading.Thread | None = None
        self._f2t_stop = threading.Event()

    async def _process_get_ld_info_packet(self, get_ld_info_request_packet: CciRequestPacket):
        if get_ld_info_request_packet.get_command_opcode() != CCI_FM_API_COMMAND_OPCODE.GET_LD_INFO:
            raise Exception("Invalid command opcode")
        logger.info(f"Get LD Info Request: {get_ld_info_request_packet}")
        memory_size = self._ld_count * 1024 * 1024 * 256
        logger.info(f"Memory Size: {memory_size:x}")
        logger.info(f"LD Count: {self._ld_count}")
        get_ld_info_response_packet = GetLdInfoResponsePacket.create(
            memory_size=memory_size,
            ld_count=self._ld_count,
            message_tag=get_ld_info_request_packet.cci_msg_header.message_tag,
        )
        await self.upstream_fifo.target_to_host.put(get_ld_info_response_packet)
        logger.info("Get LD Info Response sent done")

    async def _process_get_ld_allocations_packet(
        self, request_packet: GetLdAllocationsRequestPacket
    ):
        if request_packet.get_command_opcode() != CCI_FM_API_COMMAND_OPCODE.GET_LD_ALLOCATIONS:
            raise Exception("Invalid command opcode")
        logger.info(f"FMLD Get LD Allocations: {bytes(request_packet)}")
        start_ld_id = request_packet.payload.start_ld_id
        ld_alloc_list_limit = request_packet.payload.ld_allocation_list_limit

        if start_ld_id < 0 or start_ld_id >= len(self._ld_allocations):
            raise Exception("Invalid start_ld_id")

        # Number of keys for self._ld_allocations
        max_len_ld_list = len(self._ld_allocations) - start_ld_id
        if ld_alloc_list_limit < max_len_ld_list:
            ld_length = ld_alloc_list_limit
        else:
            ld_length = max_len_ld_list

        # Calculate number of lds
        number_of_lds = 0
        for i in range(max_len_ld_list):
            if self._ld_allocations.get(start_ld_id + i) == 1:
                number_of_lds += 1

        get_ld_allocations_response_packet = GetLdAllocationsResponsePacket.create(
            number_of_lds=number_of_lds,
            memory_granularity=0,
            start_ld_id=start_ld_id,
            ld_length=ld_length,
            ld_allocations=self._ld_allocations,
            message_tag=request_packet.cci_msg_header.message_tag,
        )

        await self.upstream_fifo.target_to_host.put(get_ld_allocations_response_packet)
        logger.info("Get LD Allocations Response sent done")

    async def _process_set_ld_allocations_packet(
        self, request_packet: SetLdAllocationsRequestPacket
    ):
        if request_packet.get_command_opcode() != CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS:
            raise Exception("Invalid command opcode")
        logger.info(f"Set LD Allocations: {request_packet}")

        LD_ALLOCATIONS_SIZE = 16
        number_of_lds = request_packet.payload.number_of_lds
        start_ld_id = request_packet.payload.start_ld_id
        ld_allocation_list_bytes = request_packet.payload.ld_allocation_list

        # Update LD allocations
        number_of_lds = min(number_of_lds, len(self._ld_allocations) - start_ld_id)
        for i in range(number_of_lds):
            ld_id = start_ld_id + i
            multiplier = ld_allocation_list_bytes[i * LD_ALLOCATIONS_SIZE]
            self._ld_allocations[ld_id] = multiplier

        response_packet = SetLdAllocationsResponsePacket.create(
            number_of_lds=number_of_lds,
            start_ld_id=start_ld_id,
            ld_allocations=self._ld_allocations,
            message_tag=request_packet.cci_msg_header.message_tag,
        )
        await self.upstream_fifo.target_to_host.put(response_packet)
        logger.info("Set LD Allocations Response sent done")

    async def _process_fm_to_target(self):
        # Async fallback (unused in thread mode). Kept for compatibility.
        logger.info(self._create_message("Started processing FM-to-LD packets (async)"))
        while True:
            packet = await self.upstream_fifo.host_to_target.get()
            logger.info(self._create_message(f"FMLD received FM-to-LD packet: {packet}"))
            if packet is None:
                logger.info(
                    self._create_message("None packet received, stopping FM-to-LD packets (async)")
                )
                break

            if packet.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.GET_LD_INFO:
                packet = cast(GetLdInfoRequestPacket, packet)
                await self._process_get_ld_info_packet(packet)
            elif packet.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.GET_LD_ALLOCATIONS:
                packet = cast(GetLdAllocationsRequestPacket, packet)
                await self._process_get_ld_allocations_packet(packet)
            elif packet.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS:
                packet = cast(SetLdAllocationsRequestPacket, packet)
                await self._process_set_ld_allocations_packet(packet)
        logger.info(self._create_message("Stopped processing FM-to-LD packets (async)"))

    # TODO: This function should be implemented for LD-to-FM API
    async def _process_target_to_fm(self):
        # Fallback async path if thread not used
        if self.downstream_fifo is None:
            logger.info(self._create_message("Skipped processing LD-to-FM packets"))
            return
        logger.info(self._create_message("Started processing LD-to-FM packets (async)"))
        while True:
            packet = await self.downstream_fifo.target_to_host.get()
            if packet is None:
                logger.info(self._create_message("Stopped LD-to-FM packets (async)"))
                break
            logger.info(self._create_message("Received LD-to-FM Packet"))
            await self.upstream_fifo.target_to_host.put(packet)

    def _t2f_worker(self) -> None:
        if self.downstream_fifo is None:
            return
        assert self._loop is not None
        logger.info(self._create_message("Started LD-to-FM worker (thread)"))
        while not self._t2f_stop.is_set():
            packet = asyncio.run_coroutine_threadsafe(
                self.downstream_fifo.target_to_host.get(), self._loop
            ).result()
            if packet is None:
                break
            asyncio.run_coroutine_threadsafe(
                self.upstream_fifo.target_to_host.put(packet), self._loop
            ).result()
        logger.info(self._create_message("Stopped LD-to-FM worker (thread)"))

    def _f2t_worker(self) -> None:
        assert self._loop is not None
        logger.info(self._create_message("Started FM-to-LD worker (thread)"))
        while not self._f2t_stop.is_set():
            packet = asyncio.run_coroutine_threadsafe(
                self.upstream_fifo.host_to_target.get(), self._loop
            ).result()
            logger.info(self._create_message(f"FMLD received FM-to-LD packet: {packet}"))
            if packet is None:
                break
            opcode = packet.get_command_opcode()
            if opcode == CCI_FM_API_COMMAND_OPCODE.GET_LD_INFO:
                req = cast(GetLdInfoRequestPacket, packet)
                asyncio.run_coroutine_threadsafe(
                    self._process_get_ld_info_packet(req), self._loop
                ).result()
            elif opcode == CCI_FM_API_COMMAND_OPCODE.GET_LD_ALLOCATIONS:
                req = cast(GetLdAllocationsRequestPacket, packet)
                asyncio.run_coroutine_threadsafe(
                    self._process_get_ld_allocations_packet(req), self._loop
                ).result()
            elif opcode == CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS:
                req = cast(SetLdAllocationsRequestPacket, packet)
                asyncio.run_coroutine_threadsafe(
                    self._process_set_ld_allocations_packet(req), self._loop
                ).result()
        logger.info(self._create_message("Stopped FM-to-LD worker (thread)"))

    async def _run(self):
        self._loop = asyncio.get_running_loop()
        # Start thread to forward LD->FM responses if downstream is present
        if self.downstream_fifo is not None:
            self._t2f_stop.clear()
            self._t2f_thread = threading.Thread(
                target=self._t2f_worker, name=f"{self.__class__.__name__}-t2f", daemon=True
            )
            self._t2f_thread.start()
        # Start FM->LD worker thread
        self._f2t_stop.clear()
        self._f2t_thread = threading.Thread(
            target=self._f2t_worker, name=f"{self.__class__.__name__}-f2t", daemon=True
        )
        self._f2t_thread.start()
        tasks = []
        await self._change_status_to_running()
        await gather(*tasks)

    async def _stop(self):
        logger.info(self._create_message("Stopping FMLD"))
        # Stop FM->LD worker
        self._f2t_stop.set()
        await self.upstream_fifo.host_to_target.put(None)
        if self._f2t_thread is not None:
            self._f2t_thread.join(timeout=1.0)
        if self.downstream_fifo is not None:
            self._t2f_stop.set()
            await self.downstream_fifo.target_to_host.put(None)
            if self._t2f_thread is not None:
                self._t2f_thread.join(timeout=1.0)
