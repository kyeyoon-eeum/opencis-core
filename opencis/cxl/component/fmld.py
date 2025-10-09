"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import threading
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
        self._loop = None
        self._t2f_thread: threading.Thread | None = None
        self._t2f_stop = threading.Event()
        self._f2t_thread: threading.Thread | None = None
        self._f2t_stop = threading.Event()

    def _process_get_ld_info_packet(self, get_ld_info_request_packet: CciRequestPacket):
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
        self.upstream_fifo.target_to_host.put(get_ld_info_response_packet)
        logger.info("Get LD Info Response sent done")

    def _process_get_ld_allocations_packet(self, request_packet: GetLdAllocationsRequestPacket):
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

        self.upstream_fifo.target_to_host.put(get_ld_allocations_response_packet)
        logger.info("Get LD Allocations Response sent done")

    def _process_set_ld_allocations_packet(self, request_packet: SetLdAllocationsRequestPacket):
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
        self.upstream_fifo.target_to_host.put(response_packet)
        logger.info("Set LD Allocations Response sent done")

    def _process_fm_to_target_worker(self) -> None:
        logger.info(self._create_message("Started processing FM-to-LD packets (thread)"))
        while not self._f2t_stop.is_set():
            packet = self.upstream_fifo.host_to_target.get()
            logger.info(self._create_message(f"FMLD received FM-to-LD packet: {packet}"))
            if packet is None:
                logger.info(self._create_message("Stopped FM-to-LD packets (thread)"))
                break
            opcode = packet.get_command_opcode()
            if opcode == CCI_FM_API_COMMAND_OPCODE.GET_LD_INFO:
                req = cast(GetLdInfoRequestPacket, packet)
                self._process_get_ld_info_packet(req)
            elif opcode == CCI_FM_API_COMMAND_OPCODE.GET_LD_ALLOCATIONS:
                req = cast(GetLdAllocationsRequestPacket, packet)
                self._process_get_ld_allocations_packet(req)
            elif opcode == CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS:
                req = cast(SetLdAllocationsRequestPacket, packet)
                self._process_set_ld_allocations_packet(req)

    # TODO: This function should be implemented for LD-to-FM API
    def _process_target_to_fm_worker(self) -> None:
        if self.downstream_fifo is None:
            logger.info(self._create_message("Skipped processing LD-to-FM packets"))
            return
        logger.info(self._create_message("Started processing LD-to-FM packets (thread)"))
        while not self._t2f_stop.is_set():
            packet = self.downstream_fifo.target_to_host.get()
            if packet is None:
                logger.info(self._create_message("Stopped LD-to-FM packets (thread)"))
                break
            logger.info(self._create_message("Received LD-to-FM Packet"))
            self.upstream_fifo.target_to_host.put(packet)

    # workers now implemented above

    # workers now implemented above

    def _run(self):
        # Start thread to forward LD->FM responses if downstream is present
        if self.downstream_fifo is not None:
            self._t2f_stop.clear()
            self._t2f_thread = threading.Thread(
                target=self._process_target_to_fm_worker,
                name=f"{self.__class__.__name__}-t2f",
                daemon=True,
            )
            self._t2f_thread.start()
        # Start FM->LD worker thread
        self._f2t_stop.clear()
        self._f2t_thread = threading.Thread(
            target=self._process_fm_to_target_worker,
            name=f"{self.__class__.__name__}-f2t",
            daemon=True,
        )
        self._f2t_thread.start()
        self._change_status_to_running()
        self._running_event.wait()

    def _stop(self):
        logger.info(self._create_message("Stopping FMLD"))
        # Stop FM->LD worker
        self._f2t_stop.set()
        try:
            self.upstream_fifo.host_to_target.put(None)
        except Exception:
            pass
        if self._f2t_thread is not None:
            self._f2t_thread.join(timeout=1.0)
        if self.downstream_fifo is not None:
            self._t2f_stop.set()
            try:
                self.downstream_fifo.target_to_host.put(None)
            except Exception:
                pass
            if self._t2f_thread is not None:
                self._t2f_thread.join(timeout=1.0)
        self._running_event.set()
