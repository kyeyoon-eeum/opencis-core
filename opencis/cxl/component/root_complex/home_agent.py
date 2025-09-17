"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from dataclasses import dataclass
import threading
from typing import cast
import time

from opencis.util.logger import logger
from opencis.util.component import RunnableComponent
from opencis.pci.component.fifo_pair import FifoPair
from opencis.cxl.transport.memory_fifo import (
    MemoryFifoPair,
    MemoryRequest,
    MEMORY_REQUEST_TYPE,
    MemoryResponse,
    MEMORY_RESPONSE_STATUS,
)
from opencis.cxl.transport.cache_fifo import (
    CacheFifoPair,
    CacheRequest,
    CACHE_REQUEST_TYPE,
    CacheResponse,
    CACHE_RESPONSE_STATUS,
)
from opencis.cxl.transport.common import BasePacket
from opencis.cxl.transport.cxl_mem_packets import (
    CxlMemBasePacket,
    CxlMemMemDataPacket,
    CxlMemMemRdPacket,
    CxlMemMemWrPacket,
    CxlMemBIRspPacket,
    CxlMemS2MNDRPacket,
    CxlMemS2MDRSPacket,
    CxlMemS2MBISnpPacket,
    CXL_MEM_M2SREQ_OPCODE,
    CXL_MEM_M2SRWD_OPCODE,
    CXL_MEM_M2SBIRSP_OPCODE,
    CXL_MEM_S2MNDR_OPCODE,
    CXL_MEM_S2MDRS_OPCODE,
    CXL_MEM_S2MBISNP_OPCODE,
    CXL_MEM_META_FIELD,
    CXL_MEM_META_VALUE,
    CXL_MEM_M2S_SNP_TYPE,
    is_cxl_mem_data,
)
from opencis.cxl.component.cache_controller import (
    CohStateMachine,
    COH_STATE_MACHINE,
)





@dataclass
class HomeAgentConfig:
    host_name: str
    memory_consumer_io_fifos: MemoryFifoPair
    memory_consumer_coh_fifos: MemoryFifoPair
    memory_producer_fifos: MemoryFifoPair
    upstream_cache_to_home_agent_fifo: CacheFifoPair
    upstream_home_agent_to_cache_fifo: CacheFifoPair
    downstream_cxl_mem_fifos: FifoPair


class HomeAgent(RunnableComponent):
    def __init__(self, config: HomeAgentConfig):
        super().__init__(lambda class_name: f"{config.host_name}:{class_name}")

        self._memory_consumer_io_fifos = config.memory_consumer_io_fifos
        self._memory_consumer_coh_fifos = config.memory_consumer_coh_fifos
        self._memory_producer_fifos = config.memory_producer_fifos
        self._upstream_cache_to_home_agent_fifos = config.upstream_cache_to_home_agent_fifo
        self._upstream_home_agent_to_cache_fifos = config.upstream_home_agent_to_cache_fifo
        self._downstream_cxl_mem_fifos = config.downstream_cxl_mem_fifos

        self._cur_state = CohStateMachine(
            state=COH_STATE_MACHINE.COH_STATE_INIT,
            packet=None,
            cache_rsp=CXL_MEM_M2SBIRSP_OPCODE.BIRSP_I,
            cache_list=[],
            birsp_sched=False,
            pending_ndr_status=None,
            waiting_for_drs=False,
        )

        self._loop = None
        self._state_lock = threading.Lock()
        self._flow_control_cv = threading.Condition(self._state_lock)
        self._fc_host_run = False
        self._state_version: int = 0
        self._downstream_worker_thread: threading.Thread = None
        self._upstream_worker_thread: threading.Thread = None

        # Instrumentation metrics
        self._metrics = {
            "ds_reenqueue": 0,
            "ds_wait_ns": 0,
            "us_reenqueue": 0,
            "us_wait_ns": 0,
            "ndr_count": 0,
            "drs_count": 0,
            "bisnp_count": 0,
        }

        # Single pending buffers to avoid re-enqueue churn
        self._pending_downstream = None
        self._pending_upstream = None

    def _create_m2s_req_packet(
        self,
        opcode: CXL_MEM_M2SREQ_OPCODE,
        meta_field: CXL_MEM_META_FIELD,
        meta_value: CXL_MEM_META_VALUE,
        snp_type: CXL_MEM_M2S_SNP_TYPE,
        addr: int,
    ) -> CxlMemMemRdPacket:
        return CxlMemMemRdPacket.create(addr, opcode, meta_field, meta_value, snp_type)

    def _create_m2s_rwd_packet(
        self,
        opcode: CXL_MEM_M2SRWD_OPCODE,
        meta_field: CXL_MEM_META_FIELD,
        meta_value: CXL_MEM_META_VALUE,
        snp_type: CXL_MEM_M2S_SNP_TYPE,
        addr: int,
        data: int,
    ) -> CxlMemMemWrPacket:
        return CxlMemMemWrPacket.create(addr, data, opcode, meta_field, meta_value, snp_type)

    def _write_memory(self, addr: int, size: int, value: int):
        packet = MemoryRequest(MEMORY_REQUEST_TYPE.WRITE, addr, size, value)
        self._memory_producer_fifos.request.put(packet)
        packet = self._memory_producer_fifos.response.get()

        assert packet.status == MEMORY_RESPONSE_STATUS.OK

    def _read_memory(self, addr: int, size: int) -> int:
        packet = MemoryRequest(MEMORY_REQUEST_TYPE.READ, addr, size)
        self._memory_producer_fifos.request.put(packet)
        packet = self._memory_producer_fifos.response.get()

        return packet.data

    def write_cxl_mem(self, addr: int, size: int, value: int):
        if addr % 64 != 0 or size % 64 != 0:
            raise Exception("Size and address must be aligned to 64!")

        chunk_count = 0
        while size > 0:
            message = self._create_message(f"CXL.mem: Writing 0x{value:08x} to 0x{addr:08x}")
            logger.debug(message)
            low_64_byte = value & ((1 << (64 * 8)) - 1)
            packet = CxlMemMemWrPacket.create(addr + (chunk_count * 64), low_64_byte)
            self._downstream_cxl_mem_fifos.host_to_target.put(packet)
            packet = self._downstream_cxl_mem_fifos.target_to_host.get()
            size -= 64
            chunk_count += 1
            value >>= 64 * 8

    def read_cxl_mem(self, addr: int, size: int) -> int:
        if addr % 64 or size % 64:
            raise Exception("Size and address must be aligned to 64!")

        result = 0
        while size > 0:
            message = self._create_message(f"CXL.mem: Reading data from 0x{addr:08x}")
            logger.debug(message)
            packet = CxlMemMemRdPacket.create(addr + (size - 64))
            self._downstream_cxl_mem_fifos.host_to_target.put(packet)
            packet = self._downstream_cxl_mem_fifos.target_to_host.get()
            assert is_cxl_mem_data(packet)
            mem_data_packet = cast(CxlMemMemDataPacket, packet)
            size -= 64
            result |= mem_data_packet.data
            result <<= 64 * 8

        return result

    def _process_memory_io_bridge_requests(self):
        while True:
            packet = self._memory_consumer_io_fifos.request.get()
            if packet is None:
                logger.debug(
                    self._create_message("Stopped processing memory access requests from IO Bridge")
                )
                break
            if packet.type == MEMORY_REQUEST_TYPE.WRITE:
                logger.info(
                    self._create_message(f"MC WRITE req addr=0x{packet.addr:x} size={packet.size}")
                )
                self._write_memory(packet.addr, packet.size, packet.data)
                logger.info(self._create_message("MC WRITE rsp OK"))
            elif packet.type == MEMORY_REQUEST_TYPE.READ:
                logger.info(
                    self._create_message(f"MC READ req addr=0x{packet.addr:x} size={packet.size}")
                )
                data = self._read_memory(packet.addr, packet.size)
                response = MemoryResponse(MEMORY_RESPONSE_STATUS.OK, data)
                self._memory_consumer_io_fifos.response.put(response)

    def _process_memory_coh_bridge_requests(self):
        while True:
            packet = self._memory_consumer_coh_fifos.request.get()
            if packet is None:
                logger.debug(
                    self._create_message(
                        "Stopped processing memory access requests from Cache Coherency Bridge"
                    )
                )
                break
            if packet.type == MEMORY_REQUEST_TYPE.WRITE:
                self._write_memory(packet.addr, packet.size, packet.data)
            elif packet.type == MEMORY_REQUEST_TYPE.READ:
                data = self._read_memory(packet.addr, packet.size)
                response = MemoryResponse(MEMORY_RESPONSE_STATUS.OK, data)
                self._memory_consumer_coh_fifos.response.put(response)

    # .mem s2m rsp handler
    def _process_cxl_s2m_rsp_packet(self, s2mndr_packet: CxlMemS2MNDRPacket):
        logger.debug(self._create_message("[HA-DS] Processing S2M NDR in HA"))
        self._metrics["ndr_count"] += 1
        if s2mndr_packet.s2mndr_header.opcode == CXL_MEM_S2MNDR_OPCODE.CMP_S:
            status = CACHE_RESPONSE_STATUS.RSP_S
        elif s2mndr_packet.s2mndr_header.opcode == CXL_MEM_S2MNDR_OPCODE.CMP_E:
            status = CACHE_RESPONSE_STATUS.RSP_I
        elif s2mndr_packet.s2mndr_header.opcode == CXL_MEM_S2MNDR_OPCODE.CMP_M:
            pass
        else:
            if self._cur_state.birsp_sched:
                bi_id = self._cur_state.packet.s2mbisnp_header.bi_id
                bi_tag = self._cur_state.packet.s2mbisnp_header.bi_tag
                cxl_packet = CxlMemBIRspPacket.create(self._cur_state.cache_rsp, bi_id, bi_tag)
                self._downstream_cxl_mem_fifos.host_to_target.put(cxl_packet)
                self._cur_state.birsp_sched = False
            self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
            return

        if (
            s2mndr_packet.s2mndr_header.meta_value == CXL_MEM_META_VALUE.ANY
            and self._cur_state.state == COH_STATE_MACHINE.COH_STATE_WAIT
        ):
            # HDM-DB: DRS should follow NDR as part of one response
            # Store the NDR status and set flag to wait for DRS
            self._cur_state.pending_ndr_status = status
            self._cur_state.waiting_for_drs = True
            logger.debug(self._create_message("[HA-DS] NDR processed, waiting for DRS packet"))
            # Wake waiters so upstream can pause new reads until DRS arrives
            self._state_version += 1
            self._flow_control_cv.notify_all()
            # Don't send CacheResponse yet - wait for DRS
            return
        else:
            # For non-WAIT state (e.g., write-like flows), avoid redundant state bump/notify
            # if the state is already INIT. This reduces unnecessary wake-ups without
            # changing protocol semantics.
            if self._cur_state.state != COH_STATE_MACHINE.COH_STATE_INIT:
                self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
                self._state_version += 1
                self._flow_control_cv.notify_all()
        # Flow control managed by _fc_host_run flag

    # .mem s2m drs handler
    # method is only used for non cacheable devices like memory expander
    def _process_cxl_s2m_drs_packet(self, s2mdrs_packet: CxlMemS2MDRSPacket):
        assert s2mdrs_packet.s2mdrs_header.opcode == CXL_MEM_S2MDRS_OPCODE.MEM_DATA
        logger.debug(self._create_message("Processing S2M DRS in HA"))
        self._metrics["drs_count"] += 1
        rsp_put = self._upstream_cache_to_home_agent_fifos.response.put

        # Check if this DRS corresponds to a pending NDR
        if self._cur_state.waiting_for_drs and self._cur_state.pending_ndr_status is not None:
            # This DRS completes a pending NDR response
            cache_packet = CacheResponse(self._cur_state.pending_ndr_status, s2mdrs_packet.get_data_as_int())
            logger.debug(self._create_message("HA posting CacheResponse for NDR+DRS sequence"))
            rsp_put(cache_packet)
            # Reset the pending state
            self._cur_state.pending_ndr_status = None
            self._cur_state.waiting_for_drs = False
        else:
            # Standalone DRS packet (for non-cacheable devices)
            cache_packet = CacheResponse(CACHE_RESPONSE_STATUS.OK, s2mdrs_packet.get_data_as_int())
            rsp_put(cache_packet)

        self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
        self._state_version += 1
        self._flow_control_cv.notify_all()
        # Flow control managed by _fc_host_run flag

    # .mem s2m bisnp handler
    def _process_cxl_s2m_bisnp_packet(self, s2mbisnp_packet: CxlMemS2MBISnpPacket):
        # This method is called while holding _state_lock by the downstream worker.
        # Only process when state is START (original behavior), otherwise return.
        state = self._cur_state.state
        if state != COH_STATE_MACHINE.COH_STATE_START:
            return
        # Remember the BISNP packet in cur_state for possible later BIRsp
        self._cur_state.packet = s2mbisnp_packet

        addr = s2mbisnp_packet.get_address()
        self._metrics["bisnp_count"] += 1

        if s2mbisnp_packet.s2mbisnp_header.opcode == CXL_MEM_S2MBISNP_OPCODE.BISNP_DATA:
            cache_packet = CacheRequest(CACHE_REQUEST_TYPE.SNP_DATA, addr)
        elif s2mbisnp_packet.s2mbisnp_header.opcode == CXL_MEM_S2MBISNP_OPCODE.BISNP_INV:
            cache_packet = CacheRequest(CACHE_REQUEST_TYPE.SNP_INV, addr)
        else:
            return

        # Ask cache controller without holding HA state lock
        self._upstream_home_agent_to_cache_fifos.request.put(cache_packet)
        bi_id = s2mbisnp_packet.s2mbisnp_header.bi_id
        bi_tag = s2mbisnp_packet.s2mbisnp_header.bi_tag
        packet = self._upstream_home_agent_to_cache_fifos.response.get()

        if packet.status == CACHE_RESPONSE_STATUS.RSP_MISS:
            # corner case handling: cacheline with same address is currently write back to device
            rsp_state = CXL_MEM_M2SBIRSP_OPCODE.BIRSP_I
            cxl_packet = CxlMemBIRspPacket.create(rsp_state, bi_id, bi_tag)
            self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
            self._state_version += 1
            self._flow_control_cv.notify_all()
        else:
            if packet.status == CACHE_RESPONSE_STATUS.RSP_S:
                birsp_state = CXL_MEM_M2SBIRSP_OPCODE.BIRSP_S
            else:
                birsp_state = CXL_MEM_M2SBIRSP_OPCODE.BIRSP_I
            self._cur_state.cache_rsp = birsp_state
            self._cur_state.birsp_sched = True
            opcode = CXL_MEM_M2SRWD_OPCODE.MEM_WR
            meta_field = CXL_MEM_META_FIELD.META0_STATE
            meta_value = CXL_MEM_META_VALUE.INVALID
            snp_type = CXL_MEM_M2S_SNP_TYPE.NO_OP
            cxl_packet = self._create_m2s_rwd_packet(
                opcode, meta_field, meta_value, snp_type, addr, packet.data
            )
            self._cur_state.state = COH_STATE_MACHINE.COH_STATE_WAIT
            self._state_version += 1
            self._flow_control_cv.notify_all()
        # Send packet to device
        self._downstream_cxl_mem_fifos.host_to_target.put(cxl_packet)

    # .mem m2s packet process
    def _process_upstream_host_to_target_packets(self, cache_packet: CacheRequest):
        logger.debug(self._create_message(f"[HA-US] enter _process_upstream_host_to_target_packets state={self._cur_state.state} type={cache_packet.type}"))
        if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_WAIT:
            logger.debug(self._create_message("[HA-US] early return due to WAIT"))
            return

        if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_START:
            meta_field = CXL_MEM_META_FIELD.NO_OP
            meta_value = CXL_MEM_META_VALUE.INVALID
            snp_type = CXL_MEM_M2S_SNP_TYPE.NO_OP
            addr = cache_packet.addr

            if cache_packet.type in (
                CACHE_REQUEST_TYPE.WRITE,
                CACHE_REQUEST_TYPE.WRITE_BACK,
                CACHE_REQUEST_TYPE.WRITE_BACK_CLEAN,
                CACHE_REQUEST_TYPE.UNCACHED_WRITE,
            ):
                opcode = CXL_MEM_M2SRWD_OPCODE.MEM_WR
                data = cache_packet.data

                # HDM-H Normal Write
                if cache_packet.type == CACHE_REQUEST_TYPE.WRITE:
                    meta_value = CXL_MEM_META_VALUE.ANY
                # HDM-DB Flush Write (Cmp: I/I)
                elif cache_packet.type in (
                    CACHE_REQUEST_TYPE.WRITE_BACK,
                    CACHE_REQUEST_TYPE.WRITE_BACK_CLEAN,
                ):
                    # TODO: should consider skip sending data back to .MEM dev
                    # if the cache line is clean (WRITE_BACK_CLEAN)
                    meta_field = CXL_MEM_META_FIELD.META0_STATE
                    meta_value = CXL_MEM_META_VALUE.INVALID
                # HDM Uncached Write
                elif cache_packet.type == CACHE_REQUEST_TYPE.UNCACHED_WRITE:
                    meta_value = CXL_MEM_META_VALUE.ANY

                logger.debug(self._create_message(f"[HA-US] H2T WR req addr=0x{addr:x}"))
                cxl_packet = self._create_m2s_rwd_packet(
                    opcode, meta_field, meta_value, snp_type, addr, data
                )
                packet = CacheResponse(CACHE_RESPONSE_STATUS.OK)
                self._upstream_cache_to_home_agent_fifos.response.put(packet)
                logger.info(self._create_message("[HA-US] WR ack posted to cache"))
                logger.debug(self._create_message("[HA-US] H2T WR ack to cache OK"))
            else:
                # HDM-H Normal Read
                if cache_packet.type == CACHE_REQUEST_TYPE.READ:
                    opcode = CXL_MEM_M2SREQ_OPCODE.MEM_RD
                    meta_value = CXL_MEM_META_VALUE.ANY
                # HDM-DB Device Shared Read (Cmp-S: S/S, Cmp-E: A/I)
                elif cache_packet.type == CACHE_REQUEST_TYPE.SNP_DATA:
                    opcode = CXL_MEM_M2SREQ_OPCODE.MEM_RD
                    meta_field = CXL_MEM_META_FIELD.META0_STATE
                    meta_value = CXL_MEM_META_VALUE.SHARED
                    snp_type = CXL_MEM_M2S_SNP_TYPE.SNP_DATA
                # HDM-DB Non-Data, Host Ownership Device Invalidation (Cmp-E: A/I)
                elif cache_packet.type == CACHE_REQUEST_TYPE.SNP_INV:
                    opcode = CXL_MEM_M2SREQ_OPCODE.MEM_INV
                    meta_field = CXL_MEM_META_FIELD.META0_STATE
                    meta_value = CXL_MEM_META_VALUE.ANY
                    snp_type = CXL_MEM_M2S_SNP_TYPE.SNP_INV
                # HDM-DB Non-Cacheable Read, Leaving Device Cache (Cmp: I/A)
                elif cache_packet.type == CACHE_REQUEST_TYPE.SNP_CUR:
                    opcode = CXL_MEM_M2SREQ_OPCODE.MEM_RD
                    meta_field = CXL_MEM_META_FIELD.META0_STATE
                    snp_type = CXL_MEM_M2S_SNP_TYPE.SNP_CUR
                elif cache_packet.type == CACHE_REQUEST_TYPE.UNCACHED_READ:
                    opcode = CXL_MEM_M2SREQ_OPCODE.MEM_RD
                    meta_value = CXL_MEM_META_VALUE.ANY
                else:
                    raise Exception(f"Invalid M2S Opcode Type: {cache_packet.type}")

                logger.debug(self._create_message(f"[HA-US] H2T RD req addr=0x{addr:x}"))
                cxl_packet = self._create_m2s_req_packet(
                    opcode, meta_field, meta_value, snp_type, addr
                )

            # Writes do not block the HA state machine
            if cache_packet.type in (
                CACHE_REQUEST_TYPE.WRITE,
                CACHE_REQUEST_TYPE.WRITE_BACK,
                CACHE_REQUEST_TYPE.WRITE_BACK_CLEAN,
                CACHE_REQUEST_TYPE.UNCACHED_WRITE,
            ):
                self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
                self._state_version += 1
                self._flow_control_cv.notify_all()
            else:
                # Cached reads/snoop ops wait for response (NDR/DRS)
                self._cur_state.state = COH_STATE_MACHINE.COH_STATE_WAIT
                self._state_version += 1
                self._flow_control_cv.notify_all()
            self._downstream_cxl_mem_fifos.host_to_target.put(cxl_packet)
        logger.debug(self._create_message(f"[HA-US] exit _process_upstream_host_to_target_packets state={self._cur_state.state}"))

    # Downstream worker handling CXL.mem packets from device
    def _process_downstream_packets_worker(self) -> None:
        # Cache frequently accessed state values to reduce function call overhead
        cur_state = self._cur_state
        target_to_host_get = self._downstream_cxl_mem_fifos.target_to_host.get
        
        while True:
            # Prefer pending packet if any
            with self._state_lock:
                packet = self._pending_downstream
                self._pending_downstream = None
            if packet is None:
                packet = target_to_host_get()
                if packet is None:
                    break

            # Avoid repeated casting by doing it once
            base_packet = cast(BasePacket, packet)
            if not base_packet.is_cxl_mem():
                raise Exception(f"Received unexpected packet: {base_packet.get_type()}")
            cxl_packet = cast(CxlMemBasePacket, packet)
            
            # Cache type checks with lazy BISNP evaluation (not used in this benchmark path)
            is_ndr = cxl_packet.is_s2mndr()
            is_drs = cxl_packet.is_s2mdrs()
            if is_ndr:
                logger.debug(self._create_message("[HA-DS] dequeued S2M NDR"))
            elif is_drs:
                logger.debug(self._create_message("[HA-DS] dequeued S2M DRS"))

            bisnp_to_process = None
            with self._state_lock:
                current_state = cur_state.state
                
                # Process packets based on current state with optimized conditionals
                if is_ndr:
                    logger.info(self._create_message("HomeAgent: received S2M NDR"))
                    self._process_cxl_s2m_rsp_packet(cast(CxlMemS2MNDRPacket, packet))
                elif is_drs and (current_state == COH_STATE_MACHINE.COH_STATE_INIT or 
                                current_state == COH_STATE_MACHINE.COH_STATE_WAIT):
                    logger.info(self._create_message("HomeAgent: received S2M DRS"))
                    self._process_cxl_s2m_drs_packet(cast(CxlMemS2MDRSPacket, packet))
                elif (not is_ndr and not is_drs) and (current_state == COH_STATE_MACHINE.COH_STATE_INIT or current_state == COH_STATE_MACHINE.COH_STATE_START) and cxl_packet.is_s2mbisnp():
                    # Handle BISNP packets with original coordination semantics:
                    # process when upstream has engaged the critical section
                    if self._fc_host_run:
                        cur_state.packet = cast(CxlMemS2MBISnpPacket, packet)
                        cur_state.state = COH_STATE_MACHINE.COH_STATE_START
                        self._state_version += 1
                        self._flow_control_cv.notify_all()
                        # Defer BISNP processing to outside of the lock to avoid deadlocks
                        bisnp_to_process = cast(CxlMemS2MBISnpPacket, packet)
                    else:
                        # Upstream worker has priority, stash and wait for state change/version bump
                        self._pending_downstream = packet
                        self._metrics["ds_reenqueue"] += 1
                        _t0 = time.monotonic_ns()
                        start_ver = self._state_version
                        while self._state_version == start_ver:
                            self._flow_control_cv.wait()
                        self._metrics["ds_wait_ns"] += time.monotonic_ns() - _t0
                        continue
                else:
                    # Packet type doesn't match current state - hold and wait for notify
                    logger.info(self._create_message(f"[HA-DS] hold packet {cxl_packet.get_type()} in state {current_state}, waiting for state change"))
                    self._pending_downstream = packet
                    self._metrics["ds_reenqueue"] += 1
                    _t0 = time.monotonic_ns()
                    start_ver = self._state_version
                    while self._state_version == start_ver:
                        self._flow_control_cv.wait()
                    self._metrics["ds_wait_ns"] += time.monotonic_ns() - _t0
                    logger.info(self._create_message(f"[HA-DS] wake: state_version {start_ver}->{self._state_version}"))
                    continue

            # Perform BISNP work after releasing the state lock
            if bisnp_to_process is not None:
                self._process_cxl_s2m_bisnp_packet(bisnp_to_process)
                # Release upstream run flag after BISNP is handled
                with self._state_lock:
                    self._fc_host_run = False
                    self._state_version += 1
                    self._flow_control_cv.notify_all()
                continue


    # Upstream worker handling cache requests from host
    def _process_upstream_packets_worker(self) -> None:
        # Cache frequently accessed objects to reduce attribute lookups
        cur_state = self._cur_state
        upstream_fifo = self._upstream_cache_to_home_agent_fifos
        upstream_req_get = upstream_fifo.request.get
        upstream_rsp_put = upstream_fifo.response.put
        downstream_put = self._downstream_cxl_mem_fifos.host_to_target.put
        
        while True:
            # Prefer pending upstream if any
            with self._state_lock:
                cache_packet = self._pending_upstream
                self._pending_upstream = None
            if cache_packet is None:
                cache_packet = upstream_req_get()
                if cache_packet is None:
                    logger.debug(self._create_message("Stop processing upstream cache requests"))
                    break

            with self._state_lock:
                try:
                    addr_dbg = f"0x{cache_packet.addr:x}"
                except Exception:
                    addr_dbg = "-"
                logger.info(self._create_message(f"[HA-US] dequeued upstream type={cache_packet.type} addr={addr_dbg} state={cur_state.state}"))
                # Cache state value to avoid repeated attribute access
                current_state = cur_state.state
                
                # Process upstream packets with flow control coordination
                if current_state == COH_STATE_MACHINE.COH_STATE_INIT:
                    if not self._fc_host_run:
                        # We have priority, set flag and prepare work under lock
                        self._fc_host_run = True
                        self._state_version += 1
                        self._flow_control_cv.notify_all()
                        cur_state.state = COH_STATE_MACHINE.COH_STATE_START
                        logger.info(self._create_message("[HA-US] state->START (upstream critical section)"))

                        # Prepare packet parameters while holding the lock
                        is_write = cache_packet.type in (
                            CACHE_REQUEST_TYPE.WRITE,
                            CACHE_REQUEST_TYPE.WRITE_BACK,
                            CACHE_REQUEST_TYPE.WRITE_BACK_CLEAN,
                            CACHE_REQUEST_TYPE.UNCACHED_WRITE,
                        )
                        addr = cache_packet.addr
                        meta_field = CXL_MEM_META_FIELD.NO_OP
                        meta_value = CXL_MEM_META_VALUE.INVALID
                        snp_type = CXL_MEM_M2S_SNP_TYPE.NO_OP
                        opcode_req = None
                        opcode_rwd = None
                        write_data = None
                        post_ack = False

                        if is_write:
                            opcode_rwd = CXL_MEM_M2SRWD_OPCODE.MEM_WR
                            write_data = cache_packet.data
                            if cache_packet.type == CACHE_REQUEST_TYPE.WRITE:
                                meta_value = CXL_MEM_META_VALUE.ANY
                            elif cache_packet.type in (
                                CACHE_REQUEST_TYPE.WRITE_BACK,
                                CACHE_REQUEST_TYPE.WRITE_BACK_CLEAN,
                            ):
                                meta_field = CXL_MEM_META_FIELD.META0_STATE
                                meta_value = CXL_MEM_META_VALUE.INVALID
                            elif cache_packet.type == CACHE_REQUEST_TYPE.UNCACHED_WRITE:
                                meta_value = CXL_MEM_META_VALUE.ANY
                            # Writes do not block the HA state machine
                            cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
                            self._state_version += 1
                            self._flow_control_cv.notify_all()
                            post_ack = True
                        else:
                            # READ or SNP
                            if cache_packet.type == CACHE_REQUEST_TYPE.READ:
                                opcode_req = CXL_MEM_M2SREQ_OPCODE.MEM_RD
                                meta_value = CXL_MEM_META_VALUE.ANY
                            elif cache_packet.type == CACHE_REQUEST_TYPE.SNP_DATA:
                                opcode_req = CXL_MEM_M2SREQ_OPCODE.MEM_RD
                                meta_field = CXL_MEM_META_FIELD.META0_STATE
                                meta_value = CXL_MEM_META_VALUE.SHARED
                                snp_type = CXL_MEM_M2S_SNP_TYPE.SNP_DATA
                            elif cache_packet.type == CACHE_REQUEST_TYPE.SNP_INV:
                                opcode_req = CXL_MEM_M2SREQ_OPCODE.MEM_INV
                                meta_field = CXL_MEM_META_FIELD.META0_STATE
                                meta_value = CXL_MEM_META_VALUE.ANY
                                snp_type = CXL_MEM_M2S_SNP_TYPE.SNP_INV
                            elif cache_packet.type == CACHE_REQUEST_TYPE.SNP_CUR:
                                opcode_req = CXL_MEM_M2SREQ_OPCODE.MEM_RD
                                meta_field = CXL_MEM_META_FIELD.META0_STATE
                                snp_type = CXL_MEM_M2S_SNP_TYPE.SNP_CUR
                            elif cache_packet.type == CACHE_REQUEST_TYPE.UNCACHED_READ:
                                opcode_req = CXL_MEM_M2SREQ_OPCODE.MEM_RD
                                meta_value = CXL_MEM_META_VALUE.ANY
                            else:
                                raise Exception(f"Invalid M2S Opcode Type: {cache_packet.type}")

                            # Cached reads/snoop ops wait for response (NDR/DRS)
                            cur_state.state = COH_STATE_MACHINE.COH_STATE_WAIT
                            self._state_version += 1
                            self._flow_control_cv.notify_all()

                        # Release the lock to perform queue puts
                        pass
                    else:
                        # Downstream worker has priority, stash and wait for notify
                        logger.info(self._create_message("[HA-US] stash upstream due to fc_host_run=True; waiting"))
                        self._pending_upstream = cache_packet
                        self._metrics["us_reenqueue"] += 1
                        _t0 = time.monotonic_ns()
                        start_ver = self._state_version
                        while self._state_version == start_ver:
                            self._flow_control_cv.wait()
                        self._metrics["us_wait_ns"] += time.monotonic_ns() - _t0
                        logger.info(self._create_message(f"[HA-US] wake: state_version {start_ver}->{self._state_version}"))
                        continue
                else:
                    # State is not INIT, stash and wait for state change
                    logger.info(self._create_message(f"[HA-US] hold upstream in state {current_state}; waiting"))
                    self._pending_upstream = cache_packet
                    self._metrics["us_reenqueue"] += 1
                    _t0 = time.monotonic_ns()
                    start_ver = self._state_version
                    while self._state_version == start_ver:
                        self._flow_control_cv.wait()
                    self._metrics["us_wait_ns"] += time.monotonic_ns() - _t0
                    logger.info(self._create_message(f"[HA-US] wake: state_version {start_ver}->{self._state_version}"))
                    continue

            # Perform the actual enqueues outside of the lock
            if 'opcode_rwd' in locals() or 'opcode_req' in locals():
                if is_write and opcode_rwd is not None:
                    logger.info(self._create_message(f"[HA-US] H2T WR req addr=0x{addr:x}"))
                    cxl_packet = self._create_m2s_rwd_packet(
                        opcode_rwd, meta_field, meta_value, snp_type, addr, write_data  # type: ignore[arg-type]
                    )
                    if post_ack:
                        packet = CacheResponse(CACHE_RESPONSE_STATUS.OK)
                        upstream_rsp_put(packet)
                        logger.info(self._create_message("[HA-US] WR ack posted to cache"))
                elif (not is_write) and opcode_req is not None:
                    logger.info(self._create_message(f"[HA-US] H2T RD req addr=0x{addr:x}"))
                    cxl_packet = self._create_m2s_req_packet(
                        opcode_req, meta_field, meta_value, snp_type, addr
                    )
                else:
                    cxl_packet = None

                if cxl_packet is not None:
                    downstream_put(cxl_packet)

                # Clear critical section under lock and notify
                with self._state_lock:
                    if self._fc_host_run:
                        self._fc_host_run = False
                        self._state_version += 1
                        self._flow_control_cv.notify_all()

    def _run(self):
        self._downstream_worker_thread = threading.Thread(
            target=self._process_downstream_packets_worker,
            name=f"{self.get_message_label()}-downstream-worker",
            daemon=True,
        )
        self._downstream_worker_thread.start()

        self._upstream_worker_thread = threading.Thread(
            target=self._process_upstream_packets_worker,
            name=f"{self.get_message_label()}-upstream-worker",
            daemon=True,
        )
        self._upstream_worker_thread.start()

        # start memory request workers
        self._io_thread = threading.Thread(
            target=self._process_memory_io_bridge_requests,
            name=f"{self.get_message_label()}-io-req",
            daemon=True,
        )
        self._io_thread.start()

        self._coh_thread = threading.Thread(
            target=self._process_memory_coh_bridge_requests,
            name=f"{self.get_message_label()}-coh-req",
            daemon=True,
        )
        self._coh_thread.start()

        self._change_status_to_running()

        # Block until workers finish
        self._io_thread.join()
        self._coh_thread.join()
        self._downstream_worker_thread.join()
        self._upstream_worker_thread.join()

    def _stop(self):
        try:
            self._memory_consumer_io_fifos.request.put(None)
            self._memory_consumer_coh_fifos.request.put(None)
            self._upstream_cache_to_home_agent_fifos.request.put(None)
        except Exception:
            pass

        # Stop downstream worker
        try:
            self._downstream_cxl_mem_fifos.target_to_host.put(None)
        except Exception:
            pass
        if self._downstream_worker_thread is not None:
            self._downstream_worker_thread.join(timeout=1.0)

        # Stop upstream worker
        if self._upstream_worker_thread is not None:
            self._upstream_worker_thread.join(timeout=1.0)

        # Log metrics at shutdown
        try:
            logger.info(
                self._create_message(
                    "[HA-METRICS] ds_reenqueue=%d us_reenqueue=%d ds_wait_ms=%.3f us_wait_ms=%.3f ndr=%d drs=%d bisnp=%d"
                    % (
                        self._metrics["ds_reenqueue"],
                        self._metrics["us_reenqueue"],
                        self._metrics["ds_wait_ns"] / 1_000_000.0,
                        self._metrics["us_wait_ns"] / 1_000_000.0,
                        self._metrics["ndr_count"],
                        self._metrics["drs_count"],
                        self._metrics["bisnp_count"],
                    )
                )
            )
        except Exception:
            pass
