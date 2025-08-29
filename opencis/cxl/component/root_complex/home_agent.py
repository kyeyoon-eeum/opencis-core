"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from dataclasses import dataclass, field
import threading
from queue import Queue
from typing import cast

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
class HomeAgentCxlChannel:
    s2m_ndr: Queue = field(default_factory=Queue)
    s2m_drs: Queue = field(default_factory=Queue)
    s2m_bisnp: Queue = field(default_factory=Queue)


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
        )

        # emulated .mem s2m channels
        self._cxl_channel = HomeAgentCxlChannel()
        self._loop = None
        self._demux_thread: threading.Thread | None = None
        self._demux_stop = threading.Event()
        self._main_thread: threading.Thread | None = None
        self._main_stop = threading.Event()

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
        logger.info(self._create_message("Processing S2M NDR in HA"))
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

        if s2mndr_packet.s2mndr_header.meta_value == CXL_MEM_META_VALUE.ANY:
            # HDM-DB: DRS immediately following NDR as part of one response
            while self._cxl_channel.s2m_drs.empty():
                pass
            cxl_packet = self._cxl_channel.s2m_drs.get()
            assert cast(CxlMemBasePacket, cxl_packet).is_s2mdrs()
            cache_packet = CacheResponse(status, cxl_packet.get_data_as_int())
        else:
            cache_packet = CacheResponse(status)
        logger.debug(self._create_message("HA posting CacheResponse for UNCACHED_READ"))
        self._upstream_cache_to_home_agent_fifos.response.put(cache_packet)
        self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT

    # .mem s2m drs handler
    # method is only used for non cacheable devices like memory expander
    def _process_cxl_s2m_drs_packet(self, s2mdrs_packet: CxlMemS2MDRSPacket):
        assert s2mdrs_packet.s2mdrs_header.opcode == CXL_MEM_S2MDRS_OPCODE.MEM_DATA
        logger.info(self._create_message("Processing S2M DRS in HA"))
        cache_packet = CacheResponse(CACHE_RESPONSE_STATUS.OK, s2mdrs_packet.get_data_as_int())
        self._upstream_cache_to_home_agent_fifos.response.put(cache_packet)
        self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT

    # .mem s2m bisnp handler
    def _process_cxl_s2m_bisnp_packet(self, s2mbisnp_packet: CxlMemS2MBISnpPacket):
        if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_WAIT:
            return

        if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_START:
            addr = s2mbisnp_packet.get_address()

            if s2mbisnp_packet.s2mbisnp_header.opcode == CXL_MEM_S2MBISNP_OPCODE.BISNP_DATA:
                cache_packet = CacheRequest(CACHE_REQUEST_TYPE.SNP_DATA, addr)
            elif s2mbisnp_packet.s2mbisnp_header.opcode == CXL_MEM_S2MBISNP_OPCODE.BISNP_INV:
                cache_packet = CacheRequest(CACHE_REQUEST_TYPE.SNP_INV, addr)
            self._upstream_home_agent_to_cache_fifos.request.put(cache_packet)
            bi_id = s2mbisnp_packet.s2mbisnp_header.bi_id
            bi_tag = s2mbisnp_packet.s2mbisnp_header.bi_tag

            packet = self._upstream_home_agent_to_cache_fifos.response.get()

            if packet.status == CACHE_RESPONSE_STATUS.RSP_MISS:
                # corner case handling
                # the cacheline w/ same address is currently write back to device
                rsp_state = CXL_MEM_M2SBIRSP_OPCODE.BIRSP_I
                cxl_packet = CxlMemBIRspPacket.create(rsp_state, bi_id, bi_tag)
                self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
            else:
                if packet.status == CACHE_RESPONSE_STATUS.RSP_S:
                    self._cur_state.cache_rsp = CXL_MEM_M2SBIRSP_OPCODE.BIRSP_S
                else:
                    self._cur_state.cache_rsp = CXL_MEM_M2SBIRSP_OPCODE.BIRSP_I
                self._cur_state.birsp_sched = True
                opcode = CXL_MEM_M2SRWD_OPCODE.MEM_WR
                meta_field = CXL_MEM_META_FIELD.META0_STATE
                meta_value = CXL_MEM_META_VALUE.INVALID
                snp_type = CXL_MEM_M2S_SNP_TYPE.NO_OP

                cxl_packet = self._create_m2s_rwd_packet(
                    opcode, meta_field, meta_value, snp_type, addr, packet.data
                )
                self._cur_state.state = COH_STATE_MACHINE.COH_STATE_WAIT
            self._downstream_cxl_mem_fifos.host_to_target.put(cxl_packet)

    # .mem m2s packet process
    def _process_upstream_host_to_target_packets(self, cache_packet: CacheRequest):
        if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_WAIT:
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

                logger.info(self._create_message(f"CXL.mem H2T WR req addr=0x{addr:x}"))
                cxl_packet = self._create_m2s_rwd_packet(
                    opcode, meta_field, meta_value, snp_type, addr, data
                )
                packet = CacheResponse(CACHE_RESPONSE_STATUS.OK)
                self._upstream_cache_to_home_agent_fifos.response.put(packet)
                logger.info(self._create_message("CXL.mem H2T WR ack to cache OK"))
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

                logger.info(self._create_message(f"CXL.mem H2T RD req addr=0x{addr:x}"))
                cxl_packet = self._create_m2s_req_packet(
                    opcode, meta_field, meta_value, snp_type, addr
                )

            # Original behavior: writes do not block; reads/snoop ops wait for response
            if cache_packet.type in (
                CACHE_REQUEST_TYPE.WRITE,
                CACHE_REQUEST_TYPE.WRITE_BACK,
                CACHE_REQUEST_TYPE.WRITE_BACK_CLEAN,
                CACHE_REQUEST_TYPE.UNCACHED_WRITE,
            ):
                self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
            else:
                self._cur_state.state = COH_STATE_MACHINE.COH_STATE_WAIT
            self._downstream_cxl_mem_fifos.host_to_target.put(cxl_packet)

    # .mem s2m packet process
    def _process_downstream_target_to_host_worker(self) -> None:
        while not self._demux_stop.is_set():
            packet = self._downstream_cxl_mem_fifos.target_to_host.get()
            if packet is None:
                break
            base_packet = cast(BasePacket, packet)
            if not base_packet.is_cxl_mem():
                raise Exception(f"Received unexpected packet: {base_packet.get_type()}")
            cxl_packet = cast(CxlMemBasePacket, packet)
            if cxl_packet.is_s2mndr():
                logger.debug(self._create_message("HomeAgent demux: received S2M NDR"))
                self._cxl_channel.s2m_ndr.put(cast(CxlMemS2MNDRPacket, packet))
            elif cxl_packet.is_s2mdrs():
                logger.debug(self._create_message("HomeAgent demux: received S2M DRS"))
                self._cxl_channel.s2m_drs.put(cast(CxlMemS2MDRSPacket, packet))
            elif cxl_packet.is_s2mbisnp():
                self._cxl_channel.s2m_bisnp.put(cast(CxlMemS2MBISnpPacket, packet))
            else:
                raise Exception(f"Received unexpected packet: {cxl_packet.get_type()}")

    # process from host/device channels one by one in state machine
    def _home_agent_coherency_main_worker(self) -> None:
        _stop_process = False
        _fc_run = False
        _fc_host_run = False

        while not _stop_process and not self._main_stop.is_set():
            # Response draining policy:
            # - While waiting (one outstanding request), handle exactly one NDR per loop.
            #   The NDR handler will consume its paired DRS when meta=ANY.
            # - When idle (INIT), allow at most one stray DRS to be handled to avoid leftovers.
            if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_WAIT:
                if not self._cxl_channel.s2m_ndr.empty():
                    packet = self._cxl_channel.s2m_ndr.get()
                    self._process_cxl_s2m_rsp_packet(packet)
            elif self._cur_state.state == COH_STATE_MACHINE.COH_STATE_INIT:
                if not self._cxl_channel.s2m_drs.empty():
                    packet = self._cxl_channel.s2m_drs.get()
                    self._process_cxl_s2m_drs_packet(packet)
            # flow control for host/device packets
            if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_INIT:
                _fc_run = False
                if _fc_host_run is False:
                    if not self._upstream_cache_to_home_agent_fifos.request.empty():
                        _fc_run = True
                        _fc_host_run = True
                    elif not self._cxl_channel.s2m_bisnp.empty():
                        _fc_run = True
                        _fc_host_run = False
                else:
                    if not self._cxl_channel.s2m_bisnp.empty():
                        _fc_run = True
                        _fc_host_run = False
                    elif not self._upstream_cache_to_home_agent_fifos.request.empty():
                        _fc_run = True
                        _fc_host_run = True

                if _fc_run:
                    if _fc_host_run:
                        self._cur_state.packet = (
                            self._upstream_cache_to_home_agent_fifos.request.get()
                        )
                        if self._cur_state.packet is None:
                            logger.debug(
                                self._create_message(
                                    "Stop processing home agent coherency main loop"
                                )
                            )
                            _stop_process = True
                        fn = self._process_upstream_host_to_target_packets
                    else:
                        self._cur_state.packet = self._cxl_channel.s2m_bisnp.get()
                        fn = self._process_cxl_s2m_bisnp_packet

                    self._cur_state.state = COH_STATE_MACHINE.COH_STATE_START

            # run request processing and response checking code continuously until state changed
            else:
                fn(self._cur_state.packet)

    def _run(self):
        self._demux_stop.clear()
        self._demux_thread = threading.Thread(
            target=self._process_downstream_target_to_host_worker,
            name=f"{self.get_message_label()}-mem-demux",
            daemon=True,
        )
        self._demux_thread.start()
        # start main coherency worker thread
        self._main_stop.clear()
        self._main_thread = threading.Thread(
            target=self._home_agent_coherency_main_worker,
            name=f"{self.get_message_label()}-mem-main",
            daemon=True,
        )
        self._main_thread.start()
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
        self._main_thread.join()
        self._demux_thread.join()

    def _stop(self):
        try:
            self._memory_consumer_io_fifos.request.put(None)
            self._memory_consumer_coh_fifos.request.put(None)
            self._upstream_cache_to_home_agent_fifos.request.put(None)
        except Exception:
            pass
        self._demux_stop.set()
        try:
            self._downstream_cxl_mem_fifos.target_to_host.put(None)
        except Exception:
            pass
        if self._demux_thread is not None:
            self._demux_thread.join(timeout=1.0)
        # stop main coherency worker
        self._main_stop.set()
        if self._main_thread is not None:
            self._main_thread.join(timeout=1.0)
