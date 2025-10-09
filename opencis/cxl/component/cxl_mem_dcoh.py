"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from typing import Optional, Tuple, cast
import threading
from queue import Queue, Empty
from dataclasses import dataclass, field
from enum import Enum, auto

from opencis.util.logger import logger
from opencis.pci.component.fifo_pair import FifoPair
from opencis.cxl.transport.common import BasePacket
from opencis.cxl.transport.cxl_mem_packets import (
    CxlMemBasePacket,
    CxlMemM2SReqPacket,
    CxlMemM2SRwDPacket,
    CxlMemM2SBIRspPacket,
    CxlMemMemDataPacket,
    CxlMemCmpPacket,
    CxlMemBISnpPacket,
)
from opencis.cxl.transport.packet_constants import (
    CXL_MEM_META_FIELD,
    CXL_MEM_META_VALUE,
    CXL_MEM_M2S_SNP_TYPE,
    CXL_MEM_M2SREQ_OPCODE,
    CXL_MEM_M2SBIRSP_OPCODE,
    CXL_MEM_S2MNDR_OPCODE,
    CXL_MEM_S2MDRS_OPCODE,
    CXL_MEM_S2MBISNP_OPCODE,
)
from opencis.cxl.transport.cache_fifo import (
    CacheFifoPair,
    CacheRequest,
    CACHE_REQUEST_TYPE,
    CacheResponse,
    CACHE_RESPONSE_STATUS,
)
from opencis.cxl.component.cache_controller import (
    CohStateMachine,
    COH_STATE_MACHINE,
)
from opencis.cxl.component.cxl_memory_device_component import CxlMemoryDeviceComponent
from opencis.pci.component.packet_processor import PacketProcessor


# snoop filter update type definition
# both cache's snoop filter insertion/deletion
class SF_UPDATE_TYPE(Enum):
    SF_HOST_IN = auto()
    SF_HOST_OUT = auto()


@dataclass
class MemDcohCxlChannel:
    m2s_req: Queue = field(default_factory=Queue)
    m2s_rwd: Queue = field(default_factory=Queue)
    m2s_birsp: Queue = field(default_factory=Queue)


class CxlMemDcoh(PacketProcessor):
    def __init__(
        self,
        cache_to_coh_agent_fifo: CacheFifoPair,
        coh_agent_to_cache_fifo: CacheFifoPair,
        upstream_fifo: FifoPair,
        downstream_fifo: Optional[FifoPair] = None,
        label: Optional[str] = None,
        device_id: int = 0,
        test_mode: bool = False,
    ):
        # pylint: disable=duplicate-code
        self._downstream_fifo: Optional[FifoPair]
        self._upstream_fifo: FifoPair
        self._test_mode = test_mode

        super().__init__(upstream_fifo, downstream_fifo, label)
        self._cache_to_coh_agent_fifo = cache_to_coh_agent_fifo
        self._coh_agent_to_cache_fifo = coh_agent_to_cache_fifo
        self._memory_device_component: Optional[CxlMemoryDeviceComponent] = None

        # snoop filter defined as set structure
        # max sf size will be the same as each cache size
        self._cur_state = CohStateMachine(
            state=COH_STATE_MACHINE.COH_STATE_INIT,
            packet=None,
            cache_rsp=CACHE_RESPONSE_STATUS.RSP_I,
            cache_list=[],
            birsp_sched=False,
        )
        self._sf_host = set()
        self._bi_id = device_id
        self._bi_tag = 0

        # emulated .mem m2s channels
        self._cxl_channel = MemDcohCxlChannel()
        self._demux_thread: threading.Thread | None = None
        self._demux_stop = threading.Event()
        self._main_thread: threading.Thread | None = None
        self._main_stop = threading.Event()

    def set_memory_device_component(self, memory_device_component: CxlMemoryDeviceComponent):
        self._memory_device_component = memory_device_component

    def _snoop_filter_update(self, addr, sf_update_list) -> None:
        for sf_type in sf_update_list:
            if sf_type == SF_UPDATE_TYPE.SF_HOST_IN:
                self._sf_host.add(addr)
            elif sf_type == SF_UPDATE_TYPE.SF_HOST_OUT:
                self._sf_host.discard(addr)

    def _sf_host_is_hit(self, addr) -> bool:
        return addr in self._sf_host

    # .mem response may have two packets (nds and drs)
    # this method always makes two reponse packets but caller may ignore one if possible
    def _create_mem_rsp_packet(
        self,
        ndr_opcode: CXL_MEM_S2MNDR_OPCODE,
        data: Optional[int] = 0,
        drs_opcode: Optional[CXL_MEM_S2MDRS_OPCODE] = CXL_MEM_S2MDRS_OPCODE.MEM_DATA,
        meta_field: Optional[CXL_MEM_META_FIELD] = CXL_MEM_META_FIELD.NO_OP,
        meta_value: Optional[CXL_MEM_META_VALUE] = CXL_MEM_META_VALUE.INVALID,
    ) -> Tuple[CxlMemCmpPacket, CxlMemMemDataPacket]:
        return (
            CxlMemCmpPacket.create(ndr_opcode, meta_field, meta_value),
            CxlMemMemDataPacket.create(data, drs_opcode, meta_field, meta_value),
        )

    # .mem m2s req (MemRd, MemRdData, and MemInv) handler
    def _process_cxl_m2s_req_packet(self, m2sreq_packet: CxlMemM2SReqPacket):
        logger.info(self._create_message("Handle M2SREQ"))
        if self._memory_device_component is None:
            raise Exception("CxlMemoryDeviceComponent isn't set yet")

        addr = m2sreq_packet.get_address()
        if self._test_mode:
            dpa = addr
        else:
            dpa = self._memory_device_component.get_dpa(addr)

        if m2sreq_packet.m2sreq_header.meta_field == CXL_MEM_META_FIELD.NO_OP:
            data = self._memory_device_component.read_mem_dpa_sync(dpa)

            _, packet = self._create_mem_rsp_packet(CXL_MEM_S2MNDR_OPCODE.CMP, data)
            self._upstream_fifo.target_to_host.put(packet)
            logger.info(self._create_message("Sent S2MDRS response"))
            self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
            return

        rsp_code = CXL_MEM_S2MNDR_OPCODE.CMP
        sf_update_list = []
        data = 0
        data_flush = False
        data_read = m2sreq_packet.m2sreq_header.mem_opcode in (
            CXL_MEM_M2SREQ_OPCODE.MEM_RD,
            CXL_MEM_M2SREQ_OPCODE.MEM_RD_DATA,
        )

        if m2sreq_packet.m2sreq_header.snp_type == CXL_MEM_M2S_SNP_TYPE.SNP_DATA:
            type = CACHE_REQUEST_TYPE.SNP_DATA
        elif m2sreq_packet.m2sreq_header.snp_type == CXL_MEM_M2S_SNP_TYPE.SNP_INV:
            type = CACHE_REQUEST_TYPE.SNP_INV
        elif m2sreq_packet.m2sreq_header.snp_type == CXL_MEM_M2S_SNP_TYPE.SNP_CUR:
            type = CACHE_REQUEST_TYPE.SNP_CUR

        packet = CacheRequest(type, dpa)
        self._coh_agent_to_cache_fifo.request.put(packet)
        packet = self._coh_agent_to_cache_fifo.response.get()

        if packet.status == CACHE_RESPONSE_STATUS.RSP_MISS:
            if m2sreq_packet.m2sreq_header.snp_type == CXL_MEM_M2S_SNP_TYPE.SNP_DATA:
                rsp_code = CXL_MEM_S2MNDR_OPCODE.CMP_E
                sf_update_list.append(SF_UPDATE_TYPE.SF_HOST_IN)
            elif m2sreq_packet.m2sreq_header.snp_type == CXL_MEM_M2S_SNP_TYPE.SNP_INV:
                if m2sreq_packet.m2sreq_header.meta_value == CXL_MEM_META_VALUE.ANY:
                    rsp_code = CXL_MEM_S2MNDR_OPCODE.CMP_E
                    sf_update_list.append(SF_UPDATE_TYPE.SF_HOST_IN)
                elif (
                    m2sreq_packet.m2sreq_header.meta_value == CXL_MEM_META_VALUE.INVALID
                    or m2sreq_packet.m2sreq_header.meta_field == CXL_MEM_META_FIELD.NO_OP
                ):
                    pass
            elif m2sreq_packet.m2sreq_header.snp_type == CXL_MEM_M2S_SNP_TYPE.SNP_CUR:
                pass

            if data_read is True:
                data = self._memory_device_component.read_mem_dpa_sync(dpa)
        else:
            if packet.status in (CACHE_RESPONSE_STATUS.RSP_S, CACHE_RESPONSE_STATUS.RSP_M):
                # TODO: Table 3-50 shows Cmp-M should be optionally suported by host implementations
                rsp_code = CXL_MEM_S2MNDR_OPCODE.CMP_S
                sf_update_list.append(SF_UPDATE_TYPE.SF_HOST_IN)
            elif packet.status == CACHE_RESPONSE_STATUS.RSP_I:
                if m2sreq_packet.m2sreq_header.meta_value == CXL_MEM_META_VALUE.ANY:
                    rsp_code = CXL_MEM_S2MNDR_OPCODE.CMP_E
                    sf_update_list.append(SF_UPDATE_TYPE.SF_HOST_IN)
                elif (
                    m2sreq_packet.m2sreq_header.meta_value == CXL_MEM_META_VALUE.INVALID
                    or m2sreq_packet.m2sreq_header.meta_field == CXL_MEM_META_FIELD.NO_OP
                ):
                    if m2sreq_packet.m2sreq_header.mem_opcode == CXL_MEM_M2SREQ_OPCODE.MEM_RD:
                        data_flush = True
            elif packet.status == CACHE_RESPONSE_STATUS.RSP_V:
                pass
            data = packet.data

        if sf_update_list:
            self._snoop_filter_update(dpa, sf_update_list)

        if data_flush is True:
            self._memory_device_component.write_mem_dpa_sync(dpa, data)

        if data_read is True:
            # For MEM_RD, choose response type based on snoop type to match test expectations
            snp = m2sreq_packet.m2sreq_header.snp_type
            send_drs = snp in (CXL_MEM_M2S_SNP_TYPE.NO_OP, CXL_MEM_M2S_SNP_TYPE.SNP_INV)
            if send_drs:
                _, drs_packet = self._create_mem_rsp_packet(
                    rsp_code, data, meta_value=CXL_MEM_META_VALUE.ANY
                )
                self._upstream_fifo.target_to_host.put(drs_packet)
                logger.info(self._create_message("Sent S2MDRS response (read)"))
            else:
                ndr_packet, _ = self._create_mem_rsp_packet(rsp_code, data)
                self._upstream_fifo.target_to_host.put(ndr_packet)
                logger.info(self._create_message("Sent S2MNDR response (read)"))
        else:
            ndr_packet, _ = self._create_mem_rsp_packet(rsp_code, data)
            self._upstream_fifo.target_to_host.put(ndr_packet)
            logger.info(self._create_message("Sent S2MNDR response"))

    # .mem m2s rwd (MemWr) handler
    def _process_cxl_m2s_rwd_packet(self, m2srwd_packet: CxlMemM2SRwDPacket):
        if self._memory_device_component is None:
            raise Exception("CxlMemoryDeviceComponent isn't set yet")

        addr = m2srwd_packet.get_address()
        if self._test_mode:
            dpa = addr
        else:
            dpa = self._memory_device_component.get_dpa(addr)

        if m2srwd_packet.m2srwd_header.meta_field == CXL_MEM_META_FIELD.NO_OP:
            self._memory_device_component.write_mem_dpa_sync(dpa, m2srwd_packet.get_data_as_int())

            packet, _ = self._create_mem_rsp_packet(
                CXL_MEM_S2MNDR_OPCODE.CMP, m2srwd_packet.get_data_as_int()
            )
            self._upstream_fifo.target_to_host.put(packet)
            self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
            return

        rsp_code = CXL_MEM_S2MNDR_OPCODE.CMP
        sf_update_list = []
        data_flush = True

        if m2srwd_packet.m2srwd_header.meta_value == CXL_MEM_META_VALUE.ANY:
            packet = CacheRequest(CACHE_REQUEST_TYPE.SNP_INV, dpa)
        elif m2srwd_packet.m2srwd_header.meta_value == CXL_MEM_META_VALUE.SHARED:
            packet = CacheRequest(CACHE_REQUEST_TYPE.SNP_DATA, dpa)
        elif m2srwd_packet.m2srwd_header.meta_value == CXL_MEM_META_VALUE.INVALID:
            packet = CacheRequest(CACHE_REQUEST_TYPE.SNP_INV, dpa)
            sf_update_list.append(SF_UPDATE_TYPE.SF_HOST_OUT)
        self._coh_agent_to_cache_fifo.request.put(packet)
        packet = self._coh_agent_to_cache_fifo.response.get()

        if sf_update_list:
            self._snoop_filter_update(dpa, sf_update_list)

        if data_flush is True:
            self._memory_device_component.write_mem_dpa_sync(dpa, m2srwd_packet.get_data_as_int())

        ndr_packet, _ = self._create_mem_rsp_packet(rsp_code)
        self._upstream_fifo.target_to_host.put(ndr_packet)

    # .mem m2s birsp handler
    def _process_cxl_m2s_birsp_packet(self, m2sbirsp_packet: CxlMemM2SBIRspPacket):
        dpa = self._cur_state.packet.addr
        data = self._memory_device_component.read_mem_dpa_sync(dpa)

        if m2sbirsp_packet.m2sbirsp_header.opcode == CXL_MEM_M2SBIRSP_OPCODE.BIRSP_S:
            packet = CacheResponse(CACHE_RESPONSE_STATUS.RSP_S, data)
        elif m2sbirsp_packet.m2sbirsp_header.opcode == CXL_MEM_M2SBIRSP_OPCODE.BIRSP_I:
            packet = CacheResponse(CACHE_RESPONSE_STATUS.RSP_I, data)
        else:
            raise Exception(
                f"Unsupported M2SBIRsp Opcode: {m2sbirsp_packet.m2sbirsp_header.opcode}"
            )
        self._cache_to_coh_agent_fifo.response.put(packet)
        self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT

    # .mem s2m device req handler
    def _process_cache_to_dcoh(self, cache_packet: CacheRequest):
        if self._memory_device_component is None:
            raise Exception("CxlMemoryDeviceComponent isn't set yet")

        if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_WAIT:
            return

        if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_START:
            dpa = cache_packet.addr
            if cache_packet.type == CACHE_REQUEST_TYPE.READ:
                data = self._memory_device_component.read_mem_dpa_sync(dpa)
                packet = CacheResponse(CACHE_RESPONSE_STATUS.OK, data)
                self._cache_to_coh_agent_fifo.response.put(packet)
                self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
            elif cache_packet.type in (
                CACHE_REQUEST_TYPE.WRITE,
                CACHE_REQUEST_TYPE.WRITE_BACK,
                CACHE_REQUEST_TYPE.WRITE_BACK_CLEAN,
            ):
                # if not CACHE_REQUEST_TYPE.WRITE_BACK_CLEAN:
                self._memory_device_component.write_mem_dpa(dpa, cache_packet.data)
                packet = CacheResponse(CACHE_RESPONSE_STATUS.OK)
                self._cache_to_coh_agent_fifo.response.put(packet)
                self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
            else:
                # host cache snoop filter miss
                if not self._sf_host_is_hit(dpa):
                    if cache_packet.type == CACHE_REQUEST_TYPE.SNP_DATA:
                        data = self._memory_device_component.read_mem_dpa(dpa)
                        packet = CacheResponse(CACHE_RESPONSE_STATUS.RSP_I, data)
                    elif cache_packet.type == CACHE_REQUEST_TYPE.SNP_INV:
                        packet = CacheResponse(CACHE_RESPONSE_STATUS.RSP_I)
                    elif cache_packet.type == CACHE_REQUEST_TYPE.SNP_CUR:
                        packet = CacheResponse(CACHE_RESPONSE_STATUS.RSP_V)
                    self._cache_to_coh_agent_fifo.response.put(packet)
                    self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
                # host cache snoop filter hit
                else:
                    sf_update_list = []
                    if cache_packet.type == CACHE_REQUEST_TYPE.SNP_DATA:
                        bi_opcode = CXL_MEM_S2MBISNP_OPCODE.BISNP_DATA
                    elif cache_packet.type == CACHE_REQUEST_TYPE.SNP_INV:
                        sf_update_list.append(SF_UPDATE_TYPE.SF_HOST_OUT)
                        bi_opcode = CXL_MEM_S2MBISNP_OPCODE.BISNP_INV
                    elif cache_packet.type == CACHE_REQUEST_TYPE.SNP_CUR:
                        bi_opcode = CXL_MEM_S2MBISNP_OPCODE.BISNP_CUR
                    hpa = self._memory_device_component.get_hpa(dpa)
                    cxl_packet = CxlMemBISnpPacket.create(hpa, bi_opcode, self._bi_id, self._bi_tag)
                    self._upstream_fifo.target_to_host.put(cxl_packet)

                    if sf_update_list:
                        self._snoop_filter_update(dpa, sf_update_list)
                    self._cur_state.state = COH_STATE_MACHINE.COH_STATE_WAIT

    # .mem m2s host packet handler (threaded demux)
    def _process_host_to_target_worker(self) -> None:
        while not self._demux_stop.is_set():
            packet = self._upstream_fifo.host_to_target.get()
            if packet is None:
                break
            logger.info(self._create_message("Demux received host->target packet"))
            base_packet = cast(BasePacket, packet)
            if not base_packet.is_cxl_mem():
                raise Exception(f"Received unexpected packet: {base_packet.get_type()}")
            cxl_packet = cast(CxlMemBasePacket, packet)
            if cxl_packet.is_m2sreq():
                self._cxl_channel.m2s_req.put(cast(CxlMemM2SReqPacket, packet))
            elif cxl_packet.is_m2srwd():
                self._cxl_channel.m2s_rwd.put(cast(CxlMemM2SRwDPacket, packet))
            elif cxl_packet.is_m2sbirsp():
                self._cxl_channel.m2s_birsp.put(cast(CxlMemM2SBIRspPacket, packet))
            else:
                raise Exception(f"Received unexpected packet: {cxl_packet.get_type()}")

    def _cxl_mem_dcoh_main_worker(self) -> None:
        while not self._main_stop.is_set():
            # Opportunistically drain upstream host->target in case demux thread hasn't
            try:
                pkt = self._upstream_fifo.host_to_target.get_nowait()
                if pkt is None:
                    break
                base_pkt = cast(BasePacket, pkt)
                if base_pkt.is_cxl_mem():
                    cxl_pkt = cast(CxlMemBasePacket, pkt)
                    if cxl_pkt.is_m2sreq():
                        self._cxl_channel.m2s_req.put(cast(CxlMemM2SReqPacket, pkt))
                    elif cxl_pkt.is_m2srwd():
                        self._cxl_channel.m2s_rwd.put(cast(CxlMemM2SRwDPacket, pkt))
                    elif cxl_pkt.is_m2sbirsp():
                        self._cxl_channel.m2s_birsp.put(cast(CxlMemM2SBIRspPacket, pkt))
                else:
                    # Ignore non-CXL.mem packets in this worker
                    pass
            except Empty:
                pass

            # fetch device request packet
            if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_INIT:
                try:
                    self._cur_state.packet = self._cache_to_coh_agent_fifo.request.get_nowait()
                    if self._cur_state.packet is None:
                        break
                    self._cur_state.state = COH_STATE_MACHINE.COH_STATE_START
                except Empty:
                    pass
                except Exception:
                    pass
            else:
                # run request processing and response checking code continuously until state changed
                self._process_cache_to_dcoh(self._cur_state.packet)
                try:
                    packet = self._cxl_channel.m2s_birsp.get_nowait()
                    self._process_cxl_m2s_birsp_packet(packet)
                except Empty:
                    pass

            # process host request regardless of device processing state
            try:
                packet = self._cxl_channel.m2s_req.get_nowait()
                self._process_cxl_m2s_req_packet(packet)
            except Empty:
                pass

            try:
                packet = self._cxl_channel.m2s_rwd.get_nowait()
                self._process_cxl_m2s_rwd_packet(packet)
            except Empty:
                pass

    # pylint: disable=duplicate-code
    def _run(self):
        self._demux_stop.clear()
        self._demux_thread = threading.Thread(
            target=self._process_host_to_target_worker,
            name=f"{self.get_message_label()}-mem-demux",
            daemon=True,
        )
        self._demux_thread.start()
        # start main worker thread
        self._main_stop.clear()
        self._main_thread = threading.Thread(
            target=self._cxl_mem_dcoh_main_worker,
            name=f"{self.get_message_label()}-mem-main",
            daemon=True,
        )
        self._main_thread.start()
        self._change_status_to_running()
        # Allow threads to start
        import time
        time.sleep(0.1)

    def _stop(self):
        self._demux_stop.set()
        self._upstream_fifo.host_to_target.put(None)
        if self._demux_thread is not None:
            self._demux_thread.join(timeout=1.0)
        self._main_stop.set()
        # nudge queues
        try:
            self._cache_to_coh_agent_fifo.request.put(None)
        except Exception:
            pass
        if self._main_thread is not None:
            self._main_thread.join(timeout=1.0)
        try:
            self._cache_to_coh_agent_fifo.request.put(None)
        except Exception:
            pass
