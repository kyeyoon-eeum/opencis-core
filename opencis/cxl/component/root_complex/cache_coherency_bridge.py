"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from dataclasses import dataclass
import threading
from itertools import cycle
from typing import cast
from enum import Enum, auto

from opencis.util.logger import logger
from opencis.util.component import RunnableComponent
from opencis.pci.component.fifo_pair import FifoPair
from opencis.cxl.transport.memory_fifo import (
    MemoryFifoPair,
    MemoryRequest,
    MEMORY_REQUEST_TYPE,
)
from opencis.cxl.transport.cache_fifo import (
    CacheFifoPair,
    CacheRequest,
    CACHE_REQUEST_TYPE,
    CacheResponse,
    CACHE_RESPONSE_STATUS,
)
from opencis.cxl.transport.common import BasePacket
from opencis.cxl.transport.cxl_cache_packets import (
    CxlCacheBasePacket,
    CxlCacheD2HReqPacket,
    CxlCacheD2HRspPacket,
    CxlCacheD2HDataPacket,
    CxlCacheCacheH2DReqPacket,
    CxlCacheCacheH2DRspPacket,
    CxlCacheCacheH2DDataPacket,
)
from opencis.cxl.transport.packet_constants import (
    CXL_CACHE_H2DREQ_OPCODE,
    CXL_CACHE_H2DRSP_OPCODE,
    CXL_CACHE_H2DRSP_CACHE_STATE,
    CXL_CACHE_D2HREQ_OPCODE,
    CXL_CACHE_D2HRSP_OPCODE,
)
from opencis.cxl.component.cache_controller import (
    CohStateMachine,
    COH_STATE_MACHINE,
)


# snoop filter update type definition
# device cache's snoop filter insertion/deletion
class SF_UPDATE_TYPE(Enum):
    SF_DEVICE_IN = auto()
    SF_DEVICE_OUT = auto()





@dataclass
class CacheCoherencyBridgeConfig:
    host_name: str
    memory_producer_fifos: MemoryFifoPair
    upstream_cache_to_coh_bridge_fifo: CacheFifoPair
    upstream_coh_bridge_to_cache_fifo: CacheFifoPair
    downstream_cxl_cache_fifos: FifoPair


class CacheCoherencyBridge(RunnableComponent):
    def __init__(self, config: CacheCoherencyBridgeConfig):
        super().__init__(lambda class_name: f"{config.host_name}:{class_name}")

        self._memory_producer_fifos = config.memory_producer_fifos
        self._upstream_cache_to_coh_bridge_fifo = config.upstream_cache_to_coh_bridge_fifo
        self._upstream_coh_bridge_to_cache_fifo = config.upstream_coh_bridge_to_cache_fifo
        self._downstream_cxl_cache_fifos = config.downstream_cxl_cache_fifos

        # snoop filter defined as set structure
        # max sf size will be the same as each cache size
        self._num_cache_devices = 1
        self._cur_state = CohStateMachine(
            state=COH_STATE_MACHINE.COH_STATE_INIT,
            packet=None,
            cache_rsp=CACHE_RESPONSE_STATUS.RSP_I,
            cache_list=[],
            birsp_sched=False,
        )
        self._sf_device = [set() for _ in range(self._num_cache_devices)]

        # Store pending packets for consumption by request processing
        self._pending_d2h_data: CxlCacheD2HDataPacket | None = None
        self._pending_memory_response = None

        self._loop = None
        self._state_lock = threading.Lock()
        self._downstream_worker_thread: threading.Thread | None = None
        self._upstream_req_worker_thread: threading.Thread | None = None
        self._upstream_rsp_worker_thread: threading.Thread | None = None
        self._memory_worker_thread: threading.Thread | None = None

        self._uqid_gen = cycle(range(0, 4096))

    def set_cache_coh_dev_count(self, count: int):
        self._num_cache_devices = count
        self._sf_device = [set() for _ in range(self._num_cache_devices)]

    def get_next_uqid(self) -> int:
        return next(self._uqid_gen)

    def _snoop_filter_update(self, addr: int, cache_id: int, sf_update_list: list) -> None:
        for sf_type in sf_update_list:
            if sf_type == SF_UPDATE_TYPE.SF_DEVICE_IN:
                self._sf_device[cache_id].add(addr)
            elif sf_type == SF_UPDATE_TYPE.SF_DEVICE_OUT:
                self._sf_device[cache_id].discard(addr)

    def _snoop_filter_find_cache_list(self, addr: int, cache_id: int = None) -> list:
        cache_list = []
        for i in range(self._num_cache_devices):
            if i == cache_id:
                continue
            if addr in self._sf_device[i]:
                cache_list.append(i)
        return cache_list

    def _snoop_invalidate_caches(self, addr: int, cache_list: list):
        # invalidate all cachelines
        for _, cache_id in enumerate(cache_list):
            opcode = CXL_CACHE_H2DREQ_OPCODE.SNP_INV
            cxl_packet = CxlCacheCacheH2DReqPacket.create(addr, cache_id, opcode)
            self._downstream_cxl_cache_fifos.host_to_target.put(cxl_packet)

    def _snoop_read_latest_data(self, addr: int, cache_list: list, opcode: CXL_CACHE_H2DREQ_OPCODE):
        assert len(cache_list) == 1

        cache_id = cache_list[0]
        self._cur_state.cache_rsp = CACHE_RESPONSE_STATUS.OK
        cxl_packet = CxlCacheCacheH2DReqPacket.create(addr, cache_id, opcode)
        self._downstream_cxl_cache_fifos.host_to_target.put(cxl_packet)

    def _sync_memory_read(self, addr: int) -> int:
        mem_packet = MemoryRequest(MEMORY_REQUEST_TYPE.READ, addr, 64)
        self._memory_producer_fifos.request.put(mem_packet)
        packet = self._memory_producer_fifos.response.get()

        return packet.data

    # .cache d2h req handler
    def _process_cxl_d2h_req_packet(self, d2hreq_packet: CxlCacheD2HReqPacket):
        with self._state_lock:
            if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_WAIT:
                return

            addr = d2hreq_packet.get_address()
            cache_id = d2hreq_packet.d2hreq_header.cache_id
            cqid = d2hreq_packet.d2hreq_header.cqid
            sf_update_list = []

            if d2hreq_packet.d2hreq_header.cache_opcode == CXL_CACHE_D2HREQ_OPCODE.CACHE_RD_OWN_NO_DATA:
                if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_START:
                    self._cur_state.cache_list = self._snoop_filter_find_cache_list(addr, cache_id)
                    # device cache snoop filter miss
                    if not self._cur_state.cache_list:
                        self._cur_state.state = COH_STATE_MACHINE.COH_STATE_DONE
                    # snoop needs to wait until all invalid requests are finished
                    else:
                        self._snoop_invalidate_caches(addr, self._cur_state.cache_list)
                        self._cur_state.state = COH_STATE_MACHINE.COH_STATE_WAIT

                # invalidate host cache and return to the target device
                if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_DONE:
                    cache_packet = CacheRequest(CACHE_REQUEST_TYPE.SNP_INV, addr)
                    self._upstream_coh_bridge_to_cache_fifo.request.put(cache_packet)
                    packet = self._upstream_coh_bridge_to_cache_fifo.response.get()

                    cxl_packet = CxlCacheCacheH2DRspPacket.create(
                        cache_id, CXL_CACHE_H2DRSP_OPCODE.GO, CXL_CACHE_H2DRSP_CACHE_STATE.EXCLUSIVE
                    )
                    sf_update_list.append(SF_UPDATE_TYPE.SF_DEVICE_IN)
                    self._downstream_cxl_cache_fifos.host_to_target.put(cxl_packet)
                    self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
            elif d2hreq_packet.d2hreq_header.cache_opcode == CXL_CACHE_D2HREQ_OPCODE.CACHE_CLEAN_EVICT:
                if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_START:
                    cxl_packet = CxlCacheCacheH2DRspPacket.create(
                        cache_id,
                        CXL_CACHE_H2DRSP_OPCODE.GO_WRITE_PULL_DROP,
                        self.get_next_uqid(),  # fake UQID allocation
                        cqid=cqid,
                    )
                    self._downstream_cxl_cache_fifos.host_to_target.put(cxl_packet)
                    self._cur_state.state = COH_STATE_MACHINE.COH_STATE_DONE

                elif self._cur_state.state == COH_STATE_MACHINE.COH_STATE_DONE:
                    sf_update_list.append(SF_UPDATE_TYPE.SF_DEVICE_OUT)
                    self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT

            elif (
                d2hreq_packet.d2hreq_header.cache_opcode
                == CXL_CACHE_D2HREQ_OPCODE.CACHE_CLEAN_EVICT_NO_DATA
            ):
                if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_START:
                    cxl_packet = CxlCacheCacheH2DRspPacket.create(
                        cache_id,
                        CXL_CACHE_H2DRSP_OPCODE.GO,
                        CXL_CACHE_H2DRSP_CACHE_STATE.INVALID,  # MESI for GO mesgs
                        cqid=cqid,
                    )
                    self._downstream_cxl_cache_fifos.host_to_target.put(cxl_packet)
                    self._cur_state.state = COH_STATE_MACHINE.COH_STATE_DONE

                elif self._cur_state.state == COH_STATE_MACHINE.COH_STATE_DONE:
                    sf_update_list.append(SF_UPDATE_TYPE.SF_DEVICE_OUT)
                    self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT

            elif d2hreq_packet.d2hreq_header.cache_opcode == CXL_CACHE_D2HREQ_OPCODE.CACHE_DIRTY_EVICT:
                if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_START:
                    cxl_packet = CxlCacheCacheH2DRspPacket.create(
                        cache_id,
                        CXL_CACHE_H2DRSP_OPCODE.GO_WRITE_PULL,
                        self.get_next_uqid(),  # fake UQID allocation
                        cqid=cqid,
                    )
                    self._downstream_cxl_cache_fifos.host_to_target.put(cxl_packet)
                    self._cur_state.state = COH_STATE_MACHINE.COH_STATE_DONE

                elif self._cur_state.state == COH_STATE_MACHINE.COH_STATE_DONE:
                    if self._pending_d2h_data is None:
                        return
                    packet = self._pending_d2h_data
                    addr = self._cur_state.packet.get_address()
                    mem_packet = MemoryRequest(
                        MEMORY_REQUEST_TYPE.WRITE, addr, 64, packet.get_data_as_int()
                    )
                    self._memory_producer_fifos.request.put(mem_packet)
                    sf_update_list.append(SF_UPDATE_TYPE.SF_DEVICE_OUT)
                    self._pending_d2h_data = None
                    self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT

            elif d2hreq_packet.d2hreq_header.cache_opcode == CXL_CACHE_D2HREQ_OPCODE.CACHE_RD_SHARED:
                if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_START:
                    self._cur_state.cache_list = self._snoop_filter_find_cache_list(addr, cache_id)
                    # device cache snoop filter miss
                    if not self._cur_state.cache_list or len(self._cur_state.cache_list) > 1:
                        self._cur_state.cache_rsp = CACHE_RESPONSE_STATUS.OK
                        self._cur_state.state = COH_STATE_MACHINE.COH_STATE_DONE
                    # snoop needs to wait until exclusive read request is finished
                    else:
                        self._snoop_read_latest_data(
                            addr, self._cur_state.cache_list, CXL_CACHE_H2DREQ_OPCODE.SNP_DATA
                        )
                        self._cur_state.state = COH_STATE_MACHINE.COH_STATE_WAIT

                # share host cache and return to the target device
                if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_DONE:
                    if self._cur_state.cache_rsp == CACHE_RESPONSE_STATUS.RSP_M:
                        if self._pending_d2h_data is None:
                            return
                        packet = self._pending_d2h_data
                        data = packet.data
                        self._pending_d2h_data = None
                    else:
                        cache_packet = CacheRequest(CACHE_REQUEST_TYPE.SNP_DATA, addr)
                        self._upstream_coh_bridge_to_cache_fifo.request.put(cache_packet)
                        packet = self._upstream_coh_bridge_to_cache_fifo.response.get()

                        if packet.status == CACHE_RESPONSE_STATUS.RSP_MISS:
                            data = self._sync_memory_read(addr)
                        else:
                            data = packet.data

                    cxl_packet = CxlCacheCacheH2DRspPacket.create(
                        cache_id, CXL_CACHE_H2DRSP_OPCODE.GO, CXL_CACHE_H2DRSP_CACHE_STATE.SHARED
                    )
                    sf_update_list.append(SF_UPDATE_TYPE.SF_DEVICE_IN)
                    self._downstream_cxl_cache_fifos.host_to_target.put(cxl_packet)

                    cxl_packet = CxlCacheCacheH2DDataPacket.create(cache_id, data, cqid)
                    self._downstream_cxl_cache_fifos.host_to_target.put(cxl_packet)
                    self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT

            if sf_update_list:
                self._snoop_filter_update(addr, cache_id, sf_update_list)

    # .cache d2h rsp handler
    def _process_cxl_d2h_rsp_packet(self, d2hrsp_packet: CxlCacheD2HRspPacket):
        with self._state_lock:
            sf_update_list = []

            if d2hrsp_packet.d2hrsp_header.cache_opcode < CXL_CACHE_D2HRSP_OPCODE.RSP_S_FWD_M:
                if d2hrsp_packet.d2hrsp_header.cache_opcode in (
                    CXL_CACHE_D2HRSP_OPCODE.RSP_I_HIT_I,
                    CXL_CACHE_D2HRSP_OPCODE.RSP_I_HIT_SE,
                ):
                    self._cur_state.cache_rsp = CACHE_RESPONSE_STATUS.RSP_I
                    sf_update_list.append(SF_UPDATE_TYPE.SF_DEVICE_OUT)
                elif d2hrsp_packet.d2hrsp_header.cache_opcode == CXL_CACHE_D2HRSP_OPCODE.RSP_S_HIT_SE:
                    self._cur_state.cache_rsp = CACHE_RESPONSE_STATUS.RSP_S

                assert len(self._cur_state.cache_list) != 0
                cache_id = self._cur_state.cache_list.pop()

                if len(self._cur_state.cache_list) == 0:
                    self._cur_state.state = COH_STATE_MACHINE.COH_STATE_DONE

            else:
                if d2hrsp_packet.d2hrsp_header.cache_opcode in (
                    CXL_CACHE_D2HRSP_OPCODE.RSP_S_FWD_M,
                    CXL_CACHE_D2HRSP_OPCODE.RSP_V_FWD_V,
                ):
                    self._cur_state.cache_rsp = CACHE_RESPONSE_STATUS.RSP_M
                elif d2hrsp_packet.d2hrsp_header.cache_opcode == CXL_CACHE_D2HRSP_OPCODE.RSP_I_FWD_M:
                    self._cur_state.cache_rsp = CACHE_RESPONSE_STATUS.RSP_I
                    sf_update_list.append(SF_UPDATE_TYPE.SF_DEVICE_OUT)
                cache_id = self._cur_state.cache_list.pop()
                self._cur_state.state = COH_STATE_MACHINE.COH_STATE_DONE

            if sf_update_list:
                addr = self._cur_state.packet.get_address()
                self._snoop_filter_update(addr, cache_id, sf_update_list)

    # .cache h2d packet process
    # pylint: disable=duplicate-code
    def _process_upstream_host_to_target_packets(self, cache_packet: CacheRequest):
        with self._state_lock:
            if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_WAIT:
                return

            if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_START:
                addr = cache_packet.addr

                if cache_packet.type in (
                    CACHE_REQUEST_TYPE.WRITE_BACK,
                    CACHE_REQUEST_TYPE.WRITE_BACK_CLEAN,
                ):
                    if cache_packet.type == CACHE_REQUEST_TYPE.WRITE_BACK:
                        mem_packet = MemoryRequest(
                            MEMORY_REQUEST_TYPE.WRITE, addr, cache_packet.size, cache_packet.data
                        )
                        self._memory_producer_fifos.request.put(mem_packet)
                    cache_packet = CacheResponse(CACHE_RESPONSE_STATUS.OK)
                    self._upstream_cache_to_coh_bridge_fifo.response.put(cache_packet)
                    self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
                else:
                    # device cache snoop filter miss
                    # host can access without sending any transaction to the devices whatsoever
                    self._cur_state.cache_list = self._snoop_filter_find_cache_list(addr)
                    if not self._cur_state.cache_list:
                        if cache_packet.type == CACHE_REQUEST_TYPE.SNP_INV:
                            status = CACHE_RESPONSE_STATUS.RSP_I
                            cache_packet = CacheResponse(status)
                        else:
                            if cache_packet.type == CACHE_REQUEST_TYPE.SNP_DATA:
                                status = CACHE_RESPONSE_STATUS.RSP_S
                            elif cache_packet.type == CACHE_REQUEST_TYPE.SNP_CUR:
                                status = CACHE_RESPONSE_STATUS.RSP_V
                            data = self._sync_memory_read(addr)
                            cache_packet = CacheResponse(status, data)
                        self._upstream_cache_to_coh_bridge_fifo.response.put(cache_packet)
                        self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
                    # device cache snoop filter hit
                    # host needs to resolve coherency for the requested line
                    elif cache_packet.type == CACHE_REQUEST_TYPE.SNP_INV:
                        self._snoop_invalidate_caches(addr, self._cur_state.cache_list)
                        self._cur_state.state = COH_STATE_MACHINE.COH_STATE_WAIT
                    else:
                        # cacheline is in shared status
                        if len(self._cur_state.cache_list) > 1:
                            data = self._sync_memory_read(addr)
                            if cache_packet.type == CACHE_REQUEST_TYPE.SNP_DATA:
                                status = CACHE_RESPONSE_STATUS.RSP_S
                            elif cache_packet.type == CACHE_REQUEST_TYPE.SNP_CUR:
                                status = CACHE_RESPONSE_STATUS.RSP_V
                            cache_packet = CacheResponse(status, data)
                            self._upstream_cache_to_coh_bridge_fifo.response.put(cache_packet)
                            self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT
                        # cacheline is in modified or exclusive status
                        else:
                            if cache_packet.type == CACHE_REQUEST_TYPE.SNP_DATA:
                                opcode = CXL_CACHE_H2DREQ_OPCODE.SNP_DATA
                            elif cache_packet.type == CACHE_REQUEST_TYPE.SNP_CUR:
                                opcode = CXL_CACHE_H2DREQ_OPCODE.SNP_CUR
                            self._snoop_read_latest_data(addr, self._cur_state.cache_list, opcode)
                            self._cur_state.state = COH_STATE_MACHINE.COH_STATE_WAIT

            if self._cur_state.state == COH_STATE_MACHINE.COH_STATE_DONE:
                if self._cur_state.cache_rsp in (
                    CACHE_RESPONSE_STATUS.RSP_I,
                    CACHE_RESPONSE_STATUS.RSP_S,
                ):
                    addr = self._cur_state.packet.addr
                    data = self._sync_memory_read(addr)
                elif self._cur_state.cache_rsp == CACHE_RESPONSE_STATUS.RSP_M:
                    if self._pending_d2h_data is None:
                        return
                    packet = self._pending_d2h_data
                    data = packet.get_data_as_int()
                    self._pending_d2h_data = None
                else:  # Unsupported for now
                    assert 0
                cache_packet = CacheResponse(self._cur_state.cache_rsp, data)
                self._upstream_cache_to_coh_bridge_fifo.response.put(cache_packet)
                self._cur_state.state = COH_STATE_MACHINE.COH_STATE_INIT

    # Downstream worker: handles D2H packets from device
    def _process_downstream_packets_worker(self) -> None:
        while True:
            packet = self._downstream_cxl_cache_fifos.target_to_host.get()
            if packet is None:
                break

            base_packet = cast(BasePacket, packet)
            if not base_packet.is_cxl_cache():
                raise Exception(f"Received unexpected packet: {base_packet.get_type()}")
            cxl_packet = cast(CxlCacheBasePacket, packet)

            # Process packets directly based on type
            if cxl_packet.is_d2hreq():
                self._process_cxl_d2h_req_packet(cast(CxlCacheD2HReqPacket, packet))
            elif cxl_packet.is_d2hrsp():
                self._process_cxl_d2h_rsp_packet(cast(CxlCacheD2HRspPacket, packet))
            elif cxl_packet.is_d2hdata():
                # Handle D2H data packets - these need special handling as they're consumed
                # by the request processing logic when needed
                # For now, we'll store it for later consumption
                self._pending_d2h_data = cast(CxlCacheD2HDataPacket, packet)
            else:
                raise Exception(f"Received unexpected packet: {cxl_packet.get_type()}")

    # Upstream request worker: handles cache requests from upstream
    def _process_upstream_requests_worker(self) -> None:
        while True:
            cache_packet = self._upstream_cache_to_coh_bridge_fifo.request.get()
            if cache_packet is None:
                logger.debug(self._create_message("Stop processing upstream cache requests"))
                break

            # Process upstream cache request directly
            self._process_upstream_host_to_target_packets(cache_packet)

    # Upstream response worker: handles responses from cache operations
    def _process_upstream_responses_worker(self) -> None:
        while True:
            cache_packet = self._upstream_coh_bridge_to_cache_fifo.response.get()
            if cache_packet is None:
                logger.debug(self._create_message("Stop processing upstream cache responses"))
                break

            # Process cache response - this would typically be handled by the request processing logic
            # For now, we'll just log it as the response handling is integrated into the request processing
            logger.debug(self._create_message(f"Received cache response: {cache_packet.status}"))

    # Memory response worker: handles responses from memory operations
    def _process_memory_responses_worker(self) -> None:
        while True:
            memory_packet = self._memory_producer_fifos.response.get()
            if memory_packet is None:
                logger.debug(self._create_message("Stop processing memory responses"))
                break

            # Memory responses are typically consumed synchronously in the request processing
            # For now, we'll store it for later consumption if needed
            self._pending_memory_response = memory_packet

    def _run(self):
        # Start downstream worker for D2H packets
        self._downstream_worker_thread = threading.Thread(
            target=self._process_downstream_packets_worker,
            name=f"{self.get_message_label()}-downstream",
            daemon=True,
        )
        self._downstream_worker_thread.start()

        # Start upstream request worker for cache requests
        self._upstream_req_worker_thread = threading.Thread(
            target=self._process_upstream_requests_worker,
            name=f"{self.get_message_label()}-upstream-req",
            daemon=True,
        )
        self._upstream_req_worker_thread.start()

        # Start upstream response worker for cache responses
        self._upstream_rsp_worker_thread = threading.Thread(
            target=self._process_upstream_responses_worker,
            name=f"{self.get_message_label()}-upstream-rsp",
            daemon=True,
        )
        self._upstream_rsp_worker_thread.start()

        # Start memory response worker
        self._memory_worker_thread = threading.Thread(
            target=self._process_memory_responses_worker,
            name=f"{self.get_message_label()}-memory",
            daemon=True,
        )
        self._memory_worker_thread.start()

        self._change_status_to_running()
        # Block until threads finish
        self._downstream_worker_thread.join()
        self._upstream_req_worker_thread.join()
        self._upstream_rsp_worker_thread.join()
        self._memory_worker_thread.join()

    def _stop(self):
        # Stop downstream worker
        try:
            self._downstream_cxl_cache_fifos.target_to_host.put(None)
        except Exception:
            pass
        if self._downstream_worker_thread is not None:
            self._downstream_worker_thread.join(timeout=1.0)

        # Stop upstream request worker
        try:
            self._upstream_cache_to_coh_bridge_fifo.request.put(None)
        except Exception:
            pass
        if self._upstream_req_worker_thread is not None:
            self._upstream_req_worker_thread.join(timeout=1.0)

        # Stop upstream response worker
        try:
            self._upstream_coh_bridge_to_cache_fifo.response.put(None)
        except Exception:
            pass
        if self._upstream_rsp_worker_thread is not None:
            self._upstream_rsp_worker_thread.join(timeout=1.0)

        # Stop memory worker
        try:
            self._memory_producer_fifos.response.put(None)
        except Exception:
            pass
        if self._memory_worker_thread is not None:
            self._memory_worker_thread.join(timeout=1.0)
