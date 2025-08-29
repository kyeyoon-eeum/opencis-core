"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from typing import Optional, cast
import threading

from opencis.util.logger import logger
from opencis.pci.component.fifo_pair import FifoPair
from opencis.cxl.transport.common import BasePacket
from opencis.cxl.transport.cxl_mem_packets import (
    CxlMemBasePacket,
    CxlMemM2SBIRspPacket,
    CxlMemM2SReqPacket,
    CxlMemM2SRwDPacket,
    CxlMemBIRspPacket,
    CxlMemBISnpPacket,
    CxlMemMemRdPacket,
    CxlMemMemWrPacket,
    CxlMemMemDataPacket,
    CxlMemCmpPacket,
)
from opencis.cxl.transport.packet_constants import CXL_MEM_S2MNDR_OPCODE
from opencis.cxl.component.cxl_memory_device_component import CxlMemoryDeviceComponent
from opencis.pci.component.packet_processor import PacketProcessor


class CxlMemManager(PacketProcessor):
    def __init__(
        self,
        upstream_fifo: FifoPair,
        downstream_fifo: Optional[FifoPair] = None,
        label: Optional[str] = None,
    ):
        self._downstream_fifo: Optional[FifoPair]
        self._upstream_fifo: FifoPair

        super().__init__(upstream_fifo, downstream_fifo, label)
        self._memory_device_component: Optional[CxlMemoryDeviceComponent] = None
        self._loop = None
        self._h2t_thread: threading.Thread | None = None
        self._h2t_stop = threading.Event()
        self._t2h_thread: threading.Thread | None = None
        self._t2h_stop = threading.Event()

    def set_memory_device_component(self, memory_device_component: CxlMemoryDeviceComponent):
        self._memory_device_component = memory_device_component

    def _process_cxl_mem_rd_packet_sync(self, mem_rd_packet: CxlMemMemRdPacket):
        if self._downstream_fifo is not None:
            logger.debug(self._create_message("Forwarding CXL.mem MEM_RD packet"))
            self._downstream_fifo.host_to_target.put(mem_rd_packet)
            return

        if self._memory_device_component is None:
            raise Exception("CxlMemoryDeviceComponent isn't set yet")

        addr = mem_rd_packet.get_address()
        logger.info(self._create_message(f"DEVICE MEM_RD addr=0x{addr:x}"))
        data = self._memory_device_component.read_mem_sync(addr)
        ld_id = mem_rd_packet.m2sreq_header.ld_id
        logger.debug(self._create_message(f"CXL.mem Read: HPA addr:0x{addr:08x} LD-ID:{ld_id}"))

        # Send NDR (Cmp-Status) followed by DRS (MemData) so host can proceed
        ndr_packet = CxlMemCmpPacket.create(ld_id=ld_id)
        data_packet = CxlMemMemDataPacket.create(data, ld_id=ld_id)
        logger.debug(self._create_message("DEVICE sending NDR+DRS"))
        self._upstream_fifo.target_to_host.put(ndr_packet)
        self._upstream_fifo.target_to_host.put(data_packet)

    def _process_cxl_mem_wr_packet_sync(self, mem_wr_packet: CxlMemMemWrPacket):
        if self._downstream_fifo is not None:
            logger.debug(self._create_message("Forwarding CXL.mem MEM_WR packet"))
            self._downstream_fifo.host_to_target.put(mem_wr_packet)
            return

        if self._memory_device_component is None:
            raise Exception("CxlMemoryDeviceComponent isn't set yet")

        addr = mem_wr_packet.get_address()
        data = mem_wr_packet.get_data_as_int()
        ld_id = mem_wr_packet.m2srwd_header.ld_id
        logger.debug(
            self._create_message(
                f"CXL.mem Write: HPA addr:0x{addr:08x} LD-ID:{ld_id} Data:0x{data:08x}"
            )
        )
        self._memory_device_component.write_mem_sync(addr, data)

        packet = CxlMemCmpPacket.create(ld_id=ld_id)
        self._upstream_fifo.target_to_host.put(packet)

    def process_cxl_mem_bisnp_packet(self, mem_bisnp_packet: CxlMemBISnpPacket):
        self._process_cxl_mem_bisnp_packet_sync(mem_bisnp_packet)

    def _process_cxl_mem_bisnp_packet_sync(self, mem_bisnp_packet: CxlMemBISnpPacket):
        if self._upstream_fifo is not None:
            logger.debug(self._create_message("Forwarding CXL.mem MEM_BISNP packet"))
            self._upstream_fifo.target_to_host.put(mem_bisnp_packet)
            return

    def _process_cxl_mem_birsp_packet_sync(self, mem_birsp_packet: CxlMemBIRspPacket):
        if self._downstream_fifo is not None:
            logger.debug(self._create_message("Forwarding CXL.mem MEM_BIRSP packet"))
            self._downstream_fifo.host_to_target.put(mem_birsp_packet)
            return
        # TODO: add logics for handling BIRsp packets
        logger.debug(self._create_message("Reached _process_cxl_mem_birsp_packet"))
        return

    def _process_host_to_target(self):
        # Sync fallback (not used in thread mode)
        logger.debug(self._create_message("Started processing incoming fifo (sync)"))
        while True:
            packet = self._upstream_fifo.host_to_target.get()
            if packet is None:
                logger.debug(self._create_message("Stopped processing incoming fifo (sync)"))
                break
            self._dispatch_mem_packet_sync(packet)

    def _h2t_worker(self) -> None:
        while not self._h2t_stop.is_set():
            packet = self._upstream_fifo.host_to_target.get()
            if packet is None:
                break
            self._dispatch_mem_packet_sync(packet)

    def _t2h_worker(self) -> None:
        if self._downstream_fifo is None:
            return
        while not self._t2h_stop.is_set():
            packet = self._downstream_fifo.target_to_host.get()
            if packet is None:
                break
            self._upstream_fifo.target_to_host.put(packet)

    def _dispatch_mem_packet_sync(self, packet: BasePacket):
        base_packet = cast(BasePacket, packet)
        if not base_packet.is_cxl_mem():
            raise Exception(f"Received unexpected packet: {base_packet.get_type()}")

        logger.debug(self._create_message("Received incoming packet"))
        cxl_mem_packet = cast(CxlMemBasePacket, packet)

        if cxl_mem_packet.is_m2sreq():
            m2sreq_packet = cast(CxlMemM2SReqPacket, packet)
            if m2sreq_packet.is_mem_rd() or m2sreq_packet.is_mem_inv():
                self._process_cxl_mem_rd_packet_sync(cast(CxlMemMemRdPacket, m2sreq_packet))
            else:
                raise Exception(
                    f"Unsupported MEM Opcode for Req: {m2sreq_packet.m2sreq_header.mem_opcode}"
                )
        elif cxl_mem_packet.is_m2srwd():
            m2srwd_packet = cast(CxlMemM2SRwDPacket, packet)
            if m2srwd_packet.is_mem_wr():
                self._process_cxl_mem_wr_packet_sync(cast(CxlMemMemWrPacket, m2srwd_packet))
            else:
                raise Exception(
                    f"Unsupported MEM Opcode for RwD: {m2srwd_packet.m2srwd_header.mem_opcode}"
                )
        elif cxl_mem_packet.is_m2sbirsp():
            m2sbirsp_packet = cast(CxlMemM2SBIRspPacket, packet)
            if m2sbirsp_packet.is_m2sbirsp():
                self._process_cxl_mem_birsp_packet_sync(cast(CxlMemBIRspPacket, m2sbirsp_packet))
            else:
                raise Exception(
                    f"Unsupported BIRsp packet, tag: {m2sbirsp_packet.m2sbirsp_header.bi_tag}"
                )
        else:
            raise Exception(f"Received unexpected packet: {base_packet.get_type()}")

    def _run(self):
        self._h2t_stop.clear()
        self._h2t_thread = threading.Thread(
            target=self._h2t_worker, name=f"{self._label or ''}-mem-h2t", daemon=True
        )
        self._h2t_thread.start()
        if self._downstream_fifo is not None:
            self._t2h_stop.clear()
            self._t2h_thread = threading.Thread(
                target=self._t2h_worker, name=f"{self._label or ''}-mem-t2h", daemon=True
            )
            self._t2h_thread.start()
        self._change_status_to_running()
        # Block until stopped
        if self._h2t_thread is not None:
            self._h2t_thread.join()
        if self._t2h_thread is not None:
            self._t2h_thread.join()

    def _stop(self):
        self._h2t_stop.set()
        try:
            self._upstream_fifo.host_to_target.put(None)
        except Exception:
            pass
        try:
            if self._h2t_thread is not None:
                self._h2t_thread.join(timeout=1.0)
        except Exception:
            pass
        if self._downstream_fifo is not None:
            self._t2h_stop.set()
            try:
                self._downstream_fifo.target_to_host.put(None)
            except Exception:
                pass
            try:
                if self._t2h_thread is not None:
                    self._t2h_thread.join(timeout=1.0)
            except Exception:
                pass
