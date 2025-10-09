"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from abc import abstractmethod
from typing import List, Optional, cast

from opencis.util.logger import logger
import threading
from opencis.util.component import RunnableComponent
from opencis.util.pci import bdf_to_string
from opencis.util.number import tlptoh16
from opencis.cxl.component.cxl_connection import FifoPair
from opencis.cxl.component.virtual_switch.routing_table import RoutingTable
from opencis.cxl.component.virtual_switch.port_binder import PortBinder, BindSlot
from opencis.cxl.component.virtual_switch.upstream_vppb import UpstreamVppb
from opencis.cxl.transport.common import BasePacket
from opencis.cxl.transport.cxl_mem_packets import (
    CxlMemBasePacket,
    CxlMemM2SReqPacket,
    CxlMemM2SRwDPacket,
    CxlMemM2SBIRspPacket,
    CxlMemS2MBISnpPacket,
)
from opencis.cxl.transport.cxl_cache_packets import (
    CxlCacheBasePacket,
    CxlCacheD2HReqPacket,
    CxlCacheH2DRspPacket,
    CxlCacheH2DReqPacket,
    CxlCacheH2DDataPacket,
)
from opencis.cxl.transport.packet_constants import CXL_IO_CPL_STATUS
from opencis.cxl.transport.cxl_io_packets import (
    CxlIoBasePacket,
    CxlIoCfgRdPacket,
    CxlIoCfgWrPacket,
    CxlIoCompletionPacket,
    CxlIoMemReqPacket,
)


class CxlRouter(RunnableComponent):
    def __init__(
        self,
        vcs_id: int,
        routing_table: RoutingTable,
    ):
        self._downstream_connections: List[BindSlot]
        self._downstream_connection_fifos: List[FifoPair]
        self._upstream_connection_fifo: FifoPair

        super().__init__()
        self._vcs_id = vcs_id
        self._routing_table = routing_table
        self._is_running = False

    def _create_message(self, message):
        message = f"[{self.__class__.__name__}:VCS{self._vcs_id}] {message}"
        return message

    @abstractmethod
    def _process_host_to_target_packets(self):
        pass

    @abstractmethod
    def _process_target_to_host_packets(self, downstream_connection_bind_slot: BindSlot):
        pass

    def _run(self):
        self._is_running = True
        # Start workers in threads
        host_thread = threading.Thread(
            target=self._process_host_to_target_packets, name=f"{self.get_message_label()}-h2t"
        )
        host_thread.start()
        self._routing_tasks.add_thread(host_thread)
        for downstream_connection_bind_slot in self._downstream_connections:
            t = threading.Thread(
                target=self._process_target_to_host_packets,
                args=(downstream_connection_bind_slot,),
                name=f"{self.get_message_label()}-t2h",
            )
            t.start()
            self._routing_tasks.add_thread(t)
        self._change_status_to_running()
        self._routing_tasks.join_all()

    def _stop(self):
        self._upstream_connection_fifo.host_to_target.put(None)
        for downstream_connection_fifo in self._downstream_connection_fifos:
            downstream_connection_fifo.target_to_host.put(None)

    def stop_for_update(self, vppb_index: int):
        if self._is_running:
            self._downstream_connection_fifos[vppb_index].target_to_host.put(None)


class CxlIoRouter(RunnableComponent):
    def __init__(
        self,
        vcs_id: int,
        routing_table: RoutingTable,
        upstream_vppb: UpstreamVppb,
        port_binder: PortBinder,
    ):
        super().__init__()
        self._config_space_router = ConfigSpaceRouter(
            vcs_id, routing_table, upstream_vppb, port_binder
        )
        self._mmio_router = MmioRouter(vcs_id, routing_table, upstream_vppb, port_binder)

    def update_router(self, vppb_index: int):
        self._config_space_router.update_router(vppb_index)
        self._mmio_router.update_router(vppb_index)

    def _run(self):
        self._config_space_router.start_wait_ready()
        self._mmio_router.start_wait_ready()
        self._change_status_to_running()
        # Allow routers to start
        import time
        time.sleep(0.1)

    def _stop(self):
        self._config_space_router.stop_sync()
        self._mmio_router.stop_sync()


class MmioRouter(CxlRouter):
    def __init__(
        self,
        vcs_id: int,
        routing_table: RoutingTable,
        upstream_vppb: UpstreamVppb,
        port_binder: PortBinder,
    ):
        upstream_vppb_connection = upstream_vppb.get_downstream_connection()

        super().__init__(vcs_id, routing_table)
        self._port_binder = port_binder
        self._upstream_connection_fifo = upstream_vppb_connection.mmio_fifo
        self._downstream_connections = port_binder.get_bind_slots()
        self._downstream_connection_fifos = []
        for bind_slot in self._downstream_connections:
            self._downstream_connection_fifos.append(
                bind_slot.vppb.get_upstream_connection().mmio_fifo
            )

    def _process_host_to_target_packets(self):
        while True:
            packet = self._upstream_connection_fifo.host_to_target.get()
            if packet is None:
                break
            logger.debug(self._create_message("Received an incoming request"))
            base_packet = cast(BasePacket, packet)
            cxl_io_base_packet = cast(CxlIoBasePacket, packet)
            if not (base_packet.is_cxl_io() and cxl_io_base_packet.is_mmio()):
                raise Exception(f"Received unexpected packet: {base_packet.get_type()}")

            mmio_packet = cast(CxlIoMemReqPacket, packet)
            address = mmio_packet.get_address()
            size = mmio_packet.get_data_size()
            req_id = tlptoh16(mmio_packet.mreq_header.req_id)
            tag = mmio_packet.mreq_header.tag
            target_port = self._routing_table.get_mmio_target_port(address)
            if target_port is None:
                if mmio_packet.is_mem_read():
                    logger.debug(self._create_message(f"RD: 0x{address:x}[{size}] OOB"))
                    self._send_completion(req_id, tag, cpl_id=0, data=None)
                elif mmio_packet.is_mem_write():
                    logger.debug(self._create_message(f"WR: 0x{address:x}[{size}] OOB"))
                continue

            if target_port >= len(self._downstream_connections):
                raise Exception("target_port is out of bound")

            vppb_downstream_connection = self._downstream_connections[
                target_port
            ].vppb.get_upstream_connection()
            if vppb_downstream_connection is None:
                logger.error(
                    self._create_message(
                        f"vppb_downstream_connection for port {target_port} is None"
                    )
                )
                continue

            vppb_downstream_connection.mmio_fifo.host_to_target.put(packet)

    def _process_target_to_host_packets(self, downstream_connection_bind_slot: BindSlot):
        # Unused in thread-based implementation
        raise NotImplementedError

    def _t2h_worker(
        self,
        downstream_connection_bind_slot: BindSlot,
        stop_evt: threading.Event,
    ) -> None:
        downstream_connection_fifo = (
            downstream_connection_bind_slot.vppb.get_upstream_connection().mmio_fifo
        )
        while not stop_evt.is_set():
            packet = downstream_connection_fifo.target_to_host.get()
            if packet is None:
                break
            self._upstream_connection_fifo.target_to_host.put(packet)

    def _send_completion(
        self, req_id: int, tag: int, cpl_id: int, data: int = None, data_len: int = 0
    ):
        """
        Note that data_len should be in bytes.
        """
        packet = CxlIoCompletionPacket.create(
            req_id=req_id, tag=tag, cpl_id=cpl_id, data=data, length=data_len
        )
        self._upstream_connection_fifo.target_to_host.put(packet)

    def update_router(self, vppb_index: int):
        self.stop_for_update(vppb_index)
        self._downstream_connections = self._port_binder.get_bind_slots()
        vppb_upstream_connection = self._downstream_connections[
            vppb_index
        ].vppb.get_upstream_connection()
        if vppb_upstream_connection is None:
            logger.debug(self._create_message("vppb_upstream_connection is None"))
            return

        self._downstream_connection_fifos[vppb_index] = vppb_upstream_connection.mmio_fifo

        if self._is_running:
            # Start a new worker thread for this port
            if not hasattr(self, "_t2h_stop"):
                self._t2h_stop = threading.Event()
                self._t2h_threads = []
            t = threading.Thread(
                target=self._t2h_worker,
                args=(self._downstream_connections[vppb_index], self._t2h_stop),
                name=f"MmioRouter-t2h-{vppb_index}",
                daemon=True,
            )
            t.start()
            self._t2h_threads.append(t)

    def _run(self):
        self._is_running = True
        self._t2h_stop = threading.Event()
        self._t2h_threads: list[threading.Thread] = []
        host_thread = threading.Thread(
            target=self._process_host_to_target_packets, name="MmioRouter-h2t", daemon=True
        )
        host_thread.start()
        # Start T2H workers for each downstream
        for downstream_connection_bind_slot in self._downstream_connections:
            t = threading.Thread(
                target=self._t2h_worker,
                args=(downstream_connection_bind_slot, self._t2h_stop),
                name="MmioRouter-t2h",
                daemon=True,
            )
            t.start()
            self._t2h_threads.append(t)
        self._change_status_to_running()
        # Allow threads to start
        import time
        time.sleep(0.1)

    def _stop(self):
        self._t2h_stop.set()
        for downstream_connection_fifo in self._downstream_connection_fifos:
            downstream_connection_fifo.target_to_host.put(None)
        for t in getattr(self, "_t2h_threads", []):
            t.join(timeout=1.0)
        self._upstream_connection_fifo.host_to_target.put(None)


class ConfigSpaceRouter(CxlRouter):
    def __init__(
        self,
        vcs_id: int,
        routing_table: RoutingTable,
        upstream_vppb: UpstreamVppb,
        port_binder: PortBinder,
    ):
        upstream_vppb_connection = upstream_vppb.get_downstream_connection()
        super().__init__(vcs_id, routing_table)
        self._port_binder = port_binder
        self._upstream_connection_fifo = upstream_vppb_connection.cfg_fifo
        self._downstream_connections = port_binder.get_bind_slots()
        self._downstream_connection_fifos = []
        for bind_slot in self._downstream_connections:
            self._downstream_connection_fifos.append(
                bind_slot.vppb.get_upstream_connection().cfg_fifo
            )

    def _process_host_to_target_packets(self):
        while True:
            packet = self._upstream_connection_fifo.host_to_target.get()
            if packet is None:
                break
            logger.debug(self._create_message("Received an incoming request"))
            base_packet = cast(BasePacket, packet)
            if not base_packet.is_cxl_io():
                raise Exception(f"Received unexpected packet: {base_packet.get_type()}")

            cxl_io_packet = cast(CxlIoBasePacket, packet)
            if cxl_io_packet.is_cfg_read():
                cfg_packet = cast(CxlIoCfgRdPacket, packet)
            elif cxl_io_packet.is_cfg_write():
                cfg_packet = cast(CxlIoCfgWrPacket, packet)
            else:
                raise Exception(f"Received unexpected packet: {base_packet.get_type()}")
            dest_id = tlptoh16(cfg_packet.cfg_req_header.dest_id)

            logger.debug(self._create_message(f"Destination ID is {bdf_to_string(dest_id)}"))

            req_id = tlptoh16(cfg_packet.cfg_req_header.req_id)
            tag = cfg_packet.cfg_req_header.tag
            target_port = self._routing_table.get_config_space_target_port(dest_id)
            if target_port is None:
                logger.debug(
                    self._create_message(f"Request to {bdf_to_string(dest_id)} is not routable")
                )
                self._send_unsupported_request(req_id, tag)
                continue
            if target_port >= len(self._downstream_connections):
                logger.warning(self._create_message("target_port is out of bound"))
                self._send_unsupported_request(req_id, tag)
                continue

            logger.debug(self._create_message(f"Target port is {target_port}"))

            vppb_upstream_connection = self._downstream_connections[
                target_port
            ].vppb.get_upstream_connection()
            if vppb_upstream_connection is None:
                logger.debug(self._create_message("vppb_upstream_connection is None"))
                self._send_unsupported_request(req_id, tag)
                continue

            downstream_connection_fifo = vppb_upstream_connection.cfg_fifo
            downstream_connection_fifo.host_to_target.put(packet)

    def _process_target_to_host_packets(self, downstream_connection_bind_slot: BindSlot):
        # Unused in thread-based implementation
        raise NotImplementedError

    def _t2h_worker(
        self,
        downstream_connection_bind_slot: BindSlot,
        stop_evt: threading.Event,
    ) -> None:
        downstream_connection_fifo = (
            downstream_connection_bind_slot.vppb.get_upstream_connection().cfg_fifo
        )
        while not stop_evt.is_set():
            packet = downstream_connection_fifo.target_to_host.get()
            if packet is None:
                break
            self._upstream_connection_fifo.target_to_host.put(packet)

    def _send_unsupported_request(self, req_id, tag):
        packet = CxlIoCompletionPacket.create(
            req_id=req_id, tag=tag, cpl_id=0, data=None, status=CXL_IO_CPL_STATUS.UR
        )
        self._upstream_connection_fifo.target_to_host.put(packet)

    def update_router(self, vppb_index: int):
        self.stop_for_update(vppb_index)
        self._downstream_connections = self._port_binder.get_bind_slots()
        vppb_upstream_connection = self._downstream_connections[
            vppb_index
        ].vppb.get_upstream_connection()
        if vppb_upstream_connection is None:
            logger.debug(self._create_message("vppb_upstream_connection is None"))
            return

        self._downstream_connection_fifos[vppb_index] = vppb_upstream_connection.cfg_fifo

        if self._is_running:
            if not hasattr(self, "_t2h_stop"):
                self._t2h_stop = threading.Event()
                self._t2h_threads = []
            t = threading.Thread(
                target=self._t2h_worker,
                args=(self._downstream_connections[vppb_index], self._t2h_stop),
                name=f"CfgRouter-t2h-{vppb_index}",
                daemon=True,
            )
            t.start()
            self._t2h_threads.append(t)

    def _run(self):
        self._is_running = True
        self._t2h_stop = threading.Event()
        self._t2h_threads: list[threading.Thread] = []
        host_thread = threading.Thread(
            target=self._process_host_to_target_packets, name="CfgRouter-h2t", daemon=True
        )
        host_thread.start()
        for downstream_connection_bind_slot in self._downstream_connections:
            t = threading.Thread(
                target=self._t2h_worker,
                args=(downstream_connection_bind_slot, self._t2h_stop),
                name="CfgRouter-t2h",
                daemon=True,
            )
            t.start()
            self._t2h_threads.append(t)
        self._change_status_to_running()
        # Allow threads to start
        import time
        time.sleep(0.1)

    def _stop(self):
        self._t2h_stop.set()
        for downstream_connection_fifo in self._downstream_connection_fifos:
            downstream_connection_fifo.target_to_host.put(None)
        for t in getattr(self, "_t2h_threads", []):
            t.join(timeout=1.0)
        self._upstream_connection_fifo.host_to_target.put(None)


class CxlMemRouter(CxlRouter):
    def __init__(
        self,
        vcs_id: int,
        routing_table: RoutingTable,
        upstream_vppb: UpstreamVppb,
        port_binder: PortBinder,
        bi_enable_override_for_test: Optional[int] = None,
        bi_forward_override_for_test: Optional[int] = None,
    ):
        upstream_vppb_connection = upstream_vppb.get_downstream_connection()
        self._upstream_vppb = upstream_vppb
        self._port_binder = port_binder

        # For testing purposes
        self._bi_enable_override_for_test = bi_enable_override_for_test
        self._bi_forward_override_for_test = bi_forward_override_for_test

        super().__init__(vcs_id, routing_table)
        self._port_binder = port_binder
        self._upstream_connection_fifo = upstream_vppb_connection.cxl_mem_fifo
        self._downstream_connections = port_binder.get_bind_slots()
        self._downstream_connection_fifos = []
        for bind_slot in self._downstream_connections:
            # Use the vPPB's upstream connection for device T2H forwarding
            self._downstream_connection_fifos.append(
                bind_slot.vppb.get_upstream_connection().cxl_mem_fifo
            )

    def _process_host_to_target_packets(self):
        while True:
            packet = self._upstream_connection_fifo.host_to_target.get()
            if packet is None:
                break

            target_port = None

            cxl_mem_base_packet = cast(CxlMemBasePacket, packet)
            if cxl_mem_base_packet.is_m2sreq():
                cxl_mem_packet = cast(CxlMemM2SReqPacket, packet)
                addr = cxl_mem_packet.get_address()
                target_port = self._routing_table.get_cxl_mem_target_port(addr)
            elif cxl_mem_base_packet.is_m2srwd():
                cxl_mem_packet = cast(CxlMemM2SRwDPacket, packet)
                addr = cxl_mem_packet.get_address()
                target_port = self._routing_table.get_cxl_mem_target_port(addr)
            elif cxl_mem_base_packet.is_m2sbirsp():
                cxl_mem_bi_packet: CxlMemM2SBIRspPacket = cast(
                    CxlMemM2SBIRspPacket, cxl_mem_base_packet
                )
                for i, bind_slot in enumerate(self._downstream_connections):
                    downstream_vppb = bind_slot.vppb
                    bus = downstream_vppb.get_secondary_bus_number()
                    if bus == cxl_mem_bi_packet.m2sbirsp_header.bi_id:
                        target_port = i
                        break
            else:
                raise Exception("Received unexpected packet")

            if target_port is None:
                logger.warning(self._create_message("Received unroutable CXL.mem packet"))
                # logger.warning(self._create_message(cxl_mem_packet.get_pretty_string()))
                continue
            if target_port >= len(self._downstream_connections):
                raise Exception("target_port is out of bound")
            downstream_connection_fifo = (
                self._downstream_connections[target_port]
                .vppb.get_upstream_connection()
                .cxl_mem_fifo
            )
            downstream_connection_fifo.host_to_target.put(packet)

    def _process_target_to_host_packets(self, downstream_connection_bind_slot: BindSlot):
        # Unused in thread-based implementation
        raise NotImplementedError

    def _t2h_worker(
        self,
        downstream_connection_bind_slot: BindSlot,
        stop_evt: threading.Event,
    ) -> None:
        logger.debug(self._create_message("Starting CXL.mem T2H worker thread"))
        downstream_connection_fifo = (
            downstream_connection_bind_slot.vppb.get_upstream_connection().cxl_mem_fifo
        )
        downstream_vppb = downstream_connection_bind_slot.vppb

        downstream_vppb_component = downstream_vppb.get_cxl_component()
        upstream_vppb_component = self._upstream_vppb.get_cxl_component()
        bi_enable = self._bi_enable_override_for_test
        bi_forward = self._bi_forward_override_for_test

        while not stop_evt.is_set():
            packet = downstream_connection_fifo.target_to_host.get()
            if packet is None:
                break

            cxl_mem_base_packet: CxlMemBasePacket = cast(CxlMemBasePacket, packet)
            # keep logging quieter during normal runs
            if cxl_mem_base_packet.is_s2mbisnp():
                bi_id = downstream_vppb.get_secondary_bus_number()
                bi_decoder_options = downstream_vppb_component.get_bi_decoder_options()

                if self._bi_enable_override_for_test is None:
                    bi_enable = bi_decoder_options["control_options"]["bi_enable"]
                if self._bi_forward_override_for_test is None:
                    bi_forward = bi_decoder_options["control_options"]["bi_forward"]

                cxl_mem_bi_packet: CxlMemS2MBISnpPacket = cast(
                    CxlMemS2MBISnpPacket, cxl_mem_base_packet
                )
                if bi_enable == bi_forward:
                    continue

                if bi_enable == 0 and bi_forward == 1:
                    logger.debug(self._create_message("MEM T2H forwarding BI Snoop upstream"))
                    self._upstream_connection_fifo.target_to_host.put(packet)
                elif bi_enable == 1 and bi_forward == 0:
                    hdm_decoder_manager = upstream_vppb_component.get_hdm_decoder_manager()
                    if hdm_decoder_manager.is_bi_capable():
                        cxl_mem_bi_packet.s2mbisnp_header.bi_id = bi_id
                        logger.debug(
                            self._create_message(f"MEM T2H rewriting BI bi_id={bi_id} upstream")
                        )
                        self._upstream_connection_fifo.target_to_host.put(packet)
                    else:
                        continue
            else:
                # quiet normal forwarding logs
                self._upstream_connection_fifo.target_to_host.put(packet)

    def update_router(self, vppb_index: int):
        self.stop_for_update(vppb_index)
        self._downstream_connections = self._port_binder.get_bind_slots()
        vppb_upstream_connection = self._downstream_connections[
            vppb_index
        ].vppb.get_upstream_connection()
        if vppb_upstream_connection is None:
            logger.debug(self._create_message("vppb_upstream_connection is None"))
            return

        self._downstream_connection_fifos[vppb_index] = vppb_upstream_connection.cxl_mem_fifo

        if self._is_running:
            if not hasattr(self, "_t2h_stop"):
                self._t2h_stop = threading.Event()
                self._t2h_threads = []
            t = threading.Thread(
                target=self._t2h_worker,
                args=(self._downstream_connections[vppb_index], self._t2h_stop),
                name=f"MemRouter-t2h-{vppb_index}",
                daemon=True,
            )
            t.start()
            self._t2h_threads.append(t)

    def _run(self):
        self._is_running = True
        self._t2h_stop = threading.Event()
        self._t2h_threads: list[threading.Thread] = []
        host_thread = threading.Thread(
            target=self._process_host_to_target_packets, name="MemRouter-h2t", daemon=True
        )
        host_thread.start()
        # Start T2H workers for each downstream
        for downstream_connection_bind_slot in self._downstream_connections:
            t = threading.Thread(
                target=self._t2h_worker,
                args=(downstream_connection_bind_slot, self._t2h_stop),
                name="MemRouter-t2h",
                daemon=True,
            )
            t.start()
            self._t2h_threads.append(t)
        self._change_status_to_running()
        # Allow threads to start
        import time
        time.sleep(0.1)

    def _stop(self):
        self._t2h_stop.set()
        for downstream_connection_fifo in self._downstream_connection_fifos:
            downstream_connection_fifo.target_to_host.put(None)
        for t in getattr(self, "_t2h_threads", []):
            t.join(timeout=1.0)
        self._upstream_connection_fifo.host_to_target.put(None)


class CxlCacheRouter(CxlRouter):
    def __init__(
        self,
        vcs_id: int,
        routing_table: RoutingTable,
        upstream_vppb: UpstreamVppb,
        port_binder: PortBinder,
    ):
        super().__init__(vcs_id, routing_table)
        upstream_vppb_connection = upstream_vppb.get_downstream_connection()
        self._upstream_vppb = upstream_vppb
        self._port_binder = port_binder

        self._upstream_connection_fifo = upstream_vppb_connection.cxl_cache_fifo
        self._downstream_connections = port_binder.get_bind_slots()
        self._downstream_connection_fifos = []
        for bind_slot in self._downstream_connections:
            self._downstream_connection_fifos.append(
                bind_slot.vppb.get_upstream_connection().cxl_cache_fifo
            )

    def _process_host_to_target_packets(self):
        while True:
            packet = self._upstream_connection_fifo.host_to_target.get()
            if packet is None:
                break

            cxl_cache_base_packet = cast(CxlCacheBasePacket, packet)
            if cxl_cache_base_packet.is_h2dreq():
                cxl_cache_packet = cast(CxlCacheH2DReqPacket, packet)
                cache_id = cxl_cache_packet.h2dreq_header.cache_id
            elif cxl_cache_base_packet.is_h2drsp():
                cxl_cache_packet = cast(CxlCacheH2DRspPacket, packet)
                cache_id = cxl_cache_packet.h2drsp_header.cache_id
            elif cxl_cache_base_packet.is_h2ddata():
                cxl_cache_packet = cast(CxlCacheH2DDataPacket, packet)
                cache_id = cxl_cache_packet.h2ddata_header.cache_id
            else:
                raise Exception("Received unexpected packet")

            upstream_vppb_component = self._upstream_vppb.get_cxl_component()

            target_fld_name = f"target{cache_id}_options"

            if target_fld_name not in upstream_vppb_component.get_cache_route_table_options():
                logger.warning(self._create_message("Received unroutable CXL.cache packet"))
                continue
            target_port: int = upstream_vppb_component.get_cache_route_table_options()[
                target_fld_name
            ]["port_number"]
            if target_port is None:
                logger.warning(self._create_message("Received unroutable CXL.cache packet"))
                logger.warning(self._create_message("Packet details: "))
                logger.warning(self._create_message(cxl_cache_base_packet.get_pretty_string()))
                continue
            if target_port >= len(self._downstream_connections):
                raise Exception("target_port is out of bound")
            downstream_connection_fifo = (
                self._downstream_connections[target_port]
                .vppb.get_upstream_connection()
                .cxl_cache_fifo
            )
            downstream_connection_fifo.host_to_target.put(packet)

    def _process_target_to_host_packets(self, downstream_connection_bind_slot: BindSlot):
        # Unused in thread-based implementation
        raise NotImplementedError

    def _t2h_worker(
        self,
        downstream_connection_bind_slot: BindSlot,
        stop_evt: threading.Event,
    ) -> None:
        downstream_connection_fifo = (
            downstream_connection_bind_slot.vppb.get_upstream_connection().cxl_cache_fifo
        )
        downstream_vppb = downstream_connection_bind_slot.vppb
        downstream_vppb_component = downstream_vppb.get_cxl_component()
        while not stop_evt.is_set():
            packet = downstream_connection_fifo.target_to_host.get()
            if packet is None:
                break
            cxl_cache_base_packet = cast(CxlCacheBasePacket, packet)
            # See CXL 3.0 specification: Section 9.15.2
            if cxl_cache_base_packet.is_d2hreq():
                cxl_cache_packet = cast(CxlCacheD2HReqPacket, packet)
                cache_id_decoder_opt_ctl = downstream_vppb_component.get_cache_decoder_options()[
                    "control_options"
                ]
                assign, fwd = (
                    cache_id_decoder_opt_ctl["assign_cache_id"],
                    cache_id_decoder_opt_ctl["forward_cache_id"],
                )
                if (assign, fwd) == (1, 1):
                    logger.error(self._create_message("Invalid setting: assign/fwd cannot be 1/1"))
                elif (assign, fwd) == (0, 1):
                    pass  # just forward upstream
                elif (assign, fwd) == (1, 0):
                    # get the local cache id
                    cache_id = cache_id_decoder_opt_ctl["local_cache_id"]
                    cxl_cache_packet.set_cache_id(cache_id)
            self._upstream_connection_fifo.target_to_host.put(packet)

    def update_router(self, vppb_index: int):
        self.stop_for_update(vppb_index)
        self._downstream_connections = self._port_binder.get_bind_slots()
        vppb_upstream_connection = self._downstream_connections[
            vppb_index
        ].vppb.get_upstream_connection()
        if vppb_upstream_connection is None:
            logger.debug(self._create_message("vppb_upstream_connection is None"))
            return

        self._downstream_connection_fifos[vppb_index] = vppb_upstream_connection.cxl_cache_fifo

        if self._is_running:
            if not hasattr(self, "_t2h_stop"):
                self._t2h_stop = threading.Event()
                self._t2h_threads = []
            t = threading.Thread(
                target=self._t2h_worker,
                args=(self._downstream_connections[vppb_index], self._t2h_stop),
                name=f"CacheRouter-t2h-{vppb_index}",
                daemon=True,
            )
            t.start()
            self._t2h_threads.append(t)

    def _run(self):
        self._is_running = True
        self._t2h_stop = threading.Event()
        self._t2h_threads: list[threading.Thread] = []
        host_thread = threading.Thread(
            target=self._process_host_to_target_packets, name="CacheRouter-h2t", daemon=True
        )
        host_thread.start()
        for downstream_connection_bind_slot in self._downstream_connections:
            t = threading.Thread(
                target=self._t2h_worker,
                args=(downstream_connection_bind_slot, self._t2h_stop),
                name="CacheRouter-t2h",
                daemon=True,
            )
            t.start()
            self._t2h_threads.append(t)
        self._change_status_to_running()
        # Allow threads to start
        import time
        time.sleep(0.1)

    def _stop(self):
        self._t2h_stop.set()
        for downstream_connection_fifo in self._downstream_connection_fifos:
            downstream_connection_fifo.target_to_host.put(None)
        for t in getattr(self, "_t2h_threads", []):
            t.join(timeout=1.0)
        self._upstream_connection_fifo.host_to_target.put(None)
