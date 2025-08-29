"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import threading
from dataclasses import dataclass
from enum import StrEnum, IntEnum
from typing import cast, Optional, Dict, Union, List, Any

from opencis.util.logger import logger
from opencis.cxl.cci.common import CCI_FM_API_COMMAND_OPCODE
from opencis.util.component import RunnableComponent
from opencis.cxl.component.common import CXL_COMPONENT_TYPE
from opencis.cxl.component.cxl_connection import CxlConnection
from opencis.cxl.transport.common import BasePacket
from opencis.cxl.transport.sideband_packets import BaseSidebandPacket
from opencis.cxl.transport.cxl_io_packets import CxlIoBasePacket
from opencis.cxl.transport.cxl_cache_packets import CxlCacheBasePacket
from opencis.cxl.transport.cxl_mem_packets import CxlMemBasePacket
from opencis.cxl.transport.packet_constants import (
    SYSTEM_PAYLOAD_TYPE,
    CXL_IO_FMT_TYPE,
    SIDEBAND_TYPES,
)
from opencis.cxl.transport.stream_types import StreamReaderLike, StreamWriterLike
from opencis.cxl.transport.cci_packets import (
    CciRequestPacket,
    CciResponsePacket,
    GetLdInfoResponsePacket,
    GetLdAllocationsResponsePacket,
    SetLdAllocationsResponsePacket,
)
from opencis.cxl.device.cxl_type3_device import CXL_T3_DEV_TYPE
from opencis.cxl.component.fmld import FMLD
from opencis.cxl.transport.shm_stream import ShmStreamReader

try:
    from opencis.cxl.transport import packet_reader_c as _prc
except Exception as e:  # pragma: no cover
    raise


@dataclass
class FifoGroup:
    cfg_space: Any
    mmio: Any
    cxl_mem: Any
    cxl_cache: Any
    cci_fifo: Optional[Any]  # To LD, TODO: Enable later when CCI towards LD is implemented


class CXL_IO_FIFO_TYPE(IntEnum):
    CFG = 0
    MMIO = 1


class PROCESSOR_DIRECTION(StrEnum):
    HOST_TO_TARGET = "host to target"
    TARGET_TO_HOST = "target to host"


class CxlPacketProcessor(RunnableComponent):
    def __init__(
        self,
        reader: StreamReaderLike,
        writer: StreamWriterLike,
        # cxl_connection for SLD & MLD
        cxl_connection: Union[CxlConnection, List[CxlConnection]],
        component_type: CXL_COMPONENT_TYPE,
        label: Optional[str] = None,
    ):
        super().__init__(label)
        if not isinstance(reader, ShmStreamReader):
            raise TypeError("CxlPacketProcessor requires ShmStreamReader")
        self._reader = _prc.ShmPacketReader(reader)
        self._reader_is_async = False
        self._writer = writer
        self._tlp_table: Dict[int, CXL_IO_FIFO_TYPE] = {}
        self._cxl_connection = cxl_connection
        self._component_type = component_type
        self._fmld = None
        self._cci_connection_for_fmld = None
        self._reader_thread = None
        self._reader_thread_stop = threading.Event()
        self._loop = None
        self._outgoing_threads: list[threading.Thread] = []
        self._writer_lock = threading.Lock()
        self._tlp_lock = threading.Lock()
        self._outgoing_stop = threading.Event()

        logger.debug(self._create_message(f"Configured for {component_type.name}"))
        if component_type in (CXL_COMPONENT_TYPE.R, CXL_COMPONENT_TYPE.DSP):
            # For both R (host end) and DSP (device end), this stream instance
            # sees T2H completions as incoming and H2T requests as outgoing.
            self._incoming = FifoGroup(
                cfg_space=self._cxl_connection.cfg_fifo.target_to_host,
                mmio=self._cxl_connection.mmio_fifo.target_to_host,
                cxl_mem=self._cxl_connection.cxl_mem_fifo.target_to_host,
                cxl_cache=self._cxl_connection.cxl_cache_fifo.target_to_host,
                cci_fifo=self._cxl_connection.cci_fifo.target_to_host,
            )
            self._incoming_dir = PROCESSOR_DIRECTION.TARGET_TO_HOST
            self._outgoing = FifoGroup(
                cfg_space=self._cxl_connection.cfg_fifo.host_to_target,
                mmio=self._cxl_connection.mmio_fifo.host_to_target,
                cxl_mem=self._cxl_connection.cxl_mem_fifo.host_to_target,
                cxl_cache=self._cxl_connection.cxl_cache_fifo.host_to_target,
                cci_fifo=self._cxl_connection.cci_fifo.host_to_target,
            )
            self._outgoing_dir = PROCESSOR_DIRECTION.HOST_TO_TARGET
        elif component_type in (
            CXL_COMPONENT_TYPE.P,
            CXL_COMPONENT_TYPE.T1,
            CXL_COMPONENT_TYPE.T2,
            CXL_COMPONENT_TYPE.D2,
            CXL_COMPONENT_TYPE.USP,
        ):
            self._incoming_dir = PROCESSOR_DIRECTION.HOST_TO_TARGET
            self._outgoing_dir = PROCESSOR_DIRECTION.TARGET_TO_HOST

            # Add common FIFOs
            self._incoming = FifoGroup(
                cfg_space=self._cxl_connection.cfg_fifo.host_to_target,
                mmio=self._cxl_connection.mmio_fifo.host_to_target,
                cxl_mem=None,
                cxl_cache=None,
                cci_fifo=None,
            )

            self._outgoing = FifoGroup(
                cfg_space=self._cxl_connection.cfg_fifo.target_to_host,
                mmio=self._cxl_connection.mmio_fifo.target_to_host,
                cxl_mem=None,
                cxl_cache=None,
                cci_fifo=None,
            )

            # Add CXL.cache and CXL.mem FIFO based on the device type
            if component_type in (
                CXL_COMPONENT_TYPE.T1,
                CXL_COMPONENT_TYPE.T2,
                CXL_COMPONENT_TYPE.USP,
            ):
                self._incoming.cxl_cache = self._cxl_connection.cxl_cache_fifo.host_to_target
                self._outgoing.cxl_cache = self._cxl_connection.cxl_cache_fifo.target_to_host

            if component_type in (
                CXL_COMPONENT_TYPE.T2,
                CXL_COMPONENT_TYPE.D2,
                CXL_COMPONENT_TYPE.USP,
            ):
                self._incoming.cxl_mem = self._cxl_connection.cxl_mem_fifo.host_to_target
                self._outgoing.cxl_mem = self._cxl_connection.cxl_mem_fifo.target_to_host
        # Add MLD
        elif component_type == CXL_COMPONENT_TYPE.LD:
            self._cci_connection_for_fmld = CxlConnection()
            self._ld_count = len(cxl_connection) if isinstance(cxl_connection, list) else 1

            self._fmld = FMLD(
                upstream_fifo=self._cci_connection_for_fmld.cci_fifo,
                ld_count=self._ld_count,
                dev_type=CXL_T3_DEV_TYPE.MLD,
            )
            self._incoming_dir = PROCESSOR_DIRECTION.HOST_TO_TARGET
            self._outgoing_dir = PROCESSOR_DIRECTION.TARGET_TO_HOST
            self._incoming = [
                FifoGroup(
                    cfg_space=cxl_conn.cfg_fifo.host_to_target,
                    mmio=cxl_conn.mmio_fifo.host_to_target,
                    cxl_mem=cxl_conn.cxl_mem_fifo.host_to_target,
                    cxl_cache=None,
                    cci_fifo=None,
                )
                for cxl_conn in self._cxl_connection
            ]

            self._outgoing = FifoGroup(
                cfg_space=self._cxl_connection[0].cfg_fifo.target_to_host,
                mmio=self._cxl_connection[0].mmio_fifo.target_to_host,
                cxl_mem=self._cxl_connection[0].cxl_mem_fifo.target_to_host,
                cxl_cache=None,
                cci_fifo=None,
            )
        else:
            raise Exception(f"Unsupported component type {component_type.name}")

    @staticmethod
    def _is_disconnection_notification(packet) -> bool:
        base_packet = cast(BasePacket, packet)
        if base_packet.system_header.payload_type != SYSTEM_PAYLOAD_TYPE.SIDEBAND:
            return False
        sideband = cast(BaseSidebandPacket, packet)
        return sideband.sideband_header.type == SIDEBAND_TYPES.CONNECTION_DISCONNECTED

    def _push_tlp_table_entry(self, cxl_io_packet: CxlIoBasePacket):
        tid = cxl_io_packet.get_transaction_id()
        # Since USP and R (Root Port) are agnostic to the existence (or even the concept of)
        # MLD, the LD-ID field is considered undefined. To ensure consistent matching of TLP
        # entries, always set LD-ID to 0.
        if self._component_type in (CXL_COMPONENT_TYPE.USP, CXL_COMPONENT_TYPE.R):
            ld_id = 0
        else:
            ld_id = cxl_io_packet.tlp_prefix.ld_id
        t_index = (tid << 8) | ld_id

        if t_index in self._tlp_table:
            raise Exception(f"t_index ({t_index:02x}) already exists in the TLP table")
        if cxl_io_packet.is_cfg():
            fifo_type = CXL_IO_FIFO_TYPE.CFG
        elif cxl_io_packet.is_mmio():
            fifo_type = CXL_IO_FIFO_TYPE.MMIO
        else:
            fmt_type_str = CXL_IO_FMT_TYPE(cxl_io_packet.cxl_io_header.fmt_type)
            raise Exception(f"pushing t_index of {fmt_type_str} type is not allowed")
        self._tlp_table[t_index] = fifo_type

    def _pop_tlp_table_entry(self, cxl_io_packet: CxlIoBasePacket) -> CXL_IO_FIFO_TYPE:
        tid = cxl_io_packet.get_transaction_id()
        # Same reasoning as push function, push and pop must have same mechanism
        # for no mismatch in TLP table
        if self._component_type in (CXL_COMPONENT_TYPE.USP, CXL_COMPONENT_TYPE.R):
            ld_id = 0
        else:
            ld_id = cxl_io_packet.tlp_prefix.ld_id
        t_index = (tid << 8) | ld_id

        if t_index not in self._tlp_table:
            raise Exception(f"t_index ({t_index:02x}) is not found in the TLP table")
        fifo_type = self._tlp_table[t_index]
        del self._tlp_table[t_index]
        return fifo_type

    def _reader_thread_main(self):
        logger.debug(self._create_message(f"Reader thread starting for {self._incoming_dir}"))
        # Reader thread loop
        while not self._reader_thread_stop.is_set():  # pylint: disable=too-many-nested-blocks
            try:
                packet = self._reader.get_packet()
                # Gracefully handle sideband frames that may appear on the stream (e.g., transport handshakes)
                base_packet = cast(BasePacket, packet)
                if base_packet.system_header.payload_type == SYSTEM_PAYLOAD_TYPE.SIDEBAND:
                    sideband = cast(BaseSidebandPacket, packet)
                    if sideband.sideband_header.type == SIDEBAND_TYPES.CONNECTION_DISCONNECTED:
                        notification_packet = BaseSidebandPacket.create(
                            SIDEBAND_TYPES.CONNECTION_DISCONNECTED
                        )
                        try:
                            self._notify_outgoing_processors_sync(notification_packet)
                        except Exception:
                            pass
                        break
                    # Ignore other sideband frames
                    logger.debug("[%s] Received sideband; ignoring", self.get_message_label())
                    continue
                if packet.is_cxl_io():
                    cxl_io_packet = cast(CxlIoBasePacket, packet)
                    if cxl_io_packet.is_cpl() or cxl_io_packet.is_cpld():
                        logger.debug(
                            "[%s] Received %s CXL.io (CPL/CPLD) packet",
                            self.get_message_label(),
                            self._incoming_dir,
                        )
                        fifo_type = self._pop_tlp_table_entry(cxl_io_packet)
                        # Add MLD
                        if self._component_type == CXL_COMPONENT_TYPE.LD:
                            ld_id = cxl_io_packet.tlp_prefix.ld_id
                            target_q = (
                                self._incoming[ld_id].cfg_space
                                if fifo_type == CXL_IO_FIFO_TYPE.CFG
                                else self._incoming[ld_id].mmio
                            )
                        else:
                            target_q = (
                                self._incoming.cfg_space
                                if fifo_type == CXL_IO_FIFO_TYPE.CFG
                                else self._incoming.mmio
                            )
                        target_q.put(cxl_io_packet)
                    elif cxl_io_packet.is_cfg():
                        logger.debug(
                            "[%s] Received %s CXL.io (CFG_RD/CFG_WR) packet",
                            self.get_message_label(),
                            self._incoming_dir,
                        )
                        self._push_tlp_table_entry(cxl_io_packet)
                        # Add MLD
                        if self._component_type == CXL_COMPONENT_TYPE.LD:
                            ld_id = cxl_io_packet.tlp_prefix.ld_id
                            self._incoming[ld_id].cfg_space.put(cxl_io_packet)
                        else:
                            self._incoming.cfg_space.put(cxl_io_packet)
                    elif cxl_io_packet.is_mmio():
                        logger.debug(
                            "[%s] Received %s CXL.io (MRD/MWR) packet",
                            self.get_message_label(),
                            self._incoming_dir,
                        )
                        if cxl_io_packet.is_mem_write() is False:
                            self._push_tlp_table_entry(cxl_io_packet)
                        # Add MLD
                        if self._component_type == CXL_COMPONENT_TYPE.LD:
                            ld_id = cxl_io_packet.tlp_prefix.ld_id
                            self._incoming[ld_id].mmio.put(cxl_io_packet)
                        else:
                            self._incoming.mmio.put(cxl_io_packet)
                    else:
                        logger.warning("[%s] Unexpected CXL.io packet", self.get_message_label())
                        logger.debug(
                            "[%s] %s", self.get_message_label(), packet.get_pretty_string()
                        )
                        continue
                elif packet.is_cxl_mem():
                    if (
                        self._component_type != CXL_COMPONENT_TYPE.LD
                        and self._incoming.cxl_mem is None
                    ):
                        logger.error(
                            "[%s] Got CXL.mem packet on no CXL.mem FIFO", self.get_message_label()
                        )
                        continue
                    logger.debug(
                        "[%s] Received %s CXL.mem packet",
                        self.get_message_label(),
                        self._incoming_dir,
                    )
                    cxl_mem_packet = cast(CxlMemBasePacket, packet)
                    if self._component_type == CXL_COMPONENT_TYPE.LD:
                        # Add LD routing code
                        if cxl_mem_packet.is_m2sreq():
                            ld_id = cxl_mem_packet.m2sreq_header.ld_id
                        elif cxl_mem_packet.is_m2srwd():
                            ld_id = cxl_mem_packet.m2srwd_header.ld_id
                        elif cxl_mem_packet.is_s2mndr():
                            ld_id = cxl_mem_packet.s2mndr_header.ld_id
                        elif cxl_mem_packet.is_s2mdrs():
                            ld_id = cxl_mem_packet.s2mdrs_header.ld_id
                        else:
                            logger.warning(
                                "[%s] Unexpected CXL.mem packet", self.get_message_label()
                            )

                        self._incoming[ld_id].cxl_mem.put(cxl_mem_packet)
                    else:
                        self._incoming.cxl_mem.put(cxl_mem_packet)

                elif packet.is_cxl_cache():
                    if self._incoming.cxl_cache is None:
                        logger.error(
                            "[%s] Got CXL.cache packet on no CXL.cache FIFO",
                            self.get_message_label(),
                        )
                        continue
                    logger.debug(
                        "[%s] Received %s CXL.cache packet",
                        self.get_message_label(),
                        self._incoming_dir,
                    )
                    cxl_cache_packet = cast(CxlCacheBasePacket, packet)
                    self._incoming.cxl_cache.put(cxl_cache_packet)
                elif packet.is_cci():
                    if self._component_type == CXL_COMPONENT_TYPE.D2:
                        logger.error(
                            "[%s] Got CCI packet on wrong device type - SLD",
                            self.get_message_label(),
                        )
                        raise Exception("Got CCI packet on wrong device type - SLD")
                    if self._component_type == CXL_COMPONENT_TYPE.LD:
                        if self._fmld.upstream_fifo is None:
                            logger.error(
                                "[%s] Got CCI packet on no CCI FIFO", self.get_message_label()
                            )
                            raise Exception("Got CCI packet on no CCI FIFO")
                        cci_packet = cast(CciRequestPacket, packet)
                        self._fmld.upstream_fifo.host_to_target.put(cci_packet)
                    elif self._component_type == CXL_COMPONENT_TYPE.DSP:
                        self._incoming.cci_fifo.put(packet)
                else:
                    message = f"Received unexpected {self._incoming_dir} packet"
                    logger.warning("[%s] %s", self.get_message_label(), message)
                    raise Exception(message)
            except Exception as e:
                msg = str(e)
                logger.debug("[%s] %s", self.get_message_label(), msg)
                if "aborted" in msg or "cancelled" in msg or "Connection disconnected" in msg:
                    notification_packet = BaseSidebandPacket.create(
                        SIDEBAND_TYPES.CONNECTION_DISCONNECTED
                    )
                    try:
                        self._notify_outgoing_processors_sync(notification_packet)
                    except Exception:
                        pass
                    break
                # Otherwise, treat as recoverable and continue
                logger.warning("[%s] Recoverable incoming error: %s", self.get_message_label(), msg)
                continue
        logger.debug("[%s] Stopped %s reader thread", self.get_message_label(), self._incoming_dir)

    def _notify_outgoing_processors_sync(self, packet):
        self._outgoing.cfg_space.put(packet)
        self._outgoing.mmio.put(packet)
        if self._outgoing.cxl_mem:
            self._outgoing.cxl_mem.put(packet)
        if self._outgoing.cxl_cache:
            self._outgoing.cxl_cache.put(packet)
        if self._cci_connection_for_fmld:
            logger.info(self._create_message("Sending disconnection notification to FMLD CCI"))
            self._fmld.upstream_fifo.target_to_host.put(packet)
        if self._outgoing.cci_fifo:
            logger.info(self._create_message("Sending disconnection notification to CCI"))
            self._outgoing.cci_fifo.put(packet)

    def _process_outgoing_cfg_packets(self):
        # Kept only for compatibility if ever invoked; main path uses threads
        logger.debug(self._create_message("Starting outgoing CFG FIFO processor (async fallback)"))
        while True:
            packet = self._outgoing.cfg_space.get()
            if self._is_disconnection_notification(packet):
                break
            cxl_io_packet = cast(CxlIoBasePacket, packet)
            if cxl_io_packet.is_cpl() or cxl_io_packet.is_cpld():
                with self._tlp_lock:
                    self._pop_tlp_table_entry(cxl_io_packet)
            else:
                with self._tlp_lock:
                    self._push_tlp_table_entry(cxl_io_packet)
            try:
                view = packet.get_view()  # type: ignore[attr-defined]
            except Exception:
                view = bytes(packet)
            with self._writer_lock:
                self._writer.write(view)
                self._writer.drain_blocking()
        logger.debug(self._create_message("Stopped outgoing CFG FIFO processor (async fallback)"))

    def _process_outgoing_mmio_packets(self):
        logger.debug(self._create_message("Starting outgoing MMIO FIFO processor (async fallback)"))
        while True:
            packet = self._outgoing.mmio.get()
            if self._is_disconnection_notification(packet):
                break
            cxl_io_packet = cast(CxlIoBasePacket, packet)
            if cxl_io_packet.is_cpl() or cxl_io_packet.is_cpld():
                with self._tlp_lock:
                    self._pop_tlp_table_entry(cxl_io_packet)
            else:
                if cxl_io_packet.is_mem_write() is False:
                    with self._tlp_lock:
                        self._push_tlp_table_entry(cxl_io_packet)
            try:
                view = packet.get_view()  # type: ignore[attr-defined]
            except Exception:
                view = bytes(packet)
            with self._writer_lock:
                self._writer.write(view)
                self._writer.drain_blocking()
        logger.debug(self._create_message("Stopped outgoing MMIO FIFO processor (async fallback)"))

    def _process_outgoing_cxl_mem_packets(self):
        logger.debug(
            self._create_message("Starting outgoing CXL.mem FIFO processor (async fallback)")
        )
        while True:
            packet = self._outgoing.cxl_mem.get()
            if self._is_disconnection_notification(packet):
                break
            try:
                view = packet.get_view()  # type: ignore[attr-defined]
            except Exception:
                view = bytes(packet)
            with self._writer_lock:
                self._writer.write(view)
                self._writer.drain_blocking()
        logger.debug(
            self._create_message("Stopped outgoing CXL.mem FIFO processor (async fallback)")
        )

    def _process_outgoing_cxl_cache_packets(self):
        logger.debug(
            self._create_message("Starting outgoing CXL.cache FIFO processor (async fallback)")
        )
        while True:
            packet = self._outgoing.cxl_cache.get()
            if self._is_disconnection_notification(packet):
                break
            try:
                view = packet.get_view()  # type: ignore[attr-defined]
            except Exception:
                view = bytes(packet)
            with self._writer_lock:
                self._writer.write(view)
                self._writer.drain_blocking()
        logger.debug(
            self._create_message("Stopped outgoing CXL.cache FIFO processor (async fallback)")
        )

    def _process_outgoing_cci_packets(self):
        logger.debug(self._create_message("Starting outgoing CCI FIFO processor"))
        while True:
            if self._component_type == CXL_COMPONENT_TYPE.LD:
                packet: CciResponsePacket = self._fmld.upstream_fifo.target_to_host.get()
                if self._is_disconnection_notification(packet):
                    logger.info(self._create_message("Stopped outgoing CCI FIFO processor"))
                    break
                opcode = packet.get_command_opcode()
                logger.info(self._create_message(f"Received CCI packet with opcode {opcode:x}"))
                if opcode == CCI_FM_API_COMMAND_OPCODE.GET_LD_INFO:
                    packet = cast(GetLdInfoResponsePacket, packet)
                    try:
                        view = packet.get_view()  # type: ignore[attr-defined]
                    except Exception:
                        view = bytes(packet)
                    self._writer.write(view)
                    self._writer.drain_blocking()
                elif opcode == CCI_FM_API_COMMAND_OPCODE.GET_LD_ALLOCATIONS:
                    packet = cast(GetLdAllocationsResponsePacket, packet)
                    try:
                        view = packet.get_view()  # type: ignore[attr-defined]
                    except Exception:
                        view = bytes(packet)
                    self._writer.write(view)
                    self._writer.drain_blocking()
                elif opcode == CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS:
                    packet = cast(SetLdAllocationsResponsePacket, packet)
                    try:
                        view = packet.get_view()  # type: ignore[attr-defined]
                    except Exception:
                        view = bytes(packet)
                    self._writer.write(view)
                    self._writer.drain_blocking()
                else:
                    logger.warning(self._create_message("Unsupported CCI packet"))
            elif self._component_type == CXL_COMPONENT_TYPE.DSP:
                packet = self._outgoing.cci_fifo.get()
                if self._is_disconnection_notification(packet):
                    break
                try:
                    view = packet.get_view()  # type: ignore[attr-defined]
                except Exception:
                    view = bytes(packet)
                self._writer.write(view)
                self._writer.drain_blocking()
            else:
                break
        logger.debug(self._create_message("Stopped outgoing CCI FIFO processor"))

    def _process_outgoing_packets(self):
        # Not used in sync mode
        return None

    def _run(self):
        # Start reader thread for blocking packet reads
        self._reader_thread_stop.clear()
        self._reader_thread = threading.Thread(
            target=self._reader_thread_main, name=f"{self.get_message_label()}-reader", daemon=True
        )
        self._reader_thread.start()
        # Start outgoing workers in threads
        self._start_outgoing_threads()
        if self._fmld:
            self._fmld.start_wait_ready()
        self._change_status_to_running()
        # Join until stop
        if self._reader_thread is not None:
            self._reader_thread.join()
        for t in self._outgoing_threads:
            t.join()
        if self._fmld:
            self._fmld.join()

    def _stop(self):
        # TODO: Enable later when CCI for LD is needed
        # if self._outgoing.cci_fifo:
        #     self._fmld._upstream_fifo.target_to_host.abort()
        # await self._fmld._upstream_fifo.target_to_host.put(None)
        if self._fmld:
            try:
                self._fmld.stop_sync()
            except Exception:
                pass
        # Stop reader thread
        try:
            self._reader_thread_stop.set()
            self._reader.abort()
            if self._reader_thread is not None:
                self._reader_thread.join(timeout=1.0)
        except Exception:
            pass
        # Stop outgoing workers
        try:
            self._outgoing_stop.set()
            # Nudge workers by notifying outgoing processors
            try:
                notification_packet = BaseSidebandPacket.create(
                    SIDEBAND_TYPES.CONNECTION_DISCONNECTED
                )
                self._notify_outgoing_processors_sync(notification_packet)
            except Exception:
                pass
            for t in self._outgoing_threads:
                t.join(timeout=1.0)
        except Exception:
            pass
        # No async stopper in sync mode

    # Thread workers for outgoing hot paths
    def _outgoing_cfg_worker(self) -> None:
        logger.debug(self._create_message("Starting outgoing CFG FIFO worker (thread)"))
        q = self._outgoing.cfg_space
        while not self._outgoing_stop.is_set():
            try:
                packet = q.get()
            except Exception:
                break
            if self._is_disconnection_notification(packet):
                break
            cxl_io_packet = cast(CxlIoBasePacket, packet)
            if cxl_io_packet.is_cpl() or cxl_io_packet.is_cpld():
                with self._tlp_lock:
                    self._pop_tlp_table_entry(cxl_io_packet)
            else:
                with self._tlp_lock:
                    self._push_tlp_table_entry(cxl_io_packet)
            try:
                view = packet.get_view()  # type: ignore[attr-defined]
            except Exception:
                view = bytes(packet)
            with self._writer_lock:
                self._writer.write(view)
                self._writer.drain_blocking()
        logger.debug(self._create_message("Stopped outgoing CFG FIFO worker (thread)"))

    def _outgoing_mmio_worker(self) -> None:
        logger.debug(self._create_message("Starting outgoing MMIO FIFO worker (thread)"))
        q = self._outgoing.mmio
        while not self._outgoing_stop.is_set():
            try:
                packet = q.get()
            except Exception:
                break
            if self._is_disconnection_notification(packet):
                break
            cxl_io_packet = cast(CxlIoBasePacket, packet)
            if cxl_io_packet.is_cpl() or cxl_io_packet.is_cpld():
                with self._tlp_lock:
                    self._pop_tlp_table_entry(cxl_io_packet)
            else:
                if cxl_io_packet.is_mem_write() is False:
                    with self._tlp_lock:
                        self._push_tlp_table_entry(cxl_io_packet)
            try:
                view = packet.get_view()  # type: ignore[attr-defined]
            except Exception:
                view = bytes(packet)
            with self._writer_lock:
                self._writer.write(view)
                self._writer.drain_blocking()
        logger.debug(self._create_message("Stopped outgoing MMIO FIFO worker (thread)"))

    def _outgoing_cxl_mem_worker(self) -> None:
        if self._outgoing.cxl_mem is None:
            return
        logger.debug(self._create_message("Starting outgoing CXL.mem FIFO worker (thread)"))
        q = self._outgoing.cxl_mem
        while not self._outgoing_stop.is_set():
            try:
                packet = q.get()
            except Exception:
                break
            if self._is_disconnection_notification(packet):
                break
            try:
                view = packet.get_view()  # type: ignore[attr-defined]
            except Exception:
                view = bytes(packet)
            with self._writer_lock:
                self._writer.write(view)
                self._writer.drain_blocking()
        logger.debug(self._create_message("Stopped outgoing CXL.mem FIFO worker (thread)"))

    def _outgoing_cxl_cache_worker(self) -> None:
        if self._outgoing.cxl_cache is None:
            return
        logger.debug(self._create_message("Starting outgoing CXL.cache FIFO worker (thread)"))
        q = self._outgoing.cxl_cache
        while not self._outgoing_stop.is_set():
            try:
                packet = q.get()
            except Exception:
                break
            if self._is_disconnection_notification(packet):
                break
            try:
                view = packet.get_view()  # type: ignore[attr-defined]
            except Exception:
                view = bytes(packet)
            with self._writer_lock:
                self._writer.write(view)
                self._writer.drain_blocking()
        logger.debug(self._create_message("Stopped outgoing CXL.cache FIFO worker (thread)"))

    def _start_outgoing_threads(self) -> None:
        self._outgoing_stop.clear()
        threads: list[threading.Thread] = [
            threading.Thread(
                target=self._outgoing_cfg_worker,
                name=f"{self.get_message_label()}-out-cfg",
                daemon=True,
            ),
            threading.Thread(
                target=self._outgoing_mmio_worker,
                name=f"{self.get_message_label()}-out-mmio",
                daemon=True,
            ),
        ]
        if self._outgoing.cxl_mem is not None:
            threads.append(
                threading.Thread(
                    target=self._outgoing_cxl_mem_worker,
                    name=f"{self.get_message_label()}-out-mem",
                    daemon=True,
                )
            )
        if self._outgoing.cxl_cache is not None:
            threads.append(
                threading.Thread(
                    target=self._outgoing_cxl_cache_worker,
                    name=f"{self.get_message_label()}-out-cache",
                    daemon=True,
                )
            )
        # CCI worker when used
        if (
            self._component_type == CXL_COMPONENT_TYPE.LD
            or self._component_type == CXL_COMPONENT_TYPE.DSP
        ):
            threads.append(
                threading.Thread(
                    target=self._outgoing_cci_worker,
                    name=f"{self.get_message_label()}-out-cci",
                    daemon=True,
                )
            )
        for t in threads:
            t.start()
        self._outgoing_threads = threads

    def _outgoing_cci_worker(self) -> None:
        logger.debug(self._create_message("Starting outgoing CCI FIFO worker (thread)"))
        while not self._outgoing_stop.is_set():
            try:
                if self._component_type == CXL_COMPONENT_TYPE.LD:
                    packet = self._fmld.upstream_fifo.target_to_host.get()
                elif self._component_type == CXL_COMPONENT_TYPE.DSP:
                    packet = self._outgoing.cci_fifo.get()
                else:
                    break
            except Exception:
                break
            if self._is_disconnection_notification(packet):
                break
            try:
                view = packet.get_view()  # type: ignore[attr-defined]
            except Exception:
                view = bytes(packet)
            with self._writer_lock:
                self._writer.write(view)
                self._writer.drain_blocking()
        logger.debug(self._create_message("Stopped outgoing CCI FIFO worker (thread)"))
