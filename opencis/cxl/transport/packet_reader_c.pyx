# cython: language_level=3
# cython: boundscheck=False, wraparound=False, nonecheck=False, initializedcheck=False

from libc.stdint cimport uint16_t
from shm_stream_c cimport ShmStreamReader



cdef class ShmPacketReader:
    cdef bint _aborted
    cdef ShmStreamReader _reader
    cdef unsigned char _packet_buffer[256]

    def __cinit__(self, object reader):
        self._aborted = False
        self._reader = <ShmStreamReader> reader

    cpdef void abort(self):
        self._aborted = True

    cdef void _read_exactly(self, unsigned char* dst, Py_ssize_t n):
        if self._aborted:
            raise RuntimeError("PacketReader is aborted")
        if n <= 0:
            return
        self._reader.read_into_buf(dst, n)

    cdef Py_ssize_t _read_one_packet_bytes(self):
        # SystemHeader: payload_type (4 bits) + payload_length (12 bits)
        cdef Py_ssize_t hdr_size = 2
        self._read_exactly(self._packet_buffer, hdr_size)

        cdef uint16_t hdr_bytes = (<uint16_t*>self._packet_buffer)[0]
        cdef Py_ssize_t total_len = <Py_ssize_t>((hdr_bytes >> 4) & 0x0FFF)
        cdef Py_ssize_t remaining = total_len - hdr_size
        if remaining < 0:
            raise RuntimeError("remaining length is less than 0")

        # Read the remaining data directly into the buffer
        self._read_exactly(&self._packet_buffer[hdr_size], remaining)
        
        return total_len

    cpdef object get_packet(self):
        # Build packet view and pick concrete class
        from opencis.cxl.transport.common import BasePacket
        from opencis.cxl.transport.cxl_io_packets import (
            CxlIoBasePacket, CxlIoCfgRdPacket, CxlIoCfgWrPacket, CxlIoMemRdPacket, CxlIoMemWrPacket, CxlIoCompletionPacket,
        )
        from opencis.cxl.transport.cxl_mem_packets import (
            CxlMemBasePacket, CxlMemM2SReqPacket, CxlMemM2SRwDPacket, CxlMemM2SBIRspPacket, CxlMemS2MBISnpPacket, CxlMemS2MNDRPacket, CxlMemS2MDRSPacket,
        )
        from opencis.cxl.transport.cxl_cache_packets import (
            CxlCacheBasePacket, CxlCacheCacheD2HReqPacket, CxlCacheCacheD2HRspPacket, CxlCacheCacheD2HDataPacket,
            CxlCacheCacheH2DReqPacket, CxlCacheCacheH2DRspPacket, CxlCacheCacheH2DDataPacket,
        )
        from opencis.cxl.transport.cci_packets import (
            CciBasePacket, CciRequestPacket, CciResponsePacket, GetLdInfoResponsePacket, GetLdAllocationsResponsePacket, SetLdAllocationsResponsePacket,
        )
        from opencis.cxl.cci.common import CCI_FM_API_COMMAND_OPCODE
        from opencis.cxl.transport.sideband_packets import BaseSidebandPacket, SidebandConnectionRequestPacket

        packet_length = self._read_one_packet_bytes()
        base = BasePacket(self._packet_buffer[:packet_length])

        if base.is_cxl_io():
            b = CxlIoBasePacket(self._packet_buffer[:packet_length])
            if b.is_cfg_read():
                return CxlIoCfgRdPacket(self._packet_buffer[:packet_length])
            elif b.is_cfg_write():
                return CxlIoCfgWrPacket(self._packet_buffer[:packet_length])
            elif b.is_mem_read():
                return CxlIoMemRdPacket(self._packet_buffer[:packet_length])
            elif b.is_mem_write():
                return CxlIoMemWrPacket(self._packet_buffer[:packet_length])
            elif b.is_cpl() or b.is_cpld():
                return CxlIoCompletionPacket(self._packet_buffer[:packet_length])
            raise RuntimeError(f"Unsupported CXL.IO protocol {b.cxl_io_header.fmt_type}")
        elif base.is_cxl_mem():
            m = CxlMemBasePacket(self._packet_buffer[:packet_length])
            if m.is_m2sreq():
                return CxlMemM2SReqPacket(self._packet_buffer[:packet_length])
            elif m.is_m2srwd():
                return CxlMemM2SRwDPacket(self._packet_buffer[:packet_length])
            elif m.is_m2sbirsp():
                return CxlMemM2SBIRspPacket(self._packet_buffer[:packet_length])
            elif m.is_s2mbisnp():
                return CxlMemS2MBISnpPacket(self._packet_buffer[:packet_length])
            elif m.is_s2mndr():
                return CxlMemS2MNDRPacket(self._packet_buffer[:packet_length])
            elif m.is_s2mdrs():
                return CxlMemS2MDRSPacket(self._packet_buffer[:packet_length])
            raise RuntimeError(f"Unsupported CXL.MEM message class: {m.cxl_mem_header.msg_class}")
        elif base.is_cxl_cache():
            c = CxlCacheBasePacket(self._packet_buffer[:packet_length])
            if c.is_d2hreq():
                return CxlCacheCacheD2HReqPacket(self._packet_buffer[:packet_length])
            elif c.is_d2hrsp():
                return CxlCacheCacheD2HRspPacket(self._packet_buffer[:packet_length])
            elif c.is_d2hdata():
                return CxlCacheCacheD2HDataPacket(self._packet_buffer[:packet_length])
            elif c.is_h2dreq():
                return CxlCacheCacheH2DReqPacket(self._packet_buffer[:packet_length])
            elif c.is_h2drsp():
                return CxlCacheCacheH2DRspPacket(self._packet_buffer[:packet_length])
            elif c.is_h2ddata():
                return CxlCacheCacheH2DDataPacket(self._packet_buffer[:packet_length])
            raise RuntimeError(f"Unsupported CXL.CACHE message class: {c.cxl_cache_header.msg_class}")
        elif base.is_sideband():
            sb = BaseSidebandPacket(self._packet_buffer[:packet_length])
            if sb.is_connection_request():
                return SidebandConnectionRequestPacket(self._packet_buffer[:packet_length])
            return sb
        elif base.is_cci():
            cb = CciBasePacket(self._packet_buffer[:packet_length])
            if cb.is_req():
                r = CciRequestPacket(self._packet_buffer[:packet_length])
                if r.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.GET_LD_INFO:
                    return GetLdInfoResponsePacket(self._packet_buffer[:packet_length])
                elif r.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.GET_LD_ALLOCATIONS:
                    return GetLdAllocationsResponsePacket(self._packet_buffer[:packet_length])
                elif r.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS:
                    return SetLdAllocationsResponsePacket(self._packet_buffer[:packet_length])
                raise RuntimeError("Unsupported CCI packet")
            elif cb.is_rsp():
                rsp = CciResponsePacket(self._packet_buffer[:packet_length])
                if rsp.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.GET_LD_INFO:
                    return GetLdInfoResponsePacket(self._packet_buffer[:packet_length])
                elif rsp.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.GET_LD_ALLOCATIONS:
                    return GetLdAllocationsResponsePacket(self._packet_buffer[:packet_length])
                elif rsp.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS:
                    return SetLdAllocationsResponsePacket(self._packet_buffer[:packet_length])
                raise RuntimeError("Unsupported CCI packet")
            raise RuntimeError("Unsupported CCI packet")
        raise RuntimeError("Unsupported packet")