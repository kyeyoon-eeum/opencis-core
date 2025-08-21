# cython: language_level=3
# cython: boundscheck=False, wraparound=False, nonecheck=False, initializedcheck=False

from libc.string cimport memcpy
from libc.stddef cimport size_t
from cpython.bytes cimport PyBytes_FromStringAndSize

cdef class ShmPacketReader:
    cdef object _ring
    cdef bint _aborted
    cdef object _left_payload
    cdef Py_ssize_t _left_off
    cdef Py_ssize_t _left_len

    def __cinit__(self, object ring):
        self._ring = ring
        self._aborted = False
        self._left_payload = None
        self._left_off = 0
        self._left_len = 0

    cpdef void abort(self):
        self._aborted = True

    cdef void _readinto_exactly(self, unsigned char* dst, Py_ssize_t n):
        if self._aborted:
            raise RuntimeError("PacketReader is aborted")
        if n <= 0:
            return

        cdef Py_ssize_t off = 0
        cdef const unsigned char[::1] mv
        cdef Py_ssize_t take, plen

        # Consume leftover slice from previous frame (zero-copy; no Python realloc)
        if self._left_payload is not None and self._left_len > 0:
            mv = self._left_payload
            take = n if n <= self._left_len else self._left_len
            memcpy(dst + 0, &mv[self._left_off], <size_t>take)
            self._left_off += take
            self._left_len -= take
            if self._left_len == 0:
                self._left_payload = None
                self._left_off = 0
            off += take

        # Pull frames until exactly n bytes are filled
        while off < n:
            if self._aborted:
                raise RuntimeError("PacketReader is aborted")
            # Blocks in C until a frame arrives (your ring handles backoff/sleep)
            payload = self._ring.pop_frame_wait(1000000)
            if payload is None:
                continue  # still empty; keep waiting in Cython (no Python allocations)
            mv = payload
            plen = mv.shape[0]
            take = n - off
            if plen <= take:
                memcpy(dst + off, &mv[0], <size_t>plen)
                off += plen
            else:
                memcpy(dst + off, &mv[0], <size_t>take)
                # Cache leftover slice for next call (no bytearray.extend/del)
                self._left_payload = payload
                self._left_off = take
                self._left_len = plen - take
                off = n

    cdef bytearray _read_one_packet_bytes(self):
        # 1. Read just the SystemHeader (small scratch on stack, one copy)
        from opencis.cxl.transport.packet_structs import SystemHeader
        from opencis.cxl.transport.common import BasePacket
        cdef Py_ssize_t hdr_size = SystemHeader.get_size()
        cdef unsigned char hdr_buf[64]  # ensure >= max SystemHeader size
        if hdr_size > 64:
            raise RuntimeError("SystemHeader too large for scratch buffer")
        self._readinto_exactly(hdr_buf, hdr_size)

        # 2. Compute remaining payload length using existing Python parser (small cost)
        cdef object hdr_py = PyBytes_FromStringAndSize(<char*>hdr_buf, hdr_size)
        cdef object base_header = BasePacket(hdr_py)
        cdef Py_ssize_t remaining = base_header.system_header.payload_length - len(base_header)
        if remaining < 0:
            raise RuntimeError("remaining length is less than 0")

        # 3. Allocate final buffer exactly once; copy header + read the rest directly
        cdef Py_ssize_t total = hdr_size + remaining
        cdef bytearray ba = bytearray(total)
        cdef unsigned char[::1] mv = ba
        memcpy(&mv[0], hdr_buf, <size_t>hdr_size)
        if remaining:
            self._readinto_exactly(&mv[hdr_size], remaining)
        return ba

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

        payload = self._read_one_packet_bytes()
        base = BasePacket(payload)
        if base.is_cxl_io():
            b = CxlIoBasePacket(payload)
            if b.is_cfg_read():
                return CxlIoCfgRdPacket(payload)
            elif b.is_cfg_write():
                return CxlIoCfgWrPacket(payload)
            elif b.is_mem_read():
                return CxlIoMemRdPacket(payload)
            elif b.is_mem_write():
                return CxlIoMemWrPacket(payload)
            elif b.is_cpl() or b.is_cpld():
                return CxlIoCompletionPacket(payload)
            raise RuntimeError(f"Unsupported CXL.IO protocol {b.cxl_io_header.fmt_type}")
        elif base.is_cxl_mem():
            m = CxlMemBasePacket(payload)
            if m.is_m2sreq():
                return CxlMemM2SReqPacket(payload)
            elif m.is_m2srwd():
                return CxlMemM2SRwDPacket(payload)
            elif m.is_m2sbirsp():
                return CxlMemM2SBIRspPacket(payload)
            elif m.is_s2mbisnp():
                return CxlMemS2MBISnpPacket(payload)
            elif m.is_s2mndr():
                return CxlMemS2MNDRPacket(payload)
            elif m.is_s2mdrs():
                return CxlMemS2MDRSPacket(payload)
            raise RuntimeError(f"Unsupported CXL.MEM message class: {m.cxl_mem_header.msg_class}")
        elif base.is_cxl_cache():
            c = CxlCacheBasePacket(payload)
            if c.is_d2hreq():
                return CxlCacheCacheD2HReqPacket(payload)
            elif c.is_d2hrsp():
                return CxlCacheCacheD2HRspPacket(payload)
            elif c.is_d2hdata():
                return CxlCacheCacheD2HDataPacket(payload)
            elif c.is_h2dreq():
                return CxlCacheCacheH2DReqPacket(payload)
            elif c.is_h2drsp():
                return CxlCacheCacheH2DRspPacket(payload)
            elif c.is_h2ddata():
                return CxlCacheCacheH2DDataPacket(payload)
            raise RuntimeError(f"Unsupported CXL.CACHE message class: {c.cxl_cache_header.msg_class}")
        elif base.is_sideband():
            sb = BaseSidebandPacket(payload)
            if sb.is_connection_request():
                return SidebandConnectionRequestPacket(payload)
            return sb
        elif base.is_cci():
            cb = CciBasePacket(payload)
            if cb.is_req():
                r = CciRequestPacket(payload)
                if r.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.GET_LD_INFO:
                    return GetLdInfoResponsePacket(payload)
                elif r.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.GET_LD_ALLOCATIONS:
                    return GetLdAllocationsResponsePacket(payload)
                elif r.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS:
                    return SetLdAllocationsResponsePacket(payload)
                raise RuntimeError("Unsupported CCI packet")
            elif cb.is_rsp():
                rsp = CciResponsePacket(payload)
                if rsp.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.GET_LD_INFO:
                    return GetLdInfoResponsePacket(payload)
                elif rsp.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.GET_LD_ALLOCATIONS:
                    return GetLdAllocationsResponsePacket(payload)
                elif rsp.get_command_opcode() == CCI_FM_API_COMMAND_OPCODE.SET_LD_ALLOCATIONS:
                    return SetLdAllocationsResponsePacket(payload)
                raise RuntimeError("Unsupported CCI packet")
            raise RuntimeError("Unsupported CCI packet")
        raise RuntimeError("Unsupported packet") 