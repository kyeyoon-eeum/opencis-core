# cython: language_level=3
# cython: boundscheck=False, wraparound=False, nonecheck=False, initializedcheck=False

from libc.stdint cimport uint16_t
from cpython.bytes cimport PyBytes_FromStringAndSize
from shm_stream_c cimport ShmStreamReader



cdef class ShmPacketReader:
    cdef bint _aborted
    cdef ShmStreamReader _reader
    cdef unsigned char _packet_buffer[512]
    cdef object _pools
    cdef object _pool_idx
    cdef Py_ssize_t _pool_size

    def __cinit__(self, object reader):
        self._aborted = False
        self._reader = <ShmStreamReader> reader
        # Pre-allocate per-type packet pools
        self._pool_size = 64
        self._init_pools()

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

    cdef void _init_pools(self):
        # Import classes here to avoid circular imports during __cinit__
        from opencis.cxl.transport.cxl_io_packets import (
            CxlIoCfgRdPacket, CxlIoCfgWrPacket, CxlIoMemRdPacket, CxlIoMemWrPacket, CxlIoCompletionPacket,
        )
        from opencis.cxl.transport.cxl_mem_packets import (
            CxlMemM2SReqPacket, CxlMemM2SRwDPacket, CxlMemM2SBIRspPacket, CxlMemS2MBISnpPacket, CxlMemS2MNDRPacket, CxlMemS2MDRSPacket,
        )
        from opencis.cxl.transport.cxl_cache_packets import (
            CxlCacheCacheD2HReqPacket, CxlCacheCacheD2HRspPacket, CxlCacheCacheD2HDataPacket,
            CxlCacheCacheH2DReqPacket, CxlCacheCacheH2DRspPacket, CxlCacheCacheH2DDataPacket,
        )
        from opencis.cxl.transport.sideband_packets import BaseSidebandPacket, SidebandConnectionRequestPacket
        from opencis.cxl.transport.cci_packets import (
            GetLdInfoResponsePacket, GetLdAllocationsResponsePacket, SetLdAllocationsResponsePacket,
        )

        # Map class -> pre-allocated list and rotating index
        cdef list classes = [
            # CXL.io
            CxlIoCfgRdPacket, CxlIoCfgWrPacket, CxlIoMemRdPacket, CxlIoMemWrPacket, CxlIoCompletionPacket,
            # CXL.mem
            CxlMemM2SReqPacket, CxlMemM2SRwDPacket, CxlMemM2SBIRspPacket, CxlMemS2MBISnpPacket, CxlMemS2MNDRPacket, CxlMemS2MDRSPacket,
            # CXL.cache
            CxlCacheCacheD2HReqPacket, CxlCacheCacheD2HRspPacket, CxlCacheCacheD2HDataPacket,
            CxlCacheCacheH2DReqPacket, CxlCacheCacheH2DRspPacket, CxlCacheCacheH2DDataPacket,
            # Sideband
            SidebandConnectionRequestPacket, BaseSidebandPacket,
            # CCI responses we emit
            GetLdInfoResponsePacket, GetLdAllocationsResponsePacket, SetLdAllocationsResponsePacket,
        ]
        pools = {}
        idx = {}
        for cls in classes:
            arr = [cls() for _ in range(self._pool_size)]
            pools[cls] = arr
            idx[cls] = 0
        self._pools = pools
        self._pool_idx = idx

    cdef object _acquire(self, object cls):
        cdef list arr = <list> self._pools[cls]
        cdef Py_ssize_t i = <Py_ssize_t> self._pool_idx[cls]
        cdef object pkt = arr[i]
        i += 1
        if i >= self._pool_size:
            i = 0
        self._pool_idx[cls] = i
        return pkt

    cpdef object get_packet(self):
        cdef Py_ssize_t packet_length = self._read_one_packet_bytes()
        cdef unsigned char* buf = &self._packet_buffer[0]
        cdef unsigned char payload_type = buf[0] & 0x0F  # low 4 bits

        # Helper to copy into pooled instance
        cdef object pkt
        cdef int hdr_len
        cdef Py_ssize_t payload_len

        # Import constants for direct comparisons
        from opencis.cxl.cci.common import CCI_FM_API_COMMAND_OPCODE

        # Import packet classes for pool access
        from opencis.cxl.transport.cxl_io_packets import (
            CxlIoCfgRdPacket, CxlIoCfgWrPacket, CxlIoMemRdPacket, CxlIoMemWrPacket, CxlIoCompletionPacket,
        )
        from opencis.cxl.transport.cxl_mem_packets import (
            CxlMemM2SReqPacket, CxlMemM2SRwDPacket, CxlMemM2SBIRspPacket, CxlMemS2MBISnpPacket, CxlMemS2MNDRPacket, CxlMemS2MDRSPacket,
        )
        from opencis.cxl.transport.cxl_cache_packets import (
            CxlCacheCacheD2HReqPacket, CxlCacheCacheD2HRspPacket, CxlCacheCacheD2HDataPacket,
            CxlCacheCacheH2DReqPacket, CxlCacheCacheH2DRspPacket, CxlCacheCacheH2DDataPacket,
        )
        from opencis.cxl.transport.sideband_packets import BaseSidebandPacket, SidebandConnectionRequestPacket
        from opencis.cxl.transport.cci_packets import (
            GetLdInfoResponsePacket, GetLdAllocationsResponsePacket, SetLdAllocationsResponsePacket,
        )

        # CXL.io
        if payload_type == 1:  # SYSTEM_PAYLOAD_TYPE.CXL_IO
            # SystemHeader(2) + TlpPrefix(4) → CxlIoHeader starts at 6
            fmt_type = buf[6]
            if fmt_type == 4 or fmt_type == 5:  # CFG_RD0, CFG_RD1
                pkt = self._acquire(CxlIoCfgRdPacket)
            elif fmt_type == 68 or fmt_type == 69:  # CFG_WR0, CFG_WR1
                pkt = self._acquire(CxlIoCfgWrPacket)
            elif fmt_type == 0 or fmt_type == 32:  # MRD_32B, MRD_64B
                pkt = self._acquire(CxlIoMemRdPacket)
            elif fmt_type == 64 or fmt_type == 96:  # MWR_32B, MWR_64B
                pkt = self._acquire(CxlIoMemWrPacket)
            elif fmt_type == 10 or fmt_type == 74 or fmt_type == 11 or fmt_type == 75:  # CPL, CPL_D, CPL_LK, CPL_D_LK
                pkt = self._acquire(CxlIoCompletionPacket)
            else:
                raise RuntimeError(f"Unsupported CXL.IO protocol 0x{fmt_type:02x}")

            hdr_len = pkt.get_payload_offset()
            if hdr_len > packet_length:
                raise RuntimeError("Invalid packet lengths for CXL.IO")
            payload_len = packet_length - hdr_len
            pkt.set_bytes(0, PyBytes_FromStringAndSize(<char*>buf, hdr_len))
            if payload_len:
                pkt.set_data(PyBytes_FromStringAndSize(<char*>buf + hdr_len, payload_len))
            else:
                pkt.set_data(b"")
            return pkt

        # CXL.mem
        elif payload_type == 2:  # SYSTEM_PAYLOAD_TYPE.CXL_MEM
            # SystemHeader(2) + CxlMemHeader(2) → msg_class at offset 3
            msg_class = buf[3]
            if msg_class == 1:  # M2S_REQ
                pkt = self._acquire(CxlMemM2SReqPacket)
            elif msg_class == 2:  # M2S_RWD
                pkt = self._acquire(CxlMemM2SRwDPacket)
            elif msg_class == 3:  # M2S_BIRSP
                pkt = self._acquire(CxlMemM2SBIRspPacket)
            elif msg_class == 4:  # S2M_BISNP
                pkt = self._acquire(CxlMemS2MBISnpPacket)
            elif msg_class == 5:  # S2M_NDR
                pkt = self._acquire(CxlMemS2MNDRPacket)
            elif msg_class == 6:  # S2M_DRS
                pkt = self._acquire(CxlMemS2MDRSPacket)
            else:
                raise RuntimeError(f"Unsupported CXL.MEM msg_class {msg_class}")

            hdr_len = pkt.get_payload_offset()
            if hdr_len > packet_length:
                raise RuntimeError("Invalid packet lengths for CXL.MEM")
            payload_len = packet_length - hdr_len
            pkt.set_bytes(0, PyBytes_FromStringAndSize(<char*>buf, hdr_len))
            if payload_len:
                pkt.set_data(PyBytes_FromStringAndSize(<char*>buf + hdr_len, payload_len))
            else:
                pkt.set_data(b"")
            return pkt

        # CXL.cache
        elif payload_type == 3:  # SYSTEM_PAYLOAD_TYPE.CXL_CACHE
            # SystemHeader(2) + CxlCacheHeader(2) → msg_class at offset 3
            msg_class_cache = buf[3]
            if msg_class_cache == 1:  # D2H_REQ
                pkt = self._acquire(CxlCacheCacheD2HReqPacket)
            elif msg_class_cache == 2:  # D2H_RSP
                pkt = self._acquire(CxlCacheCacheD2HRspPacket)
            elif msg_class_cache == 3:  # D2H_DATA
                pkt = self._acquire(CxlCacheCacheD2HDataPacket)
            elif msg_class_cache == 4:  # H2D_REQ
                pkt = self._acquire(CxlCacheCacheH2DReqPacket)
            elif msg_class_cache == 5:  # H2D_RSP
                pkt = self._acquire(CxlCacheCacheH2DRspPacket)
            elif msg_class_cache == 6:  # H2D_DATA
                pkt = self._acquire(CxlCacheCacheH2DDataPacket)
            else:
                raise RuntimeError(f"Unsupported CXL.CACHE msg_class {msg_class_cache}")

            hdr_len = pkt.get_payload_offset()
            if hdr_len > packet_length:
                raise RuntimeError("Invalid packet lengths for CXL.CACHE")
            payload_len = packet_length - hdr_len
            pkt.set_bytes(0, PyBytes_FromStringAndSize(<char*>buf, hdr_len))
            if payload_len:
                pkt.set_data(PyBytes_FromStringAndSize(<char*>buf + hdr_len, payload_len))
            else:
                pkt.set_data(b"")
            return pkt

        # Sideband
        elif payload_type == 15:  # SYSTEM_PAYLOAD_TYPE.SIDEBAND
            # SystemHeader(2) + SidebandHeader(1) → type at offset 2
            sb_type = buf[2]
            if sb_type == 0:  # CONNECTION_REQUEST
                pkt = self._acquire(SidebandConnectionRequestPacket)
            else:
                pkt = self._acquire(BaseSidebandPacket)

            hdr_len = pkt.get_payload_offset()
            if hdr_len > packet_length:
                raise RuntimeError("Invalid packet lengths for SIDEBAND")
            payload_len = packet_length - hdr_len
            pkt.set_bytes(0, PyBytes_FromStringAndSize(<char*>buf, hdr_len))
            if payload_len:
                pkt.set_data(PyBytes_FromStringAndSize(<char*>buf + hdr_len, payload_len))
            else:
                pkt.set_data(b"")
            return pkt

        # CCI (MCTP)
        elif payload_type == 4:  # SYSTEM_PAYLOAD_TYPE.CCI_MCTP
            # SystemHeader(2) + CciHeader(2)
            # command_opcode is at: 2 (sys) + 2 (cci hdr) + 3 bytes into CciMessageHeader
            opcode = buf[7] | (buf[8] << 8)

            # We convert both REQ and RSP to the Response packet variants, as before
            if opcode == 0:  # GET_LD_INFO
                pkt = self._acquire(GetLdInfoResponsePacket)
            elif opcode == 1:  # GET_LD_ALLOCATIONS
                pkt = self._acquire(GetLdAllocationsResponsePacket)
            elif opcode == 2:  # SET_LD_ALLOCATIONS
                pkt = self._acquire(SetLdAllocationsResponsePacket)
            else:
                raise RuntimeError("Unsupported CCI packet")

            hdr_len = pkt.get_payload_offset()
            if hdr_len > packet_length:
                raise RuntimeError("Invalid packet lengths for CCI")
            payload_len = packet_length - hdr_len
            pkt.set_bytes(0, PyBytes_FromStringAndSize(<char*>buf, hdr_len))
            if payload_len:
                pkt.set_data(PyBytes_FromStringAndSize(<char*>buf + hdr_len, payload_len))
            else:
                pkt.set_data(b"")

            # Adjust dynamic payload widths for specific responses without mutating header fields
            if isinstance(pkt, GetLdAllocationsResponsePacket):
                try:
                    ld_len = pkt.payload.ld_allocation_list_length
                    pkt.payload.set_dynamic_field_width("ld_allocation_list", ld_len * 16 * 8)
                except Exception:
                    pass
            elif isinstance(pkt, SetLdAllocationsResponsePacket):
                try:
                    num_lds = pkt.payload.number_of_lds
                    pkt.payload.set_dynamic_field_width("ld_allocation_list", num_lds * 16 * 8)
                except Exception:
                    pass
            return pkt

        raise RuntimeError("Unsupported packet")