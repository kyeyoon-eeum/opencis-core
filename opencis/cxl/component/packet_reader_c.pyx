# cython: language_level=3
# cython: boundscheck=False, wraparound=False, nonecheck=False, initializedcheck=False

cdef class ShmPacketReader:
	cdef object _ring
	cdef bytearray _pending
	cdef bint _aborted

	def __cinit__(self, object ring):
		self._ring = ring
		self._pending = bytearray()
		self._aborted = False

	cpdef void abort(self):
		self._aborted = True

	cdef void _readinto_exactly(self, object dst, Py_ssize_t n):
		if self._aborted:
			raise RuntimeError("PacketReader is aborted")
		if n <= 0:
			return
		mv = memoryview(dst)
		if mv.readonly:
			raise TypeError("destination buffer must be writable")
		offset = 0
		# Drain pending first
		if self._pending:
			take = n if n <= len(self._pending) else len(self._pending)
			mv[:take] = self._pending[:take]
			del self._pending[:take]
			offset = take
		while offset < n:
			if self._aborted:
				raise RuntimeError("PacketReader is aborted")
			payload = self._ring.pop_frame_wait(1000000)
			if payload is None:
				continue
			plen = len(payload)
			need = n - offset
			if plen <= need:
				mv[offset:offset+plen] = payload
				offset += plen
			else:
				mv[offset:n] = payload[:need]
				self._pending.extend(payload[need:])
				offset = n

	cdef bytearray _read_one_packet_bytes(self):
		from opencis.cxl.transport.packet_structs import SystemHeader
		from opencis.cxl.transport.common import BasePacket
		hdr_size = SystemHeader.get_size()
		hdr_ba = bytearray(hdr_size)
		self._readinto_exactly(hdr_ba, hdr_size)
		base_header = BasePacket(hdr_ba)
		remaining = base_header.system_header.payload_length - len(base_header)
		if remaining < 0:
			raise RuntimeError("remaining length is less than 0")
		total = hdr_size + remaining
		ba = bytearray(total)
		mv = memoryview(ba)
		mv[:hdr_size] = hdr_ba
		if remaining:
			self._readinto_exactly(mv[hdr_size:hdr_size+remaining], remaining)
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