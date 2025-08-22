cdef class ShmStreamReader:
    cdef object _ep
    cdef object _in_ring
    cdef bytearray _buf
    cdef Py_ssize_t _debug_reads
    cdef object _left_payload
    cdef Py_ssize_t _left_off
    cdef Py_ssize_t _left_len
    cdef void _readinto_exactly_blocking(self, unsigned char* dst, Py_ssize_t n)

cdef class ShmStreamWriter:
    cdef object _ep
    cdef object _out_ring
    cdef long _debug_writes
    cdef object _pending 