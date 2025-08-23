cimport shm_ring

cdef class ShmStreamReader:
    cdef object _ep
    cdef shm_ring.ShmRing _in_ring
    cdef unsigned char _left_buf[256]
    cdef Py_ssize_t _left_off
    cdef Py_ssize_t _left_len
    cdef unsigned char _tmp_buf[256]
    cdef void _readinto_exactly_blocking(self, unsigned char* dst, Py_ssize_t n)

cdef class ShmStreamWriter:
    cdef object _ep
    cdef shm_ring.ShmRing _out_ring
    cdef object _pending