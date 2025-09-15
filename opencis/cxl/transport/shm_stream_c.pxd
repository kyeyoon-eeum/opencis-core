cimport shm_ring

cdef class ShmStreamReader:
    cdef shm_ring.ShmRing _in_ring
    cdef unsigned char _left_buf[256]
    cdef Py_ssize_t _left_off
    cdef Py_ssize_t _left_len
    cdef unsigned char _tmp_buf[256]
    cdef void read_into_buf(self, unsigned char* dst, Py_ssize_t n)

cdef class ShmStreamWriter:
    cdef shm_ring.ShmRing _out_ring
    cdef object _last_obj
    cdef const unsigned char* _last_ptr
    cdef Py_ssize_t _last_len