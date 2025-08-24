cdef class ShmRing:
    cdef unsigned char* base
    cdef size_t capacity
    cdef size_t elem_size
    cdef size_t region_size
    cdef str path
    cdef int notify_fd_rx
    cdef int notify_fd_tx
    cdef bint notify_is_server
    cdef object notify_path

    cdef unsigned long long _read_u64(self, size_t off)
    cdef void _write_u64(self, size_t off, unsigned long long v)
    cdef unsigned int _read_u32(self, size_t off)
    cdef void _write_u32(self, size_t off, unsigned int val)

    cpdef void setup_unix_notify(self, bint is_server)
    cpdef bint try_push_frame_from(self, const unsigned char* src, size_t payload_len)
    cpdef Py_ssize_t try_pop_frame_into(self, unsigned char* dst, size_t dst_capacity)
    cpdef object pop_frame_wait(self, unsigned int max_sleep_ns=*)
    cpdef Py_ssize_t pop_frame_wait_into(self, unsigned char* dst, size_t dst_capacity, unsigned int max_sleep_ns=*)
    cpdef bytes read_up_to(self, size_t max_bytes)
    cpdef str get_path(self)
    cpdef void teardown_unix_notify(self)
    cpdef bint wait_for_space(self, unsigned int max_sleep_ns=*)
    cpdef bint push_frame_wait_from(self, const unsigned char* src, size_t payload_len, unsigned int max_sleep_ns=*)

