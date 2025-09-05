from libc.stdint cimport uint64_t

cdef class ShmRing:
    cdef unsigned char* base
    cdef uint64_t capacity
    cdef uint64_t elem_size
    cdef uint64_t region_size
    cdef str path
    cdef int notify_fd_rx
    cdef int notify_fd_tx
    cdef bint notify_is_server
    cdef char notify_path[108]
    cdef unsigned int notify_path_len

    cdef unsigned long long _read_u64(self, size_t off) noexcept nogil
    cdef void _write_u64(self, size_t off, unsigned long long v) noexcept nogil
    cdef unsigned int _read_u32(self, size_t off) noexcept nogil
    cdef void _write_u32(self, size_t off, unsigned int val) noexcept nogil

    cpdef bint create(self, str path, size_t capacity, size_t elem_size)
    cpdef bint open(self, str path)
    cpdef bint setup_unix_notify(self, bint is_server)
    cdef bint try_push_frame_from(self, const unsigned char* src, size_t payload_len) noexcept nogil
    cdef Py_ssize_t pop_frame_wait_into(self, unsigned char* dst, size_t dst_capacity, unsigned int max_sleep_ns=*) noexcept nogil
    cdef bint push_frame_wait_from(self, const unsigned char* src, size_t payload_len, unsigned int max_sleep_ns=*) noexcept nogil
    cdef bint empty(self) noexcept nogil
    cdef bint is_full(self) noexcept nogil
    cdef unsigned long long qsize(self) noexcept nogil
    cdef bint wait_for_space(self, unsigned int max_sleep_ns=*) noexcept nogil
    cpdef void close(self)
    cpdef void teardown_unix_notify(self)


