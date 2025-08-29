# cython: language_level=3
# cython: boundscheck=False, wraparound=False, nonecheck=False, initializedcheck=False

import os
from libc.stddef cimport size_t
from libc.stdint cimport uint8_t
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AsString
from libc.string cimport memcpy
from libc.stdlib cimport malloc, free
from libc.stdint cimport uint64_t

cdef extern from "sys/socket.h":
    cdef int AF_UNIX
    cdef int SOCK_DGRAM
    int socket(int domain, int type, int protocol)
    int bind(int sockfd, const void* addr, unsigned int addrlen)
    ssize_t sendto(int sockfd, const void* buf, size_t len, int flags, const void* dest_addr, unsigned int addrlen) nogil
    ssize_t recv(int sockfd, void* buf, size_t len, int flags) nogil

cdef extern from "sys/un.h":
    ctypedef unsigned short sa_family_t
    cdef struct sockaddr_un:
        sa_family_t sun_family
        char sun_path[108]

cdef extern from "unistd.h":
    int close(int fd)

cdef extern from "errno.h":
    int errno
    int EINTR

cdef extern from "time.h":
    cdef struct timespec:
        long tv_sec
        long tv_nsec
    int nanosleep(timespec* req, timespec* rem) nogil

cdef extern from "sys/mman.h":
    void* mmap(void* addr, size_t length, int prot, int flags, int fd, long offset)
    int munmap(void* addr, size_t length)
    int PROT_READ
    int PROT_WRITE
    int MAP_SHARED


cdef class ShmRing:
    def __cinit__(self):
        self.base = NULL
        self.capacity = 0
        self.elem_size = 0
        self.region_size = 0
        self.path = ""
        self.notify_fd_rx = -1
        self.notify_fd_tx = -1
        self.notify_is_server = False
        self.notify_path_len = 0
        for i in range(108):
            self.notify_path[i] = '\x00'

    def create(self, str path, size_t capacity, size_t elem_size):
        """Create or truncate a shared ring buffer file and map it."""
        self.path = path
        self.capacity = capacity
        self.elem_size = elem_size
        cdef size_t header_size = 32
        self.region_size = header_size + capacity * elem_size
        # Ensure stale files are removed and permissions are sane
        try:
            os.unlink(path)
        except Exception:
            pass
        cdef int fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o666)
        try:
            os.ftruncate(fd, self.region_size)
            self.base = <unsigned char*> mmap(NULL, self.region_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)
        finally:
            os.close(fd)
        try:
            os.chmod(path, 0o666)
        except Exception:
            pass
        if self.base == <unsigned char*> -1 or self.base == NULL:
            self.base = NULL
            raise OSError("mmap failed")
        # Initialize header: [cap(8)][elem(8)][head(8)][tail(8)]
        self._write_u64(0, capacity)
        self._write_u64(8, elem_size)
        self._write_u64(16, 0)
        self._write_u64(24, 0)

    def open(self, str path):
        """Open an existing shared ring buffer file and map it."""
        self.path = path
        cdef size_t sz = <size_t> os.path.getsize(path)
        cdef int fd = os.open(path, os.O_RDWR)
        try:
            self.base = <unsigned char*> mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)
        finally:
            os.close(fd)
        if self.base == <unsigned char*> -1 or self.base == NULL:
            self.base = NULL
            raise OSError("mmap failed")
        self.capacity = <size_t> self._read_u64(0)
        self.elem_size = <size_t> self._read_u64(8)
        self.region_size = sz

    cpdef void setup_unix_notify(self, bint is_server):
        """
        Configure a UNIX DGRAM socket used only to wake a peer when an empty ring becomes non-empty.
        Server binds to path ":.notify"; client sends datagrams to that path.
        """
        if not self.path:
            return
        self.notify_is_server = is_server
        cdef str npath = self.path + ".notify"
        cdef int fd = socket(AF_UNIX, SOCK_DGRAM, 0)
        cdef sockaddr_un addr
        cdef bytes pb
        cdef Py_ssize_t l
        if fd < 0:
            return
        # Cache notify path into C buffer for nogil use
        pb = npath.encode("utf-8")
        l = pb.__len__()
        if l > 107:
            l = 107
        self.notify_path_len = <unsigned int> l
        for i in range(108):
            self.notify_path[i] = '\x00'
        for i in range(self.notify_path_len):
            self.notify_path[i] = <char>pb[i]
        if is_server:
            try:
                os.unlink(npath)
            except Exception:
                pass
            addr.sun_family = <sa_family_t>AF_UNIX
            # fill sun_path with zeros then copy
            for i in range(108):
                addr.sun_path[i] = '\x00'
            for i in range(self.notify_path_len):
                addr.sun_path[i] = self.notify_path[i]
            if bind(fd, <const void*>&addr, <unsigned int>(sizeof(sockaddr_un)) ) != 0:
                close(fd)
                return
            self.notify_fd_rx = fd
        else:
            self.notify_fd_tx = fd

    cdef inline unsigned long long _read_u64(self, size_t off) noexcept nogil:
        return (<unsigned long long*> (self.base + off))[0]

    cdef inline void _write_u64(self, size_t off, unsigned long long v) noexcept nogil:
        (<unsigned long long*> (self.base + off))[0] = v

    cdef inline unsigned int _read_u32(self, size_t off) noexcept nogil:
        return (<unsigned int*> (self.base + off))[0]

    cdef inline void _write_u32(self, size_t off, unsigned int v) noexcept nogil:
        (<unsigned int*> (self.base + off))[0] = v

    cdef bint try_push_frame_from(self, const unsigned char* src, size_t payload_len) noexcept nogil:
        """Non-blocking push (header + payload) directly from a raw pointer.
        Returns True on success, False if full.
        """
        if payload_len > self.elem_size - 4:
            return False
        cdef unsigned long long head = self._read_u64(16)
        cdef unsigned long long tail = self._read_u64(24)
        cdef bint was_empty = (head == tail)
        cdef sockaddr_un addr2
        cdef unsigned int l2
        cdef char one
        if head - tail >= self.capacity:
            return False
        cdef unsigned long long idx = head % self.capacity
        cdef size_t off = 32 + idx * <size_t> self.elem_size
        # header
        self._write_u32(off, <unsigned int> payload_len)
        off += 4
        # payload (raw-pointer memcpy)
        memcpy(<void*>(self.base + off), <const void*>src, <size_t>payload_len)
        self._write_u64(16, head + 1)
        if was_empty and self.notify_fd_tx >= 0 and self.notify_path_len > 0:
            addr2.sun_family = <sa_family_t>AF_UNIX
            l2 = self.notify_path_len
            if l2 > 107:
                l2 = 107
            for i in range(108):
                addr2.sun_path[i] = '\x00'
            for i in range(l2):
                addr2.sun_path[i] = self.notify_path[i]
            one = '\x01'
            sendto(self.notify_fd_tx, &one, 1, 0, <const void*>(&addr2), <unsigned int>(sizeof(sockaddr_un)))
        return True

    cdef Py_ssize_t pop_frame_wait_into(self, unsigned char* dst, size_t dst_capacity, unsigned int max_sleep_ns=100000) noexcept nogil:
        """Blocking-ish wait up to max_sleep_ns for a frame and copy into dst.
        Returns payload length (>0) on success, 0 if timed out, or -1 if corrupted/invalid.
        """
        cdef unsigned long long head
        cdef unsigned long long tail
        cdef unsigned long long idx
        cdef size_t off
        cdef unsigned int slept_ns = 0
        cdef unsigned int step = 100
        cdef timespec ts
        cdef unsigned int payload_len
        cdef char sink
        while True:
            head = self._read_u64(16)
            tail = self._read_u64(24)
            if tail < head:
                idx = tail % self.capacity
                off = 32 + idx * self.elem_size
                payload_len = self._read_u32(off)
                if payload_len > self.elem_size - 4 or payload_len > dst_capacity:
                    self._write_u64(24, tail + 1)
                    return -1
                off += 4
                memcpy(<void*>dst, <const void*>(self.base + off), <size_t>payload_len)
                self._write_u64(24, tail + 1)
                return <Py_ssize_t>payload_len
            if self.notify_fd_rx >= 0:
                with nogil:
                    while recv(self.notify_fd_rx, &sink, 1, 0) < 0:
                        if errno != EINTR:
                            break
                continue
            if slept_ns >= max_sleep_ns:
                return 0
            ts.tv_sec = 0
            ts.tv_nsec = step
            with nogil:
                nanosleep(&ts, <timespec*>0)
            slept_ns += step
            if step < 1000000:
                step <<= 1

    def empty(self):
        cdef unsigned long long head = self._read_u64(16)
        cdef unsigned long long tail = self._read_u64(24)
        return tail >= head

    def is_full(self):
        cdef unsigned long long head = self._read_u64(16)
        cdef unsigned long long tail = self._read_u64(24)
        return head - tail >= self.capacity

    def qsize(self):
        cdef unsigned long long head = self._read_u64(16)
        cdef unsigned long long tail = self._read_u64(24)
        return head - tail

    cdef bint wait_for_space(self, unsigned int max_sleep_ns=1000000) noexcept nogil:
        """Busy-wait with exponential backoff (nanosleep) until ring is not full.
        Returns True if space became available before timeout, False otherwise.
        """
        cdef unsigned int slept_ns = 0
        cdef unsigned int step = 100
        cdef timespec ts
        while True:
            # Inline is_full() for speed
            if (self._read_u64(16) - self._read_u64(24)) < self.capacity:
                return True
            if slept_ns >= max_sleep_ns:
                return False
            ts.tv_sec = 0
            ts.tv_nsec = step
            with nogil:
                nanosleep(&ts, <timespec*>0)
            slept_ns += step
            if step < 1000000:
                step <<= 1

    cpdef bint push_frame_wait_from(self, const unsigned char* src, size_t payload_len, unsigned int max_sleep_ns=1000000):
        """Push a frame (header + payload) waiting for space if needed.
        Returns True on success, False if timed out before space became available.
        """
        if payload_len > self.elem_size - 4:
            raise ValueError("Frame too large for element")
        with nogil:
            while not self.try_push_frame_from(src, payload_len):
                if not self.wait_for_space(max_sleep_ns):
                    return False
        return True

    def close(self):
        if self.base != NULL:
            try:
                pass
            finally:
                munmap(self.base, <size_t> self.region_size)
                self.base = NULL

    cpdef void teardown_unix_notify(self):
        if self.notify_fd_rx >= 0:
            try:
                close(self.notify_fd_rx)
            except Exception:
                pass
            self.notify_fd_rx = -1
            if self.notify_is_server:
                try:
                    os.unlink(self.path + ".notify")
                except Exception:
                    pass
        if self.notify_fd_tx >= 0:
            try:
                close(self.notify_fd_tx)
            except Exception:
                pass
            self.notify_fd_tx = -1
