# cython: language_level=3
# distutils: language = c

# ─── C / POSIX headers ────────────────────────────────────────────────────────
from libc.stdint  cimport uint16_t, uint32_t
from libc.stddef  cimport size_t
from libc.stdlib  cimport malloc, free
from libc.string  cimport memset, memcpy
from posix.unistd cimport read, write, close
from posix.fcntl  cimport fcntl, F_SETFL, O_NONBLOCK
from libc.errno   cimport errno, EINPROGRESS, EAGAIN
from cpython.ref  cimport PyObject, Py_INCREF, Py_DECREF
from cpython.bytes cimport PyBytes_FromStringAndSize

ctypedef long ssize_t

cdef extern from "errno.h":
    int EWOULDBLOCK

cdef extern from "arpa/inet.h":
    uint32_t inet_addr(const char*)

cdef extern from "sys/socket.h":
    int socket(int, int, int)
    int bind(int, void*, uint32_t)
    int listen(int, int)
    int accept(int, void*, void*)
    int connect(int, void*, uint32_t)
    int setsockopt(int, int, int, void*, uint32_t)
    uint16_t htons(uint16_t)
    int AF_INET, SOCK_STREAM, SOCK_NONBLOCK, SOL_SOCKET, SO_REUSEADDR

cdef extern from "netinet/in.h":
    struct in_addr:
        uint32_t  s_addr
    struct sockaddr_in:
        uint16_t  sin_family
        uint16_t  sin_port
        in_addr   sin_addr
        char      sin_zero[8]

# ─── Python std-lib imports ───────────────────────────────────────────────────
import threading, time, selectors
from enum import IntEnum
from select import select

# ─── Lock-free single-producer / single-consumer ring (PyObject*) ─────────────
cdef class Ring:
    cdef PyObject **slots
    cdef int size, head, tail

    def __cinit__(self, int size):
        if size & (size - 1):
            raise ValueError("size must be power-of-2")
        self.size  = size
        self.head  = self.tail = 0
        self.slots = <PyObject **> malloc(sizeof(PyObject*) * size)
        memset(self.slots, 0, sizeof(PyObject*) * size)

    def __dealloc__(self):
        if self.slots != NULL:
            free(self.slots)

    cdef inline bint _push(self, PyObject* obj):
        cdef int nxt = (self.head + 1) & (self.size - 1)
        if nxt == self.tail:            # full
            return False
        Py_INCREF(<object> obj)
        self.slots[self.head] = obj
        self.head = nxt
        return True

    cdef inline PyObject* _pop(self):
        if self.tail == self.head:      # empty
            return <PyObject*> 0
        cdef PyObject* obj = self.slots[self.tail]
        self.tail = (self.tail + 1) & (self.size - 1)
        return obj

    cpdef bint push(self, object item):
        return self._push(<PyObject*> item)

    cpdef object pop(self):
        cdef PyObject* raw = self._pop()
        if raw == <PyObject*> 0:
            return None
        pyobj = <object> raw
        Py_DECREF(<object> raw)
        return pyobj

# ─── Transport base / kernel (non-blocking TCP) ───────────────────────────────
cdef class _BaseTransport:
    cdef public int fd

    def __cinit__(self):
        self.fd = -1

    cdef ssize_t _send(self, const char* buf, size_t n) nogil:
        return write(self.fd, buf, n)

    cdef ssize_t _recv(self, char* buf, size_t n) nogil:
        return read(self.fd, buf, n)

    cpdef int send(self, bytes data):
        cdef const char* p = data
        return self._send(p, len(data))

    cpdef bytes recv(self, size_t n):
        cdef char[::1] buf = bytearray(n)
        cdef ssize_t r = self._recv(&buf[0], n)
        if r <= 0:
            return b""
        return bytes(buf[:r])

    cpdef void close(self):
        if self.fd >= 0:
            close(self.fd)
            self.fd = -1

cdef class _KernelTransport(_BaseTransport):
    cpdef int connect(self, const char* ip, uint16_t port):
        self.fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)
        if self.fd < 0:
            raise OSError("socket")
        cdef sockaddr_in addr
        memset(&addr, 0, sizeof(sockaddr_in))
        addr.sin_family = AF_INET
        addr.sin_port   = htons(port)
        addr.sin_addr.s_addr = inet_addr(ip)
        if connect(self.fd, <sockaddr_in*> &addr, sizeof(sockaddr_in)) == -1 \
           and errno != EINPROGRESS:
            close(self.fd)
            self.fd = -1
            raise OSError("connect")
        return 0

# ─── Server ───────────────────────────────────────────────────────────────────
cdef class Server:
    cdef Ring           _rx
    cdef public _KernelTransport _tp
    cdef object         _thr
    cdef bint           _stop

    def __init__(self, bytes ip, int port, int kind=0):
        self._rx   = Ring(1024)
        self._tp   = _KernelTransport()
        self._stop = False

        # build listening socket
        lsock = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)
        cdef int one = 1
        setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(int))

        cdef sockaddr_in addr
        memset(&addr, 0, sizeof(addr))
        addr.sin_family = AF_INET
        addr.sin_port   = htons(port)
        addr.sin_addr.s_addr = inet_addr(<const char*> ip)

        if bind(lsock, <sockaddr_in*> &addr, sizeof(addr)) == -1:
            close(lsock)
            raise OSError("bind")
        if listen(lsock, 16) == -1:
            close(lsock)
            raise OSError("listen")

        # wait (blocking with select) for a single connection
        sel = selectors.DefaultSelector()
        sel.register(lsock, selectors.EVENT_READ)
        while True:
            events = sel.select(timeout=5.0)
            if not events:
                raise TimeoutError("accept timed out")
            conn = accept(lsock, NULL, NULL)
            if conn != -1:
                break
        sel.unregister(lsock)
        close(lsock)

        # make conn non-blocking
        fcntl(conn, F_SETFL, O_NONBLOCK)
        self._tp.fd = conn

        # start reader thread
        self._thr = threading.Thread(target=self._reader, daemon=True)
        self._thr.start()

    def _reader(self):
        cdef char[4096] buf
        poller = selectors.DefaultSelector()
        poller.register(self._tp.fd, selectors.EVENT_READ)
        while not self._stop:
            events = poller.select(timeout=1.0)
            if not events:
                continue
            n = self._tp._recv(&buf[0], 4096)
            if n <= 0:
                continue
            # fast zero-copy PyBytes from existing buffer
            pkt = <object> PyBytes_FromStringAndSize(<char*> &buf[0], n)
            while not self._rx.push(pkt):
                # back-off very briefly; no full 100 µs delay
                time.sleep(0.000005)

    def run(self, handler):
        try:
            while True:
                msg = self._rx.pop()
                if msg is not None:
                    handler(msg)
                else:
                    # light sleep to yield CPU when idle
                    time.sleep(0.00002)
        except KeyboardInterrupt:
            self.stop()

    cpdef void stop(self):
        self._stop = True
        if self._thr is not None:
            self._thr.join()
        self._tp.close()

# ─── Client ───────────────────────────────────────────────────────────────────
cdef class Client:
    cdef Ring           _tx
    cdef public _KernelTransport _tp
    cdef object         _thr
    cdef bint           _stop

    def __init__(self, bytes ip, int port):
        self._tp   = _KernelTransport()
        self._tp.connect(<const char*> ip, port)
        self._tx   = Ring(1024)
        self._stop = False
        self._thr  = threading.Thread(target=self._flusher, daemon=True)
        self._thr.start()

    def _flusher(self):
        poller = selectors.DefaultSelector()
        poller.register(self._tp.fd, selectors.EVENT_WRITE)
        cdef object msg
        while not self._stop:
            msg = self._tx.pop()
            if msg is None:
                time.sleep(0.00002)
                continue
            # wait until socket is writable; no blanket sleep
            while True:
                ev = poller.select(timeout=1.0)
                if ev:
                    break
            self._tp.send(msg)

    def send(self, bytes data):
        while not self._tx.push(data):
            time.sleep(0.000005)

    cpdef void stop(self):
        self._stop = True
        if self._thr is not None:
            self._thr.join()
        self._tp.close()
