# cython: language_level=3
# distutils: language = c

# ─── C headers ────────────────────────────────────────────────────────────────
from libc.stdint  cimport uint16_t, uint32_t, uint64_t
from libc.stddef  cimport size_t
from libc.stdlib  cimport malloc, free
from libc.string  cimport memset
from posix.unistd cimport read, write, close
from posix.fcntl  cimport fcntl, F_SETFL, O_NONBLOCK
from libc.errno   cimport errno, EINPROGRESS, EAGAIN
from cpython.ref  cimport PyObject, Py_INCREF, Py_DECREF

ctypedef long ssize_t

cdef extern from "errno.h":
    int EWOULDBLOCK

cdef extern from "arpa/inet.h":
    int inet_aton(const char*, void*)
    uint32_t inet_addr(const char*)

cdef extern from "sys/socket.h":
    ctypedef struct sockaddr
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

# ─── Compile-time flags ───────────────────────────────────────────────────────
DEF CY_HAVE_VPP = 0
DEF CY_HAVE_XDP = 0
HAVE_VPP = CY_HAVE_VPP != 0
HAVE_XDP = CY_HAVE_XDP != 0

import threading, time
from enum import IntEnum

# ─── Lock-free single-producer/single-consumer ring ───────────────────────────
cdef class Ring:
    cdef PyObject **slots
    cdef int size, head, tail

    def __cinit__(self, int size):
        #print(f"[Ring] Initializing with size {size}")
        if size & (size - 1):
            raise ValueError("size must be power-of-2")
        self.size  = size
        self.head  = self.tail = 0
        self.slots = <PyObject **> malloc(sizeof(PyObject*) * size)
        memset(self.slots, 0, sizeof(PyObject*) * size)

    def __dealloc__(self):
        if self.slots != NULL:
            #print("[Ring] Deallocating")
            free(self.slots)

    cdef inline bint _push(self, PyObject* obj):
        cdef int nxt = (self.head + 1) & (self.size - 1)
        if nxt == self.tail:
            return False
        Py_INCREF(<object> obj)
        self.slots[self.head] = obj
        self.head = nxt
        return True

    cdef inline PyObject* _pop(self):
        if self.tail == self.head:
            return <PyObject*> 0
        cdef PyObject* obj = self.slots[self.tail]
        self.tail = (self.tail + 1) & (self.size - 1)
        return obj

    cpdef bint push(self, object item):
        return self._push(<PyObject*> item)

    cpdef object pop(self):
        cdef PyObject* obj = self._pop()
        if obj == <PyObject*> 0:
            return None
        pyobj = <object> obj
        Py_DECREF(<object> obj)
        return pyobj

# ─── Transport abstraction ────────────────────────────────────────────────────
ctypedef enum:
    KERNEL = 0
    VPP    = 1
    RAW    = 2

cdef class _BaseTransport:
    cdef public int fd

    def __cinit__(self):
        #print("[BaseTransport] Initializing")
        self.fd = -1
        #print(f"[BaseTransport] fd initialized to {self.fd}")

    cdef ssize_t _send(self, const char* buf, size_t n) nogil:
        return write(self.fd, buf, n)

    cdef ssize_t _recv(self, char* buf, size_t n) nogil:
        return read(self.fd, buf, n)

    cpdef int send(self, bytes data):
        #print(f"[BaseTransport] Sending {len(data)} bytes")
        cdef const char* p = data
        return self._send(p, len(data))

    cpdef bytes recv(self, size_t n):
        #print(f"[BaseTransport] Receiving up to {n} bytes")
        cdef char[::1] buf = bytearray(n)
        cdef ssize_t r = self._recv(&buf[0], n)
        if r <= 0:
            #print(f"[BaseTransport] Received {r} bytes (empty or error)")
            return b""
        #print(f"[BaseTransport] Received {r} bytes")
        return bytes(buf[:r])

    cpdef void close(self):
        if self.fd >= 0:
            #print("[BaseTransport] Closing socket")
            close(self.fd)
            self.fd = -1

cdef class _KernelTransport(_BaseTransport):
    cpdef int connect(self, const char* ip, uint16_t port):
        #print(f"[KernelTransport] Connecting to {ip.decode()}:{port}")
        self.fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)
        if self.fd < 0:
            #print("[KernelTransport] Socket creation failed")
            raise OSError("socket")
        #print(f"[KernelTransport] Socket created (fd={self.fd})")
        cdef sockaddr_in addr
        memset(&addr, 0, sizeof(sockaddr_in))
        addr.sin_family = AF_INET
        addr.sin_port = htons(port)
        addr.sin_addr.s_addr = inet_addr(ip)
        if connect(self.fd, <sockaddr_in*> &addr, sizeof(sockaddr_in)) == -1 \
           and errno != EINPROGRESS:
            #print(f"[KernelTransport] Connect failed with errno {errno}")
            close(self.fd)
            self.fd = -1
            raise OSError("connect")
        #print(f"[KernelTransport] Connect initiated (fd={self.fd})")
        return 0

# ─── Server ───────────────────────────────────────────────────────────────────
cdef class Server:
    cdef public Ring _rx
    cdef public _KernelTransport _tp
    cdef public object _thr
    cdef public bint _stop

    def __init__(self, bytes ip, int port, int kind=0):
        #print(f"[Server] Initializing with ip={ip.decode()}, port={port}, kind={kind}")
        self._rx = Ring(1024)
        #print("[Server] Ring initialized")
        self._tp = _KernelTransport()
        #print(f"[Server] Transport initialized (fd={self._tp.fd})")
        self._stop = False
        #print(f"[Server] Stop flag initialized to {self._stop}")

        lsock = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)
        #print(f"[Server] Listening socket created (fd={lsock})")
        cdef int one = 1
        setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(int))
        cdef sockaddr_in addr
        memset(&addr, 0, sizeof(addr))
        addr.sin_family = AF_INET
        addr.sin_port = htons(port)
        addr.sin_addr.s_addr = inet_addr(<const char*> ip)
        if bind(lsock, <sockaddr_in*> &addr, sizeof(addr)) == -1:
            #print(f"[Server] Bind failed with errno {errno}")
            close(lsock)
            raise OSError("bind")
        #print("[Server] Socket bound")
        if listen(lsock, 16) == -1:
            #print(f"[Server] Listen failed with errno {errno}")
            close(lsock)
            raise OSError("listen")
        #print("[Server] Listening started")
        
        # Non-blocking accept loop with timeout
        cdef int conn = -1
        cdef double start_time = time.time()
        while conn == -1 and (time.time() - start_time) < 5.0:  # 5-second timeout
            conn = accept(lsock, NULL, NULL)
            if conn == -1 and errno != EAGAIN and errno != EWOULDBLOCK:
                #print(f"[Server] Accept failed with errno {errno}")
                close(lsock)
                raise OSError(f"accept failed with errno {errno}")
            #print("[Server] Waiting for client connection...")
            time.sleep(0.001)
        if conn == -1:
            #print("[Server] Accept timed out")
            close(lsock)
            raise TimeoutError("accept timed out waiting for client")
        
        #print(f"[Server] Client connected (fd={conn})")
        close(lsock)
        self._tp.fd = conn
        #print(f"[Server] Connection set to non-blocking (fd={self._tp.fd})")
        fcntl(conn, F_SETFL, O_NONBLOCK)
        self._thr = threading.Thread(target=self._reader, daemon=True)
        #print("[Server] Starting reader thread")
        self._thr.start()

    def _reader(self):
        #print("[Server] Reader thread started")
        cdef char[4096] buf
        while not self._stop:
            n = self._tp._recv(&buf[0], 4096)
            if n <= 0:
                if n == -1 and (errno == EAGAIN or errno == EWOULDBLOCK):
                    time.sleep(0.0001)
                    continue
                #print(f"[Server] Reader stopped: n={n}")
                break
            pkt = bytes(buf[:n])
            #print(f"[Server] Reader received {n} bytes")
            while not self._rx.push(pkt):
                time.sleep(0.0001)
        #print("[Server] Reader thread exiting")

    def run(self, handler):
        #print("[Server] Starting run loop")
        try:
            while True:
                msg = self._rx.pop()
                if msg is not None:
                    #print(f"[Server] Processing message: {len(msg)} bytes")
                    handler(msg)
                else:
                    time.sleep(0.0001)
        except KeyboardInterrupt:
            #print("[Server] KeyboardInterrupt received")
            self._stop = True
            self._thr.join()
            self._tp.close()
            #print("[Server] Run loop stopped")

    cpdef void stop(self):
        #print("[Server] Stopping via method")
        self._stop = True
        self._thr.join()
        self._tp.close()

# ─── Client ───────────────────────────────────────────────────────────────────
cdef class Client:
    cdef public Ring _tx
    cdef public _KernelTransport _tp
    cdef public object _thr
    cdef public bint _stop

    def __init__(self, bytes ip, int port):
        #print(f"[Client] Initializing with ip={ip.decode()}, port={port}")
        self._tp = _KernelTransport()
        #print(f"[Client] Transport initialized (fd={self._tp.fd})")
        self._tp.connect(<const char*> ip, port)
        #print(f"[Client] Connect completed (fd={self._tp.fd})")
        self._tx = Ring(1024)
        #print("[Client] Ring initialized")
        self._stop = False
        #print(f"[Client] Stop flag initialized to {self._stop}")
        self._thr = threading.Thread(target=self._flusher, daemon=True)
        #print("[Client] Starting flusher thread")
        self._thr.start()

    def _flusher(self):
        #print("[Client] Flusher thread started")
        while not self._stop:
            msg = self._tx.pop()
            if msg is None:
                time.sleep(0.0001)
                continue
            self._tp.send(msg)
            #print(f"[Client] Flushed message: {len(msg)} bytes")
        #print("[Client] Flusher thread exiting")

    def send(self, bytes data):
        #print(f"[Client] Pushing {len(data)} bytes to send")
        while not self._tx.push(data):
            time.sleep(0.0001)

    def close(self):
        #print("[Client] Closing")
        self._stop = True
        self._thr.join()
        self._tp.close()
        #print("[Client] Closed")

    cpdef void stop(self):
        #print("[Client] Stopping via method")
        self._stop = True
        self._thr.join()
        self._tp.close()