# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False, initializedcheck=False

# High-performance stdlib-compatible logger in a single Cython file.
# Implements per-thread SPSC rings feeding a single flusher thread (nogil) using writev.

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.unicode cimport PyUnicode_AsUTF8AndSize
from libc.string cimport memcpy
from libc.stdint cimport uint64_t, uint32_t, uint16_t
from libc.stddef cimport size_t
from libc.stdlib cimport malloc, free

# ---- C imports ----
cdef extern from "stdatomic.h":
    ctypedef int atomic_int
    ctypedef unsigned long atomic_ulong
    int atomic_load_explicit(atomic_int*, int) nogil
    void atomic_store_explicit(atomic_int*, int, int) nogil
    unsigned long atomic_fetch_add_explicit(atomic_ulong*, unsigned long, int) nogil
    void atomic_thread_fence(int) nogil
    cdef int memory_order_relaxed, memory_order_acquire, memory_order_release, memory_order_seq_cst

cdef extern from "pthread.h":
    int pthread_create(unsigned long*, void*, void*, void*) nogil
    int pthread_join(unsigned long, void**) nogil
    int pthread_setname_np(unsigned long, const char*)
    unsigned long pthread_self() nogil

# Replace iovec with a local ABI-compatible struct to avoid header typedef issues
cdef struct IOVec:
    void* iov_base
    size_t iov_len

cdef extern from "sys/uio.h":
    long writev(int fd, const void* iov, int iovcnt) nogil

cdef extern from "fcntl.h":
    int open(const char *pathname, int flags, int mode)
    int fcntl(int fd, int cmd, ...)

cdef extern from "unistd.h":
    int close(int fd) nogil
    long write(int fd, const void *buf, size_t count) nogil
    int usleep(unsigned int usec) nogil

# Avoid reliance on macros like IOV_MAX; we set runtime globals

DEF COMPILED_ENTRY_MAX = 512  # Upper bound for per-entry data bytes stored in ring
DEF STDERR_FILENO = 2
DEF STDOUT_FILENO = 1

# ---- Data structures ----
cdef int ENTRY_DATA_MAX = COMPILED_ENTRY_MAX  # effective clamp, set in install()
cdef int RING_CAP_MASK = 4096 - 1            # default, set in install()

cdef struct Entry:
    uint64_t ts_ns
    uint32_t lvl
    uint16_t len
    uint16_t reserved
    char data[COMPILED_ENTRY_MAX]

cdef struct Ring:
    Entry* buf
    size_t cap_mask
    atomic_ulong head_resv   # producer reservation counter
    atomic_ulong head_pub    # published entries count
    unsigned long tail       # consumer-only
    char _pad[64]

# Python TLS holder for Ring* pointer
cdef class _RingHolder:
    cdef Ring* ring

# ---- Globals ----
cdef atomic_int g_level       # current effective level (numeric)
cdef atomic_int g_running     # 1 while flusher should run
cdef int g_fd = STDOUT_FILENO # sink fd (1 for stdout)
cdef int g_immediate_level = 30  # WARNING
cdef int g_flush_interval_ms = 20
cdef int g_iov_cap = 256          # maximum iovecs per writev batch
cdef int g_iov_byte_cap = 4096    # max total bytes per writev batch (for pipes/ttys)
cdef atomic_int g_fd_close_flag    # 1 when an old fd should be closed in flusher
cdef int g_old_fd_to_close = -1

# registry of per-thread rings (append-only)
cdef struct RegItem:
    unsigned long tid
    Ring* ring

cdef RegItem* g_registry = NULL
cdef int g_reg_cap = 0
cdef int g_reg_size = 0
cdef object g_reg_lock = None
cdef object g_tls_local = None

cdef unsigned long g_flusher_thread = 0

# optional cached path for reopen()
cdef object g_path_obj = None

# ---- Utilities ----
cdef inline size_t _round_up_pow2(size_t v) nogil:
    if v <= 1:
        return 1
    v -= 1
    v |= v >> 1
    v |= v >> 2
    v |= v >> 4
    v |= v >> 8
    v |= v >> 16
    if sizeof(size_t) == 8:
        v |= v >> 32
    return v + 1

cdef inline int _min_int(int a, int b) nogil:
    return a if a < b else b

cdef Ring* _ensure_tls_ring():
    cdef Ring* r
    global g_tls_local
    if g_tls_local is None:
        import threading
        g_tls_local = threading.local()
    try:
        rh = g_tls_local.h
        if isinstance(rh, _RingHolder) and (<_RingHolder>rh).ring != NULL:
            return (<_RingHolder>rh).ring
    except Exception:
        pass

    # Allocate ring and buffer
    cdef size_t cap_mask = <size_t>RING_CAP_MASK
    cdef size_t cap = cap_mask + 1
    r = <Ring*>PyMem_Malloc(sizeof(Ring))
    if r == NULL:
        raise MemoryError()
    r.buf = <Entry*>PyMem_Malloc(sizeof(Entry) * cap)
    if r.buf == NULL:
        PyMem_Free(r)
        raise MemoryError()
    r.cap_mask = cap_mask
    # Initialize producer/consumer indices
    r.head_resv = 0
    r.head_pub = 0
    r.tail = 0

    # Prepare variables used in registry growth before entering with-block
    cdef int new_cap
    cdef RegItem* new_arr

    # Append to global registry (append-only)
    global g_reg_lock, g_registry, g_reg_cap, g_reg_size
    if g_reg_lock is None:
        import threading
        g_reg_lock = threading.Lock()
    with g_reg_lock:
        if g_reg_cap == 0:
            g_reg_cap = 128
            g_registry = <RegItem*>PyMem_Malloc(g_reg_cap * sizeof(RegItem))
            if g_registry == NULL:
                PyMem_Free(r.buf)
                PyMem_Free(r)
                raise MemoryError()
        elif g_reg_size >= g_reg_cap:
            new_cap = g_reg_cap * 2
            new_arr = <RegItem*>PyMem_Malloc(new_cap * sizeof(RegItem))
            if new_arr == NULL:
                PyMem_Free(r.buf)
                PyMem_Free(r)
                raise MemoryError()
            # copy old
            memcpy(new_arr, g_registry, g_reg_cap * sizeof(RegItem))
            # NOTE: keep old array allocated to avoid races; memory is small and growth is rare.
            g_registry = new_arr
            g_reg_cap = new_cap
        # Append new item
        g_registry[g_reg_size].tid = pthread_self()
        g_registry[g_reg_size].ring = r
        atomic_thread_fence(memory_order_release)
        g_reg_size += 1

    # Store into thread-local holder
    rh_new = _RingHolder()
    rh_new.ring = r
    try:
        g_tls_local.h = rh_new
    except Exception:
        pass
    return r

# ---- Producer hot path ----
cdef inline void _emit_copy_gil(Ring* r, int lvl, const char* p, size_t n, bint add_nl):
    cdef size_t max_n = <size_t>ENTRY_DATA_MAX
    cdef size_t n_eff = n
    if add_nl:
        if n_eff + 1 > max_n:
            n_eff = max_n - 1
    else:
        if n_eff > max_n:
            n_eff = max_n
    if r == NULL:
        return
    cdef unsigned long idx = atomic_fetch_add_explicit(&r.head_resv, 1, memory_order_relaxed)
    cdef Entry* e = &r.buf[idx & r.cap_mask]
    e.ts_ns = 0
    e.lvl = <uint32_t>lvl
    # set length after data copy to act as ready flag
    if n_eff:
        memcpy(<void*>e.data, <const void*>p, n_eff)
        e.len = <uint16_t>n_eff
    else:
        e.len = 0
    if add_nl and n_eff < max_n:
        e.data[n_eff] = '\n'
        e.len = <uint16_t>(n_eff + 1)
    atomic_thread_fence(memory_order_release)
    atomic_fetch_add_explicit(&r.head_pub, 1, memory_order_release)

cdef inline void _emit_nogil(Ring* r, int lvl, const char* p, size_t n) noexcept nogil:
    cdef size_t n_clamped = n
    if n_clamped > <size_t>ENTRY_DATA_MAX:
        n_clamped = <size_t>ENTRY_DATA_MAX
    if r == NULL:
        return
    cdef unsigned long idx = atomic_fetch_add_explicit(&r.head_resv, 1, memory_order_relaxed)
    cdef Entry* e = &r.buf[idx & r.cap_mask]
    e.ts_ns = 0
    e.lvl = <uint32_t>lvl
    if n_clamped:
        memcpy(<void*>e.data, <const void*>p, n_clamped)
        e.len = <uint16_t>n_clamped
    else:
        e.len = 0
    atomic_thread_fence(memory_order_release)
    atomic_fetch_add_explicit(&r.head_pub, 1, memory_order_release)

# ---- Flusher ----
cdef void* _flusher_main(void* arg) nogil:
    cdef int interval_ms
    while atomic_load_explicit(&g_running, memory_order_acquire) != 0:
        _drain_once_nogil()
        interval_ms = g_flush_interval_ms
        if interval_ms <= 0:
            interval_ms = 1
        usleep(<unsigned int>(interval_ms * 1000))
    # final drain
    _drain_once_nogil()
    return <void*>0

cdef inline void _update_tails_after_write(int full_count, Ring** iov_rings, int iovcnt) noexcept nogil:
    if full_count <= 0:
        return
    # Aggregate counts per ring (bounded by min(g_reg_size, iovcnt))
    cdef int max_rings = iovcnt if iovcnt < 256 else 256
    cdef Ring* rings[256]
    cdef int counts[256]
    cdef int used = 0
    cdef int i, j
    cdef Ring* r
    for i in range(full_count):
        r = iov_rings[i]
        # find or add
        for j in range(used):
            if rings[j] is r:
                counts[j] += 1
                break
        else:
            if used < max_rings:
                rings[used] = r
                counts[used] = 1
                used += 1
            else:
                # fallback: direct increment (rare)
                r.tail += 1
    for i in range(used):
        rings[i].tail += <unsigned long>counts[i]

cdef void _drain_once_nogil() noexcept nogil:
    # Declarations
    cdef int cap
    cdef IOVec* iov
    cdef Ring** iov_rings
    cdef int iovcnt
    cdef size_t total_bytes
    cdef int i
    cdef Ring* r
    cdef unsigned long head
    cdef unsigned long tail
    cdef unsigned long avail
    cdef Entry* e
    cdef long wrote
    cdef long w
    cdef int start
    cdef size_t offset
    cdef size_t remaining
    cdef IOVec first
    cdef IOVec* cur
    cdef char* base0
    global g_old_fd_to_close

    # Handle deferred close after fd swap
    if atomic_load_explicit(&g_fd_close_flag, memory_order_acquire) != 0:
        if g_old_fd_to_close != STDERR_FILENO and g_old_fd_to_close >= 0:
            close(g_old_fd_to_close)
        g_old_fd_to_close = -1
        atomic_store_explicit(&g_fd_close_flag, 0, memory_order_release)
    if g_fd < 0:
        return
    cap = g_iov_cap
    if cap <= 0:
        cap = 64
    iov = <IOVec*>malloc(sizeof(IOVec) * cap)
    if iov == NULL:
        return
    iov_rings = <Ring**>malloc(sizeof(Ring*) * cap)
    if iov_rings == NULL:
        free(iov)
        return

    # Drain in batches until empty
    while True:
        iovcnt = 0
        total_bytes = 0

        # Build iovec from available entries up to cap and byte-cap
        for i in range(g_reg_size):
            r = g_registry[i].ring
            if r == NULL:
                continue
            head = atomic_fetch_add_explicit(&r.head_pub, 0, memory_order_acquire)
            tail = r.tail
            if head == tail:
                continue
            avail = head - tail
            while avail > 0 and iovcnt < cap:
                e = &r.buf[tail & r.cap_mask]
                # fence to ensure data visible
                atomic_thread_fence(memory_order_acquire)
                if total_bytes + <size_t>e.len > <size_t>g_iov_byte_cap and iovcnt > 0:
                    # stop here to keep this batch within byte-cap
                    avail = 0
                    break
                iov[iovcnt].iov_base = <void*>e.data
                iov[iovcnt].iov_len = <size_t>e.len
                iov_rings[iovcnt] = r
                total_bytes += <size_t>e.len
                iovcnt += 1
                tail += 1
                avail -= 1
                if iovcnt >= cap:
                    break
            if iovcnt >= cap or total_bytes >= <size_t>g_iov_byte_cap:
                break

        if iovcnt == 0:
            break

        # Perform writev with handling of partial writes
        start = 0
        offset = 0
        remaining = total_bytes
        while start < iovcnt and remaining > 0:
            # Adjust first iov by offset
            first = iov[start]
            cur = &iov[start]
            if offset != 0:
                base0 = <char*>first.iov_base
                cur[0].iov_base = <void*>(base0 + offset)
                cur[0].iov_len = first.iov_len - offset
            wrote = writev(g_fd, <const void*>cur, iovcnt - start)
            if wrote <= 0:
                # Give up this round to avoid busy loop
                remaining = 0
                break
            remaining -= <size_t>wrote
            # advance start/offset according to bytes written
            w = wrote
            while start < iovcnt and w >= <long>iov[start].iov_len:
                w -= <long>iov[start].iov_len
                start += 1
                offset = 0
            if start < iovcnt:
                offset = <size_t>w

        # Number of fully written entries
        _update_tails_after_write(start, iov_rings, iovcnt)

        # If we couldn't write anything, break to avoid infinite loop
        if start == 0 and remaining == total_bytes:
            break

    free(iov)
    free(iov_rings)

# ---- Python API and Logger class ----
import logging as _py_logging
import os as _py_os

# At-fork child hook
def _after_fork_child() -> None:
    global g_reg_size
    # stop any inherited running flag, then restart fresh flusher
    atomic_store_explicit(&g_running, 0, memory_order_release)
    # reset registry so child will register its own rings
    g_reg_size = 0
    start()

# Fast path helper used by HPLogger methods
cdef inline void _log_fast_impl_c(object logger, int level, object msg, tuple args) except *:
    cdef int gl = atomic_load_explicit(&g_level, memory_order_acquire)
    if level < gl:
        return
    if args:
        try:
            msg = (<str>msg) % args
        except Exception:
            msg = f"{msg} {args}"
    elif not isinstance(msg, str):
        try:
            msg = str(msg)
        except Exception:
            msg = "<unprintable>"
    cdef const char* p
    cdef Py_ssize_t n
    cdef IOVec iov2[2]
    cdef long rv
    cdef long rv2
    p = PyUnicode_AsUTF8AndSize(msg, &n)
    if p == NULL:
        return
    if level >= g_immediate_level:
        if n == 0 or (<unsigned char>p[n - 1]) != 10:
            iov2[0].iov_base = <void*>p
            iov2[0].iov_len = <size_t>n
            iov2[1].iov_base = <void*>"\n"
            iov2[1].iov_len = <size_t>1
            with nogil:
                rv = writev(g_fd, <const void*>iov2, 2)
            if rv < 0:
                pass
        else:
            with nogil:
                rv2 = write(g_fd, <const void*>p, <size_t>n)
            if rv2 < 0:
                pass
        return
    cdef Ring* r = _ensure_tls_ring()
    _emit_copy_gil(r, level, p, <size_t>n, <bint>(n == 0 or (<unsigned char>p[n - 1]) != 10))


class HPLogger(_py_logging.Logger):
    def isEnabledFor(self, level: int) -> bool:
        cdef int gl = atomic_load_explicit(&g_level, memory_order_acquire)
        return level >= gl

    def debug(self, msg, *args, **kwargs):
        _log_fast_impl_c(self, 10, msg, args)

    def info(self, msg, *args, **kwargs):
        _log_fast_impl_c(self, 20, msg, args)

    def warning(self, msg, *args, **kwargs):
        _log_fast_impl_c(self, 30, msg, args)

    def error(self, msg, *args, **kwargs):
        _log_fast_impl_c(self, 40, msg, args)

    def critical(self, msg, *args, **kwargs):
        _log_fast_impl_c(self, 50, msg, args)

    def log(self, level: int, msg, *args, **kwargs):
        _log_fast_impl_c(self, <int>level, msg, args)

    def _log(self, level: int, msg, args, exc_info=None, extra=None, stack_info=False, stacklevel: int = 1, **kwargs) -> None:  # signature-compatible
        _log_fast_impl_c(self, <int>level, msg, args)

# ---- Helpers for level mapping ----
cdef int _to_level(object level) except *:
    if isinstance(level, int):
        return <int>level
    if isinstance(level, str):
        s = (<str>level).upper()
        if s == "DEBUG":
            return 10
        if s == "INFO":
            return 20
        if s == "WARNING" or s == "WARN":
            return 30
        if s == "ERROR":
            return 40
        if s == "CRITICAL" or s == "FATAL":
            return 50
    raise ValueError("Invalid log level")

# ---- Control functions ----
def install_fast_logger(level: object = "INFO",
                        path: object | None = None,
                        flush_interval_ms: int = 20,
                        ring_capacity: int = 4096,
                        entry_data_max: int = 256,
                        immediate_level: object = "WARNING",
                        disable_findcaller: bool = True) -> None:
    """Install the fast logger as the stdlib logger class and start flusher."""
    cdef int lvl
    cdef int imm
    cdef size_t cap
    cdef bytes pbytes
    cdef int flags
    global g_fd, g_immediate_level, g_flush_interval_ms, ENTRY_DATA_MAX, RING_CAP_MASK, g_path_obj, g_iov_cap, g_iov_byte_cap, g_tls_local, g_reg_lock

    # Configure levels
    lvl = _to_level(level)
    imm = _to_level(immediate_level)
    atomic_store_explicit(&g_level, lvl, memory_order_release)
    g_immediate_level = imm

    # Configure sink
    if path is None:
        g_fd = STDOUT_FILENO
        g_path_obj = None
    else:
        import os
        g_path_obj = path
        pbytes = (<str>path).encode('utf-8')
        flags = os.O_APPEND | os.O_WRONLY | os.O_CREAT
        if hasattr(os, 'O_CLOEXEC'):
            flags |= os.O_CLOEXEC
        g_fd = open(pbytes, flags, 0o644)
        if g_fd < 0:
            # fallback to stdout
            g_fd = STDOUT_FILENO

    # Configure buffering
    g_flush_interval_ms = flush_interval_ms if flush_interval_ms > 0 else 20
    ENTRY_DATA_MAX = _min_int(entry_data_max, COMPILED_ENTRY_MAX)
    # Ring capacity must be power of two
    cap = _round_up_pow2(ring_capacity if ring_capacity > 0 else 4096)
    if cap < 2:
        cap = 2
    RING_CAP_MASK = <int>(cap - 1)

    # iovec capacity: try to read from os.sysconf('SC_IOV_MAX'), else fallback
    try:
        import os
        _sc = os.sysconf_names.get('SC_IOV_MAX') if hasattr(os, 'sysconf_names') else None
        if _sc is not None:
            g_iov_cap = int(os.sysconf('SC_IOV_MAX'))
        else:
            g_iov_cap = 256
    except Exception:
        g_iov_cap = 256
    if g_iov_cap <= 0 or g_iov_cap > 1024:
        g_iov_cap = 256
    # byte cap for atomic-like writes on pipes
    try:
        import os
        _pb = os.sysconf_names.get('SC_PIPE_BUF') if hasattr(os, 'sysconf_names') else None
        if _pb is not None:
            g_iov_byte_cap = int(os.sysconf('SC_PIPE_BUF'))
        else:
            g_iov_byte_cap = 4096
    except Exception:
        g_iov_byte_cap = 4096
    if g_iov_byte_cap <= 0:
        g_iov_byte_cap = 4096

    # Registry lock and TLS local
    if g_reg_lock is None:
        import threading
        g_reg_lock = threading.Lock()
    if g_tls_local is None:
        import threading
        g_tls_local = threading.local()

    # Swap in our Logger class
    _py_logging.setLoggerClass(HPLogger)
    # Patch existing Logger instances to use our fast path as well
    try:
        _py_logging.Logger._log = HPLogger._log  # type: ignore[attr-defined]
        _py_logging.Logger.isEnabledFor = HPLogger.isEnabledFor  # type: ignore[attr-defined]
    except Exception:
        pass
    root = _py_logging.getLogger()
    root.setLevel(lvl)

    # Optionally reduce stdlib overhead by disabling findCaller in formatters/handlers (we do not use them)
    if disable_findcaller:
        try:
            _py_logging.Logger.findCaller = lambda self, stacklevel=1, stack_info=False: ("", 0, "", None)
        except Exception:
            pass

    # Register at-fork child hook to restart flusher in children
    try:
        _py_os.register_at_fork(after_in_child=_after_fork_child)
    except Exception:
        pass

    start()


def set_level(level: object) -> None:
    cdef int lvl = _to_level(level)
    atomic_store_explicit(&g_level, lvl, memory_order_release)


def get_level() -> int:
    return atomic_load_explicit(&g_level, memory_order_acquire)


def reopen() -> None:
    cdef bytes pbytes
    cdef int flags
    cdef int new_fd
    cdef int old_fd
    global g_fd
    if g_path_obj is None:
        return
    import os
    pbytes = (<str>g_path_obj).encode('utf-8')
    flags = os.O_APPEND | os.O_WRONLY | os.O_CREAT
    if hasattr(os, 'O_CLOEXEC'):
        flags |= os.O_CLOEXEC
    new_fd = open(pbytes, flags, 0o644)
    if new_fd >= 0:
        old_fd = g_fd
        g_fd = new_fd
        if old_fd != STDERR_FILENO and old_fd >= 0:
            g_old_fd_to_close = old_fd
            atomic_store_explicit(&g_fd_close_flag, 1, memory_order_release)


def flush() -> None:
    with nogil:
        _drain_once_nogil()


def start() -> None:
    if atomic_load_explicit(&g_running, memory_order_acquire) != 0:
        return
    atomic_store_explicit(&g_running, 1, memory_order_release)
    cdef int err
    with nogil:
        err = pthread_create(&g_flusher_thread, NULL, <void*>_flusher_main, NULL)
    if err != 0:
        # Failed to spawn flusher; fallback to no background flushing
        atomic_store_explicit(&g_running, 0, memory_order_release)
        raise RuntimeError("Failed to start flusher thread")
    try:
        pthread_setname_np(g_flusher_thread, b"fastlog_flush")
    except Exception:
        pass


def stop(timeout_ms: int = 500) -> None:
    if atomic_load_explicit(&g_running, memory_order_acquire) == 0:
        return
    atomic_store_explicit(&g_running, 0, memory_order_release)
    with nogil:
        pthread_join(g_flusher_thread, NULL)
    # final drain just in case
    with nogil:
        _drain_once_nogil()


# Optional: patch disabled levels to become near-free no-ops
def bind_noop_for_disabled_levels() -> None:
    lvl = atomic_load_explicit(&g_level, memory_order_acquire)
    Logger = _py_logging.Logger
    def _noop(self, *args, **kw):
        return None
    if lvl > 10 and getattr(Logger, 'debug', None) is not _noop:
        try:
            Logger.debug = _noop
        except Exception:
            pass
    if lvl > 20 and getattr(Logger, 'info', None) is not _noop:
        try:
            Logger.info = _noop
        except Exception:
            pass

# Set default level to INFO at import
atomic_store_explicit(&g_level, 20, memory_order_relaxed)
