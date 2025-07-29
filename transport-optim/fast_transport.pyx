# cython: language_level=3
# cython: boundscheck=False  
# cython: wraparound=False
# cython: cdivision=True
# cython: initializedcheck=False

import os
import mmap
import tempfile
from libc.stdint cimport uint64_t
from libc.string cimport memcpy
from cpython.bytes cimport PyBytes_FromStringAndSize

# CPU optimization functions
cdef extern from *:
    """
    static inline void cpu_relax() {
        #ifdef __x86_64__
        __asm__ __volatile__("pause" ::: "memory");
        #else
        __asm__ __volatile__("" ::: "memory");
        #endif
    }
    
    static inline void compiler_barrier() {
        __asm__ __volatile__("" ::: "memory");
    }
    """
    void cpu_relax() nogil
    void compiler_barrier() nogil

# Fast ring buffer using memory-mapped files
cdef class FastRingBuffer:
    cdef object memory_map
    cdef char* data_ptr
    cdef uint64_t* head_ptr
    cdef uint64_t* tail_ptr
    cdef uint64_t buffer_size
    cdef uint64_t mask
    cdef str filepath
    
    def __cinit__(self, str name, uint64_t size_mb=64):  # 64MB for maximum throughput
        # Ensure power of 2
        if size_mb & (size_mb - 1):
            size_mb = 1 << (size_mb.bit_length() - 1)
        
        self.buffer_size = size_mb * 1024 * 1024
        self.mask = self.buffer_size - 1
        self.filepath = f"/tmp/fastbuf_{name}.bin"
        
        # Total size: 64 bytes for cache-aligned head/tail + buffer
        total_size = 64 + self.buffer_size
        
        # Create or open file
        cdef char[:] view
        try:
            fd = os.open(self.filepath, os.O_CREAT | os.O_RDWR, 0o666)
            os.ftruncate(fd, total_size)
            
            self.memory_map = mmap.mmap(fd, total_size, access=mmap.ACCESS_WRITE)
            os.close(fd)
            
            # Set up cache-aligned pointers using buffer protocol
            view = self.memory_map
            self.head_ptr = <uint64_t*>&view[0]
            self.tail_ptr = <uint64_t*>&view[32]  # 32-byte separation for cache alignment
            self.data_ptr = &view[64]
            
            # Initialize counters for first time
            if self.head_ptr[0] == 0 and self.tail_ptr[0] == 0:
                self.head_ptr[0] = 0
                self.tail_ptr[0] = 0
            
        except Exception as e:
            raise RuntimeError(f"Failed to create shared buffer: {e}")
    
    def __dealloc__(self):
        if self.memory_map:
            self.memory_map.close()
        try:
            os.unlink(self.filepath)
        except:
            pass
    
    cdef inline bint try_push(self, const char* data, uint64_t size):
        cdef uint64_t head = self.head_ptr[0]
        cdef uint64_t tail = self.tail_ptr[0] 
        cdef uint64_t total_size = size + 8  # Include size header
        cdef uint64_t available
        cdef uint64_t pos
        cdef uint64_t first
        
        # Check available space
        available = self.buffer_size - (head - tail)
        if available < total_size:
            return False
        
        pos = head & self.mask
        
        # Write size header
        if pos + 8 <= self.buffer_size:
            (<uint64_t*>(self.data_ptr + pos))[0] = size
        else:
            # Wrap around for size
            first = self.buffer_size - pos
            memcpy(self.data_ptr + pos, &size, first)
            memcpy(self.data_ptr, <char*>&size + first, 8 - first)
        
        # Write data
        pos = (head + 8) & self.mask
        if pos + size <= self.buffer_size:
            memcpy(self.data_ptr + pos, data, size)
        else:
            # Wrap around for data
            first = self.buffer_size - pos
            memcpy(self.data_ptr + pos, data, first)
            memcpy(self.data_ptr, data + first, size - first)
        
        # Update head atomically
        self.head_ptr[0] = head + total_size
        return True
    
    cdef inline uint64_t try_pop(self, char* buffer, uint64_t max_size):
        cdef uint64_t head = self.head_ptr[0]
        cdef uint64_t tail = self.tail_ptr[0]
        cdef uint64_t pos
        cdef uint64_t size
        cdef uint64_t first
        
        if head == tail:
            return 0
        
        pos = tail & self.mask
        
        # Read size header
        if pos + 8 <= self.buffer_size:
            size = (<uint64_t*>(self.data_ptr + pos))[0]
        else:
            # Wrap around for size
            first = self.buffer_size - pos
            memcpy(&size, self.data_ptr + pos, first)
            memcpy(<char*>&size + first, self.data_ptr, 8 - first)
        
        if size > max_size:
            return 0
        
        # Read data
        pos = (tail + 8) & self.mask
        if pos + size <= self.buffer_size:
            memcpy(buffer, self.data_ptr + pos, size)
        else:
            # Wrap around for data
            first = self.buffer_size - pos
            memcpy(buffer, self.data_ptr + pos, first)
            memcpy(buffer + first, self.data_ptr, size - first)
        
        # Update tail atomically
        self.tail_ptr[0] = tail + size + 8
        return size
    
    cpdef bint push(self, bytes data):
        cdef const char* ptr = data
        return self.try_push(ptr, len(data))
    
    cpdef bytes pop(self):
        cdef char buffer[65536]
        cdef uint64_t size = self.try_pop(buffer, 65536)
        if size == 0:
            return None
        return PyBytes_FromStringAndSize(buffer, size)
    
    # Ultra-fast fixed-size methods for 82-byte messages
    cdef inline bint try_push_82(self, const char* data) nogil:
        cdef uint64_t head = self.head_ptr[0]
        cdef uint64_t tail = self.tail_ptr[0] 
        cdef uint64_t next_head = head + 82
        cdef uint64_t pos
        cdef uint64_t first
        
        # Fast space check for fixed-size messages (no size header needed)
        if next_head - tail > self.buffer_size:
            return False
        
        pos = head & self.mask
        
        # Fast path: no wrap-around (most common case)
        if pos + 82 <= self.buffer_size:
            memcpy(self.data_ptr + pos, data, 82)
        else:
            # Wrap-around case
            first = self.buffer_size - pos
            memcpy(self.data_ptr + pos, data, first)
            memcpy(self.data_ptr, data + first, 82 - first)
        
        # Memory barrier and update head atomically
        compiler_barrier()
        self.head_ptr[0] = next_head
        return True
    
    cdef inline bint try_pop_82(self, char* buffer) nogil:
        cdef uint64_t head = self.head_ptr[0]
        cdef uint64_t tail = self.tail_ptr[0]
        cdef uint64_t pos
        cdef uint64_t first
        
        if head == tail:
            return False
        
        pos = tail & self.mask
        
        # Fast path: no wrap-around (most common case)
        if pos + 82 <= self.buffer_size:
            memcpy(buffer, self.data_ptr + pos, 82)
        else:
            # Wrap-around case
            first = self.buffer_size - pos
            memcpy(buffer, self.data_ptr + pos, first)
            memcpy(buffer + first, self.data_ptr, 82 - first)
        
        # Memory barrier and update tail atomically
        compiler_barrier()
        self.tail_ptr[0] = tail + 82
        return True
    
    cpdef bint push_82(self, bytes data):
        """Ultra-fast push for exactly 82-byte messages"""
        cdef const char* ptr = data
        cdef uint64_t spins = 0
        
        while not self.try_push_82(ptr):
            spins += 1
            if spins & 1023 == 0:  # Every 1024 spins
                import time
                time.sleep(0.000001)  # 1 microsecond yield
            else:
                cpu_relax()  # Use CPU pause instruction for tight loops
        return True
    
    cpdef bytes pop_82(self):
        """Ultra-fast pop for exactly 82-byte messages"""
        cdef char buffer[82]
        cdef uint64_t spins = 0
        
        while not self.try_pop_82(buffer):
            spins += 1
            if spins & 1023 == 0:  # Every 1024 spins
                import time
                time.sleep(0.000001)  # 1 microsecond yield
            else:
                cpu_relax()  # Use CPU pause instruction for tight loops
        
        return PyBytes_FromStringAndSize(buffer, 82)

# High-performance transport classes
cdef class FastProducer:
    cdef FastRingBuffer tx_buffer
    cdef FastRingBuffer rx_buffer
    
    def __init__(self, str name):
        self.tx_buffer = FastRingBuffer(f"{name}_tx", 64)  # 64MB buffers
        self.rx_buffer = FastRingBuffer(f"{name}_rx", 64)
    
    cpdef void send(self, bytes data):
        while not self.tx_buffer.push(data):
            pass  # Busy wait for maximum speed
    
    cpdef bytes recv(self):
        cdef bytes result
        while True:
            result = self.rx_buffer.pop()
            if result is not None:
                return result
    
    # Ultra-fast methods for 82-byte messages
    cpdef void send_82(self, bytes data):
        """Ultra-fast send for 82-byte messages"""
        self.tx_buffer.push_82(data)
    
    cpdef bytes recv_82(self):
        """Ultra-fast receive for 82-byte messages"""
        return self.rx_buffer.pop_82()

cdef class FastConsumer:
    cdef FastRingBuffer tx_buffer  
    cdef FastRingBuffer rx_buffer
    
    def __init__(self, str name):
        # Consumer uses opposite buffers
        self.rx_buffer = FastRingBuffer(f"{name}_tx", 64)  # 64MB buffers
        self.tx_buffer = FastRingBuffer(f"{name}_rx", 64)
    
    cpdef void send(self, bytes data):
        while not self.tx_buffer.push(data):
            pass
    
    cpdef bytes recv(self):
        cdef bytes result
        while True:
            result = self.rx_buffer.pop()
            if result is not None:
                return result
    
    # Ultra-fast methods for 82-byte messages
    cpdef void send_82(self, bytes data):
        """Ultra-fast send for 82-byte messages"""
        self.tx_buffer.push_82(data)
    
    cpdef bytes recv_82(self):
        """Ultra-fast receive for 82-byte messages"""
        return self.rx_buffer.pop_82()

# Factory for creating transports
class FastTransport:
    @staticmethod
    def create_producer(name: str):
        return FastProducer(name)
    
    @staticmethod  
    def create_consumer(name: str):
        return FastConsumer(name) 