# cython: language_level=3
# cython: boundscheck=False, wraparound=False, nonecheck=False, initializedcheck=False

from libc.stdint cimport uint8_t
from libc.stddef cimport size_t


cpdef void write_int_to_mmap(object mm, size_t offset, object value, size_t size):
    """
    Write an unsigned integer value to the mmap at the given offset using little-endian byte order.
    Supports size up to 8 bytes.
    """
    cdef unsigned long long v = <unsigned long long> value
    cdef size_t i
    for i in range(size):
        mm[offset + i] = <uint8_t> (v & 0xFF)
        v >>= 8


cpdef object read_int_from_mmap(object mm, size_t offset, size_t size):
    """
    Read an unsigned little-endian integer of the given size (<=8 bytes) from the mmap at offset.
    Returns a Python int.
    """
    cdef unsigned long long result = 0
    cdef size_t i
    for i in range(size, 0, -1):
        result = (result << 8) | <unsigned long long> mm[offset + (i - 1)]
    return result