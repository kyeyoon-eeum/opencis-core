"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from dataclasses import dataclass, field
from struct import pack, unpack_from
from typing import List, Tuple

from opencis.cxl.component.cci_executor import CciCommand


@dataclass
class SetLdAllocationsRequestPayload:
    number_of_lds: int = field(default=0)  # 1 byte
    start_ld_id: int = field(default=0)  # 1 byte
    ld_allocation_list: List[Tuple[int, int]] = field(default_factory=list)

    @classmethod
    def parse(cls, data: bytes) -> "SetLdAllocationsRequestPayload":
        if len(data) < 4:
            raise ValueError("Payload too short")
        number_of_lds = data[0]
        start_ld_id = data[1]
        # data[2:4] reserved
        offset = 4
        allocations: List[Tuple[int, int]] = []
        for _ in range(number_of_lds):
            if offset + 16 > len(data):
                raise ValueError("Payload truncated while reading allocation list")
            range_1, range_2 = unpack_from("<QQ", data, offset)
            allocations.append((range_1, range_2))
            offset += 16
        return cls(number_of_lds, start_ld_id, allocations)

    def dump(self) -> bytes:
        data = bytearray()
        data.extend(pack("<BB", self.number_of_lds, self.start_ld_id))
        data.extend(b"\x00\x00")  # reserved
        for range_1, range_2 in self.ld_allocation_list:
            data.extend(pack("<QQ", range_1, range_2))
        return bytes(data)

    def get_pretty_print(self) -> str:
        allocation_str = "\n".join(
            f"  - Range 1: {range_1}, Range 2: {range_2}"
            for range_1, range_2 in self.ld_allocation_list
        )
        return (
            f"- NUMBER_OF_LDS: {self.number_of_lds}\n"
            f"- START_LD_ID: {self.start_ld_id}\n"
            f"- LD_ALLOCATION_LIST:\n{allocation_str}"
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "SetLdAllocationsRequestPayload":
        return cls.parse(data)


@dataclass
class SetLdAllocationsResponsePayload:
    number_of_lds: int = field(default=0)  # 1 byte
    start_ld_id: int = field(default=0)  # 1 byte
    ld_allocation_list: List[Tuple[int, int]] = field(default_factory=list)

    @classmethod
    def parse(cls, data: bytes) -> "SetLdAllocationsResponsePayload":
        if len(data) < 4:
            raise ValueError("Payload too short")
        number_of_lds = data[0]
        start_ld_id = data[1]
        # data[2:4] reserved
        offset = 4
        allocations: List[Tuple[int, int]] = []
        for _ in range(number_of_lds):
            if offset + 16 > len(data):
                raise ValueError("Payload truncated while reading allocation list")
            range_1, range_2 = unpack_from("<QQ", data, offset)
            allocations.append((range_1, range_2))
            offset += 16
        return cls(number_of_lds, start_ld_id, allocations)

    def dump(self) -> bytes:
        data = bytearray()
        data.extend(pack("<BB", self.number_of_lds, self.start_ld_id))
        data.extend(b"\x00\x00")  # reserved
        for range_1, range_2 in self.ld_allocation_list:
            data.extend(pack("<QQ", range_1, range_2))
        return bytes(data)

    def get_pretty_print(self) -> str:
        allocation_str = "\n".join(
            f"  - Range 1: {range_1}, Range 2: {range_2}"
            for range_1, range_2 in self.ld_allocation_list
        )
        return (
            f"- NUMBER_OF_LDS: {self.number_of_lds}\n"
            f"- START_LD_ID: {self.start_ld_id}\n"
            f"- LD_ALLOCATION_LIST:\n{allocation_str}"
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "SetLdAllocationsResponsePayload":
        return cls.parse(data)


class SetLdAllocationsCommand(CciCommand):
    def __init__(self):
        super().__init__()

    def get_opcode(self) -> int:
        # Use real opcode if available in constants; placeholder kept for compatibility
        return 0x0001

    def _execute(self, request: SetLdAllocationsRequestPayload) -> SetLdAllocationsResponsePayload:
        # Echo back structure with same counts; actual allocation handling occurs upstream
        return SetLdAllocationsResponsePayload(
            number_of_lds=request.number_of_lds,
            start_ld_id=request.start_ld_id,
            ld_allocation_list=list(request.ld_allocation_list),
        )

    @staticmethod
    def create_cci_request() -> "SetLdAllocationsCommand":
        return SetLdAllocationsCommand()

    @staticmethod
    def parse_request_payload(payload: bytes) -> SetLdAllocationsRequestPayload:
        return SetLdAllocationsRequestPayload.from_bytes(payload)

    @staticmethod
    def parse_response_payload(payload: bytes) -> SetLdAllocationsResponsePayload:
        return SetLdAllocationsResponsePayload.from_bytes(payload)
