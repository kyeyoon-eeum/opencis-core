"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

import asyncio
from asyncio import create_task
from typing import Optional

from opencis.cxl.transport.cci_packets import CciMessagePacket, CciPayloadPacket
from opencis.util.logger import logger
from opencis.util.component import LabeledComponent
from opencis.cxl.transport.packet_structs import SystemHeader
from opencis.cxl.transport.common import BasePacket
from opencis.cxl.transport.stream_types import StreamReaderLike

# pylint: disable=duplicate-code


class MctpPacketReader(LabeledComponent):
    def __init__(
        self,
        reader: StreamReaderLike,
        label: Optional[str] = None,
        parent_name: Optional[str] = None,
    ):
        label_prefix = parent_name + ":" if parent_name else ""
        super().__init__(lambda class_name: f"{label_prefix}{class_name}")
        self._reader = reader
        self._aborted = False
        self._task = None
        self._hdr_buf = bytearray(SystemHeader.get_size())
        self._payload_buf = bytearray(2048)

    async def get_packet(self) -> CciMessagePacket:
        if self._aborted:
            raise Exception("PacketReader is already aborted")
        try:
            self._task = create_task(self._get_packet_in_task())
            packet = await self._task
        except Exception as e:
            logger.debug(self._create_message("Aborted"))
            raise Exception("PacketReader is aborted") from e
        finally:
            self._task = None
        return packet

    def get_packet_blocking(self) -> CciMessagePacket:
        if self._aborted:
            raise Exception("PacketReader is already aborted")
        base_packet, payload = self._get_payload_blocking()
        payload = bytearray(payload)
        if base_packet.is_cci() is not True:
            raise ValueError(f"Must be CCI packet {type(base_packet)}")
        return CciPayloadPacket(payload)

    def abort(self):
        if self._aborted:
            return
        logger.debug(self._create_message("Aborting"))
        self._aborted = True
        if self._task is not None:
            self._task.cancel()

    async def _get_packet_in_task(self):
        base_packet, payload = await self._get_payload()
        payload = bytearray(payload)
        if base_packet.is_cci() is not True:
            raise ValueError(f"Must be CCI packet {type(base_packet)}")

        # Wrap the payload with CciPayloadPacket
        packet = CciPayloadPacket(payload)
        return packet

    async def _get_payload(self):
        # logger.debug(self._create_message("Waiting Packet"))
        await self._readinto_exactly(self._hdr_buf, SystemHeader.get_size())
        base_packet = BasePacket(self._hdr_buf)
        remaining_length = base_packet.system_header.payload_length - len(base_packet)
        if remaining_length < 0:
            raise Exception("remaining length is less than 0")
        total_len = len(self._hdr_buf) + remaining_length
        if total_len > len(self._payload_buf):
            self._payload_buf = bytearray(total_len)
        payload = self._payload_buf[:total_len]
        payload[: len(self._hdr_buf)] = self._hdr_buf
        if remaining_length:
            await self._readinto_exactly(payload[len(self._hdr_buf) :], remaining_length)
        # logger.debug(self._create_message("Received Packet"))
        return base_packet, payload

    async def _readinto_exactly(self, buf: bytearray, n: int) -> None:
        reader = self._reader
        if hasattr(reader, "readexactly_blocking"):
            data = await asyncio.to_thread(reader.readexactly_blocking, n)
            memoryview(buf)[:n] = data
            return
        if hasattr(reader, "readinto_exactly"):
            await reader.readinto_exactly(buf, n)  # type: ignore[attr-defined]
            return
        data = await reader.readexactly(n)
        memoryview(buf)[:n] = data

    def _get_payload_blocking(self):
        self._readinto_exactly_blocking(self._hdr_buf, SystemHeader.get_size())
        base_packet = BasePacket(self._hdr_buf)
        remaining_length = base_packet.system_header.payload_length - len(base_packet)
        if remaining_length < 0:
            raise Exception("remaining length is less than 0")
        total_len = len(self._hdr_buf) + remaining_length
        if total_len > len(self._payload_buf):
            self._payload_buf = bytearray(total_len)
        payload = self._payload_buf[:total_len]
        payload[: len(self._hdr_buf)] = self._hdr_buf
        if remaining_length:
            self._readinto_exactly_blocking(payload[len(self._hdr_buf) :], remaining_length)
        return base_packet, payload

    def _readinto_exactly_blocking(self, buf: bytearray, n: int) -> None:
        reader = self._reader
        if hasattr(reader, "readexactly_blocking"):
            data = reader.readexactly_blocking(n)
            memoryview(buf)[:n] = data
            return
        raise RuntimeError("Blocking read is unavailable for the provided reader")
