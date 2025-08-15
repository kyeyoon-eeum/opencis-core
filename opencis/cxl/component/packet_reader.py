"""
Copyright (c) 2024-2025, Eeum, Inc.

This software is licensed under the terms of the Revised BSD License.
See LICENSE for details.
"""

from typing import Optional

from opencis.util.component import LabeledComponent
from opencis.util.logger import logger

from opencis.cxl.transport.shm_stream import ShmStreamReader
from opencis.cxl.transport.packet_constants import SYSTEM_PAYLOAD_TYPE

try:
    from opencis.cxl.transport import packet_reader_c as _prc
except Exception as e:  # pragma: no cover
    raise


class PacketReader(LabeledComponent):
    def __init__(
        self,
        reader: ShmStreamReader,
        label: Optional[str] = None,
        parent_name: Optional[str] = None,
    ):
        label_prefix = f"{parent_name}:" if parent_name else ""
        label_suffix = f":{label}" if label else ""
        super().__init__(lambda class_name: f"{label_prefix}{class_name}{label_suffix}")
        if not isinstance(reader, ShmStreamReader):
            raise TypeError("PacketReader requires ShmStreamReader")
        # Reach into SHM ring directly for best performance
        ep = getattr(reader, "_ep", None)
        if ep is None or getattr(ep, "_in_ring", None) is None:
            raise TypeError("ShmStreamReader is missing underlying ring")
        self._ring = ep._in_ring
        self._reader = _prc.ShmPacketReader(self._ring)
        self._aborted = False

    async def get_packet(self):
        if self._aborted:
            raise Exception("PacketReader is already aborted")
        # Cython path returns already-parsed packet object
        return self._reader.get_packet()

    def abort(self):
        if self._aborted:
            return
        logger.debug(self._create_message("Aborting"))
        self._aborted = True
        self._reader.abort()
