"""
Page-based block file used as the foundation for a WiredTiger-like engine.

Slice A: fixed-size blocks with a small file header and per-block checksums.

Layout:
  - Block 0 is the header page.
  - Every block is `page_size` bytes.
  - First 4 bytes of each block store a CRC32 of the remaining bytes.
  - The header payload (after checksum) contains a packed FileHeader struct.

This is intentionally minimal: no allocation, freelist, or B+tree yet.
"""

from __future__ import annotations

import os
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, ClassVar

from .errors import StorageError


_CHECKSUM_SIZE = 4
_DEFAULT_PAGE_SIZE = 4096
_MAGIC = b"MMWT0001"  # 8 bytes
_VERSION = 1


@dataclass(slots=True)
class FileHeader:
    magic: bytes = _MAGIC
    version: int = _VERSION
    page_size: int = _DEFAULT_PAGE_SIZE
    root_block_id: int = -1
    free_list_head: int = -1

    _STRUCT: ClassVar[struct.Struct] = struct.Struct("<8sH I q q")
    _PAD_SIZE: ClassVar[int] = 64

    def pack(self) -> bytes:
        data = self._STRUCT.pack(
            self.magic,
            self.version,
            self.page_size,
            self.root_block_id,
            self.free_list_head,
        )
        if len(data) > self._PAD_SIZE:
            raise StorageError("header struct too large")
        return data.ljust(self._PAD_SIZE, b"\x00")

    @classmethod
    def unpack(cls, buf: bytes) -> "FileHeader":
        if len(buf) < cls._STRUCT.size:
            raise StorageError("header buffer too small")
        magic, version, page_size, root_block_id, free_list_head = cls._STRUCT.unpack(
            buf[: cls._STRUCT.size]
        )
        return cls(
            magic=magic,
            version=version,
            page_size=page_size,
            root_block_id=root_block_id,
            free_list_head=free_list_head,
        )


class BlockFile:
    """
    Fixed-size block file with a small header page and CRC32 checksums.
    """

    def __init__(self, path: Path, fh: BinaryIO, header: FileHeader) -> None:
        self.path = Path(path)
        self._fh = fh
        self.header = header
        self.page_size = header.page_size

    # --- lifecycle -----------------------------------------------------

    @classmethod
    def create(cls, path: str | Path, page_size: int = _DEFAULT_PAGE_SIZE) -> "BlockFile":
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if path.exists() and path.stat().st_size > 0:
            raise StorageError(f"file already exists: {path}")

        if page_size < _CHECKSUM_SIZE + FileHeader._PAD_SIZE:
            raise StorageError("page_size too small for header")

        fh = path.open("w+b")
        header = FileHeader(page_size=page_size)
        cls._write_header_page(fh, header)
        fh.flush()
        os.fsync(fh.fileno())
        return cls(path, fh, header)

    @classmethod
    def open(cls, path: str | Path) -> "BlockFile":
        path = Path(path)
        if not path.exists():
            raise StorageError(f"file not found: {path}")

        fh = path.open("r+b")

        # Read a fixed prefix to parse header without knowing page_size yet.
        prefix = fh.read(_CHECKSUM_SIZE + FileHeader._PAD_SIZE)
        if len(prefix) < _CHECKSUM_SIZE + FileHeader._STRUCT.size:
            raise StorageError("file too small to contain header")

        stored_checksum = struct.unpack("<I", prefix[:_CHECKSUM_SIZE])[0]
        header_payload = prefix[_CHECKSUM_SIZE:]
        header = FileHeader.unpack(header_payload)

        if header.magic != _MAGIC:
            raise StorageError("invalid file magic")
        if header.version != _VERSION:
            raise StorageError(f"unsupported version: {header.version}")
        if header.page_size < _CHECKSUM_SIZE + FileHeader._PAD_SIZE:
            raise StorageError("corrupt header page_size")

        # Verify header checksum using the full first block.
        fh.seek(0)
        page0 = fh.read(header.page_size)
        if len(page0) != header.page_size:
            raise StorageError("short read on header page")
        payload0 = page0[_CHECKSUM_SIZE:]
        if cls._crc32(payload0) != stored_checksum:
            raise StorageError("header checksum mismatch")

        return cls(path, fh, header)

    def close(self) -> None:
        self._fh.close()

    # --- block IO ------------------------------------------------------

    def read_block(self, block_id: int, verify: bool = True) -> bytes:
        if block_id < 0:
            raise StorageError("block_id must be >= 0")

        self._fh.seek(block_id * self.page_size)
        page = self._fh.read(self.page_size)
        if len(page) != self.page_size:
            raise StorageError(f"short read for block {block_id}")

        stored_checksum = struct.unpack("<I", page[:_CHECKSUM_SIZE])[0]
        payload = page[_CHECKSUM_SIZE:]

        if verify and self._crc32(payload) != stored_checksum:
            raise StorageError(f"checksum mismatch for block {block_id}")

        return payload

    def write_block(self, block_id: int, payload: bytes) -> None:
        if block_id < 0:
            raise StorageError("block_id must be >= 0")

        max_payload = self.page_size - _CHECKSUM_SIZE
        if len(payload) > max_payload:
            raise StorageError(f"payload too large for page (max {max_payload} bytes)")

        padded = payload.ljust(max_payload, b"\x00")
        checksum = self._crc32(padded)
        page = struct.pack("<I", checksum) + padded

        self._fh.seek(block_id * self.page_size)
        self._fh.write(page)
        self._fh.flush()

    def num_blocks(self) -> int:
        self._fh.seek(0, os.SEEK_END)
        size = self._fh.tell()
        return size // self.page_size

    # --- header helpers ------------------------------------------------

    @staticmethod
    def _crc32(data: bytes) -> int:
        return zlib.crc32(data) & 0xFFFFFFFF

    @classmethod
    def _write_header_page(cls, fh: BinaryIO, header: FileHeader) -> None:
        max_payload = header.page_size - _CHECKSUM_SIZE
        payload = header.pack()
        if len(payload) > max_payload:
            raise StorageError("header does not fit in page")

        padded = payload.ljust(max_payload, b"\x00")
        checksum = cls._crc32(padded)
        page0 = struct.pack("<I", checksum) + padded

        fh.seek(0)
        fh.write(page0)

