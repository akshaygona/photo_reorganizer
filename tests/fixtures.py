"""Builders for minimal-but-valid media files used across the test suite."""

from __future__ import annotations

import struct
import zlib
from datetime import datetime, timezone


def build_tiff_exif(date: str) -> bytes:
    """Little-endian TIFF with IFD0 -> ExifIFD -> DateTimeOriginal."""
    date_bytes = date.encode("ascii") + b"\x00"  # "YYYY:MM:DD HH:MM:SS" + NUL
    assert len(date_bytes) == 20

    header = b"II*\x00" + struct.pack("<I", 8)
    # IFD0 @8: one entry (ExifIFD pointer), next-IFD offset 0  -> 18 bytes
    ifd0 = struct.pack("<H", 1)
    ifd0 += struct.pack("<HHI I", 0x8769, 4, 1, 26)
    ifd0 += struct.pack("<I", 0)
    # ExifIFD @26: one entry (DateTimeOriginal at offset 44) -> 18 bytes
    exif_ifd = struct.pack("<H", 1)
    exif_ifd += struct.pack("<HHI I", 0x9003, 2, len(date_bytes), 44)
    exif_ifd += struct.pack("<I", 0)
    return header + ifd0 + exif_ifd + date_bytes


def build_jpeg(date: str) -> bytes:
    exif = b"Exif\x00\x00" + build_tiff_exif(date)
    app1 = b"\xff\xe1" + struct.pack(">H", len(exif) + 2) + exif
    return b"\xff\xd8" + app1 + b"\xff\xd9"


def build_jpeg_no_exif(payload: bytes = b"x" * 32) -> bytes:
    app0 = b"\xff\xe0" + struct.pack(">H", len(payload) + 2) + payload
    return b"\xff\xd8" + app0 + b"\xff\xd9"


def build_png(date: str) -> bytes:
    def chunk(ctype: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + ctype
            + data
            + struct.pack(">I", zlib.crc32(ctype + data))
        )

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"eXIf", build_tiff_exif(date))
        + chunk(b"IEND", b"")
    )


def build_webp(date: str) -> bytes:
    exif_data = b"Exif\x00\x00" + build_tiff_exif(date)
    chunk = b"EXIF" + struct.pack("<I", len(exif_data)) + exif_data
    if len(exif_data) % 2:
        chunk += b"\x00"
    body = b"WEBP" + chunk
    return b"RIFF" + struct.pack("<I", len(body)) + body


def _box(btype: bytes, payload: bytes) -> bytes:
    return struct.pack(">I4s", len(payload) + 8, btype) + payload


def build_mp4(dt: datetime) -> bytes:
    epoch = datetime(1904, 1, 1, tzinfo=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ctime = int((dt - epoch).total_seconds())
    # mvhd version 0: version/flags, ctime, mtime, timescale, duration
    mvhd = _box(b"mvhd", struct.pack(">B3xIIII", 0, ctime, ctime, 1000, 0) + b"\x00" * 80)
    ftyp = _box(b"ftyp", b"isom\x00\x00\x02\x00isomiso2")
    return ftyp + _box(b"moov", mvhd)


def build_heic(date: str) -> bytes:
    exif_payload = b"Exif\x00\x00" + build_tiff_exif(date)

    def build(exif_abs_offset: int) -> bytes:
        infe = _box(
            b"infe",
            struct.pack(">B3xHH4s", 2, 1, 0, b"Exif") + b"\x00",
        )
        iinf = _box(b"iinf", struct.pack(">B3xH", 0, 1) + infe)
        # iloc v0: offset_size=4, length_size=4, base_offset_size=0,
        # 1 item: id=1, dref=0, 1 extent
        iloc = _box(
            b"iloc",
            struct.pack(
                ">B3xBBHHHHII",
                0, 0x44, 0x00, 1, 1, 0, 1,
                exif_abs_offset, len(exif_payload),
            ),
        )
        meta = _box(b"meta", struct.pack(">B3x", 0) + iinf + iloc)
        ftyp = _box(b"ftyp", b"heic\x00\x00\x00\x00mif1heic")
        mdat_header_size = 8
        prefix_len = len(ftyp) + len(meta) + mdat_header_size
        return ftyp + meta + _box(b"mdat", exif_payload), prefix_len

    # Two passes: first to learn the layout, second with the real offset.
    _, prefix_len = build(0)
    data, _ = build(prefix_len)
    return data
