"""Pure-Python capture-date extraction from media file headers.

Zero dependencies. Each parser reads only the bytes it needs and returns a
naive or tz-aware ``datetime``, or ``None`` if no usable timestamp is found.

Supported containers:
  * JPEG  - EXIF in APP1 segment
  * TIFF  - EXIF IFDs directly
  * PNG   - eXIf chunk (TIFF payload)
  * WebP  - RIFF EXIF chunk
  * HEIC/HEIF - ISO-BMFF ``meta`` box, Exif item via iinf/iloc
  * MP4/MOV/M4V/3GP - ISO-BMFF ``moov/mvhd`` creation time
"""

from __future__ import annotations

import struct
from datetime import datetime, timedelta, timezone
from pathlib import Path

# EXIF tag IDs, in priority order of the date they carry.
_TAG_DATETIME_ORIGINAL = 0x9003
_TAG_DATETIME_DIGITIZED = 0x9004
_TAG_DATETIME = 0x0132
_TAG_EXIF_IFD_POINTER = 0x8769

# QuickTime/MP4 epoch (seconds since 1904-01-01 UTC).
_MP4_EPOCH = datetime(1904, 1, 1, tzinfo=timezone.utc)

_HEADER_READ = 256 * 1024  # enough for EXIF in virtually all files


def parse_exif_datetime(value: str) -> datetime | None:
    """Parse the EXIF ``YYYY:MM:DD HH:MM:SS`` format (and close variants)."""
    value = value.strip().strip("\x00")
    for fmt in ("%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _parse_tiff(buf: bytes) -> datetime | None:
    """Parse a TIFF blob (the body of an EXIF block) for date tags."""
    if len(buf) < 8:
        return None
    if buf[:2] == b"II":
        endian = "<"
    elif buf[:2] == b"MM":
        endian = ">"
    else:
        return None

    def u16(off: int) -> int:
        return struct.unpack_from(endian + "H", buf, off)[0]

    def u32(off: int) -> int:
        return struct.unpack_from(endian + "I", buf, off)[0]

    def read_ifd(off: int, wanted: dict[int, str | None]) -> int | None:
        """Fill ``wanted`` with ASCII tag values; return Exif IFD pointer if seen."""
        exif_ptr = None
        if off + 2 > len(buf):
            return None
        count = u16(off)
        for i in range(count):
            entry = off + 2 + i * 12
            if entry + 12 > len(buf):
                break
            tag = u16(entry)
            if tag == _TAG_EXIF_IFD_POINTER:
                exif_ptr = u32(entry + 8)
            elif tag in wanted:
                vtype, vcount = u16(entry + 2), u32(entry + 4)
                if vtype != 2:  # ASCII
                    continue
                voff = entry + 8 if vcount <= 4 else u32(entry + 8)
                if voff + vcount <= len(buf):
                    wanted[tag] = buf[voff : voff + vcount].decode(
                        "ascii", errors="replace"
                    )
        return exif_ptr

    found: dict[int, str | None] = {
        _TAG_DATETIME_ORIGINAL: None,
        _TAG_DATETIME_DIGITIZED: None,
        _TAG_DATETIME: None,
    }
    try:
        exif_ptr = read_ifd(u32(4), found)
        if exif_ptr:
            read_ifd(exif_ptr, found)
    except struct.error:
        return None

    for tag in (_TAG_DATETIME_ORIGINAL, _TAG_DATETIME_DIGITIZED, _TAG_DATETIME):
        if found[tag]:
            dt = parse_exif_datetime(found[tag])
            if dt:
                return dt
    return None


def _find_tiff(blob: bytes, search_window: int = 64) -> datetime | None:
    """Locate a TIFF header near the start of ``blob`` and parse it."""
    for marker in (b"II*\x00", b"MM\x00*"):
        idx = blob.find(marker, 0, search_window)
        if idx != -1:
            return _parse_tiff(blob[idx:])
    return None


# ---------------------------------------------------------------- JPEG / TIFF


def _date_from_jpeg(f) -> datetime | None:
    if f.read(2) != b"\xff\xd8":
        return None
    while True:
        head = f.read(2)
        if len(head) < 2 or head[0] != 0xFF:
            return None
        marker = head[1]
        if marker == 0xD8 or 0xD0 <= marker <= 0xD7:  # standalone markers
            continue
        if marker == 0xDA:  # start of scan: no EXIF past this point
            return None
        size_raw = f.read(2)
        if len(size_raw) < 2:
            return None
        size = struct.unpack(">H", size_raw)[0] - 2
        if size < 0:
            return None
        if marker == 0xE1:  # APP1
            data = f.read(size)
            if data.startswith(b"Exif\x00\x00"):
                return _parse_tiff(data[6:])
        else:
            f.seek(size, 1)


def _date_from_tiff(f) -> datetime | None:
    return _parse_tiff(f.read(_HEADER_READ))


# ----------------------------------------------------------------------- PNG


def _date_from_png(f) -> datetime | None:
    if f.read(8) != b"\x89PNG\r\n\x1a\n":
        return None
    while True:
        head = f.read(8)
        if len(head) < 8:
            return None
        length, ctype = struct.unpack(">I4s", head)
        if ctype == b"eXIf":
            return _find_tiff(f.read(length))
        if ctype == b"IDAT":  # metadata chunks precede image data in practice
            return None
        f.seek(length + 4, 1)  # skip data + CRC


# ---------------------------------------------------------------------- WebP


def _date_from_webp(f) -> datetime | None:
    head = f.read(12)
    if len(head) < 12 or head[:4] != b"RIFF" or head[8:12] != b"WEBP":
        return None
    while True:
        chunk = f.read(8)
        if len(chunk) < 8:
            return None
        fourcc, size = struct.unpack("<4sI", chunk)
        if fourcc == b"EXIF":
            return _find_tiff(f.read(size))
        f.seek(size + (size & 1), 1)  # chunks are word-aligned


# ------------------------------------------------------------------ ISO-BMFF


def _iter_boxes(f, end: int | None):
    """Yield (box_type, payload_start, payload_end) for boxes until ``end``."""
    while True:
        pos = f.tell()
        if end is not None and pos >= end:
            return
        head = f.read(8)
        if len(head) < 8:
            return
        size, btype = struct.unpack(">I4s", head)
        payload_start = pos + 8
        if size == 1:
            large = f.read(8)
            if len(large) < 8:
                return
            size = struct.unpack(">Q", large)[0]
            payload_start = pos + 16
        if size == 0:  # box extends to end of file
            f.seek(0, 2)
            yield btype, payload_start, f.tell()
            return
        box_end = pos + size
        if box_end <= payload_start:
            return
        yield btype, payload_start, box_end
        f.seek(box_end)


def _date_from_mp4(f) -> datetime | None:
    f.seek(0, 2)
    file_end = f.tell()
    f.seek(0)
    for btype, start, end in _iter_boxes(f, file_end):
        if btype != b"moov":
            continue
        f.seek(start)
        for inner, istart, _ in _iter_boxes(f, end):
            if inner != b"mvhd":
                continue
            f.seek(istart)
            data = f.read(20)
            if len(data) < 8:
                return None
            version = data[0]
            if version == 1:
                ctime = struct.unpack(">Q", data[4:12])[0]
            else:
                ctime = struct.unpack(">I", data[4:8])[0]
            if ctime == 0:
                return None
            return _MP4_EPOCH + timedelta(seconds=ctime)
        return None
    return None


def _date_from_heic(f) -> datetime | None:
    f.seek(0, 2)
    file_end = f.tell()
    f.seek(0)
    meta_span = None
    for btype, start, end in _iter_boxes(f, file_end):
        if btype == b"meta":
            meta_span = (start + 4, end)  # skip FullBox version/flags
            break
    if not meta_span:
        return None

    exif_item_id = None
    iloc_raw = None
    f.seek(meta_span[0])
    for btype, start, end in _iter_boxes(f, meta_span[1]):
        if btype == b"iinf":
            f.seek(start)
            vf = f.read(4)
            if len(vf) < 4:
                continue
            version = vf[0]
            count_bytes = f.read(2 if version == 0 else 4)
            f.seek(start + 4 + len(count_bytes))
            for ibtype, istart, iend in _iter_boxes(f, end):
                if ibtype != b"infe":
                    continue
                f.seek(istart)
                infe = f.read(min(iend - istart, 32))
                if len(infe) < 12:
                    continue
                iversion = infe[0]
                if iversion == 2:
                    item_id = struct.unpack(">H", infe[4:6])[0]
                    item_type = infe[8:12]
                elif iversion == 3:
                    item_id = struct.unpack(">I", infe[4:8])[0]
                    item_type = infe[10:14]
                else:
                    continue
                if item_type == b"Exif":
                    exif_item_id = item_id
        elif btype == b"iloc":
            f.seek(start)
            iloc_raw = f.read(end - start)

    if exif_item_id is None or not iloc_raw:
        return None

    extent = _find_iloc_extent(iloc_raw, exif_item_id)
    if not extent:
        return None
    offset, length = extent
    f.seek(offset)
    return _find_tiff(f.read(min(length, _HEADER_READ)))


def _find_iloc_extent(raw: bytes, target_id: int) -> tuple[int, int] | None:
    """Parse an iloc box body, return (absolute_offset, length) of the item."""
    try:
        version = raw[0]
        pos = 4
        offset_size = raw[pos] >> 4
        length_size = raw[pos] & 0xF
        base_offset_size = raw[pos + 1] >> 4
        index_size = (raw[pos + 1] & 0xF) if version in (1, 2) else 0
        pos += 2
        if version < 2:
            item_count = struct.unpack_from(">H", raw, pos)[0]
            pos += 2
        else:
            item_count = struct.unpack_from(">I", raw, pos)[0]
            pos += 4

        def read_uint(p: int, size: int) -> int:
            return int.from_bytes(raw[p : p + size], "big") if size else 0

        for _ in range(item_count):
            if version < 2:
                item_id = struct.unpack_from(">H", raw, pos)[0]
                pos += 2
            else:
                item_id = struct.unpack_from(">I", raw, pos)[0]
                pos += 4
            if version in (1, 2):
                pos += 2  # construction_method (low 4 bits); 0 = file offset
            pos += 2  # data_reference_index
            base_offset = read_uint(pos, base_offset_size)
            pos += base_offset_size
            extent_count = struct.unpack_from(">H", raw, pos)[0]
            pos += 2
            for _ in range(extent_count):
                pos += index_size
                extent_offset = read_uint(pos, offset_size)
                pos += offset_size
                extent_length = read_uint(pos, length_size)
                pos += length_size
                if item_id == target_id:
                    return base_offset + extent_offset, extent_length
    except (IndexError, struct.error):
        return None
    return None


# ------------------------------------------------------------------ dispatch

_PARSERS = {
    ".jpg": _date_from_jpeg,
    ".jpeg": _date_from_jpeg,
    ".tif": _date_from_tiff,
    ".tiff": _date_from_tiff,
    ".png": _date_from_png,
    ".webp": _date_from_webp,
    ".heic": _date_from_heic,
    ".heif": _date_from_heic,
    ".mp4": _date_from_mp4,
    ".mov": _date_from_mp4,
    ".m4v": _date_from_mp4,
    ".3gp": _date_from_mp4,
}


def extract_date(path: Path) -> datetime | None:
    """Best-effort capture date from file headers; never raises."""
    parser = _PARSERS.get(path.suffix.lower())
    if not parser:
        return None
    try:
        with path.open("rb") as f:
            return parser(f)
    except (OSError, struct.error, ValueError):
        return None
