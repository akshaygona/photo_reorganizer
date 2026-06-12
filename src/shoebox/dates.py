"""Capture-date resolution: exiftool -> built-in parsers -> mdls -> mtime."""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path

from . import exiftool, parsers

# Dates before this are treated as garbage (uninitialized epochs: 1904, 1970).
MIN_VALID_YEAR = 1990


def parse_date_string(value: str) -> datetime | None:
    """Parse the date string formats exiftool and mdls emit."""
    if not value:
        return None
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    for candidate in (
        value,
        value.replace(" +0000", "+00:00"),
        value.replace(" -0000", "+00:00"),
    ):
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            pass

    for fmt in (
        "%Y:%m:%d %H:%M:%S.%f%z",
        "%Y:%m:%d %H:%M:%S%z",
        "%Y:%m:%d %H:%M:%S.%f",
        "%Y:%m:%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    return None


def date_from_exiftool_record(record: dict) -> datetime | None:
    for field in exiftool.DATE_FIELDS:
        raw = record.get(field)
        if raw:
            dt = parse_date_string(str(raw))
            if dt:
                return dt
    return None


def _mdls_date(path: Path) -> datetime | None:
    """Spotlight creation date; macOS only, used as a rare last resort."""
    if sys.platform != "darwin":
        return None
    for attr in ("kMDItemContentCreationDate", "kMDItemFSCreationDate"):
        try:
            out = subprocess.run(
                ["mdls", "-raw", "-name", attr, str(path)],
                capture_output=True,
                text=True,
                check=False,
            ).stdout.strip()
        except OSError:
            return None
        if out and out != "(null)":
            dt = parse_date_string(out)
            if dt:
                return dt
    return None


def _stat_date(path: Path) -> datetime | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime)
    except OSError:
        return None


def _validate(dt: datetime | None) -> datetime | None:
    if dt and dt.year < MIN_VALID_YEAR:
        return None
    return dt


def resolve_date(path: Path, exiftool_record: dict | None) -> datetime | None:
    """Resolve a capture date using every available strategy, best first."""
    if exiftool_record:
        dt = _validate(date_from_exiftool_record(exiftool_record))
        if dt:
            return dt
    dt = _validate(parsers.extract_date(path))
    if dt:
        return dt
    dt = _validate(_mdls_date(path))
    if dt:
        return dt
    return _validate(_stat_date(path))
