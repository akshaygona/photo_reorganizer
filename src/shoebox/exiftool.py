"""Batched exiftool integration.

The original implementation spawned one exiftool process per file, which
dominated runtime on large libraries. Here we pass files to exiftool in
batches of several hundred, cutting process spawns by ~200x. exiftool is
optional; callers fall back to the built-in parsers when it is absent.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from functools import lru_cache
from pathlib import Path
from typing import Callable, Iterable

# Date fields in priority order (mirrors the original script's behavior).
DATE_FIELDS = [
    "SubSecDateTimeOriginal",
    "DateTimeOriginal",
    "CreationDate",
    "CreateDate",
    "MediaCreateDate",
    "TrackCreateDate",
    "FileCreateDate",
]

_BATCH_SIZE = 200


@lru_cache(maxsize=1)
def available() -> bool:
    return shutil.which("exiftool") is not None


def read_dates_batch(
    paths: list[Path],
    on_progress: Callable[[int], None] | None = None,
) -> dict[str, dict]:
    """Return {absolute_path_str: {field: value, ...}} for all readable files.

    Files exiftool cannot read are simply absent from the result; callers
    fall back to other strategies for those.
    """
    results: dict[str, dict] = {}
    for batch in _chunks(paths, _BATCH_SIZE):
        cmd = (
            ["exiftool", "-j", "-fast2", "-charset", "filename=utf8"]
            + [f"-{field}" for field in DATE_FIELDS]
            + [str(p) for p in batch]
        )
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if proc.stdout.strip():
                for entry in json.loads(proc.stdout):
                    source = entry.pop("SourceFile", None)
                    if source:
                        results[str(Path(source).resolve())] = entry
        except (OSError, json.JSONDecodeError):
            pass
        if on_progress:
            on_progress(len(batch))
    return results


def _chunks(items: list, size: int) -> Iterable[list]:
    for i in range(0, len(items), size):
        yield items[i : i + size]
