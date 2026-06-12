"""Source discovery: walk directories and Apple Photos libraries for media."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

MEDIA_EXTS = {
    ".jpg", ".jpeg", ".png", ".heic", ".heif",
    ".tif", ".tiff", ".gif", ".bmp", ".webp",
    ".mov", ".mp4", ".m4v", ".avi", ".mts", ".3gp", ".mpg", ".mpeg",
}

# Internal Photos.app / Lightroom structures that hold derivatives, not originals.
SKIP_DIR_NAMES = {
    "resources", "database", "private", "previews",
    "thumbnails", "proxies", "renders",
}


def is_media(path: Path) -> bool:
    return (
        path.is_file()
        and not path.name.startswith("._")  # AppleDouble sidecar files
        and path.suffix.lower() in MEDIA_EXTS
    )


def photos_library_roots(library: Path) -> list[Path]:
    """Original-media roots inside a .photoslibrary bundle."""
    roots = [r for r in (library / "originals", library / "Masters") if r.exists()]
    return roots or [library]


def iter_media(source: Path) -> Iterator[Path]:
    """Yield every media file under ``source``, skipping derivative dirs."""
    if source.suffix.lower() == ".photoslibrary":
        roots = photos_library_roots(source)
    else:
        roots = [source]

    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = sorted(
                d for d in dirnames if d.lower() not in SKIP_DIR_NAMES
            )
            base = Path(dirpath)
            for filename in sorted(filenames):
                path = base / filename
                if is_media(path):
                    yield path
