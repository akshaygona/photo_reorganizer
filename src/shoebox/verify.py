"""Verification: prove the destination contains every byte of the sources.

Hashes every media file on both sides (in parallel) and diffs the hash sets.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable

from .engine import Event, ProgressCallback, file_hash
from .scanning import iter_media


def _hash_tree(
    label: str,
    roots: list[Path],
    workers: int,
    emit: ProgressCallback,
) -> dict[str, list[str]]:
    files: list[Path] = []
    for root in roots:
        root = root.expanduser().resolve()
        if not root.exists():
            emit(Event(label, message=f"not found: {root}", level="warn"))
            continue
        emit(Event(label, message=f"scanning {root}"))
        files.extend(iter_media(root))

    by_hash: dict[str, list[str]] = {}
    done = 0

    def hash_one(path: Path) -> tuple[Path, str | None]:
        try:
            return path, file_hash(path)
        except OSError:
            return path, None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for path, digest in pool.map(hash_one, files):
            done += 1
            if digest is None:
                emit(Event(label, done, len(files), f"unreadable: {path}", level="error"))
                continue
            by_hash.setdefault(digest, []).append(str(path))
            if done % 100 == 0 or done == len(files):
                emit(Event(label, done, len(files)))
    return by_hash


def verify(
    sources: list[Path],
    dest: Path,
    workers: int = 8,
    on_event: ProgressCallback | None = None,
    example_limit: int = 100,
) -> dict:
    emit = on_event or (lambda event: None)

    source_hashes = _hash_tree("verify-source", sources, workers, emit)
    dest_hashes = _hash_tree("verify-dest", [dest], workers, emit)

    missing = sorted(set(source_hashes) - set(dest_hashes))
    extra = sorted(set(dest_hashes) - set(source_hashes))

    return {
        "summary": {
            "source_files": sum(len(v) for v in source_hashes.values()),
            "unique_source_hashes": len(source_hashes),
            "dest_files": sum(len(v) for v in dest_hashes.values()),
            "unique_dest_hashes": len(dest_hashes),
            "missing_in_dest": len(missing),
            "extra_in_dest": len(extra),
            "verified_complete": not missing,
        },
        "missing_examples": [
            {"hash": h, "source_paths": source_hashes[h]} for h in missing[:example_limit]
        ],
        "extra_examples": [
            {"hash": h, "dest_paths": dest_hashes[h]} for h in extra[:example_limit]
        ],
    }
