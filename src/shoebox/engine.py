"""The organizing engine.

Pipeline: scan -> hash (dedupe) -> resolve dates -> plan destinations -> transfer.

Every stage reports progress through a callback so the CLI and TUI can render
it however they like. The engine itself never prints.

Correctness properties:
  * Deterministic: files are processed in sorted order, so the same inputs
    always keep the same "first copy" of a duplicate set.
  * Idempotent: re-running against the same output skips files whose content
    already exists at the planned destination instead of duplicating them.
  * Crash-safe: copies go to a temporary name in the destination directory
    and are renamed into place only when complete.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable

from . import exiftool
from .dates import resolve_date
from .scanning import iter_media

_HASH_CHUNK = 1024 * 1024


@dataclass
class Event:
    """A progress update from the engine."""

    stage: str  # scan | hash | dates | transfer | done
    done: int = 0
    total: int = 0
    message: str = ""
    level: str = "info"  # info | warn | error


ProgressCallback = Callable[[Event], None]


@dataclass
class Options:
    sources: list[Path]
    output: Path
    mode: str = "copy"  # copy | move | hardlink
    dry_run: bool = False
    workers: int = 8
    use_exiftool: bool = True
    keep_undated: bool = False  # place undated files in Unsorted/ instead of skipping
    verify_writes: bool = False  # re-hash files after copying

    def __post_init__(self) -> None:
        if self.mode not in ("copy", "move", "hardlink"):
            raise ValueError(f"invalid mode: {self.mode!r}")
        if self.workers < 1:
            raise ValueError("workers must be >= 1")


@dataclass
class Action:
    source: Path
    outcome: str  # transferred | duplicate | no_date | already_present | error
    dest: Path | None = None
    detail: str = ""


@dataclass
class Report:
    scanned: int = 0
    transferred: int = 0
    duplicates: int = 0
    no_date: int = 0
    already_present: int = 0
    errors: int = 0
    dry_run: bool = False
    duration_seconds: float = 0.0
    actions: list[Action] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "summary": {
                "scanned": self.scanned,
                "transferred": self.transferred,
                "duplicates": self.duplicates,
                "no_date": self.no_date,
                "already_present": self.already_present,
                "errors": self.errors,
                "dry_run": self.dry_run,
                "duration_seconds": round(self.duration_seconds, 2),
            },
            "actions": [
                {
                    "source": str(a.source),
                    "outcome": a.outcome,
                    "dest": str(a.dest) if a.dest else None,
                    "detail": a.detail,
                }
                for a in self.actions
            ],
        }


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(_HASH_CHUNK):
            h.update(chunk)
    return h.hexdigest()


class Cancelled(Exception):
    pass


class Organizer:
    def __init__(
        self,
        options: Options,
        on_event: ProgressCallback | None = None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        self.options = options
        self._emit = on_event or (lambda event: None)
        self._cancel = cancel_event or threading.Event()

    def cancel(self) -> None:
        self._cancel.set()

    def _check_cancel(self) -> None:
        if self._cancel.is_set():
            raise Cancelled()

    # ----------------------------------------------------------------- run

    def run(self) -> Report:
        start = time.monotonic()
        report = Report(dry_run=self.options.dry_run)

        files = self._scan(report)
        unique, hashes = self._dedupe(files, report)
        dated = self._resolve_dates(unique, report)
        plan = self._plan(dated, hashes, report)
        self._transfer(plan, report)

        report.duration_seconds = time.monotonic() - start
        self._emit(Event("done", message="complete"))
        return report

    # ---------------------------------------------------------------- scan

    def _scan(self, report: Report) -> list[Path]:
        files: list[Path] = []
        for source in self.options.sources:
            source = source.expanduser().resolve()
            if not source.exists():
                self._emit(
                    Event("scan", message=f"source not found: {source}", level="warn")
                )
                continue
            self._emit(Event("scan", message=f"scanning {source}"))
            for path in iter_media(source):
                self._check_cancel()
                files.append(path)
                if len(files) % 500 == 0:
                    self._emit(Event("scan", done=len(files)))
        report.scanned = len(files)
        self._emit(Event("scan", done=len(files), total=len(files),
                         message=f"found {len(files)} media files"))
        return files

    # -------------------------------------------------------------- dedupe

    def _dedupe(
        self, files: list[Path], report: Report
    ) -> tuple[list[Path], dict[Path, str]]:
        """Hash everything in parallel; keep the first file of each hash."""
        hashes: dict[Path, str] = {}
        done = 0

        def hash_one(path: Path) -> tuple[Path, str | None, str]:
            if self._cancel.is_set():
                return path, None, "cancelled"
            try:
                return path, file_hash(path), ""
            except OSError as e:
                return path, None, str(e)

        with ThreadPoolExecutor(max_workers=self.options.workers) as pool:
            for path, digest, error in pool.map(hash_one, files):
                self._check_cancel()
                done += 1
                if digest is None:
                    report.errors += 1
                    report.actions.append(Action(path, "error", detail=error))
                    self._emit(Event("hash", done, len(files),
                                     f"unreadable: {path}", level="error"))
                    continue
                hashes[path] = digest
                if done % 100 == 0 or done == len(files):
                    self._emit(Event("hash", done, len(files)))

        seen: dict[str, Path] = {}
        unique: list[Path] = []
        for path in files:  # already in deterministic sorted-walk order
            digest = hashes.get(path)
            if digest is None:
                continue
            if digest in seen:
                report.duplicates += 1
                report.actions.append(
                    Action(path, "duplicate", detail=f"same content as {seen[digest]}")
                )
            else:
                seen[digest] = path
                unique.append(path)
        self._emit(Event("hash", len(files), len(files),
                         f"{len(unique)} unique, {report.duplicates} duplicates"))
        return unique, hashes

    # --------------------------------------------------------------- dates

    def _resolve_dates(
        self, files: list[Path], report: Report
    ) -> list[tuple[Path, datetime | None]]:
        exif_records: dict[str, dict] = {}
        if self.options.use_exiftool and exiftool.available() and files:
            done = 0

            def on_batch(count: int) -> None:
                nonlocal done
                done += count
                self._check_cancel()
                self._emit(Event("dates", done, len(files), "reading metadata"))

            exif_records = exiftool.read_dates_batch(files, on_progress=on_batch)

        results: list[tuple[Path, datetime | None]] = []

        def resolve_one(path: Path) -> tuple[Path, datetime | None]:
            if self._cancel.is_set():
                return path, None
            record = exif_records.get(str(path.resolve()))
            return path, resolve_date(path, record)

        done = 0
        with ThreadPoolExecutor(max_workers=self.options.workers) as pool:
            for path, dt in pool.map(resolve_one, files):
                self._check_cancel()
                done += 1
                results.append((path, dt))
                if done % 100 == 0 or done == len(files):
                    self._emit(Event("dates", done, len(files)))
        return results

    # ---------------------------------------------------------------- plan

    def _plan(
        self,
        dated: list[tuple[Path, datetime | None]],
        hashes: dict[Path, str],
        report: Report,
    ) -> list[tuple[Path, Path]]:
        output = self.options.output.expanduser().resolve()
        reserved: set[Path] = set()
        plan: list[tuple[Path, Path]] = []

        for path, dt in dated:
            if dt is None:
                if self.options.keep_undated:
                    dest_dir = output / "Unsorted"
                else:
                    report.no_date += 1
                    report.actions.append(Action(path, "no_date"))
                    self._emit(Event("dates", message=f"no date: {path}", level="warn"))
                    continue
            else:
                dest_dir = output / dt.strftime("%Y") / dt.strftime("%Y-%m")

            dest = self._claim_destination(dest_dir / path.name, hashes[path], reserved)
            if dest is None:
                report.already_present += 1
                report.actions.append(
                    Action(path, "already_present", dest=dest_dir / path.name)
                )
                continue
            plan.append((path, dest))
        return plan

    def _claim_destination(
        self, candidate: Path, digest: str, reserved: set[Path]
    ) -> Path | None:
        """First free name for this file, or None if its content is already there."""
        stem, suffix = candidate.stem, candidate.suffix
        i = 0
        while True:
            if candidate not in reserved:
                if not candidate.exists():
                    reserved.add(candidate)
                    return candidate
                try:
                    if file_hash(candidate) == digest:
                        return None  # identical content already organized
                except OSError:
                    pass  # unreadable existing file: pick another name
            i += 1
            candidate = candidate.with_name(f"{stem}__{i}{suffix}")

    # ------------------------------------------------------------ transfer

    def _transfer(self, plan: list[tuple[Path, Path]], report: Report) -> None:
        total = len(plan)
        if self.options.dry_run:
            for i, (src, dest) in enumerate(plan, 1):
                report.transferred += 1
                report.actions.append(Action(src, "transferred", dest=dest, detail="dry run"))
                self._emit(Event("transfer", i, total, f"[dry run] {src.name} -> {dest}"))
            return

        done = 0
        lock = threading.Lock()

        def transfer_one(item: tuple[Path, Path]) -> Action:
            src, dest = item
            if self._cancel.is_set():
                return Action(src, "error", detail="cancelled")
            try:
                self._execute_transfer(src, dest)
                return Action(src, "transferred", dest=dest)
            except OSError as e:
                return Action(src, "error", dest=dest, detail=str(e))

        with ThreadPoolExecutor(max_workers=self.options.workers) as pool:
            for action in pool.map(transfer_one, plan):
                self._check_cancel()
                with lock:
                    done += 1
                    report.actions.append(action)
                    if action.outcome == "transferred":
                        report.transferred += 1
                        self._emit(Event("transfer", done, total,
                                         f"{action.source.name} -> {action.dest}"))
                    else:
                        report.errors += 1
                        self._emit(Event("transfer", done, total,
                                         f"failed: {action.source}: {action.detail}",
                                         level="error"))

    def _execute_transfer(self, src: Path, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        mode = self.options.mode

        if mode == "hardlink":
            try:
                os.link(src, dest)
                return
            except OSError:
                mode = "copy"  # cross-device or unsupported FS: fall back

        if mode == "move":
            shutil.move(str(src), str(dest))
            return

        # Copy to a temp name in the destination dir, then rename into place,
        # so an interrupted run never leaves a truncated file at a final name.
        fd, tmp_name = tempfile.mkstemp(dir=dest.parent, prefix=f".{dest.name}.", suffix=".part")
        os.close(fd)
        tmp = Path(tmp_name)
        try:
            shutil.copy2(src, tmp)
            if tmp.stat().st_size != src.stat().st_size:
                raise OSError(f"size mismatch after copy: {src}")
            if self.options.verify_writes and file_hash(tmp) != file_hash(src):
                raise OSError(f"content mismatch after copy: {src}")
            os.replace(tmp, dest)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
