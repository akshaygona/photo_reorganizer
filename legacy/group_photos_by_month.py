#!/usr/bin/env python3

import argparse
import concurrent.futures
import hashlib
import json
import os
import shutil
import subprocess
import threading
from datetime import datetime
from pathlib import Path

MEDIA_EXTS = {
    ".jpg", ".jpeg", ".png", ".heic", ".heif",
    ".tif", ".tiff", ".gif", ".bmp", ".webp",
    ".mov", ".mp4", ".m4v", ".avi", ".mts", ".3gp", ".mpg", ".mpeg"
}

SKIP_DIR_NAMES = {
    "resources", "database", "private", "previews",
    "thumbnails", "proxies", "renders"
}

EXIF_DATE_FIELDS = [
    "SubSecDateTimeOriginal",
    "DateTimeOriginal",
    "CreationDate",
    "CreateDate",
    "MediaCreateDate",
    "TrackCreateDate",
    "FileCreateDate",
    "FileModifyDate",
]

def run_cmd(cmd):
    try:
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False
        ).stdout.strip()
    except Exception:
        return ""

def parse_date_string(value: str):
    if not value:
        return None

    value = value.strip()

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    candidates = [
        value,
        value.replace(" +0000", "+00:00"),
        value.replace(" -0000", "+00:00"),
    ]

    for candidate in candidates:
        try:
            return datetime.fromisoformat(candidate)
        except Exception:
            pass

    formats = [
        "%Y:%m:%d %H:%M:%S.%f%z",
        "%Y:%m:%d %H:%M:%S%z",
        "%Y:%m:%d %H:%M:%S.%f",
        "%Y:%m:%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except Exception:
            pass

    return None

def exiftool_date(path: Path):
    try:
        result = subprocess.run(
            ["exiftool", "-j", str(path)],
            capture_output=True,
            text=True,
            check=False
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None

        data = json.loads(result.stdout)
        if not data:
            return None

        meta = data[0]
        for field in EXIF_DATE_FIELDS:
            if field in meta and meta[field]:
                dt = parse_date_string(str(meta[field]))
                if dt:
                    return dt
    except Exception:
        pass

    return None

def mdls_date(path: Path):
    for attr in ["kMDItemContentCreationDate", "kMDItemFSCreationDate"]:
        out = run_cmd(["mdls", "-raw", "-name", attr, str(path)])
        if out and out != "(null)":
            dt = parse_date_string(out)
            if dt:
                return dt
    return None

def stat_date(path: Path):
    try:
        return datetime.fromtimestamp(path.stat().st_mtime)
    except Exception:
        return None

def get_date(path: Path):
    dt = exiftool_date(path) or mdls_date(path) or stat_date(path)
    if dt and dt.year < 1990:
        return None
    return dt

def is_media(path: Path):
    return (
        path.is_file()
        and not path.name.startswith("._")
        and path.suffix.lower() in MEDIA_EXTS
    )

def file_hash(path: Path, chunk_size=1024 * 1024):
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def unique_path(dest: Path, reserved=None):
    reserved = reserved or set()
    if not dest.exists() and dest not in reserved:
        return dest
    i = 1
    while True:
        candidate = dest.with_name(f"{dest.stem}__{i}{dest.suffix}")
        if not candidate.exists() and candidate not in reserved:
            return candidate
        i += 1

def copy_file(src: Path, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)

def photos_roots(lib: Path):
    originals = lib / "originals"
    masters = lib / "Masters"
    roots = []
    if originals.exists():
        roots.append(originals)
    if masters.exists():
        roots.append(masters)
    return roots or [lib]

def walk_source(source: Path):
    roots = photos_roots(source) if source.suffix.lower() == ".photoslibrary" else [source]

    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d.lower() not in SKIP_DIR_NAMES]
            for filename in filenames:
                path = Path(dirpath) / filename
                if is_media(path):
                    yield path

def process_file(src_file: Path, output_root: Path, dry_run: bool, seen_hashes, reserved_destinations, lock):
    try:
        h = file_hash(src_file)
        with lock:
            if h in seen_hashes:
                return "duplicates", f"[DUPLICATE] {src_file}"
            seen_hashes.add(h)

        dt = get_date(src_file)
        if not dt:
            return "no_date", f"[NO DATE] {src_file}"

        year_folder = dt.strftime("%Y")
        month_folder = dt.strftime("%Y-%m")
        dest_dir = output_root / year_folder / month_folder

        with lock:
            dest_file = unique_path(dest_dir / src_file.name, reserved_destinations)
            reserved_destinations.add(dest_file)

        if dry_run:
            return "copied", f"[DRY RUN] {src_file} -> {dest_file}"

        copy_file(src_file, dest_file)
        return "copied", f"[COPIED] {src_file} -> {dest_file}"
    except KeyboardInterrupt:
        raise
    except Exception as e:
        return "errors", f"[ERROR] {src_file}: {e}"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", nargs="+", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    if args.workers < 1:
        parser.error("--workers must be >= 1")

    output_root = Path(args.output).expanduser().resolve()
    seen_hashes = set()
    reserved_destinations = set()
    lock = threading.Lock()

    stats = {
        "processed": 0,
        "copied": 0,
        "duplicates": 0,
        "no_date": 0,
        "errors": 0,
    }

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        pending = set()
        pending_limit = max(1, args.workers * 4)

        for source_str in args.sources:
            source = Path(source_str).expanduser().resolve()

            if not source.exists():
                print(f"[MISSING] {source}")
                continue

            print(f"[SCAN] {source}")

            for src_file in walk_source(source):
                stats["processed"] += 1
                pending.add(
                    executor.submit(
                        process_file,
                        src_file,
                        output_root,
                        args.dry_run,
                        seen_hashes,
                        reserved_destinations,
                        lock,
                    )
                )

                if len(pending) >= pending_limit:
                    done, pending = concurrent.futures.wait(
                        pending,
                        return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    for future in done:
                        outcome, message = future.result()
                        stats[outcome] += 1
                        print(message)

        for future in concurrent.futures.as_completed(pending):
            outcome, message = future.result()
            stats[outcome] += 1
            print(message)

    print("\nDONE")
    for k, v in stats.items():
        print(f"{k}: {v}")

if __name__ == "__main__":
    main()