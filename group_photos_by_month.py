#!/usr/bin/env python3

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

MEDIA_EXTS = {
    ".jpg", ".jpeg", ".png", ".heic", ".heif", ".tif", ".tiff", ".gif",
    ".bmp", ".webp", ".mov", ".mp4", ".m4v", ".avi", ".mts", ".3gp", ".mpg", ".mpeg"
}

SKIP_DIR_NAMES = {
    "resources", "database", "private", "previews", "thumbnails", "proxies", "renders"
}

def run_cmd(cmd):
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        return result.stdout.strip()
    except Exception:
        return ""

def mdls_date(path: Path):
    for attr in ["kMDItemContentCreationDate", "kMDItemFSCreationDate"]:
        out = run_cmd(["mdls", "-raw", "-name", attr, str(path)])
        if out and out != "(null)":
            try:
                cleaned = out.replace(" +0000", "").strip()
                return datetime.fromisoformat(cleaned)
            except Exception:
                pass
    return None

def stat_date(path: Path):
    try:
        return datetime.fromtimestamp(path.stat().st_mtime)
    except Exception:
        return None

def get_best_date(path: Path):
    return mdls_date(path) or stat_date(path)

def is_media_file(path: Path):
    return path.is_file() and path.suffix.lower() in MEDIA_EXTS

def sha256_file(path: Path, chunk_size=1024 * 1024):
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def unique_dest(dest: Path):
    if not dest.exists():
        return dest
    stem = dest.stem
    suffix = dest.suffix
    parent = dest.parent
    i = 1
    while True:
        candidate = parent / f"{stem}__{i}{suffix}"
        if not candidate.exists():
            return candidate
        i += 1

def copy_file(src: Path, dest: Path, mode: str):
    dest.parent.mkdir(parents=True, exist_ok=True)
    if mode == "copy":
        shutil.copy2(src, dest)
    elif mode == "hardlink":
        try:
            os.link(src, dest)
        except OSError:
            shutil.copy2(src, dest)
    elif mode == "symlink":
        os.symlink(src, dest)
    else:
        raise ValueError(f"Unsupported mode: {mode}")

def find_photoslibrary_roots(lib_path: Path):
    candidates = []
    originals = lib_path / "originals"
    masters = lib_path / "Masters"

    if originals.exists():
        candidates.append(originals)
    if masters.exists():
        candidates.append(masters)

    if candidates:
        return candidates
    return [lib_path]

def walk_media_files(source: Path):
    if source.suffix.lower() == ".photoslibrary":
        roots = find_photoslibrary_roots(source)
    else:
        roots = [source]

    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d.lower() not in SKIP_DIR_NAMES]
            for name in filenames:
                path = Path(dirpath) / name
                if is_media_file(path):
                    yield path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", nargs="+", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=["copy", "hardlink", "symlink"], default="copy")
    parser.add_argument("--dedupe", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    output_root = Path(args.output).expanduser().resolve()
    seen_hashes = set()
    processed = 0
    copied = 0
    skipped_dupe = 0
    skipped_no_date = 0
    errors = 0

    for source_str in args.sources:
        source = Path(source_str).expanduser().resolve()

        if not source.exists():
            print(f"[MISSING] {source}", file=sys.stderr)
            continue

        print(f"[SCAN] {source}")

        for src_file in walk_media_files(source):
            processed += 1

            try:
                dt = get_best_date(src_file)
                if not dt:
                    skipped_no_date += 1
                    print(f"[NO DATE] {src_file}")
                    continue

                if args.dedupe:
                    file_hash = sha256_file(src_file)
                    if file_hash in seen_hashes:
                        skipped_dupe += 1
                        print(f"[DUPLICATE] {src_file}")
                        continue
                    seen_hashes.add(file_hash)

                year_folder = dt.strftime("%Y")
                month_folder = dt.strftime("%Y-%m")
                dest_dir = output_root / year_folder / month_folder
                dest_file = unique_dest(dest_dir / src_file.name)

                if args.dry_run:
                    print(f"[DRY RUN] {src_file} -> {dest_file}")
                else:
                    copy_file(src_file, dest_file, args.mode)
                    print(f"[COPIED] {src_file} -> {dest_file}")

                copied += 1

            except Exception as e:
                errors += 1
                print(f"[ERROR] {src_file}: {e}", file=sys.stderr)

    print()
    print("Done")
    print(f"Processed: {processed}")
    print(f"Copied: {copied}")
    print(f"Skipped duplicates: {skipped_dupe}")
    print(f"Skipped no date: {skipped_no_date}")
    print(f"Errors: {errors}")

if __name__ == "__main__":
    main()