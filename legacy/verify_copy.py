#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
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

def is_media(path: Path):
    return (
        path.is_file()
        and not path.name.startswith("._")
        and path.suffix.lower() in MEDIA_EXTS
    )

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

def file_hash(path: Path, chunk_size=1024 * 1024):
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def build_source_manifest(sources):
    by_hash = {}
    total_files = 0

    for source_str in sources:
        source = Path(source_str).expanduser().resolve()
        if not source.exists():
            print(f"[MISSING SOURCE] {source}")
            continue

        print(f"[SCAN SOURCE] {source}")
        for path in walk_source(source):
            total_files += 1
            h = file_hash(path)
            by_hash.setdefault(h, []).append(str(path))

    return {
        "total_source_files_seen": total_files,
        "unique_source_hashes": len(by_hash),
        "files_by_hash": by_hash,
    }

def build_dest_manifest(dest):
    dest = Path(dest).expanduser().resolve()
    by_hash = {}
    total_files = 0

    print(f"[SCAN DEST] {dest}")
    for path in walk_source(dest):
        total_files += 1
        h = file_hash(path)
        by_hash.setdefault(h, []).append(str(path))

    return {
        "total_dest_files_seen": total_files,
        "unique_dest_hashes": len(by_hash),
        "files_by_hash": by_hash,
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", nargs="+", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--report", default="verify_report.json")
    args = parser.parse_args()

    source_manifest = build_source_manifest(args.sources)
    dest_manifest = build_dest_manifest(args.dest)

    source_hashes = set(source_manifest["files_by_hash"].keys())
    dest_hashes = set(dest_manifest["files_by_hash"].keys())

    missing_hashes = sorted(source_hashes - dest_hashes)
    extra_hashes = sorted(dest_hashes - source_hashes)

    missing_examples = []
    for h in missing_hashes[:100]:
        missing_examples.append({
            "hash": h,
            "source_paths": source_manifest["files_by_hash"][h],
        })

    extra_examples = []
    for h in extra_hashes[:100]:
        extra_examples.append({
            "hash": h,
            "dest_paths": dest_manifest["files_by_hash"][h],
        })

    report = {
        "summary": {
            "total_source_files_seen": source_manifest["total_source_files_seen"],
            "unique_source_hashes": source_manifest["unique_source_hashes"],
            "total_dest_files_seen": dest_manifest["total_dest_files_seen"],
            "unique_dest_hashes": dest_manifest["unique_dest_hashes"],
            "missing_unique_hashes_in_dest": len(missing_hashes),
            "extra_unique_hashes_in_dest": len(extra_hashes),
            "verified_complete": len(missing_hashes) == 0,
        },
        "missing_examples": missing_examples,
        "extra_examples": extra_examples,
    }

    report_path = Path(args.report).expanduser().resolve()
    report_path.write_text(json.dumps(report, indent=2))

    print("\nVERIFICATION SUMMARY")
    for k, v in report["summary"].items():
        print(f"{k}: {v}")

    print(f"\nReport written to: {report_path}")

if __name__ == "__main__":
    main()