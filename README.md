# Photo Reorganizer

This folder contains two Python scripts you can run to organize photo/video files by capture date and verify the results.

Replace the placeholders in the example commands (`/path/to/...`) with your own paths.

- `group_photos_by_month.py`: scans one or more photo sources, extracts each media file's date (EXIF/metadata/stat), computes a SHA-256 hash for deduplication, and copies unique media into `YYYY/YYYY-MM` folders under an output directory.
- `verify_copy.py`: verifies that the destination contains the same media content as the source(s) by comparing SHA-256 hashes, and writes a JSON report.

## Requirements

- `exiftool` must be installed and available on your `PATH` (used to read capture timestamps).
- `mdls` (macOS) is used as an optional fallback for date extraction; on non-macOS systems it will be skipped.
- Python 3.

## `group_photos_by_month.py`

### Purpose

- Walks each input source recursively (with some directories skipped).
- For each media file:
  - computes SHA-256 hash (dedupe)
  - extracts a date (EXIF via `exiftool`, else `mdls`, else file modified time)
  - copies the file to: `OUTPUT_ROOT/YYYY/YYYY-MM/`
  - avoids filename collisions by reserving destination paths in-memory while running in parallel

### Common command examples

#### Dry run (no copying)

```bash
python3 group_photos_by_month.py \
  --sources \
  "/path/to/source1" \
  "/path/to/source2" \
  "/path/to/source3" \
  --output "/path/to/output_root" \
  --dry-run
```

#### Real run (copy files)

```bash
python3 group_photos_by_month.py \
  --sources \
  "/path/to/source1" \
  "/path/to/source2" \
  "/path/to/source3" \
  --output "/path/to/output_root"
```

#### Control parallelism

By default it uses `--workers 8`. If you want to tune performance:

```bash
python3 group_photos_by_month.py ... --workers 12
```

## `verify_copy.py`

### Purpose

- Scans the same set of `--sources` plus the `--dest` directory.
- Computes SHA-256 for each media file and groups paths by hash.
- Produces a JSON report (missing/extra hashes) and prints a summary.

### Common command examples

#### Verify after a copy run

```bash
python3 verify_copy.py \
  --sources \
  "/path/to/source1" \
  "/path/to/source2" \
  "/path/to/source3" \
  --dest "/path/to/output_root" \
  --report "verify_report.json"
```

After completion, check:

- `verified_complete` in the printed summary (and the same field in the JSON report).
- `missing_unique_hashes_in_dest` and `extra_unique_hashes_in_dest`.

