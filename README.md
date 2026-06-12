<div align="center">

# 📦 Shoebox

**Turn a shoebox of 30,000 unsorted photos into a clean, deduplicated, chronological archive — in one command.**

[![CI](https://github.com/akshaygona/photo_reorganizer/actions/workflows/ci.yml/badge.svg)](https://github.com/akshaygona/photo_reorganizer/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

</div>

---

Everyone has the digital shoebox: a decade of camera dumps, WhatsApp exports, old phone backups, and `New Folder (3)` directories — full of duplicates, scattered across drives. Shoebox reads the *real* capture date out of every photo and video, skips exact duplicates by content hash, and files everything into a tidy archive:

```
Archive/
├── 2017/
│   ├── 2017-08/
│   │   ├── IMG_2041.jpg
│   │   └── beach_trip.mov
│   └── 2017-09/
├── 2018/
…
```

## ✨ Highlights

- 🖥️ **Full interactive UI in your terminal** — just run `shoebox` and follow the screens. Live progress bars, streaming log, final summary.
- 🔍 **Real capture dates** — EXIF for photos, QuickTime/MP4 metadata for videos, with a clear fallback chain (`exiftool` → built-in parsers → filesystem). Works on JPEG, HEIC/HEIF, PNG, TIFF, WebP, MP4, MOV, M4V, 3GP and more.
- 🧬 **Content-hash deduplication** — the same photo saved five times is archived once. SHA-256, not filenames.
- 🍎 **Apple Photos aware** — point it directly at a `.photoslibrary` bundle; it pulls originals and skips thumbnails/derivatives.
- 🔁 **Idempotent** — re-run it any time; already-archived files are detected by content and skipped, never duplicated.
- 🛡️ **Crash-safe & non-destructive** — copies by default, writes via temp-file + atomic rename, never overwrites. `--dry-run` previews everything.
- ⚡ **Fast** — parallel hashing/copying and batched metadata reads (hundreds of files per `exiftool` call).
- ✅ **Verifiable** — `shoebox verify` re-hashes both sides and proves your archive contains every source byte before you delete anything.
- 📦 **Zero required external tools** — built-in pure-Python metadata parsers; installing [`exiftool`](https://exiftool.org/) is optional and extends format coverage (RAW formats, AVI, etc.).

## 🚀 Install

```bash
pipx install git+https://github.com/akshaygona/photo_reorganizer
# or: pip install git+https://github.com/akshaygona/photo_reorganizer
```

Optional, for maximum format coverage:

```bash
brew install exiftool        # macOS
sudo apt install libimage-exiftool-perl   # Debian/Ubuntu
```

## 🕹️ Use it

### The easy way — interactive UI

```bash
shoebox
```

Pick your folders, flip the options you want, hit **Start**, watch it go.

### The scriptable way — CLI

```bash
# Preview first (always a good idea)
shoebox organize ~/Desktop/dump ~/Pictures/Photos\ Library.photoslibrary \
  -o ~/Pictures/Archive --dry-run

# Do it for real
shoebox organize ~/Desktop/dump ~/Pictures/Photos\ Library.photoslibrary \
  -o ~/Pictures/Archive

# Prove the archive is complete before deleting the originals
shoebox verify ~/Desktop/dump -o ~/Pictures/Archive
```

### Options worth knowing

| Flag | What it does |
|---|---|
| `--dry-run` | Plan the whole run, write nothing |
| `--move` | Move instead of copy |
| `--hardlink` | Instant, zero extra disk space (same filesystem only) |
| `--keep-undated` | File undated media under `Unsorted/` instead of skipping |
| `--verify-writes` | Re-hash every file after copying (paranoid mode) |
| `--report out.json` | Full machine-readable report of every decision |
| `-w 16` | Parallel workers (default 8) |

## 🧠 How it decides the date

For every file, the first hit wins:

1. **`exiftool`** (if installed) — `DateTimeOriginal`, `CreateDate`, `MediaCreateDate`, … queried in batches of 200 files per process.
2. **Built-in parsers** — pure-Python readers for EXIF in JPEG/TIFF/PNG/WebP, the `Exif` item in HEIC/HEIF, and the `mvhd` creation time in MP4/MOV.
3. **Spotlight** (macOS) — content creation date.
4. **File modified time** — last resort.

Timestamps before 1990 are treated as uninitialized garbage (hello, 1904-01-01 camera epochs) and rejected.

## 🔬 Safety model

- **Copy is the default.** Your originals are never touched unless you pass `--move`.
- Every write goes to a temporary file and is atomically renamed into place — an interrupted run can't leave a truncated photo at a final filename.
- Name collisions get `__1`, `__2`, … suffixes; existing files are never overwritten.
- `shoebox verify` gives you a cryptographic, file-by-file proof of completeness before you delete anything.

## 🛠️ Development

```bash
git clone https://github.com/akshaygona/photo_reorganizer && cd photo_reorganizer
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
```

The test suite builds real binary fixtures (JPEG APP1/EXIF, PNG `eXIf` chunks, ISO-BMFF HEIC with `iinf`/`iloc`, MP4 `mvhd`) and runs the full engine, CLI, and a headless TUI session against them.

## 📄 License

[MIT](LICENSE)
