import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

import shoebox.dates
from shoebox.engine import Options, Organizer

from fixtures import build_jpeg, build_mp4

JULY = "2019:07:04 12:30:00"
MARCH = "2021:03:15 08:00:00"


@pytest.fixture(autouse=True)
def hermetic_dates(monkeypatch):
    """Keep tests deterministic: no exiftool, no Spotlight lookups."""
    monkeypatch.setattr(shoebox.dates, "_mdls_date", lambda path: None)


def make_options(src: Path, out: Path, **kwargs) -> Options:
    kwargs.setdefault("use_exiftool", False)
    kwargs.setdefault("workers", 4)
    return Options(sources=[src], output=out, **kwargs)


def make_source(tmp_path: Path) -> Path:
    src = tmp_path / "src"
    (src / "nested").mkdir(parents=True)
    (src / "july.jpg").write_bytes(build_jpeg(JULY))
    (src / "march.jpg").write_bytes(build_jpeg(MARCH))
    (src / "nested" / "july_copy.jpg").write_bytes(build_jpeg(JULY))  # duplicate content
    (src / "video.mp4").write_bytes(
        build_mp4(datetime(2020, 5, 17, 9, 0, tzinfo=timezone.utc))
    )
    undated = src / "mystery.gif"
    undated.write_bytes(b"GIF89a" + b"\x00" * 64)
    os.utime(undated, (315532800, 315532800))  # 1980: below MIN_VALID_YEAR
    return src


def run(options: Options):
    return Organizer(options).run()


def test_basic_organize(tmp_path):
    src, out = make_source(tmp_path), tmp_path / "out"
    report = run(make_options(src, out))

    assert report.scanned == 5
    assert report.transferred == 3
    assert report.duplicates == 1
    assert report.no_date == 1
    assert report.errors == 0

    assert (out / "2019" / "2019-07" / "july.jpg").exists()
    assert (out / "2021" / "2021-03" / "march.jpg").exists()
    assert (out / "2020" / "2020-05" / "video.mp4").exists()
    assert not (out / "2019" / "2019-07" / "july_copy.jpg").exists()


def test_rerun_is_idempotent(tmp_path):
    src, out = make_source(tmp_path), tmp_path / "out"
    run(make_options(src, out))
    report = run(make_options(src, out))

    assert report.transferred == 0
    assert report.already_present == 3
    # No __1 suffixed duplicates were created on the second pass.
    organized = [p for p in out.rglob("*") if p.is_file()]
    assert len(organized) == 3
    assert not any("__" in p.name for p in organized)


def test_name_collision_gets_suffix(tmp_path):
    src = tmp_path / "src"
    (src / "a").mkdir(parents=True)
    (src / "b").mkdir()
    (src / "a" / "photo.jpg").write_bytes(build_jpeg(JULY))
    different = build_jpeg(JULY) + b"\x00"  # same name+date, different content
    (src / "b" / "photo.jpg").write_bytes(different)

    report = run(make_options(src, tmp_path / "out"))
    dest_dir = tmp_path / "out" / "2019" / "2019-07"
    assert report.transferred == 2
    assert (dest_dir / "photo.jpg").exists()
    assert (dest_dir / "photo__1.jpg").exists()


def test_dry_run_writes_nothing(tmp_path):
    src, out = make_source(tmp_path), tmp_path / "out"
    report = run(make_options(src, out, dry_run=True))
    assert report.transferred == 3
    assert not out.exists()


def test_move_mode_removes_sources(tmp_path):
    src, out = make_source(tmp_path), tmp_path / "out"
    report = run(make_options(src, out, mode="move"))
    assert report.transferred == 3
    assert not (src / "july.jpg").exists()
    assert (src / "nested" / "july_copy.jpg").exists()  # duplicates stay put
    assert (out / "2019" / "2019-07" / "july.jpg").exists()


def test_hardlink_mode(tmp_path):
    src, out = make_source(tmp_path), tmp_path / "out"
    run(make_options(src, out, mode="hardlink"))
    linked = out / "2019" / "2019-07" / "july.jpg"
    assert linked.exists()
    assert linked.stat().st_ino == (src / "july.jpg").stat().st_ino


def test_keep_undated_goes_to_unsorted(tmp_path):
    src, out = make_source(tmp_path), tmp_path / "out"
    report = run(make_options(src, out, keep_undated=True))
    assert report.no_date == 0
    assert report.transferred == 4
    assert (out / "Unsorted" / "mystery.gif").exists()


def test_verify_writes_mode(tmp_path):
    src, out = make_source(tmp_path), tmp_path / "out"
    report = run(make_options(src, out, verify_writes=True))
    assert report.transferred == 3
    assert report.errors == 0


def test_mtime_fallback_dates_file(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    photo = src / "noexif.jpg"
    from fixtures import build_jpeg_no_exif

    photo.write_bytes(build_jpeg_no_exif())
    stamp = datetime(2018, 11, 2, 10, 0).timestamp()
    os.utime(photo, (stamp, stamp))

    run(make_options(src, tmp_path / "out"))
    assert (tmp_path / "out" / "2018" / "2018-11" / "noexif.jpg").exists()


def test_missing_source_is_reported_not_fatal(tmp_path):
    options = Options(
        sources=[tmp_path / "nope"], output=tmp_path / "out", use_exiftool=False
    )
    report = run(options)
    assert report.scanned == 0
    assert report.errors == 0


def test_invalid_options_rejected(tmp_path):
    with pytest.raises(ValueError):
        Options(sources=[tmp_path], output=tmp_path / "o", mode="teleport")
    with pytest.raises(ValueError):
        Options(sources=[tmp_path], output=tmp_path / "o", workers=0)


def test_report_serializes(tmp_path):
    import json

    src, out = make_source(tmp_path), tmp_path / "out"
    report = run(make_options(src, out))
    payload = json.loads(json.dumps(report.to_dict()))
    assert payload["summary"]["transferred"] == 3
    assert len(payload["actions"]) == report.scanned
