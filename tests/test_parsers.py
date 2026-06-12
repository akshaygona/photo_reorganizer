from datetime import datetime, timezone

from shoebox.parsers import extract_date

from fixtures import (
    build_heic,
    build_jpeg,
    build_jpeg_no_exif,
    build_mp4,
    build_png,
    build_webp,
)

DATE = "2019:07:04 12:30:00"
EXPECTED = datetime(2019, 7, 4, 12, 30, 0)


def write(tmp_path, name, data):
    path = tmp_path / name
    path.write_bytes(data)
    return path


def test_jpeg_exif(tmp_path):
    assert extract_date(write(tmp_path, "a.jpg", build_jpeg(DATE))) == EXPECTED


def test_jpeg_without_exif(tmp_path):
    assert extract_date(write(tmp_path, "a.jpg", build_jpeg_no_exif())) is None


def test_png_exif_chunk(tmp_path):
    assert extract_date(write(tmp_path, "a.png", build_png(DATE))) == EXPECTED


def test_webp_exif_chunk(tmp_path):
    assert extract_date(write(tmp_path, "a.webp", build_webp(DATE))) == EXPECTED


def test_mp4_mvhd(tmp_path):
    dt = datetime(2020, 5, 17, 9, 0, 0, tzinfo=timezone.utc)
    path = write(tmp_path, "a.mp4", build_mp4(dt))
    assert extract_date(path) == dt


def test_mp4_zero_ctime_rejected(tmp_path):
    epoch = datetime(1904, 1, 1, tzinfo=timezone.utc)
    assert extract_date(write(tmp_path, "a.mp4", build_mp4(epoch))) is None


def test_heic_exif_item(tmp_path):
    assert extract_date(write(tmp_path, "a.heic", build_heic(DATE))) == EXPECTED


def test_tiff_direct(tmp_path):
    from fixtures import build_tiff_exif

    assert extract_date(write(tmp_path, "a.tif", build_tiff_exif(DATE))) == EXPECTED


def test_garbage_bytes_never_raise(tmp_path):
    for ext in ("jpg", "png", "webp", "mp4", "heic", "tif"):
        assert extract_date(write(tmp_path, f"junk.{ext}", b"\x00\x01\x02" * 50)) is None


def test_truncated_files_never_raise(tmp_path):
    full = build_jpeg(DATE)
    for cut in (1, 4, 10, len(full) - 2):
        assert extract_date(write(tmp_path, "t.jpg", full[:cut])) in (None, EXPECTED)


def test_unknown_extension(tmp_path):
    assert extract_date(write(tmp_path, "a.xyz", b"data")) is None
