from datetime import datetime, timezone

from shoebox.engine import Options, Organizer
from shoebox.verify import verify

from fixtures import build_jpeg, build_mp4


def make_archive(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.jpg").write_bytes(build_jpeg("2019:07:04 12:30:00"))
    (src / "b.jpg").write_bytes(build_jpeg("2021:03:15 08:00:00"))
    (src / "c.mp4").write_bytes(
        build_mp4(datetime(2020, 5, 17, 9, 0, tzinfo=timezone.utc))
    )
    out = tmp_path / "out"
    Organizer(Options(sources=[src], output=out, use_exiftool=False)).run()
    return src, out


def test_verify_complete_archive(tmp_path):
    src, out = make_archive(tmp_path)
    result = verify([src], out)
    assert result["summary"]["verified_complete"] is True
    assert result["summary"]["missing_in_dest"] == 0


def test_verify_detects_missing_file(tmp_path):
    src, out = make_archive(tmp_path)
    next((out / "2019" / "2019-07").iterdir()).unlink()

    result = verify([src], out)
    assert result["summary"]["verified_complete"] is False
    assert result["summary"]["missing_in_dest"] == 1
    assert result["missing_examples"][0]["source_paths"] == [str(src / "a.jpg")]


def test_verify_detects_extra_file(tmp_path):
    src, out = make_archive(tmp_path)
    (out / "stray.jpg").write_bytes(build_jpeg("1999:01:01 00:00:00"))

    result = verify([src], out)
    assert result["summary"]["verified_complete"] is True  # nothing missing
    assert result["summary"]["extra_in_dest"] == 1
