import os
from unittest.mock import patch, MagicMock

import pytest

from equicast_ingestion.uploader import UploadConfig, Uploader  # adjust import if needed


@pytest.fixture
def tmp_dir(tmp_path):
    d = tmp_path / "data"
    d.mkdir()
    (d / "AAPL").mkdir()
    (d / "AAPL" / "stock_price.parquet").write_text("data")
    (d / "EURUSD.parquet").write_text("data")
    (d / "random.txt").write_text("text")
    return d


@pytest.fixture
def mock_s3():
    with patch("equicast_ingestion.uploader.S3") as mock_class:
        instance = MagicMock()
        mock_class.return_value = instance
        yield instance


@pytest.mark.ca
def test_collect_files(tmp_dir):
    cfg = UploadConfig(directory=tmp_dir, pattern="*.parquet", message="msg", bucket="bucket", mode="generic")
    up = Uploader(cfg)
    files = up._collect_files()
    assert all(f.suffix == ".parquet" for f in files)
    assert len(files) == 2


@pytest.mark.ca
def test_make_key_generic(tmp_dir):
    cfg = UploadConfig(directory=tmp_dir, pattern="*.parquet", message="msg", bucket="bucket", mode="generic")
    up = Uploader(cfg)
    file = tmp_dir / "random.txt"
    assert up._make_key(file.with_suffix(".dat")) == "random.dat"


@pytest.mark.ca
def test_make_key_stock(tmp_dir):
    cfg = UploadConfig(directory=tmp_dir, pattern="*.parquet", message="msg", bucket="bucket", mode="stock")
    up = Uploader(cfg)
    file = tmp_dir / "AAPL" / "stock_price.parquet"
    assert up._make_key(file) == "ticker=AAPL/stock_price.parquet"


@pytest.mark.ca
def test_make_key_fx(tmp_dir):
    cfg = UploadConfig(directory=tmp_dir, pattern="*.parquet", message="msg", bucket="bucket", mode="fx")
    up = Uploader(cfg)
    file = tmp_dir / "EURUSD.parquet"
    assert up._make_key(file) == "fxpair=EURUSD/fx_history.parquet"


@pytest.mark.ca
def test_make_key_invalid_mode(tmp_dir):
    cfg = UploadConfig(directory=tmp_dir, pattern="*.parquet", message="msg", bucket="bucket", mode="invalid")
    up = Uploader(cfg)
    with pytest.raises(ValueError):
        up._make_key(tmp_dir / "EURUSD.parquet")


@pytest.mark.ca
def test_upload_all_success(tmp_dir, mock_s3, capsys):
    cfg = UploadConfig(directory=tmp_dir, pattern="*.parquet", message="msg", bucket="bucket", mode="generic")
    up = Uploader(cfg)
    mock_s3.upload_files.return_value = {"uploaded": ["a", "b"], "failed": []}
    up.upload()
    captured = capsys.readouterr()
    assert "✅ Successfully uploaded" in captured.out


@pytest.mark.ca
def test_upload_all_fail(tmp_dir, mock_s3, capsys):
    cfg = UploadConfig(directory=tmp_dir, pattern="*.parquet", message="msg", bucket="bucket", mode="generic")
    up = Uploader(cfg)
    mock_s3.upload_files.return_value = {"uploaded": [], "failed": ["a", "b"]}
    up.upload()
    captured = capsys.readouterr()
    assert "⚠️ Upload failed" in captured.out


@pytest.mark.ca
def test_upload_some_success(tmp_dir, mock_s3, capsys):
    cfg = UploadConfig(directory=tmp_dir, pattern="*.parquet", message="msg", bucket="bucket", mode="generic")
    up = Uploader(cfg)
    mock_s3.upload_files.return_value = {"uploaded": ["a"], "failed": ["b"]}
    up.upload()
    captured = capsys.readouterr()
    assert "⚠️ Upload failed" in captured.out


@pytest.mark.ca
def test_upload_no_files(tmp_path, mock_s3, capsys):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    cfg = UploadConfig(directory=empty_dir, pattern="*.json", message="msg", bucket="bucket", mode="generic")
    up = Uploader(cfg)
    up.upload()
    captured = capsys.readouterr()
    assert "No files found" in captured.out
    mock_s3.upload_files.assert_not_called()


@pytest.mark.ca
def test_write_summary_and_outputs(tmp_dir, mock_s3, tmp_path):
    cfg = UploadConfig(directory=tmp_dir, pattern="*.parquet", message="msg", bucket="bucket", mode="generic")
    up = Uploader(cfg)

    summary_file = tmp_path / "summary.md"
    output_file = tmp_path / "output.txt"
    os.environ["GITHUB_STEP_SUMMARY"] = str(summary_file)
    os.environ["GITHUB_OUTPUT"] = str(output_file)

    up.write_summary(["file1"], ["file2"])
    up.write_outputs(1, 1)

    content_summary = summary_file.read_text(encoding="utf-8")
    assert "☁️" in content_summary
    assert "✅ Uploaded" in content_summary
    assert "❌ Failed" in content_summary

    content_output = output_file.read_text(encoding="utf-8")
    assert "uploaded_count=1" in content_output
    assert "failed_count=1" in content_output
