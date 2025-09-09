import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from equicast_ingestion import UploadConfig, Uploader


@pytest.fixture
def temp_files(tmp_path):
    stock_dir = tmp_path / "AAPL"
    stock_dir.mkdir()
    stock_file = stock_dir / "stock_price.parquet"
    stock_file.write_text("dummy")

    fx_dir = tmp_path / "fx"
    fx_dir.mkdir()
    fx_file = fx_dir / "EURUSD.parquet"
    fx_file.write_text("dummy")

    generic_file = tmp_path / "ticker_status.json"
    generic_file.write_text("dummy")

    return tmp_path, stock_file, fx_file, generic_file


def make_client_mock(success=True):
    client = MagicMock()
    if success:
        client.upload_file.return_value = None
    else:
        client.upload_file.side_effect = ClientError(
            {"Error": {"Code": "500", "Message": "Mocked failure"}}, "UploadFile"
        )
    return client


@pytest.mark.noca
def test_collect_files_generic(temp_files):
    tmp_path, _, _, generic_file = temp_files
    config = UploadConfig(directory=tmp_path, pattern="*.json", message="msg", bucket="bkt", mode="generic")
    uploader = Uploader(config)
    uploader.s3 = make_client_mock()

    files = uploader._collect_files()
    assert generic_file in files
    assert all(isinstance(f, Path) for f in files)


@pytest.mark.noca
def test_make_key_generic(temp_files):
    _, _, _, generic_file = temp_files
    config = UploadConfig(directory=generic_file.parent, pattern="*.json", message="msg", bucket="bkt", mode="generic")
    uploader = Uploader(config)
    key = uploader._make_key(generic_file)
    assert key == "ticker_status.json"


@pytest.mark.noca
def test_make_key_stock(temp_files):
    _, stock_file, _, _ = temp_files
    config = UploadConfig(directory=stock_file.parent.parent, pattern="*.parquet", message="msg", bucket="bkt",
                          mode="stock")
    uploader = Uploader(config)
    key = uploader._make_key(stock_file)
    assert key == "ticker=AAPL/stock_price.parquet"


@pytest.mark.noca
def test_make_key_fx(temp_files):
    _, _, fx_file, _ = temp_files
    config = UploadConfig(directory=fx_file.parent, pattern="*.parquet", message="msg", bucket="bkt", mode="fx")
    uploader = Uploader(config)
    key = uploader._make_key(fx_file)
    assert key == "fxpair=EURUSD/fx_history.parquet"


@pytest.mark.noca
def test_upload_success(temp_files, capsys):
    tmp_path, stock_file, _, _ = temp_files
    config = UploadConfig(directory=tmp_path, pattern="*.parquet", message="msg", bucket="bkt", mode="stock")
    uploader = Uploader(config)
    uploader.s3 = make_client_mock(success=True)

    uploader.upload()
    captured = capsys.readouterr()
    assert "✅ Uploaded" in captured.out
    assert config.uploaded[0].startswith("ticker=")


@pytest.mark.noca
def test_upload_failure(temp_files, capsys):
    tmp_path, stock_file, _, _ = temp_files
    config = UploadConfig(directory=stock_file.parent, pattern="*.parquet", message="msg", bucket="bkt", mode="stock")
    uploader = Uploader(config)
    uploader.s3 = make_client_mock(success=False)

    uploader.upload()
    captured = capsys.readouterr()
    assert "❌ Failed" in captured.out
    assert len(config.failed) == 1


@pytest.mark.noca
def test_write_summary(temp_files, tmp_path):
    _, stock_file, _, _ = temp_files
    summary_file = tmp_path / "summary.md"
    os.environ["GITHUB_STEP_SUMMARY"] = str(summary_file)

    config = UploadConfig(directory=stock_file.parent.parent, pattern="*.parquet", message="msg", bucket="bkt",
                          mode="stock")
    config.uploaded.append("ticker=AAPL/stock_price.parquet")
    uploader = Uploader(config)
    uploader.write_summary()

    content = summary_file.read_text(encoding="utf-8")
    assert "### ☁️ msg" in content
    assert "ticker=AAPL/stock_price.parquet" in content


@pytest.mark.noca
def test_write_outputs(capsys):
    config = UploadConfig(directory=Path("."), pattern="*", message="msg", bucket="bkt", mode="generic")
    config.uploaded = ["file1"]
    config.failed = [("file2", "some error")]

    uploader = Uploader(config)
    uploader.write_outputs()
    captured = capsys.readouterr()
    assert "::set-output name=uploaded_count::1" in captured.out
    assert "::set-output name=failed_count::1" in captured.out
