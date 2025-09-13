import os
from unittest.mock import patch, MagicMock

import pytest

from equicast_ingestion import Downloader


@pytest.fixture
def mock_s3():
    with patch("equicast_ingestion.downloader.S3") as mock_s3_class:
        mock_instance = MagicMock()
        mock_s3_class.return_value = mock_instance
        yield mock_instance


@pytest.mark.ca
def test_single_file_success(mock_s3, capsys):
    mock_s3.download_files.return_value = {
        "downloaded": ["fxpairs.json"],
        "missing_mandatory": [],
    }

    dl = Downloader(download_dir="test_downloads")
    dl.download("fx")

    captured = capsys.readouterr()
    assert "✅ Successfully downloaded" in captured.out
    mock_s3.download_files.assert_called_once()


@pytest.mark.ca
def test_multiple_file_success(mock_s3, capsys):
    mock_s3.download_files.return_value = {
        "downloaded": ["tickers.json", "ticker_status.json"],
        "missing_mandatory": [],
    }

    dl = Downloader(download_dir="test_downloads")
    dl.download("stock")

    captured = capsys.readouterr()
    assert "✅ Successfully downloaded" in captured.out
    assert "⚠️" not in captured.out


@pytest.mark.ca
def test_mandatory_file_missing(mock_s3, capsys):
    mock_s3.download_files.return_value = {
        "downloaded": [],
        "missing_mandatory": ["tickers.json"],
    }

    dl = Downloader(download_dir="test_downloads")
    dl.download("stock")

    captured = capsys.readouterr()
    assert "⚠️ Some of the mandatory files are missing" in captured.out


@pytest.mark.ca
def test_optional_file_missing(mock_s3, capsys):
    mock_s3.download_files.return_value = {
        "downloaded": ["tickers.json"],
        "missing_mandatory": [],
    }

    dl = Downloader(download_dir="test_downloads")
    dl.download("stock")

    captured = capsys.readouterr()
    assert "⚠️" not in captured.out
    assert "✅" not in captured.out


@pytest.mark.ca
def test_invalid_data_type(mock_s3):
    dl = Downloader(download_dir="test_downloads")
    with pytest.raises(ValueError, match="data_type must be either 'fx' or 'stock'"):
        dl.download("crypto")


@pytest.mark.ca
def test_download_dir_created(tmp_path, mock_s3):
    test_dir = tmp_path / "downloads"
    Downloader(download_dir=str(test_dir))
    assert os.path.exists(test_dir)
