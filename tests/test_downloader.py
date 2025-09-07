import os
from unittest.mock import patch, MagicMock

import pytest
from botocore.exceptions import ClientError

from equicast_ingestion.downloader import Downloader


@pytest.fixture
def mock_s3_client(tmp_path):
    with patch("boto3.client") as mock_client:
        s3 = MagicMock()
        mock_client.return_value = s3
        yield s3


@pytest.mark.noca
def test_download_mandatory_file_success(mock_s3_client, tmp_path):
    d = Downloader(download_dir=str(tmp_path))
    d.download("fx")

    mock_s3_client.download_file.assert_any_call(
        "equicast-fxpairs", "fxpairs.json", os.path.join(str(tmp_path), "fxpairs.json")
    )


@pytest.mark.noca
def test_download_mandatory_file_missing(mock_s3_client, tmp_path):
    def side_effect(bucket, key, dest):
        if key == "tickers.json":
            raise ClientError({"Error": {"Code": "404"}}, "download_file")

    mock_s3_client.download_file.side_effect = side_effect

    d = Downloader(download_dir=str(tmp_path))
    with pytest.raises(FileNotFoundError) as exc:
        d.download("stock")

    assert "Mandatory file tickers.json not found" in str(exc.value)


@pytest.mark.noca
def test_invalid_data_type(mock_s3_client, tmp_path):
    d = Downloader(download_dir=str(tmp_path))
    with pytest.raises(ValueError):
        d.download("crypto")
