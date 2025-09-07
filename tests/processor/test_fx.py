import json
import os
from unittest.mock import patch, MagicMock

import pytest

from equicast_ingestion.processor import FxProcessor


@pytest.fixture
def tmp_download_dir(tmp_path):
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    fx_file = downloads / "fxpairs.json"
    fx_file.write_text(json.dumps([
        {"from": "USD", "to": "GBP", "export": True},
        {"from": "GBP", "to": "USD", "export": True},
        {"from": "GBP", "to": "CHF", "export": False}
    ]))
    return downloads


@pytest.fixture
def mock_fx_extractor():
    with patch("equicast_pyutils.extractors.fx_data_extractor.FxDataExtractor") as mock_class:
        mock_instance = MagicMock()
        df_mock = MagicMock()
        df_mock.to_parquet = MagicMock()
        mock_instance.extract_fx_data.return_value = df_mock
        mock_class.return_value = mock_instance
        yield mock_class, df_mock


@pytest.fixture(autouse=True)
def patch_tqdm_sleep():
    with patch("tqdm.tqdm", lambda x, **kwargs: x), patch("time.sleep", lambda x: None):
        yield


@pytest.mark.ca
def test_fxprocessor_init_creates_fx_download_dir(tmp_download_dir):
    processor = FxProcessor(download_dir=str(tmp_download_dir))
    assert os.path.exists(processor.fx_download_dir)


@pytest.mark.ca
def test_fxprocessor_raises_if_fx_file_missing(tmp_path):
    with pytest.raises(RuntimeError):
        FxProcessor(download_dir=str(tmp_path))


@pytest.mark.ca
@patch("equicast_ingestion.processor.fx.FxDataExtractor")
def test_process_success(mock_fx_class, tmp_download_dir):
    mock_instance = MagicMock()
    df_mock = MagicMock()
    mock_instance.extract_fx_data.return_value = df_mock
    mock_fx_class.return_value = mock_instance

    processor = FxProcessor(download_dir=str(tmp_download_dir))
    processor.process()

    assert mock_fx_class.call_count == 3
    assert df_mock.to_parquet.call_count == 3


@pytest.mark.ca
@patch("equicast_ingestion.processor.fx.FxDataExtractor")
def test_process_handles_failure(mock_fx_class, tmp_download_dir):
    mock_instance = MagicMock()
    mock_instance.extract_fx_data.side_effect = Exception("fail")
    mock_fx_class.return_value = mock_instance

    processor = FxProcessor(download_dir=str(tmp_download_dir))
    processor.process()

    error_log = os.path.join(processor.fx_download_dir, "error.log")
    assert os.path.exists(error_log)

    with open(error_log, "r") as f:
        content = f.read()
        assert "USDGBP" in content
        assert "GBPUSD" in content
        assert "GBPCHF" in content
