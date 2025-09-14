import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from equicast_ingestion.processor import StockProcessor


@pytest.fixture
def tmp_download_dir(tmp_path):
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    return downloads


@pytest.fixture
def ticker_file(tmp_download_dir):
    file_path = tmp_download_dir / "chunk_1.json"
    file_path.write_text(json.dumps(["AAPL", "MSFT"]))
    return file_path


@pytest.fixture
def mock_stock_extractor():
    with patch("equicast_pyutils.extractors.stock_data_extractor.StockDataExtractor", autospec=True) as MockExtractor:
        yield MockExtractor


@pytest.fixture(autouse=True)
def patch_tqdm_sleep():
    with patch("tqdm.tqdm", lambda x, **kwargs: x), patch("time.sleep", lambda x: None):
        yield


@pytest.mark.ca
def test_stockprocessor_init_creates_stock_download_dir(tmp_download_dir, ticker_file):
    stock_dir = tmp_download_dir / "stock_downloads"
    StockProcessor(ticker_file=str(ticker_file), stock_download_dir=str(stock_dir))
    assert stock_dir.exists()


@pytest.mark.ca
def test_stockprocessor_raises_if_input_file_missing(tmp_download_dir, ticker_file):
    bad_file = tmp_download_dir / "does_not_exist.json"
    with pytest.raises(RuntimeError, match="does not exist"):
        StockProcessor(ticker_file=str(bad_file))


@pytest.mark.ca
def test_ticker_status_file_missing(tmp_download_dir, ticker_file, mock_stock_extractor, patch_tqdm_sleep):
    mock_df = MagicMock()
    mock_df.to_parquet.side_effect = lambda path: Path(path).write_text("parquet-data")
    mock_stock_extractor.return_value.extract_stock_price_data.return_value = mock_df

    processor = StockProcessor(
        ticker_file=str(ticker_file),
        stock_download_dir=str(tmp_download_dir / "stock_downloads"),
        download_dir=str(tmp_download_dir),
    )
    processor.process()

    for ticker in ["AAPL", "MSFT"]:
        parquet_path = tmp_download_dir / "stock_downloads" / ticker / "stock_price.parquet"
        assert parquet_path.exists()


@pytest.mark.ca
def test_stock_price_export(tmp_download_dir, ticker_file, mock_stock_extractor, patch_tqdm_sleep):
    mock_df = MagicMock()

    with patch("equicast_pyutils.extractors.stock_data_extractor.StockDataExtractor") as MockExtractor, \
            patch("pandas.DataFrame.to_parquet") as mock_to_parquet:
        MockExtractor.return_value.extract_stock_price_data.return_value = mock_df

        def fake_to_parquet(path, *args, **kwargs):
            Path(path).write_text("parquet-data")

        mock_to_parquet.side_effect = fake_to_parquet

        processor = StockProcessor(
            ticker_file=str(ticker_file),
            stock_download_dir=str(tmp_download_dir / "stock_downloads"),
            download_dir=str(tmp_download_dir),
        )
        processor.process()

    for ticker in ["AAPL", "MSFT"]:
        parquet_path = tmp_download_dir / "stock_downloads" / ticker / "stock_price.parquet"
        assert parquet_path.exists()
        assert parquet_path.read_text() == "parquet-data"


@pytest.mark.ca
def test_dividends_export(tmp_download_dir, ticker_file, mock_stock_extractor, patch_tqdm_sleep):
    mock_df = MagicMock()

    with patch("equicast_pyutils.extractors.stock_data_extractor.StockDataExtractor") as MockExtractor, \
            patch("pandas.DataFrame.to_parquet") as mock_to_parquet:
        MockExtractor.return_value.extract_stock_price_data.return_value = mock_df

        def fake_to_parquet(path, *args, **kwargs):
            Path(path).write_text("div-data")

        mock_to_parquet.side_effect = fake_to_parquet

        processor = StockProcessor(
            ticker_file=str(ticker_file),
            stock_download_dir=str(tmp_download_dir / "stock_downloads"),
            download_dir=str(tmp_download_dir),
        )
        processor.process()

    for ticker in ["AAPL", "MSFT"]:
        parquet_path = tmp_download_dir / "stock_downloads" / ticker / "dividends.parquet"
        assert parquet_path.exists()
        assert parquet_path.read_text() == "div-data"


@pytest.mark.ca
def test_comp_profile_export(tmp_download_dir, ticker_file, mock_stock_extractor, patch_tqdm_sleep):
    mock_df = MagicMock()

    with patch("equicast_pyutils.extractors.stock_data_extractor.StockDataExtractor") as MockExtractor, \
            patch("pandas.DataFrame.to_parquet") as mock_to_parquet:
        MockExtractor.return_value.extract_stock_price_data.return_value = mock_df

        def fake_to_parquet(path, *args, **kwargs):
            Path(path).write_text("comp-profile-data")

        mock_to_parquet.side_effect = fake_to_parquet

        processor = StockProcessor(
            ticker_file=str(ticker_file),
            stock_download_dir=str(tmp_download_dir / "stock_downloads"),
            download_dir=str(tmp_download_dir),
        )
        processor.process()

    for ticker in ["AAPL", "MSFT"]:
        parquet_path = tmp_download_dir / "stock_downloads" / ticker / "company_profile.parquet"
        assert parquet_path.exists()
        assert parquet_path.read_text() == "comp-profile-data"


@pytest.mark.ca
def test_failure_flow(tmp_download_dir, ticker_file, mock_stock_extractor, patch_tqdm_sleep):
    with patch("equicast_ingestion.processor.StockProcessor._process_ticker") as mock_process_ticker, \
            patch("time.sleep", return_value=None):
        mock_process_ticker.side_effect = lambda ticker: {"error": "Boom!"}

        processor = StockProcessor(
            ticker_file=str(ticker_file),
            stock_download_dir=str(tmp_download_dir / "stock_downloads"),
            download_dir=str(tmp_download_dir),
        )
        processor.process()

    error_log = tmp_download_dir / "stock_downloads" / "error.log"
    assert error_log.exists(), "error.log should be created after retries"
    content = error_log.read_text()
    assert "Boom!" in content
