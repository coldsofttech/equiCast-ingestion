import concurrent.futures
import json
import os
import random
import time
from dataclasses import dataclass, field

from equicast_pyutils.extractors.stock_data_extractor import StockDataExtractor
from tqdm import tqdm


@dataclass
class StockProcessor:
    ticker_file: str
    download_dir: str = "downloads"
    stock_download_dir: str = "stock_downloads"
    ticker_status_file: str = "ticker_status.json"
    tickers: dict = field(init=False)
    ticker_status: dict = field(default=None, init=False)

    def __post_init__(self):
        os.makedirs(self.stock_download_dir, exist_ok=True)
        if not os.path.exists(self.ticker_file):
            raise RuntimeError(f"File {self.ticker_file} does not exist!")

    @staticmethod
    def _process_prices(extractor: StockDataExtractor, folder: str):
        filename = os.path.join(folder, "stock_price.parquet")
        if not os.path.exists(filename):
            print(f"💲 Fetching stock prices for {extractor.ticker}.")
            price_data = extractor.extract_stock_price_data()
            price_data.to_parquet(filename)

    @staticmethod
    def _process_dividends(extractor: StockDataExtractor, folder: str):
        filename = os.path.join(folder, "dividends.parquet")
        if not os.path.exists(filename):
            print(f"💰 Fetching dividends for {extractor.ticker}.")
            dividends = extractor.extract_dividends()
            dividends.to_parquet(filename)

    @staticmethod
    def _process_company_profile(extractor: StockDataExtractor, folder: str):
        filename = os.path.join(folder, "company_profile.parquet")
        if not os.path.exists(filename):
            print(f"🏢 Fetching company profile for {extractor.ticker}.")
            comp_profile = extractor.extract_company_profile()
            comp_profile.to_parquet(filename)

    def _process_ticker(self, ticker: str):
        print(f"📥 Fetching ticker data for {ticker}.")
        stock_extractor = StockDataExtractor(ticker=ticker)
        try:
            folder_path = os.path.join(self.stock_download_dir, ticker)
            os.makedirs(folder_path, exist_ok=True)
            self._process_prices(stock_extractor, folder_path)
            self._process_dividends(stock_extractor, folder_path)
            self._process_company_profile(stock_extractor, folder_path)
            return {"success": True, "folder": folder_path}
        except Exception as e:
            return {"success": False, "error": f"Failed to extract ticker data: {e}."}

    def _remove_delisted_tickers(self):
        for ticker in self.ticker_status:
            if ticker in self.tickers and ticker["is_delisted"]:
                self.tickers.pop(ticker)

    def process(self):
        with open(self.ticker_file, "r") as f:
            self.tickers = json.load(f)

        if os.path.exists(os.path.join(self.download_dir, self.ticker_status_file)):
            with open(os.path.join(self.download_dir, self.ticker_status_file), "r") as f:
                self.ticker_status = json.load(f)
            self._remove_delisted_tickers()

        remaining = self.tickers.copy()
        max_retries = 5
        decay_rate = 0.2
        max_workers = 80
        min_workers = 10
        errors = {}

        for attempt in range(0, max_retries):
            factor = (1 - decay_rate) ** attempt
            c_workers = max(int(max_workers * factor), min_workers)
            print(f"🔁 Attempt: {attempt + 1} with {c_workers} workers.")

            results = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=c_workers) as executor:
                futures = {executor.submit(self._process_ticker, ticker): ticker for ticker in remaining}
                for future in tqdm(
                        concurrent.futures.as_completed(futures),
                        total=len(remaining),
                        desc="Fetching Tickers",
                        unit="ticker"
                ):
                    ticker = futures[future]
                    result = future.result()
                    results[ticker] = result

                    if attempt == max_retries - 1 and result.get("error", None):
                        errors[ticker] = result["error"]

            failed = [ticker for ticker in remaining if results[ticker].get("error")]
            if not failed:
                break

            print(f"🔁 Retrying {len(failed)} failed Ticker: {[t for t in failed]}.")
            remaining = failed
            time.sleep(random.uniform(5, 10))

        if errors:
            log_path = os.path.join(self.stock_download_dir, "error.log")
            with open(log_path, "w", encoding="utf-8") as f:
                for fx, err in errors.items():
                    f.write(f"{fx}: {err}\n")
            print(f"⚠️ {len(errors)} errors logged to '{log_path}'.")
