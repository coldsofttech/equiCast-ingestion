import concurrent.futures
import json
import os
import random
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from equicast_pyutils.extractors.fx_data_extractor import FxDataExtractor
from tqdm import tqdm


@dataclass
class FxProcessor:
    input_file: str
    max_workers: int = 20
    max_retries: int = 5
    fx_status_file: str = "fx_status.json"
    fx_pairs: dict = field(init=False)
    fx_status: dict = field(default=None, init=False)
    full_run: bool = False
    temp_dir: str = field(init=False)

    def __post_init__(self):
        self.temp_dir = tempfile.mkdtemp(prefix="fx_downloads_")
        os.makedirs(self.temp_dir, exist_ok=True)

        if not os.path.exists(self.input_file):
            raise RuntimeError(f"File {self.input_file} does not exist!")

        with open(self.input_file, "r") as f:
            self.fx_pairs = json.load(f)

    def _extractor(self, fx: str, method: str, file_name: str):
        from_currency, to_currency = fx.split("/")
        end = datetime.now(timezone.utc)
        start = datetime(end.year, 1, 1)
        print(f"📥 Fetching FX '{method}' for {from_currency} > {to_currency}.")

        extractor = FxDataExtractor(
            from_currency=from_currency,
            to_currency=to_currency,
            start_date=None if self.full_run else start,
            end_date=None if self.full_run else end
        )

        try:
            data = getattr(extractor, method)()
            data.to_parquet(file_name, self.temp_dir)
            return {"success": True, "file": file_name}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _process_all(self, method: str, file_name: str):
        remaining = self.fx_pairs
        max_retries = self.max_retries
        decay_rate = 0.2
        max_workers = self.max_workers
        min_workers = int(self.max_workers / self.max_retries)
        errors = {}

        for attempt in range(max_retries):
            factor = (1 - decay_rate) ** attempt
            c_workers = max(int(max_workers * factor), min_workers)
            print(f"🔁 Attempt {attempt + 1} with {c_workers} workers.")

            results = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=c_workers) as executor:
                futures = {executor.submit(self._extractor, fx, method, file_name): fx for fx in remaining}
                for future in tqdm(
                        concurrent.futures.as_completed(futures),
                        total=len(remaining),
                        desc=f"Fetching FX '{method}'",
                        unit="fx"
                ):
                    fx: str = futures[future]
                    result: dict = future.result()
                    results[fx] = result
                    if attempt == max_retries - 1 and result.get("error"):
                        errors[fx] = result["error"]

            failed = [fx for fx in remaining if results[fx].get("error")]
            if not failed:
                break

            remaining = failed
            print(f"🔁 Retrying {len(failed)} failed FX: {failed}.")
            time.sleep(random.uniform(5, 10))

        if errors:
            log_path = os.path.join(self.temp_dir, f"error_{method}.log")
            with open(log_path, "w", encoding="utf-8") as f:
                for fx, err in errors.items():
                    f.write(f"{fx}: {err}\n")
            print(f"⚠️ {len(errors)} errors logged to '{log_path}'.")

    def process_prices(self):
        self._process_all("extract_fx_prices", "fx_prices.parquet")
        return self.temp_dir

    def process_profile(self):
        self._process_all("extract_fx_profile", "fx_profile.parquet")
        return self.temp_dir

    def process_fundamentals(self):
        self._process_all("extract_fx_fundamentals", "fx_fundamentals.parquet")
        return self.temp_dir

    def process_calculations(self):
        self._process_all("extract_fx_calculations", "fx_calculations.parquet")
        return self.temp_dir

    def process_forecast(self):
        self._process_all("extract_fx_forecast", "fx_forecast.parquet")
        return self.temp_dir
