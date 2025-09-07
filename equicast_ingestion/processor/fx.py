import concurrent.futures
import json
import os
import random
import time
from dataclasses import dataclass, field

from equicast_pyutils.extractors.fx_data_extractor import FxDataExtractor
from tqdm import tqdm


@dataclass
class FxProcessor:
    download_dir: str = "downloads"
    fx_download_dir: str = "fx_downloads"
    fx_file: str = "fxpairs.json"
    fx_status_file: str = "fxpair_status.json"
    fx_pairs: dict = field(init=False)
    fxpair_status: dict = field(default=None, init=False)

    def __post_init__(self):
        os.makedirs(self.fx_download_dir, exist_ok=True)
        if not os.path.exists(os.path.join(self.download_dir, self.fx_file)):
            raise RuntimeError(f"File {self.fx_file} does not exist!")

    def _process_fx(self, fx: dict):
        from_currency, to_currency = fx["from"], fx["to"]
        print(f"📥 Fetching FX data for {from_currency} > {to_currency}.")
        fx_extractor = FxDataExtractor(from_currency=from_currency, to_currency=to_currency)
        try:
            fx_data = fx_extractor.extract_fx_data()
            filepath = os.path.join(self.fx_download_dir, f"{from_currency}{to_currency}.parquet")
            fx_data.to_parquet(filepath)
            return {"success": True, "file": filepath}
        except Exception as e:
            return {"error": f"Failed to extract fx data: {e}."}

    def process(self):
        with open(os.path.join(self.download_dir, self.fx_file), "r") as f:
            self.fx_pairs = json.load(f)

        remaining = self.fx_pairs.copy()
        max_retries = 5
        decay_rate = 0.2
        max_workers = 20
        min_workers = 5
        errors = {}

        for attempt in range(0, max_retries):
            factor = (1 - decay_rate) ** attempt
            c_workers = max(int(max_workers * factor), min_workers)
            print(f"🔁 Attempt: {attempt + 1} with {c_workers} workers.")

            results = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=c_workers) as executor:
                futures = {executor.submit(self._process_fx, fx): fx for fx in remaining}
                for future in tqdm(
                        concurrent.futures.as_completed(futures),
                        total=len(remaining),
                        desc="Fetching FX",
                        unit="fx"
                ):
                    fx = futures[future]
                    result = future.result()
                    fx_key = f"{fx['from']}{fx['to']}"
                    results[fx_key] = result

                    if attempt == max_retries - 1 and result.get("error", None):
                        errors[fx_key] = result["error"]

            failed = [fx for fx_key, fx in zip(results.keys(), remaining) if results[fx_key].get("error")]
            if not failed:
                break

            print(f"🔁 Retrying {len(failed)} failed FX: {[f'{fx['from']}>{fx['to']}' for fx in failed]}.")
            remaining = failed
            time.sleep(random.uniform(5, 10))

        if errors:
            with open(os.path.join(self.fx_download_dir, "error.log"), "w", encoding="utf-8") as f:
                for fx, err in errors.items():
                    f.write(f"{fx}: {err}\n")

        if errors:
            print(f"⚠️ {len(errors)} errors logged to 'error.log'.")
