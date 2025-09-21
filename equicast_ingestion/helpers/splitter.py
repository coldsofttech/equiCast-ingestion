import json
import os
import tempfile
from dataclasses import dataclass, field


@dataclass
class Splitter:
    mode: str  # Example: "stock" or "fx". FX is not currently supported
    filepath: str
    pref_chunk_size: int
    max_chunks: int = field(default=256, init=False)
    temp_dir: str = field(init=False)

    def __post_init__(self):
        self.temp_dir = tempfile.mkdtemp(prefix=f"chunks_{self.mode}_")
        os.makedirs(self.temp_dir, exist_ok=True)

    def split(self):
        if self.mode not in ["stock", "fx"]:
            raise ValueError("mode must be 'stock' or 'fx'")

        with open(self.filepath, "r", encoding="utf-8") as f:
            elements = json.load(f)

        seen = set()
        unique_elements = []
        if self.mode in ["stock", "fx"]:
            for element in elements:
                if element not in seen:
                    seen.add(element)
                    unique_elements.append(element)

            total_elements = len(unique_elements)
            chunk_size = self.pref_chunk_size
            num_chunks = (total_elements + chunk_size - 1) // chunk_size
            if num_chunks > self.max_chunks:
                chunk_size = (total_elements + self.max_chunks - 1) // self.max_chunks
                num_chunks = (total_elements + chunk_size - 1) // chunk_size
                print(f"⚠️ Too many chunks ({num_chunks}) for preferred chunk size {self.pref_chunk_size}.")
                print(f"➡️ Increasing chunk size to {chunk_size} to keep chunks <= {self.max_chunks}.")

            chunks = [
                unique_elements[i:i + chunk_size]
                for i in range(0, len(unique_elements), chunk_size)
            ]
            for idx, chunk in enumerate(chunks):
                output_filepath = os.path.join(self.temp_dir, f"chunk_{idx + 1}.json")
                with open(output_filepath, "w", encoding="utf-8") as f:
                    json.dump(chunk, f, indent=4, sort_keys=True)

                print(f"✅ Saved chunk {idx + 1} with {len(chunk)} tickers to {output_filepath}.")

            chunk_ids = list(range(1, len(chunks) + 1))
            with open(os.path.join(self.temp_dir, f"{self.mode}_chunks.json"), "w", encoding="utf-8") as f:
                json.dump(chunk_ids, f)

            print("✅ Processed all tickers.")

        return self.temp_dir
