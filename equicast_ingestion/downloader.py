import os
from dataclasses import dataclass, field
from typing import Dict, List

from equicast_awsutils import S3


@dataclass
class Downloader:
    download_dir: str = "downloads"
    region_name: str = "eu-west-1"
    buckets: Dict[str, str] = field(
        default_factory=lambda: {
            "fx": "equicast-fxpairs",
            "stock": "equicast-tickers"
        }
    )
    files: Dict[str, Dict[str, List[str]]] = field(
        default_factory=lambda: {
            "fx": {
                "mandatory": ["fxpairs.json"],
                "optional": []
            },
            "stock": {
                "mandatory": ["tickers.json"],
                "optional": ["ticker_status.json"]
            }
        }
    )

    def __post_init__(self):
        os.makedirs(self.download_dir, exist_ok=True)

    def download(self, data_type: str):
        if data_type not in self.buckets:
            raise ValueError("data_type must be either 'fx' or 'stock'.")

        bucket_name = self.buckets[data_type]
        files = []
        for file_name in self.files[data_type]["mandatory"]:
            files.append({'key': file_name, 'mandatory': True})

        for file_name in self.files[data_type]["optional"]:
            files.append({'key': file_name, 'mandatory': False})

        s3_obj = S3(bucket_name=bucket_name, region_name=self.region_name)
        status = s3_obj.download_files(local_dir=self.download_dir, files=files)
        if len(status.get("missing_mandatory", [])) > 0:
            print(f"⚠️ Some of the mandatory files are missing: {status.get('missing_mandatory')}")
        elif len(files) == len(status.get("downloaded", [])):
            print(f"✅ Successfully downloaded {len(files)} files")
