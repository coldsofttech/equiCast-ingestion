import os
from dataclasses import dataclass, field
from typing import Dict, List

import boto3
from botocore.exceptions import ClientError


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
                "optional": ["fxpair_status.json"]
            },
            "stock": {
                "mandatory": ["tickers.json"],
                "optional": ["ticker_status.json"]
            }
        }
    )
    s3: boto3.client = field(init=False)

    def __post_init__(self):
        os.makedirs(self.download_dir, exist_ok=True)
        self.s3 = boto3.client("s3", region_name=self.region_name)

    def _download_file(self, bucket: str, key: str, required: bool):
        local_path = os.path.join(self.download_dir, key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        try:
            print(f"Downloading {key} from {bucket} to {local_path}.")
            self.s3.download_file(bucket, key, local_path)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                if required:
                    raise FileNotFoundError(f"❌ Mandatory file {key} not found in {bucket}.") from e
                else:
                    print(f"⚠️ Optional file {key} not available in {bucket}.")
            else:
                raise e

    def download(self, data_type: str):
        if data_type not in self.buckets:
            raise ValueError("data_type must be either 'fx' or 'stock'.")

        bucket_name = self.buckets[data_type]
        m_files = self.files[data_type]["mandatory"]
        o_files = self.files[data_type]["optional"]

        for file_name in m_files:
            self._download_file(bucket_name, file_name, required=True)

        for file_name in o_files:
            self._download_file(bucket_name, file_name, required=False)
