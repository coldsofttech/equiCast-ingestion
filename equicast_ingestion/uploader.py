import fnmatch
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Tuple

import boto3
from botocore.exceptions import ClientError


@dataclass
class UploadConfig:
    directory: Path
    pattern: str
    message: str
    bucket: str
    mode: str  # "generic", "stock", "fx"
    prefix: str = ""  # optional prefix before key
    uploaded: List[str] = field(default_factory=list)
    failed: List[Tuple[str, str]] = field(default_factory=list)


@dataclass
class Uploader:
    config: UploadConfig
    region_name: str = "eu-west-1"
    s3: boto3.client = field(init=False)

    def __post_init__(self):
        self.s3 = boto3.client("s3", region_name=self.region_name)

    def _collect_files(self) -> List[Path]:
        all_files = [f for f in self.config.directory.rglob("*") if f.is_file()]
        return [f for f in all_files if fnmatch.fnmatch(f.name, self.config.pattern)]

    def _make_key(self, file: Path) -> str:
        if self.config.mode == "generic":
            key = file.name
        elif self.config.mode == "stock":
            ticker = file.parent.name
            key = f"ticker={ticker}/{file.name}"
        elif self.config.mode == "fx":
            fxpair = file.stem
            key = f"fxpair={fxpair}/fx_history.parquet"
        else:
            raise ValueError(f"Unknown mode: {self.config.mode}")

        return key

    def upload(self):
        artifacts = self._collect_files()

        if not artifacts:
            print(f"No files found in {self.config.directory}/ matching pattern: {self.config.pattern}")
            return

        for file in artifacts:
            key = self._make_key(file)
            try:
                self.s3.upload_file(str(file), self.config.bucket, key)
                self.config.uploaded.append(key)
                print(f"✅ Uploaded: {file} > s3://{self.config.bucket}/{key}")
            except ClientError as e:
                self.config.failed.append((key, str(e)))
                print(f"❌ Failed: {file} ({e}")

    def write_summary(self):
        summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
        if not summary_path:
            return

        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(f"### ☁️ {self.config.message}\n")
            f.write(f"**Bucket:** `{self.config.bucket}`\n\n")
            f.write("| Local File | S3 Key | Status |\n")
            f.write("|------------|--------|--------|\n")
            for key in self.config.uploaded:
                f.write(f"| ✅ Uploaded | `{key}` | Success |\n")
            for key, error in self.config.failed:
                f.write(f"| ❌ Failed | `{key}` | {error} |\n")

    def write_outputs(self):
        print(f"::set-output name=uploaded_count::{len(self.config.uploaded)}")
        print(f"::set-output name=failed_count::{len(self.config.failed)}")
