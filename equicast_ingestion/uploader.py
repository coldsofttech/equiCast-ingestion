import fnmatch
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

from equicast_awsutils import S3


@dataclass
class UploadConfig:
    directory: Path
    pattern: str
    message: str
    bucket: str
    mode: str  # "generic", "stock", "fx"
    prefix: str = ""  # optional prefix before key


@dataclass
class Uploader:
    config: UploadConfig
    region_name: str = "eu-west-1"

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

        files = []
        for file in artifacts:
            key = self._make_key(file)
            files.append({'key': key, 'path': file})

        s3_obj = S3(bucket_name=self.config.bucket, region_name=self.region_name)
        status = s3_obj.upload_files(files=files)

        if len(status.get("failed", [])) > 0:
            print(f"⚠️ Upload failed for some of the files: {status.get('failed')}")
        elif len(files) == len(status.get("uploaded", [])):
            print(f"✅ Successfully uploaded {len(files)} files")

        self.write_summary(status.get("uploaded", []), status.get("failed", []))
        self.write_outputs(len(status.get("uploaded", [])), len(status.get("failed", [])))

    def write_summary(self, uploaded: list, failed: list):
        summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
        if not summary_path:
            return

        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(f"### ☁️ {self.config.message}\n")
            f.write(f"**Bucket:** `{self.config.bucket}`\n\n")
            f.write("| Local File | S3 Key |\n")
            f.write("|------------|--------|\n")
            for file in uploaded:
                f.write(f"| ✅ Uploaded | `{file}` |\n")
            for file in failed:
                f.write(f"| ❌ Failed | `{file}` |\n")

    def write_outputs(self, uploaded: int, failed: int):
        gh_output = os.environ.get("GITHUB_OUTPUT")
        if gh_output:
            with open(gh_output, "a", encoding="utf-8") as f:
                f.write(f"uploaded_count={uploaded}\n")
                f.write(f"failed_count={failed}\n")
