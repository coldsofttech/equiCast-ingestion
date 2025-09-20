import argparse
from pathlib import Path

from equicast_ingestion.helpers import UploadConfig, Uploader


def main():
    parser = argparse.ArgumentParser(description="S3: Upload Files")
    parser.add_argument("--directory-path", required=True, help="Directory Path")
    parser.add_argument("--file-pattern", required=True, help="File Pattern. Example: *.json, *.parquet")
    parser.add_argument("--custom-message", required=True, help="Custom Message")
    parser.add_argument("--s3-bucket", required=True, help="S3 Bucket")
    parser.add_argument("--mode", required=True, choices=["generic", "fx", "stock"], default="generic", help="Mode")
    parser.add_argument("--s3-prefix", required=False, default="", help="S3 Prefix")
    args = parser.parse_args()

    dir_path = Path(args.directory_path)
    if not dir_path.exists() or not dir_path.is_dir():
        print(f"Error: Path '{dir_path}' does not exist or is not a directory.")
        exit(1)

    config = UploadConfig(
        directory=dir_path,
        pattern=args.file_pattern,
        message=args.custom_message,
        bucket=args.s3_bucket,
        mode=args.mode,
        prefix=args.s3_prefix
    )

    uploader = Uploader(config=config)
    uploader.upload()


if __name__ == "__main__":
    main()
