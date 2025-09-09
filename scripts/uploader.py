import sys
from pathlib import Path

from equicast_ingestion import UploadConfig, Uploader


def main():
    if len(sys.argv) < 6:
        print(
            "Usage: python uploader.py <directory_path> <file_pattern> <custom_message> <s3_bucket> <mode> [s3_prefix]"
        )
        print("Modes: generic | stock | fx")
        print(
            "Example 1 (Generic): "
            "python s3_uploader.py ./dist 'ticker_status.json' 'Upload ticker status' my-bucket generic"
        )
        print(
            "Example 2 (Stock):   "
            "python s3_uploader.py ./stock_downloads '*.parquet' 'Upload stock parquet' my-bucket stock"
        )
        print(
            "Example 3 (FX):      "
            "python s3_uploader.py ./fx_downloads '*.parquet' 'Upload FX files' my-bucket fx"
        )
        sys.exit(1)

    dir_path = Path(sys.argv[1])
    file_pattern = sys.argv[2]
    custom_message = sys.argv[3]
    bucket_name = sys.argv[4]
    mode = sys.argv[5]
    prefix = sys.argv[6] if len(sys.argv) > 6 else ""

    if not dir_path.exists() or not dir_path.is_dir():
        print(f"Error: Path '{dir_path}' does not exist or is not a directory.")
        exit(1)

    config = UploadConfig(
        directory=dir_path,
        pattern=file_pattern,
        message=custom_message,
        bucket=bucket_name,
        mode=mode,
        prefix=prefix
    )

    uploader = Uploader(config=config)
    uploader.upload()
    uploader.write_summary()
    uploader.write_outputs()


if __name__ == "__main__":
    main()
