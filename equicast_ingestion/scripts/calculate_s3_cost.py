import argparse
import fnmatch
from pathlib import Path

from equicast_awsutils.cost import S3


def main():
    parser = argparse.ArgumentParser(description="S3: Calculate Cost")
    parser.add_argument("--directory-path", required=True, help="Directory Path")
    parser.add_argument("--file-pattern", required=True, help="File Pattern. Example: *.json, *.parquet")
    args = parser.parse_args()

    dir_path = Path(args.directory_path)
    if not dir_path.exists() or not dir_path.is_dir():
        print(f"Error: Path '{dir_path}' does not exist or is not a directory.")
        exit(1)

    all_files = [f for f in dir_path.rglob("*") if f.is_file()]
    artifacts = [f for f in all_files if fnmatch.fnmatch(f.name, args.file_pattern)]

    s3_obj = S3(files=artifacts, threshold=1.0, github_output=True, github_summary=True)
    s3_obj.calculate()


if __name__ == "__main__":
    main()
