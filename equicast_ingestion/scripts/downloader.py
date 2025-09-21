import argparse

from equicast_ingestion.helpers import Downloader


def main():
    parser = argparse.ArgumentParser(description="S3: Download Files")
    parser.add_argument("--mode", required=True, choices=["fx"], help="Download Mode")
    args = parser.parse_args()

    downloader = Downloader()
    temp_dir = downloader.download(args.mode)
    print(temp_dir)


if __name__ == "__main__":
    main()
