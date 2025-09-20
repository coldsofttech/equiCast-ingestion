import argparse

from equicast_ingestion.helpers import Downloader


def main():
    parser = argparse.ArgumentParser(description="S3: Download Files")
    parser.add_argument("--mode", required=True, choices=["fx"], help="Download Mode")
    args = parser.parse_args()

    downloader = Downloader()
    if args.mode == "fx":
        downloader.download("fx")


if __name__ == "__main__":
    main()
