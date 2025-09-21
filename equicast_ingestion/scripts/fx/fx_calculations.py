import argparse

from equicast_ingestion.processor import FxProcessor


def main():
    parser = argparse.ArgumentParser(description="Process FX: 'fx_calculations.parquet'")
    parser.add_argument("--file", required=True, help="FX Input File Path")
    parser.add_argument("--max-workers", type=int, default=20, help="Max number of workers")
    parser.add_argument("--max-retries", type=int, default=5, help="Max number of retries")
    args = parser.parse_args()

    processor = FxProcessor(args.file, max_workers=args.max_workers, max_retries=args.max_retries)
    temp_dir = processor.process_calculations()
    print(temp_dir)


if __name__ == "__main__":
    main()
