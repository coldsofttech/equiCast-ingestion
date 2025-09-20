import argparse

from equicast_ingestion.processor import FxProcessor


def main():
    parser = argparse.ArgumentParser(description="Process FX: 'fx_prices.parquet'")
    parser.add_argument("--file", required=True, help="FX Input File Path")
    parser.add_argument("--full-run", action="store_true", help="Full Load FX Input")
    args = parser.parse_args()

    processor = FxProcessor(args.file, full_run=args.full_run)
    processor.process_prices()


if __name__ == "__main__":
    main()
