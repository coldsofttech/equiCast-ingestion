import argparse

from equicast_ingestion.processor import FxProcessor


def main():
    parser = argparse.ArgumentParser(description="Process FX: 'fx_forecast.parquet'")
    parser.add_argument("--file", required=True, help="FX Input File Path")
    args = parser.parse_args()

    processor = FxProcessor(args.file)
    processor.process_forecast()


if __name__ == "__main__":
    main()
