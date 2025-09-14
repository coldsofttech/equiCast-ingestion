import argparse

from equicast_ingestion.processor import StockProcessor


def main():
    parser = argparse.ArgumentParser(description="Process stock tickers")
    parser.add_argument("--ticker-file", required=True, help="Ticker file path")
    args = parser.parse_args()

    processor = StockProcessor(ticker_file=args.ticker_file)
    processor.process()


if __name__ == "__main__":
    main()
