import sys

from equicast_ingestion.processor import StockProcessor

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python stock_processor.py <ticker_file>")
        print("Example: python stock_processor.py chunk_1.json")
        sys.exit(1)

    processor = StockProcessor(ticker_file=sys.argv[1])
    processor.process()
