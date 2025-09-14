import argparse

from equicast_ingestion.splitter import Splitter


def main():
    parser = argparse.ArgumentParser(description="Split tickers into chunks")
    parser.add_argument("--file-path", required=True, help="File path")
    parser.add_argument("--chunk-size", type=int, required=True, help="Chunk size")
    parser.add_argument("--mode", required=True, choices=["fx", "stock"], help="Splitter mode")
    args = parser.parse_args()

    splitter = Splitter(mode=args.mode, filepath=args.file_path, pref_chunk_size=args.chunk_size)
    splitter.split()


if __name__ == "__main__":
    main()
