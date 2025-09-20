import argparse

from equicast_ingestion.helpers import Splitter


def main():
    parser = argparse.ArgumentParser(description="Splitter: Tickers into Chunks")
    parser.add_argument("--file", required=True, help="Tickers Input File Path")
    parser.add_argument("--chunk-size", type=int, required=True, help="Chunk size")
    parser.add_argument("--mode", required=True, choices=["fx", "stock"], help="Splitter Mode")
    args = parser.parse_args()

    splitter = Splitter(mode=args.mode, filepath=args.file, pref_chunk_size=args.chunk_size)
    splitter.split()


if __name__ == "__main__":
    main()
