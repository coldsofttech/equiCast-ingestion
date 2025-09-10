import sys

from equicast_ingestion.splitter import Splitter


def main():
    if len(sys.argv) < 4:
        print("Usage: python splitter.py <file_path> <chunk_size> <mode>")
        print("Modes: stock | fx")
        print(
            "Example 1 (Stock):   "
            "python splitter.py ./tickers.json 200 stock"
        )
        print(
            "Example 2 (FX):      "
            "python splitter.py ./fxpairs.json 100 fx"
        )
        sys.exit(1)

    filepath = sys.argv[1]
    chunk_size = sys.argv[2]
    mode = sys.argv[3]

    splitter = Splitter(
        filepath=filepath,
        pref_chunk_size=int(chunk_size),
        mode=mode
    )
    splitter.split()


if __name__ == "__main__":
    main()
