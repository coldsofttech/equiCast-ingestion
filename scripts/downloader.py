from equicast_ingestion.downloader import Downloader

if __name__ == "__main__":
    downloader = Downloader()
    downloader.download("fx")
    downloader.download("stock")
