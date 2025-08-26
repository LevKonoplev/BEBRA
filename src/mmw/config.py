"""Configuration constants for Maritime Market Watch."""

from pathlib import Path

WATCHLIST_TICKERS = [
    "ZIM",
    "MATX",
    "SBLK",
    "GOGL",
    "GNK",
    "FRO",
    "DHT",
    "EURN",
    "GSL",
    "DAC",
    "CMRE",
    "TRMD",
    "BDRY",
    "BOAT",
]

INDEX_CODES = [
    "BDIY",
    "HARPEX",
    "SCFI",
    "WCI",
    "FBX",
]

RSS_FEEDS = [
    "https://example.com/feed1",
    "https://example.com/feed2",
]

DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "mmw.sqlite"
DOCS_DIR = Path("docs")
