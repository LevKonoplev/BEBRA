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
    "https://www.hellenicshippingnews.com/feed/",
    "https://www.maritime-executive.com/rss",
    "https://gcaptain.com/feed/",
    # TODO: add more sources
]

import os

extra = os.getenv("MMW_EXTRA_FEEDS")
if extra:
    RSS_FEEDS.extend([u.strip() for u in extra.split(",") if u.strip()])

DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "mmw.sqlite"
DOCS_DIR = Path("docs")
