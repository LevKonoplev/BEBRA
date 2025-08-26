"""Import and scrape shipping indices."""

from __future__ import annotations

import logging
import os
import re
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from .config import DATA_DIR
from .db import Index, IndexPoint, engine

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MMWBot/0.1; +https://example.com)"
}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _upsert_df(df: pd.DataFrame) -> None:
    """Upsert index points into the database."""

    Session = sessionmaker(bind=engine, future=True)
    with Session.begin() as session:
        for row in df.itertuples(index=False):
            date = pd.to_datetime(row.date).to_pydatetime()
            index_code = str(row.index_code)
            value = float(row.value)

            idx = (
                session.execute(select(Index).where(Index.code == index_code))
                .scalar_one_or_none()
            )
            if idx is None:
                idx = Index(code=index_code)
                session.add(idx)
                session.flush()

            existing = (
                session.execute(
                    select(IndexPoint).where(
                        IndexPoint.index_id == idx.id, IndexPoint.date == date
                    )
                ).scalar_one_or_none()
            )
            if existing is None:
                session.add(IndexPoint(index_id=idx.id, date=date, value=value))
            else:
                existing.value = value


def _request_soup(url: str, delay: float = 1.0) -> Optional[BeautifulSoup]:
    """Fetch a URL and return BeautifulSoup or None on failure."""

    time.sleep(delay)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as exc:  # pragma: no cover - network
        logger.warning("request failed for %s: %s", url, exc)
        return None


def _disabled(name: str) -> bool:
    """Check if scraper is disabled via environment variable."""

    return os.getenv(f"MMW_SKIP_{name.upper()}") is not None


# ---------------------------------------------------------------------------
# CSV import
# ---------------------------------------------------------------------------

def import_indices_from_csv(path: Path = DATA_DIR / "indices_manual.csv") -> None:
    """Import index values from a CSV file and upsert them into the DB."""

    path = Path(path)
    if not path.exists():
        logger.info("CSV %s not found, skipping", path)
        return

    df = pd.read_csv(path)
    expected = {"date", "index_code", "value", "source"}
    missing = expected - set(df.columns)
    if missing:
        logger.error("CSV missing columns: %s", ", ".join(sorted(missing)))
        return

    df = df.dropna(subset=["date", "index_code", "value", "source"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["source"] = df["source"].astype(str)
    df = df.dropna(subset=["date", "value"])

    if df.empty:
        logger.info("No valid rows in %s", path)
        return

    _upsert_df(df)


# ---------------------------------------------------------------------------
# Scrapers
# ---------------------------------------------------------------------------

def scrape_harpex_current() -> pd.DataFrame | None:  # pragma: no cover - network
    """Scrape current HARPEX value."""

    if _disabled("harpex"):
        logger.info("HARPEX scraper disabled via env")
        return None

    soup = _request_soup("https://www.harperpetersen.com/en/harpex")
    if not soup:
        return None

    try:
        value_tag = soup.find(class_=re.compile("harpex"))
        date_tag = soup.find(text=re.compile(r"\d{4}-\d{2}-\d{2}"))
        if not value_tag:
            logger.warning("HARPEX value element not found")
            return None
        value = float(value_tag.get_text(strip=True).replace(",", ""))
        date = (
            pd.to_datetime(date_tag, errors="coerce")
            if date_tag
            else pd.Timestamp.utcnow().normalize()
        )
        return pd.DataFrame(
            [
                {
                    "date": date,
                    "index_code": "HARPEX",
                    "value": value,
                    "source": "harperpetersen.com",
                }
            ]
        )
    except Exception as exc:
        logger.warning("HARPEX parsing failed: %s", exc)
        return None


def scrape_wci_latest() -> pd.DataFrame | None:  # pragma: no cover - network
    """Scrape latest WCI value."""

    if _disabled("wci"):
        logger.info("WCI scraper disabled via env")
        return None

    soup = _request_soup(
        "https://www.drewry.co.uk/supply-chain-expertise/world-container-index-drewry"
    )
    if not soup:
        return None

    try:
        text = soup.get_text(" ", strip=True)
        match = re.search(r"([0-9]+[\.,][0-9]+)", text)
        if not match:
            logger.warning("WCI value not found")
            return None
        value = float(match.group(1).replace(",", ""))
        date = pd.Timestamp.utcnow().normalize()
        return pd.DataFrame(
            [
                {
                    "date": date,
                    "index_code": "WCI",
                    "value": value,
                    "source": "drewry.co.uk",
                }
            ]
        )
    except Exception as exc:
        logger.warning("WCI parsing failed: %s", exc)
        return None


def scrape_scfi_latest() -> pd.DataFrame | None:  # pragma: no cover - network
    """Scrape latest SCFI value."""

    if _disabled("scfi"):
        logger.info("SCFI scraper disabled via env")
        return None

    soup = _request_soup("https://en.sse.net.cn/indices/" )
    if not soup:
        return None

    try:
        text = soup.get_text(" ", strip=True)
        match = re.search(r"SCFI[^0-9]*([0-9]+[\.,][0-9]+)", text)
        if not match:
            logger.warning("SCFI value not found")
            return None
        value = float(match.group(1).replace(",", ""))
        date = pd.Timestamp.utcnow().normalize()
        return pd.DataFrame(
            [
                {
                    "date": date,
                    "index_code": "SCFI",
                    "value": value,
                    "source": "sse.net.cn",
                }
            ]
        )
    except Exception as exc:
        logger.warning("SCFI parsing failed: %s", exc)
        return None


def scrape_fbx_latest() -> pd.DataFrame | None:  # pragma: no cover - network
    """Scrape latest FBX value."""

    if _disabled("fbx"):
        logger.info("FBX scraper disabled via env")
        return None

    soup = _request_soup("https://fbx.freightos.com")
    if not soup:
        return None

    try:
        value_tag = soup.find(text=re.compile(r"FBX[\s:]*([0-9,\.]+)"))
        match = (
            re.search(r"([0-9]+[\.,][0-9]+)", value_tag) if value_tag else None
        )
        if not match:
            logger.warning("FBX value not found")
            return None
        value = float(match.group(1).replace(",", ""))
        date = pd.Timestamp.utcnow().normalize()
        return pd.DataFrame(
            [
                {
                    "date": date,
                    "index_code": "FBX",
                    "value": value,
                    "source": "fbx.freightos.com",
                }
            ]
        )
    except Exception as exc:
        logger.warning("FBX parsing failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# orchestration
# ---------------------------------------------------------------------------

def refresh_indices() -> None:
    """Import manual CSV first, then attempt to scrape latest indices."""

    import_indices_from_csv()

    for scraper in (
        scrape_harpex_current,
        scrape_wci_latest,
        scrape_scfi_latest,
        scrape_fbx_latest,
    ):
        try:
            df = scraper()
            if df is not None and not df.empty:
                _upsert_df(df)
        except Exception as exc:  # pragma: no cover - network
            logger.warning("scraper %s failed: %s", scraper.__name__, exc)

