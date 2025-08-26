from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import select

from .config import DATA_DIR
from .db import SessionLocal, Index, IndexPoint

logger = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (compatible; MMWBot/0.1)"
REQUEST_TIMEOUT = 10
REQUEST_DELAY = 1.0


def _upsert_df(df: pd.DataFrame) -> None:
    """Upsert rows into ``index_points`` from a tidy DataFrame."""
    if df is None or df.empty:
        return
    required = {"date", "index_code", "value"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise ValueError(f"missing columns: {missing}")

    df = df.dropna(subset=["date", "index_code", "value"])
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)

    with SessionLocal.begin() as session:
        for row in df.to_dict("records"):
            index = session.execute(
                select(Index).where(Index.code == row["index_code"])
            ).scalar_one_or_none()
            if not index:
                index = Index(code=row["index_code"])
                session.add(index)
                session.flush()

            point = session.execute(
                select(IndexPoint).where(
                    IndexPoint.index_id == index.id, IndexPoint.date == row["date"]
                )
            ).scalar_one_or_none()
            if point:
                point.value = float(row["value"])
            else:
                session.add(
                    IndexPoint(
                        index_id=index.id,
                        date=row["date"],
                        value=float(row["value"]),
                    )
                )


def import_indices_from_csv(path: Path = DATA_DIR / "indices_manual.csv") -> pd.DataFrame:
    """Read a CSV of indices and upsert into the database."""
    df = pd.read_csv(path)
    expected = {"date", "index_code", "value", "source"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"missing columns: {missing}")

    _upsert_df(df)
    return df


def _fetch(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        time.sleep(REQUEST_DELAY)
        return resp.text
    except Exception as exc:  # pragma: no cover - network errors
        logger.warning("fetch failed for %s: %s", url, exc)
        return None


def scrape_harpex_current() -> Optional[pd.DataFrame]:
    if os.getenv("MMW_SKIP_HARPEX"):
        logger.info("HARPEX scraping skipped via env")
        return None
    url = "https://harpex.harperpetersen.com/"
    html = _fetch(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    try:
        value_tag = soup.find(class_="harpex-value")
        date_tag = soup.find(class_="harpex-date")
        value = float(value_tag.get_text(strip=True).replace(",", ""))
        date = pd.to_datetime(date_tag.get_text(strip=True))
        return pd.DataFrame(
            [{"date": date, "index_code": "HARPEX", "value": value, "source": url}]
        )
    except Exception as exc:  # pragma: no cover - parser fragile
        logger.warning("HARPEX parsing failed: %s", exc)
        return None


def scrape_wci_latest() -> Optional[pd.DataFrame]:
    if os.getenv("MMW_SKIP_WCI"):
        logger.info("WCI scraping skipped via env")
        return None
    url = (
        "https://www.drewry.co.uk/supply-chain-advisors/supply-chain-expertise/world-container-index"
    )
    html = _fetch(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    try:
        value_tag = soup.find("span", class_="price")
        date_tag = soup.find("span", class_="date")
        value = float(value_tag.get_text(strip=True).replace(",", ""))
        date = pd.to_datetime(date_tag.get_text(strip=True))
        return pd.DataFrame(
            [{"date": date, "index_code": "WCI", "value": value, "source": url}]
        )
    except Exception as exc:
        logger.warning("WCI parsing failed: %s", exc)
        return None


def scrape_scfi_latest() -> Optional[pd.DataFrame]:
    if os.getenv("MMW_SKIP_SCFI"):
        logger.info("SCFI scraping skipped via env")
        return None
    url = "https://en.sse.net.cn/indices/"
    html = _fetch(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    try:
        table = soup.find("table")
        row = table.find("tr")
        cells = row.find_all("td")
        value = float(cells[1].get_text(strip=True).replace(",", ""))
        date = pd.to_datetime(cells[0].get_text(strip=True))
        return pd.DataFrame(
            [{"date": date, "index_code": "SCFI", "value": value, "source": url}]
        )
    except Exception as exc:
        logger.warning("SCFI parsing failed: %s", exc)
        return None


def scrape_fbx_latest() -> Optional[pd.DataFrame]:
    if os.getenv("MMW_SKIP_FBX"):
        logger.info("FBX scraping skipped via env")
        return None
    url = "https://fbx.freightos.com/"
    html = _fetch(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    try:
        value_tag = soup.find("div", class_="current-price")
        date_tag = soup.find("div", class_="current-date")
        value = float(value_tag.get_text(strip=True).replace(",", ""))
        date = pd.to_datetime(date_tag.get_text(strip=True))
        return pd.DataFrame(
            [{"date": date, "index_code": "FBX", "value": value, "source": url}]
        )
    except Exception as exc:
        logger.warning("FBX parsing failed: %s", exc)
        return None


def refresh_indices() -> None:
    """Refresh index points from CSV and optional scrapers."""
    total = 0
    csv_df = import_indices_from_csv()
    total += len(csv_df)
    for scraper in (
        scrape_harpex_current,
        scrape_wci_latest,
        scrape_scfi_latest,
        scrape_fbx_latest,
    ):
        df = scraper()
        if df is not None and not df.empty:
            _upsert_df(df)
            total += len(df)
    logger.info("updated %d index points", total)
