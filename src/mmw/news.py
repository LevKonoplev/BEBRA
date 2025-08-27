"""Fetch and store news from RSS feeds."""

from __future__ import annotations

import logging
from typing import List, Dict

import feedparser
import pandas as pd
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import sessionmaker

from .config import RSS_FEEDS
from .db import News, engine

logger = logging.getLogger(__name__)


def fetch_rss(feed_url: str) -> List[Dict]:
    """Fetch an RSS/Atom feed and return list of items."""

    try:
        parsed = feedparser.parse(feed_url)
    except Exception as exc:
        logger.error("Failed to parse RSS feed %s: %s", feed_url, exc)
        return []
    source = parsed.feed.get("title", feed_url)
    items: List[Dict] = []
    for entry in parsed.entries:
        items.append(
            {
                "source": source,
                "url": entry.get("link"),
                "title": entry.get("title", ""),
                "summary": entry.get("summary", ""),
                "published": entry.get("published"),
            }
        )
    return items


def normalize_news(items: List[Dict]) -> pd.DataFrame:
    """Normalize parsed feed items into a tidy DataFrame."""

    if not items:
        return pd.DataFrame(columns=["ts", "source", "url", "title", "summary"])
    df = pd.DataFrame(items)
    df["ts"] = pd.to_datetime(df["published"], errors="coerce")
    df = df[["ts", "source", "url", "title", "summary"]]
    return df


def upsert_news(df: pd.DataFrame) -> int:
    """Upsert news rows into the database by unique URL."""

    if df.empty:
        return 0
    urls = df["url"].tolist()
    Session = sessionmaker(bind=engine, future=True)
    with Session.begin() as session:
        existing = set(
            session.scalars(select(News.url).where(News.url.in_(urls))).all()
        )
        for row in df.itertuples(index=False):
            stmt = sqlite_insert(News).values(
                url=row.url,
                title=row.title,
                summary=row.summary,
                source=row.source,
                published_at=row.ts.to_pydatetime() if pd.notnull(row.ts) else None,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[News.__table__.c.url],
                set_={
                    "title": row.title,
                    "summary": row.summary,
                    "source": row.source,
                    "published_at": row.ts.to_pydatetime()
                    if pd.notnull(row.ts)
                    else None,
                },
            )
            session.execute(stmt)
    return len(set(urls) - existing)


def refresh_news() -> None:
    """Fetch, normalize and upsert news from all configured feeds."""

    total_new = 0
    for feed in RSS_FEEDS:
        items = fetch_rss(feed)
        if not items:
            logger.info("%s: no articles fetched", feed)
            continue
        df = normalize_news(items)
        df.drop_duplicates(subset="url", inplace=True)
        new_rows = upsert_news(df)
        logger.info("%s: %s new articles", feed, new_rows)
        total_new += new_rows
    logger.info("Total new articles: %s", total_new)


if __name__ == "__main__":  # pragma: no cover - manual run
    refresh_news()

