"""Link news articles to assets and indices."""

from __future__ import annotations

import logging
from typing import Dict, List

from sqlalchemy import select, delete
from sqlalchemy.orm import sessionmaker

from .db import Entity, Link, News, engine, init_db

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# mappings
# ---------------------------------------------------------------------------

MAPPINGS: Dict[str, List[str]] = {
    "MAERSK": ["MAERSK-B.CO"],
    "ZIM": ["ZIM"],
    "HAPAG": ["HLAG.DE", "HLAG.F"],
    "COSCO": ["1919.HK"],
}

INDEX_KEYWORDS: Dict[str, List[str]] = {
    "SCFI": ["SCFI"],
    "HARPEX": ["HARPEX"],
    "WCI": ["Drewry", "World Container Index"],
    "FBX": ["FBX", "Freightos"],
}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _match_score(text: str, keywords: List[str]) -> int:
    """Return score based on keyword occurrences in text."""

    text_l = text.lower()
    return sum(1 for kw in keywords if kw.lower() in text_l)


# ---------------------------------------------------------------------------
# main logic
# ---------------------------------------------------------------------------

def link_news(engine=engine) -> None:
    """Link news articles to asset tickers or index codes."""

    Session = sessionmaker(bind=engine, future=True)
    with Session.begin() as session:
        news_items = session.execute(select(News)).scalars().all()
        for item in news_items:
            entity_vals = session.execute(
                select(Entity.value).where(Entity.news_id == item.id)
            ).scalars().all()
            full_text = " ".join(
                [item.title or "", item.summary or "", " ".join(entity_vals)]
            )

            # remove previous links for this news item
            session.execute(delete(Link).where(Link.news_id == item.id))

            # asset mappings
            for name, tickers in MAPPINGS.items():
                keywords = [name] + tickers
                score = _match_score(full_text, keywords)
                if score:
                    for ticker in tickers:
                        session.add(
                            Link(
                                news_id=item.id,
                                asset_ticker=ticker,
                                index_code=None,
                                score=float(score),
                            )
                        )

            # index keywords
            for code, keywords in INDEX_KEYWORDS.items():
                score = _match_score(full_text, keywords)
                if score:
                    session.add(
                        Link(
                            news_id=item.id,
                            asset_ticker=None,
                            index_code=code,
                            score=float(score),
                        )
                    )


if __name__ == "__main__":  # pragma: no cover - CLI
    init_db()
    link_news(engine)
