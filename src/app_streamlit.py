"""Simple Streamlit dashboard for Maritime Market Watch."""
from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import select

from mmw.db import engine, News


def main() -> None:
    """Show latest news from the SQLite database."""

    st.title("Maritime Market Watch")
    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                select(News.published_at, News.source, News.title, News.url)
                .order_by(News.published_at.desc())
                .limit(50),
                conn,
            )
    except Exception:
        df = pd.DataFrame(columns=["published_at", "source", "title", "url"])
    st.dataframe(df)


if __name__ == "__main__":  # pragma: no cover - streamlit
    main()
