"""Analytics helpers for Maritime Market Watch.

This module provides a small collection of analytical utilities built on top
of the project database.  The functions are intentionally lightweight and use
only pandas/SQLAlchemy to remain easy to test and extend.
"""

from __future__ import annotations

import json
from itertools import combinations
from typing import Iterable, Tuple

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from .config import WATCHLIST_TICKERS
from .db import Asset, Link, News, Price, Run


# ---------------------------------------------------------------------------
# Helpers

def _save_run(engine, name: str, data: pd.DataFrame) -> int:
    """Persist analysis result as a JSON payload in the ``runs`` table.

    The payload is stored in the ``status`` column as JSON text.  The function
    returns the created run identifier.
    """

    payload = {"name": name, "data": data.to_dict(orient="records")}
    Session = sessionmaker(bind=engine, future=True)
    with Session.begin() as session:
        run = Run(status=json.dumps(payload))
        session.add(run)
        session.flush()
        return run.id


# ---------------------------------------------------------------------------
# Core analytics

def compute_daily_returns(engine, tickers: Iterable[str]) -> pd.DataFrame:
    """Compute simple daily returns for the provided tickers.

    Parameters
    ----------
    engine:
        SQLAlchemy engine connected to the project database.
    tickers:
        Iterable of ticker symbols to compute returns for.

    Returns
    -------
    pandas.DataFrame
        Tidy data frame with columns ``date``, ``ticker`` and ``ret``.
    """

    if not tickers:
        return pd.DataFrame(columns=["date", "ticker", "ret"])

    q = (
        select(Asset.ticker, Price.date, Price.close)
        .join(Price, Asset.id == Price.asset_id)
        .where(Asset.ticker.in_(list(tickers)))
        .order_by(Asset.ticker, Price.date)
    )

    with engine.connect() as conn:
        df = pd.read_sql(q, conn)

    if df.empty:
        return pd.DataFrame(columns=["date", "ticker", "ret"])

    df["date"] = pd.to_datetime(df["date"])
    df.sort_values(["ticker", "date"], inplace=True)
    df["ret"] = df.groupby("ticker")["close"].pct_change()
    df = df.dropna(subset=["ret"])
    return df[["date", "ticker", "ret"]]


def news_intensity(engine) -> pd.DataFrame:
    """Aggregate news intensity by day.

    The function counts the number of news items published per day and uses the
    length of ``summary_ai`` as a crude proxy for sentiment (higher value
    roughly corresponds to longer/"more positive" summaries).
    """

    q = select(News.published_at, News.summary_ai)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)

    if df.empty:
        return pd.DataFrame(columns=["date", "news_count", "avg_sentiment"])

    df["date"] = pd.to_datetime(df["published_at"]).dt.date
    df["sent_len"] = df["summary_ai"].fillna(""").str.len()

    agg = (
        df.groupby("date").agg(
            news_count=("summary_ai", "size"),
            avg_sentiment=("sent_len", "mean"),
        )
    ).reset_index()
    return agg


def rolling_corr(ret_df: pd.DataFrame, window: int = 30) -> pd.DataFrame:
    """Compute rolling correlations for all ticker pairs.

    Parameters
    ----------
    ret_df:
        Data frame as returned by :func:`compute_daily_returns`.
    window:
        Rolling window size in days.

    Returns
    -------
    pandas.DataFrame
        Columns: ``date``, ``pair`` and ``corr`` where ``pair`` is a string
        ``"TICKER1-TICKER2"``.
    """

    if ret_df.empty:
        return pd.DataFrame(columns=["date", "pair", "corr"])

    wide = ret_df.pivot(index="date", columns="ticker", values="ret").sort_index()

    pairs = list(combinations(wide.columns, 2))
    frames = []
    for a, b in pairs:
        series = wide[a].rolling(window).corr(wide[b])
        frames.append(
            pd.DataFrame({"date": series.index, "pair": f"{a}-{b}", "corr": series.values})
        )

    if not frames:
        return pd.DataFrame(columns=["date", "pair", "corr"])

    return pd.concat(frames, ignore_index=True)


def event_study(
    engine, ticker: str, window: Tuple[int, int] = (-3, 3)
) -> pd.DataFrame:
    """Perform a simple event study for ``ticker`` around news events.

    For each day with at least one linked news item for ``ticker`` an event
    window is extracted from daily returns.  Abnormal returns are computed as
    the difference between the ticker return and the equal-weight mean of all
    watchlist tickers.  The function returns aggregated abnormal returns over
    all events.
    """

    # Compute returns for baseline and target ticker
    ret_df = compute_daily_returns(engine, WATCHLIST_TICKERS)
    if ret_df.empty:
        return pd.DataFrame(columns=["rel_day", "abret_mean", "abret_std", "n_events"])

    market = ret_df.groupby("date")["ret"].mean().rename("mkt_ret").reset_index()
    ticker_ret = ret_df[ret_df["ticker"] == ticker][["date", "ret"]].rename(
        columns={"ret": "ret_ticker"}
    )

    merged = ticker_ret.merge(market, on="date", how="left")
    merged["abret"] = merged["ret_ticker"] - merged["mkt_ret"]

    # Determine event dates from news links
    q = (
        select(News.published_at)
        .join(Link, Link.news_id == News.id)
        .where(Link.asset_ticker == ticker)
    )
    with engine.connect() as conn:
        news_df = pd.read_sql(q, conn)

    if news_df.empty:
        return pd.DataFrame(columns=["rel_day", "abret_mean", "abret_std", "n_events"])

    event_dates = pd.to_datetime(news_df["published_at"]).dt.date.unique()

    frames = []
    merged.set_index("date", inplace=True)
    for ed in event_dates:
        start = pd.to_datetime(ed) + pd.Timedelta(days=window[0])
        end = pd.to_datetime(ed) + pd.Timedelta(days=window[1])
        sub = merged.loc[start:end].copy()
        if sub.empty:
            continue
        sub["rel_day"] = (sub.index.date - ed).astype("timedelta64[D]").astype(int)
        frames.append(sub[["rel_day", "abret"]])

    if not frames:
        return pd.DataFrame(columns=["rel_day", "abret_mean", "abret_std", "n_events"])

    events = pd.concat(frames, ignore_index=True)
    agg = (
        events.groupby("rel_day")["abret"].agg([
            ("abret_mean", "mean"),
            ("abret_std", "std"),
            ("n_events", "count"),
        ])
    ).reset_index()

    _save_run(engine, f"event_study_{ticker}", agg)
    return agg


__all__ = [
    "compute_daily_returns",
    "news_intensity",
    "rolling_corr",
    "event_study",
]

