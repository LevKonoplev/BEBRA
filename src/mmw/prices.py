"""Fetch and store asset prices using yfinance."""
from __future__ import annotations

from typing import List

import logging

import pandas as pd
import yfinance as yf
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import sessionmaker

from .config import WATCHLIST_TICKERS
from .db import Asset, Price, engine


def fetch_prices_yf(tickers: List[str], start: str | None = None, end: str | None = None) -> pd.DataFrame:
    """Download prices from Yahoo Finance via yfinance and return a tidy DataFrame."""

    try:
        data = yf.download(
            tickers,
            start=start,
            end=end,
            group_by="ticker",
            auto_adjust=True,
            threads=True,
        )
    except Exception as exc:
        logging.warning("Failed to download prices: %s", exc)
        return pd.DataFrame(
            columns=["ticker", "date", "open", "high", "low", "close", "volume"]
        )

    if data.empty:
        return pd.DataFrame(
            columns=["ticker", "date", "open", "high", "low", "close", "volume"]
        )

    if isinstance(data.columns, pd.MultiIndex):
        df = data.stack(level=0).rename_axis(["date", "ticker"]).reset_index()
    else:
        df = data.reset_index()
        df["ticker"] = tickers[0]

    df = df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    df = df[["ticker", "date", "open", "high", "low", "close", "volume"]]
    return df


def upsert_prices(df: pd.DataFrame, engine) -> None:
    """Insert or update prices into the database by unique (ticker, date)."""

    Session = sessionmaker(bind=engine, future=True)
    with Session.begin() as session:
        for ticker, group in df.groupby("ticker"):
            asset = session.execute(select(Asset).where(Asset.ticker == ticker)).scalar_one_or_none()
            if asset is None:
                asset = Asset(ticker=ticker)
                session.add(asset)
                session.flush()
            for row in group.itertuples(index=False):
                stmt = sqlite_insert(Price).values(
                    asset_id=asset.id,
                    date=pd.to_datetime(row.date).to_pydatetime(),
                    open=row.open,
                    high=row.high,
                    low=row.low,
                    close=row.close,
                    volume=row.volume,
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=[Price.__table__.c.asset_id, Price.__table__.c.date],
                    set_={
                        "open": row.open,
                        "high": row.high,
                        "low": row.low,
                        "close": row.close,
                        "volume": row.volume,
                    },
                )
                session.execute(stmt)


def refresh_watchlist_prices() -> None:
    """Fetch last 3 years of prices for watchlist tickers and store them in the DB."""

    start = (pd.Timestamp.utcnow() - pd.DateOffset(years=3)).strftime("%Y-%m-%d")
    df = fetch_prices_yf(WATCHLIST_TICKERS, start=start)
    upsert_prices(df, engine)
    print(f"Inserted/updated {len(df)} rows for {df['ticker'].nunique()} tickers since {start}")


def main(since: str | None = None) -> None:
    """CLI entry point for fetching prices."""

    start = since if since else (pd.Timestamp.utcnow() - pd.DateOffset(years=3)).strftime("%Y-%m-%d")
    df = fetch_prices_yf(WATCHLIST_TICKERS, start=start)
    upsert_prices(df, engine)
    print(f"Processed {len(df)} rows for {df['ticker'].nunique()} tickers since {start}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch Yahoo Finance prices for watchlist tickers")
    parser.add_argument("--since", dest="since", help="Start date YYYY-MM-DD", default=None)
    args = parser.parse_args()
    main(args.since)
