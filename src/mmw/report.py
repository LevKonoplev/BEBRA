"""Generate static HTML reports for Maritime Market Watch."""
from __future__ import annotations

from pathlib import Path
from typing import List
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import select

from .config import DOCS_DIR
from .db import Asset, Index, IndexPoint, News, Price
from .utils import ensure_dirs


def _write_html(fig: go.Figure, path: Path) -> None:
    """Write ``fig`` to ``path`` including plotly.js via CDN."""

    ensure_dirs(path.parent)
    fig.write_html(path, include_plotlyjs="cdn", full_html=True)


def build_price_charts(engine) -> List[Path]:
    """Build interactive price charts for each ticker."""

    q = (
        select(
            Asset.ticker,
            Price.date,
            Price.open,
            Price.high,
            Price.low,
            Price.close,
        )
        .join(Price, Asset.id == Price.asset_id)
        .order_by(Asset.ticker, Price.date)
    )
    try:
        with engine.connect() as conn:
            df = pd.read_sql(q, conn)
    except Exception:
        df = pd.DataFrame()

    if df.empty:
        return []

    df["date"] = pd.to_datetime(df["date"])
    out_paths: List[Path] = []
    for ticker, group in df.groupby("ticker"):
        group = group.sort_values("date")
        group["ma20"] = group["close"].rolling(20).mean()
        fig = go.Figure()
        fig.add_trace(
            go.Candlestick(
                x=group["date"],
                open=group["open"],
                high=group["high"],
                low=group["low"],
                close=group["close"],
                name="OHLC",
            )
        )
        fig.add_trace(
            go.Scatter(x=group["date"], y=group["ma20"], name="MA20")
        )
        fig.update_layout(title=ticker, xaxis_title="Date", yaxis_title="Price")
        out_path = DOCS_DIR / "assets" / f"price_{ticker}.html"
        _write_html(fig, out_path)
        out_paths.append(out_path)
    return out_paths


def build_index_charts(engine) -> List[Path]:
    """Build line charts for each shipping index."""

    q = (
        select(Index.code, IndexPoint.date, IndexPoint.value)
        .join(IndexPoint, Index.id == IndexPoint.index_id)
        .order_by(Index.code, IndexPoint.date)
    )
    try:
        with engine.connect() as conn:
            df = pd.read_sql(q, conn)
    except Exception:
        df = pd.DataFrame()

    if df.empty:
        return []

    df["date"] = pd.to_datetime(df["date"])
    out_paths: List[Path] = []
    for code, group in df.groupby("code"):
        group = group.sort_values("date")
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(x=group["date"], y=group["value"], mode="lines", name=code)
        )
        fig.update_layout(title=code, xaxis_title="Date", yaxis_title="Value")
        out_path = DOCS_DIR / "assets" / f"index_{code}.html"
        _write_html(fig, out_path)
        out_paths.append(out_path)
    return out_paths


def build_news_dash(engine) -> Path:
    """Build table of latest news items with summaries."""

    q = (
        select(
            News.published_at,
            News.source,
            News.title,
            News.summary_ai,
            News.summary,
            News.url,
        )
        .order_by(News.published_at.desc())
        .limit(50)
    )
    try:
        with engine.connect() as conn:
            df = pd.read_sql(q, conn)
    except Exception:
        df = pd.DataFrame()

    df["published_at"] = pd.to_datetime(df.get("published_at"))
    df["text"] = df.get("summary_ai", pd.Series(dtype=str)).fillna("")
    if "summary" in df:
        df.loc[df["text"] == "", "text"] = df.loc[df["text"] == "", "summary"].fillna("")

    if df.empty:
        table_html = "<p>No news available.</p>"
    else:
        df["link"] = df.apply(
            lambda r: f'<a href="{r.url}">{r.title}</a>' if r.url else r.title,
            axis=1,
        )
        display = df[["published_at", "source", "link", "text"]].rename(
            columns={"published_at": "date", "link": "title", "text": "summary"}
        )
        table_html = display.to_html(escape=False, index=False)

    html = (
        "<html><head><meta charset='utf-8'>"
        "<link rel='stylesheet' href='../style.css'></head><body>"
        "<h1>Latest news</h1>"
        f"{table_html}</body></html>"
    )
    out_path = DOCS_DIR / "assets" / "news.html"
    ensure_dirs(out_path.parent)
    out_path.write_text(html, encoding="utf-8")
    return out_path


def build_insights(engine) -> Path:
    """Build simple text insights about recent activity."""

    now = pd.Timestamp.utcnow().tz_localize(None)
    week_ago = now - pd.Timedelta(days=7)
    month_ago = now - pd.Timedelta(days=30)

    try:
        with engine.connect() as conn:
            news_df = pd.read_sql(select(News.published_at), conn)
    except Exception:
        news_df = pd.DataFrame(columns=["published_at"])
    try:
        with engine.connect() as conn:
            price_df = pd.read_sql(
                select(Asset.ticker, Price.date, Price.close)
                .join(Price, Asset.id == Price.asset_id)
                .where(Price.date >= month_ago)
                .order_by(Asset.ticker, Price.date),
                conn,
            )
    except Exception:
        price_df = pd.DataFrame(columns=["ticker", "date", "close"])

    news_df["published_at"] = pd.to_datetime(news_df.get("published_at"))
    count_week = int((news_df["published_at"] >= week_ago).sum())
    count_month = int((news_df["published_at"] >= month_ago).sum())

    top_week = "N/A"
    top_week_val = 0.0
    top_month = "N/A"
    top_month_val = 0.0

    if not price_df.empty:
        price_df["date"] = pd.to_datetime(price_df["date"])
        wide = (
            price_df.pivot_table(index="date", columns="ticker", values="close")
            .sort_index()
            .ffill()
        )
        if len(wide) > 1:
            last = wide.iloc[-1]
            week_start = wide[wide.index >= week_ago].iloc[0]
            month_start = wide.iloc[0]
            week_change = (last - week_start) / week_start
            month_change = (last - month_start) / month_start
            if not week_change.empty:
                top_week = week_change.abs().idxmax()
                top_week_val = week_change[top_week]
            if not month_change.empty:
                top_month = month_change.abs().idxmax()
                top_month_val = month_change[top_month]

    html = (
        "<html><head><meta charset='utf-8'>"
        "<link rel='stylesheet' href='../style.css'></head><body>"
        "<h1>Insights</h1>"
        "<div class='card'><h2>Week</h2>"
        f"<p>News: {count_week}</p>"
        f"<p>Top move: {top_week} {top_week_val:+.2%}</p></div>"
        "<div class='card'><h2>Month</h2>"
        f"<p>News: {count_month}</p>"
        f"<p>Top move: {top_month} {top_month_val:+.2%}</p></div>"
        "</body></html>"
    )
    out_path = DOCS_DIR / "assets" / "insights.html"
    ensure_dirs(out_path.parent)
    out_path.write_text(html, encoding="utf-8")
    return out_path


def build_site(engine = None) -> Path:
    """Generate full static site under ``docs`` directory."""

    from .db import engine as default_engine

    engine = engine or default_engine
    ensure_dirs(DOCS_DIR, DOCS_DIR / "assets")

    price_files = build_price_charts(engine)
    index_files = build_index_charts(engine)
    build_news_dash(engine)
    build_insights(engine)

    price_links = "".join(
        f'<li><a href="assets/{p.name}">{p.stem.replace("price_", "")}</a></li>'
        for p in price_files
    )
    index_links = "".join(
        f'<li><a href="assets/{p.name}">{p.stem.replace("index_", "")}</a></li>'
        for p in index_files
    )

    index_html = (
        "<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'>"
        "<title>Maritime Market Watch</title><link rel='stylesheet' href='style.css'>"
        "</head><body>"
        "<h1>Maritime Market Watch</h1>"
        "<h2>Prices</h2><ul>" + price_links + "</ul>"
        "<h2>Indices</h2><ul>" + index_links + "</ul>"
        "<h2>News</h2><p><a href='assets/news.html'>Latest news</a></p>"
        "<h2>Insights</h2><p><a href='assets/insights.html'>Insights</a></p>"
        "</body></html>"
    )
    index_path = DOCS_DIR / "index.html"
    index_path.write_text(index_html, encoding="utf-8")

    css = (
        "body{font-family:Arial, sans-serif;margin:2rem;}"
        "ul{list-style:none;padding:0;}li{margin:0.5rem 0;}"
        "a{text-decoration:none;color:#0366d6;}"
        ".card{border:1px solid #ddd;padding:1rem;margin:1rem 0;border-radius:4px;}"
        "table{border-collapse:collapse;width:100%;}"
        "th,td{border:1px solid #ddd;padding:4px;}"
    )
    (DOCS_DIR / "style.css").write_text(css, encoding="utf-8")

    return index_path


def main() -> None:
    """CLI entry point to build the report site."""

    build_site()
    print("Site generated in docs/")


if __name__ == "__main__":  # pragma: no cover - CLI entry
    main()
