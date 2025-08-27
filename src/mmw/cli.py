"""Command-line interface for Maritime Market Watch."""
from __future__ import annotations

import webbrowser
from pathlib import Path

import click

from .analytics import compute_daily_returns, event_study, news_intensity
from .config import WATCHLIST_TICKERS
from .db import engine
from .indices import import_indices_from_csv, refresh_indices
from .linker import link_news
from .news import refresh_news
from .nlp import enrich_news
from .prices import refresh_watchlist_prices
from .report import build_site


@click.group()
def cli() -> None:
    """MMW command line utilities."""


@cli.command("refresh-all")
def refresh_all() -> None:
    """Refresh prices, indices and news."""

    refresh_watchlist_prices()
    refresh_indices()
    refresh_news()
    enrich_news()
    link_news()
    click.echo("Data refreshed")


@cli.command("import-indices")
@click.argument(
    "path",
    type=click.Path(path_type=Path),
    required=False,
    default=Path("data/indices_manual.csv"),
)
def import_indices_cmd(path: Path) -> None:
    """Import indices from a CSV file into the database."""

    import_indices_from_csv(path)
    click.echo(f"Imported indices from {path}")


@cli.command("build-site")
def build_site_cmd() -> None:
    """Build static report site into docs/."""

    build_site()
    click.echo("Site generated in docs/")


@cli.command("open-site")
def open_site() -> None:
    """Open docs/index.html in the default browser."""

    index_path = Path("docs/index.html").resolve()
    if not index_path.exists():
        raise click.ClickException("docs/index.html not found. Run 'mmw build-site' first.")
    webbrowser.open(index_path.as_uri())
    click.echo(f"Opened {index_path}")


@cli.group()
def analyze() -> None:
    """Run analytical helpers."""


@analyze.command("returns")
@click.argument("tickers", nargs=-1)
def analyze_returns(tickers: tuple[str, ...]) -> None:
    """Compute daily returns for TICKERS."""

    tickers = tickers or WATCHLIST_TICKERS
    df = compute_daily_returns(engine, tickers)
    if df.empty:
        click.echo("No data found")
    else:
        click.echo(df.to_csv(index=False))


@analyze.command("event-study")
@click.argument("ticker")
def analyze_event_study(ticker: str) -> None:
    """Run event study around news events for TICKER."""

    df = event_study(engine, ticker)
    if df.empty:
        click.echo("No data found")
    else:
        click.echo(df.to_csv(index=False))


@analyze.command("news-intensity")
def analyze_news_intensity() -> None:
    """Aggregate news intensity by day."""

    df = news_intensity(engine)
    if df.empty:
        click.echo("No data found")
    else:
        click.echo(df.to_csv(index=False))


if __name__ == "__main__":  # pragma: no cover - CLI entry
    cli()
