"""Command-line interface for Maritime Market Watch."""
from __future__ import annotations

import webbrowser
from pathlib import Path

import click

from .indices import refresh_indices
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


if __name__ == "__main__":  # pragma: no cover - CLI entry
    cli()
