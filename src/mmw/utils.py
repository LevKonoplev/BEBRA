"""Utility helpers for Maritime Market Watch."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


def ensure_dirs(*paths: Path) -> None:
    """Ensure that each provided directory exists."""

    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)


def utc_now() -> datetime:
    """Return current UTC time."""

    return datetime.now(timezone.utc)
