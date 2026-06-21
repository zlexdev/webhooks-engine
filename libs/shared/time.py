"""UTC clock shim — mirrors the monorepo's libs.shared.time API."""

from __future__ import annotations

from datetime import UTC, datetime

__all__ = ["now_utc"]


def now_utc() -> datetime:
    return datetime.now(UTC)
