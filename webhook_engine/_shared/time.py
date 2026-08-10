"""UTC clock shim, private to the package. Not part of the public API."""

from __future__ import annotations

from datetime import UTC, datetime

__all__ = ["now_utc"]


def now_utc() -> datetime:
    return datetime.now(UTC)
