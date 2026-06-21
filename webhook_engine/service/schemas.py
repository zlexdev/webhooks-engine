"""Shared response envelopes for the HTTP surface.

Only truly cross-route DTOs live here (per the backend convention — feature DTOs
stay next to their handler). Currently: the offset-paginated :class:`Page`.
"""

from __future__ import annotations

from pydantic import BaseModel

__all__ = ["Page"]


class Page[T](BaseModel):
    """Offset page returned by every collection endpoint — never a bare list.

    Cursor pagination is intentionally omitted: the collections here (subs per
    owner, deliveries per subscription) are small and bounded, so offset is
    sufficient. Add a cursor only when a collection grows past a few thousand.
    """

    items: list[T]
    total: int
    limit: int
    offset: int
