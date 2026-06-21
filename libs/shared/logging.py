"""Structlog shim — mirrors the monorepo's libs.shared.logging API."""

from __future__ import annotations

import logging
from typing import cast

import structlog

__all__ = ["get_logger"]


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    logging.basicConfig(
        format="%(message)s",
        level=logging.INFO,
    )
    structlog.configure(
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    return cast(structlog.stdlib.BoundLogger, structlog.get_logger(name))
