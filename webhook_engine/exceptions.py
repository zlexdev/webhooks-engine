"""Webhook exception hierarchy.

All errors raised by ``libs/webhooks`` inherit from :class:`WebhookError`
and stay inside the package — registry, dispatcher, and ssrf_guard
raise and catch them internally. None bubble to HTTP handlers, so there
is no entry in ``app/api/middleware/error_handler.py::_DOMAIN_ERROR_MAP``;
the ``status_code`` / ``error_code`` attributes are metadata for
structured logging and future API surface mapping, not active routing.
"""

from __future__ import annotations

from typing import Any

from webhook_engine.enums import InvalidTargetReason

__all__ = [
    "DeliveryInFlight",
    "DeliveryNotFound",
    "EventLimitExceeded",
    "InvalidWebhookTarget",
    "SignatureVerificationError",
    "SubscriptionLimitExceeded",
    "SubscriptionNotFound",
    "UnknownEventName",
    "WebhookError",
    "WebhookEventCollision",
    "WebhookMetaMissing",
    "WebhookPermissionDenied",
    "WebhookStartupError",
    "WebhookTenantFieldMissing",
]


class WebhookError(Exception):
    """Base for every error raised inside ``libs/webhooks``.

    Accepts arbitrary keyword context that gets stashed on the instance
    so structured loggers can pull it out without re-parsing the message.
    """

    status_code: int = 500
    error_code: str = "WEBHOOK_ERROR"

    def __init__(self, message: str = "", **context: Any) -> None:
        super().__init__(message)
        for key, value in context.items():
            setattr(self, key, value)


class WebhookStartupError(WebhookError):
    """Raised during registry discovery — app should fail to boot."""

    status_code = 500
    error_code = "WEBHOOK_STARTUP_ERROR"


class WebhookMetaMissing(WebhookStartupError):
    error_code = "WEBHOOK_META_MISSING"


class WebhookEventCollision(WebhookStartupError):
    error_code = "WEBHOOK_EVENT_COLLISION"

    def __init__(self, name: str, a: str, b: str) -> None:
        super().__init__(f"{name}: {a} vs {b}", name=name, classes=(a, b))


class WebhookTenantFieldMissing(WebhookStartupError):
    error_code = "WEBHOOK_TENANT_FIELD_MISSING"

    def __init__(self, event_name: str, event_cls: str, tenant_field: str) -> None:
        super().__init__(
            f"{event_name}: event class {event_cls} has no attribute {tenant_field!r}",
            event_name=event_name,
            event_cls=event_cls,
            tenant_field=tenant_field,
        )


class UnknownEventName(WebhookError):
    status_code = 400
    error_code = "WEBHOOK_UNKNOWN_EVENT"


class InvalidWebhookTarget(WebhookError):
    status_code = 400
    error_code = "WEBHOOK_INVALID_TARGET"
    reason: InvalidTargetReason

    def __init__(self, reason: InvalidTargetReason, detail: str = "") -> None:
        super().__init__(detail or str(reason), reason=reason)


class WebhookPermissionDenied(WebhookError):
    status_code = 403
    error_code = "WEBHOOK_FORBIDDEN"


class SubscriptionLimitExceeded(WebhookError):
    status_code = 409
    error_code = "WEBHOOK_SUBSCRIPTION_LIMIT"


class EventLimitExceeded(WebhookError):
    status_code = 409
    error_code = "WEBHOOK_EVENT_LIMIT"


class SubscriptionNotFound(WebhookError):
    status_code = 404
    error_code = "WEBHOOK_SUBSCRIPTION_NOT_FOUND"


class DeliveryNotFound(WebhookError):
    status_code = 404
    error_code = "WEBHOOK_DELIVERY_NOT_FOUND"


class DeliveryInFlight(WebhookError):
    """Admin redeliver blocked: current delivery is IN_FLIGHT."""

    status_code = 409
    error_code = "WEBHOOK_DELIVERY_IN_FLIGHT"


class SignatureVerificationError(Exception):
    """Consumer-side helper raised by :meth:`HmacSigner.verify`."""
