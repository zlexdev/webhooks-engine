"""Webhook lifecycle + dead-letter meta-events.

All ``use_outbox=False`` (direct-dispatch) so they never re-enter the
outbox — otherwise a dead-letter event for a failing subscription
would itself accumulate retries forever.

Scope is ``GLOBAL`` with ``tenant_field="owner_id"`` so admins can
audit per-user behaviour without exposing these events to the
seller's own subscriptions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from asyncbus import PostEvent

from webhook_engine.enums import WebhookScope
from webhook_engine.meta import WebhookMeta, webhook_event

__all__ = [
    "WebhookDeliveryExhausted",
    "WebhookSubscriptionAutoSuspended",
    "WebhookSubscriptionCreated",
    "WebhookSubscriptionDeleted",
    "WebhookSubscriptionPaused",
    "WebhookSubscriptionResumed",
    "WebhookSubscriptionSecretRotated",
    "WebhookSubscriptionUpdated",
]


@webhook_event(
    WebhookMeta(
        name="webhook.delivery.exhausted",
        description="Webhook delivery reached max attempts; moved to DEAD_LETTERED.",
        scopes=(WebhookScope.GLOBAL,),
        tenant_field=None,
    )
)
@dataclass(frozen=True, slots=True)
class WebhookDeliveryExhausted(PostEvent):
    delivery_id: str = ""
    subscription_id: str = ""
    owner_id: str = ""
    source_event: str = ""
    attempts: int = 0
    last_error: str = ""

    use_outbox: ClassVar[bool] = False


@webhook_event(
    WebhookMeta(
        name="webhook.subscription.created",
        description="New webhook subscription was created.",
        scopes=(WebhookScope.GLOBAL,),
        tenant_field="owner_id",
    )
)
@dataclass(frozen=True, slots=True)
class WebhookSubscriptionCreated(PostEvent):
    subscription_id: str = ""
    owner_id: str = ""
    url_host: str = ""
    event_names: tuple[str, ...] = ()
    scope: str = ""

    use_outbox: ClassVar[bool] = False


@webhook_event(
    WebhookMeta(
        name="webhook.subscription.updated",
        description="Webhook subscription config was updated.",
        scopes=(WebhookScope.GLOBAL,),
        tenant_field="owner_id",
    )
)
@dataclass(frozen=True, slots=True)
class WebhookSubscriptionUpdated(PostEvent):
    subscription_id: str = ""
    owner_id: str = ""
    changed_fields: tuple[str, ...] = ()

    use_outbox: ClassVar[bool] = False


@webhook_event(
    WebhookMeta(
        name="webhook.subscription.paused",
        description="Subscription paused (by user or auto-suspend).",
        scopes=(WebhookScope.GLOBAL,),
        tenant_field="owner_id",
    )
)
@dataclass(frozen=True, slots=True)
class WebhookSubscriptionPaused(PostEvent):
    subscription_id: str = ""
    owner_id: str = ""
    reason: str = "user_action"

    use_outbox: ClassVar[bool] = False


@webhook_event(
    WebhookMeta(
        name="webhook.subscription.resumed",
        description="Subscription resumed.",
        scopes=(WebhookScope.GLOBAL,),
        tenant_field="owner_id",
    )
)
@dataclass(frozen=True, slots=True)
class WebhookSubscriptionResumed(PostEvent):
    subscription_id: str = ""
    owner_id: str = ""

    use_outbox: ClassVar[bool] = False


@webhook_event(
    WebhookMeta(
        name="webhook.subscription.deleted",
        description="Subscription soft-deleted.",
        scopes=(WebhookScope.GLOBAL,),
        tenant_field="owner_id",
    )
)
@dataclass(frozen=True, slots=True)
class WebhookSubscriptionDeleted(PostEvent):
    subscription_id: str = ""
    owner_id: str = ""

    use_outbox: ClassVar[bool] = False


@webhook_event(
    WebhookMeta(
        name="webhook.subscription.secret_rotated",
        description="Subscription secret rotated.",
        scopes=(WebhookScope.GLOBAL,),
        tenant_field="owner_id",
    )
)
@dataclass(frozen=True, slots=True)
class WebhookSubscriptionSecretRotated(PostEvent):
    subscription_id: str = ""
    owner_id: str = ""

    use_outbox: ClassVar[bool] = False


@webhook_event(
    WebhookMeta(
        name="webhook.subscription.auto_suspended",
        description=(
            "Subscription auto-suspended by ReResolveWorker "
            "(host resolved to a blocked IP or became unresolvable)."
        ),
        scopes=(WebhookScope.GLOBAL,),
        tenant_field="owner_id",
    )
)
@dataclass(frozen=True, slots=True)
class WebhookSubscriptionAutoSuspended(PostEvent):
    subscription_id: str = ""
    owner_id: str = ""
    url_host: str = ""
    reason: str = ""

    use_outbox: ClassVar[bool] = False
