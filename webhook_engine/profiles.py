"""Delivery profiles: retry, body, signer and response cap per KIND of delivery.

A host with several kinds (a subscription webhook, a push to a plugin) builds the
registry once with `ProfileRegistry.complete(Kind, {...})`; a kind without a profile
fails at start, not at its first delivery. `ProfileRegistry.single()` is the 0.2 setup.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Self

from webhook_engine.body import BaseBodyEncoder, EnvelopeEncoder
from webhook_engine.config import WebhookRetryConfig
from webhook_engine.exceptions import IncompleteProfiles, UnknownProfile
from webhook_engine.protocols import SignerProtocol
from webhook_engine.types import DEFAULT_PROFILE, DeliveryRecord

__all__ = ["DefaultKind", "DeliveryProfile", "ProfileRegistry"]


class DefaultKind(StrEnum):
    DEFAULT = DEFAULT_PROFILE


@dataclass(frozen=True, slots=True, kw_only=True)
class DeliveryProfile:
    """`None` fields fall back to the dispatcher's `WebhookPolicyConfig` and signer."""

    retry: WebhookRetryConfig | None = None
    body: BaseBodyEncoder = field(default_factory=EnvelopeEncoder)
    signer: SignerProtocol | None = None
    max_response_bytes: int | None = None
    honour_retry_after: bool = True


class ProfileRegistry[K: StrEnum]:
    def __init__(self, profiles: Mapping[str, DeliveryProfile]) -> None:
        self._profiles = dict(profiles)

    @classmethod
    def complete(cls, kinds: type[K], profiles: Mapping[K, DeliveryProfile]) -> Self:
        foreign = [key for key in profiles if not isinstance(key, kinds)]
        if foreign:
            raise TypeError(f"profile keys must be {kinds.__name__} members, got {foreign!r}")
        missing = tuple(member.value for member in kinds if member not in profiles)
        if missing:
            raise IncompleteProfiles(missing)
        return cls({member.value: profile for member, profile in profiles.items()})

    @classmethod
    def single(cls, profile: DeliveryProfile | None = None) -> ProfileRegistry[DefaultKind]:
        return ProfileRegistry({DefaultKind.DEFAULT.value: profile or DeliveryProfile()})

    def of(self, record: DeliveryRecord) -> DeliveryProfile:
        found = self._profiles.get(record.profile)
        if found is None:
            raise UnknownProfile(record.profile)
        return found
