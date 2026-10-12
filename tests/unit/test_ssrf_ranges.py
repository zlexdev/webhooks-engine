"""WE-T2: the default block set, a reason for every refusal, and no address in the text."""

from __future__ import annotations

import pytest

from webhook_engine import (
    InvalidTargetReason,
    InvalidWebhookTarget,
    UrlSafetyValidator,
    WebhookSecurityConfig,
)

GUARD = UrlSafetyValidator(WebhookSecurityConfig())


@pytest.mark.parametrize(
    ("ip", "reason"),
    [
        ("0.1.2.3", InvalidTargetReason.PRIVATE_IP),
        ("100.64.1.1", InvalidTargetReason.CGNAT),
        ("198.18.0.1", InvalidTargetReason.PRIVATE_IP),
        ("224.0.0.1", InvalidTargetReason.MULTICAST),
        ("240.0.0.1", InvalidTargetReason.RESERVED),
        ("255.255.255.255", InvalidTargetReason.RESERVED),
        ("::", InvalidTargetReason.RESERVED),
        ("::ffff:10.0.0.1", InvalidTargetReason.PRIVATE_IP),
        ("64:ff9b::a00:1", InvalidTargetReason.NAT64),
        ("2002:a00:1::1", InvalidTargetReason.SIX_TO_FOUR),
        ("ff02::1", InvalidTargetReason.MULTICAST),
        ("fd00::1", InvalidTargetReason.ULA),
        ("fe80::1", InvalidTargetReason.LINK_LOCAL),
    ],
)
def test_each_default_range_is_refused_with_a_reason_and_no_address(
    ip: str, reason: InvalidTargetReason
) -> None:
    with pytest.raises(InvalidWebhookTarget) as refused:
        GUARD.check_ip(ip)
    assert refused.value.reason is reason
    assert ip not in str(refused.value)


@pytest.mark.parametrize("ip", ["1.1.1.1", "::ffff:1.1.1.1", "2606:4700::1111"])
def test_public_addresses_still_pass(ip: str) -> None:
    GUARD.check_ip(ip)
