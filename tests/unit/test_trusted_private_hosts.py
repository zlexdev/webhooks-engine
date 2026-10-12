"""trusted_private_hosts: a named host may resolve into a blocked CIDR."""

from __future__ import annotations

import pytest

from webhook_engine import InvalidWebhookTarget, UrlSafetyValidator, WebhookSecurityConfig

COMPOSE_IP = "172.18.0.5"  # inside the default-blocked 172.16.0.0/12


def _guard(*trusted: str) -> UrlSafetyValidator:
    return UrlSafetyValidator(WebhookSecurityConfig(trusted_private_hosts=trusted))


def test_private_ip_is_blocked_by_default() -> None:
    with pytest.raises(InvalidWebhookTarget):
        _guard().check_ip(COMPOSE_IP, host="receiver", port=9000)


def test_trusted_host_reaches_its_private_ip() -> None:
    _guard("receiver").check_ip(COMPOSE_IP, host="receiver", port=9000)


def test_exemption_is_per_name_not_per_range() -> None:
    """The whole point: trusting one host must not trust the CIDR it sits in."""
    with pytest.raises(InvalidWebhookTarget):
        _guard("receiver").check_ip(COMPOSE_IP, host="evil.example.com", port=9000)


def test_omitting_host_keeps_the_check_at_full_strength() -> None:
    with pytest.raises(InvalidWebhookTarget):
        _guard("receiver").check_ip(COMPOSE_IP)


def test_port_scoped_entry_pins_the_port() -> None:
    """Trusting a host must not hand over its Redis / admin socket too."""
    _guard("receiver:9000").check_ip(COMPOSE_IP, host="receiver", port=9000)
    with pytest.raises(InvalidWebhookTarget):
        _guard("receiver:9000").check_ip(COMPOSE_IP, host="receiver", port=6379)


def test_portless_entry_trusts_every_port_as_documented() -> None:
    _guard("receiver").check_ip(COMPOSE_IP, host="receiver", port=6379)


def test_no_wildcards_so_an_internal_zone_cannot_be_handed_over() -> None:
    """`*.svc.cluster.local` must not be a pattern — it would trust the cluster."""
    with pytest.raises(InvalidWebhookTarget):
        _guard("*.internal").check_ip(COMPOSE_IP, host="receiver.internal", port=80)


def test_case_and_trailing_dot_do_not_defeat_the_match() -> None:
    _guard("receiver").check_ip(COMPOSE_IP, host="RECEIVER.", port=80)


def test_suffix_lookalike_is_not_a_match() -> None:
    for impostor in ("receiver.attacker.com", "xreceiver", "receiverx"):
        with pytest.raises(InvalidWebhookTarget):
            _guard("receiver").check_ip(COMPOSE_IP, host=impostor, port=80)


def test_idn_homograph_is_folded_before_matching() -> None:
    """Cyrillic 'с' looks identical to ASCII 'c' and resolves the same."""
    with pytest.raises(InvalidWebhookTarget):
        _guard("receiver").check_ip(COMPOSE_IP, host="reсeiver", port=80)


def test_validate_accepts_a_trusted_compose_target() -> None:
    target = _guard("receiver:9000").validate("http://receiver:9000/hook", (COMPOSE_IP,), env="dev")
    assert target.host == "receiver"
    assert target.port == 9000


def test_validate_still_rejects_an_untrusted_private_target() -> None:
    with pytest.raises(InvalidWebhookTarget):
        _guard().validate("http://receiver:9000/hook", (COMPOSE_IP,), env="dev")


def test_public_ip_is_unaffected() -> None:
    _guard().check_ip("93.184.216.34", host="example.com", port=443)


def test_malformed_ip_still_raises_for_a_trusted_host() -> None:
    with pytest.raises(InvalidWebhookTarget):
        _guard("receiver").check_ip("not-an-ip", host="receiver", port=80)


@pytest.mark.parametrize("wildcard_ip", ["0.0.0.0", "::"])
def test_this_host_wildcards_are_blocked(wildcard_ip: str) -> None:
    """0.0.0.0 and :: reach the local machine and are not inside 127.0.0.0/8."""
    with pytest.raises(InvalidWebhookTarget):
        _guard().check_ip(wildcard_ip, host="example.com", port=80)
