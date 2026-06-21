"""Pure URL / IP safety checks. No DNS I/O — resolution happens in the
services-layer ``UrlValidator`` so ``libs/webhooks/`` stays I/O-free.

Mitigates SSRF via DNS rebinding: resolved IPs are pinned at the
subscription doc and re-validated on every dispatcher send.
"""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from urllib.parse import SplitResult, urlsplit

from webhook_engine.config import WebhookSecurityConfig
from webhook_engine.enums import InvalidTargetReason
from webhook_engine.exceptions import InvalidWebhookTarget
from webhook_engine.types import ResolvedTarget

__all__ = ["BlockedCidrSet", "UrlSafetyValidator"]


@dataclass(frozen=True, slots=True)
class BlockedCidrSet:
    nets_v4: tuple[ipaddress.IPv4Network, ...]
    nets_v6: tuple[ipaddress.IPv6Network, ...]

    @classmethod
    def from_strings(cls, cidrs: tuple[str, ...]) -> BlockedCidrSet:
        v4: list[ipaddress.IPv4Network] = []
        v6: list[ipaddress.IPv6Network] = []
        for c in cidrs:
            if ":" in c:
                v6.append(ipaddress.IPv6Network(c, strict=False))
            else:
                v4.append(ipaddress.IPv4Network(c, strict=False))
        return cls(tuple(v4), tuple(v6))

    def contains(self, ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
        if isinstance(ip, ipaddress.IPv4Address):
            return any(ip in n for n in self.nets_v4)
        return any(ip in n for n in self.nets_v6)


class UrlSafetyValidator:
    """Pure (no I/O) URL + IP safety checks.

    Callers resolve DNS first (in the services layer) and pass the
    resolved IP list into :meth:`validate`. The guard keeps the heavy
    rules (scheme, creds, CIDR block, allow-list) stateless.
    """

    def __init__(self, security: WebhookSecurityConfig) -> None:
        self._security = security
        self._blocked = BlockedCidrSet.from_strings(security.blocked_cidrs)

    def validate(
        self,
        url: str,
        resolved_ips: tuple[str, ...],
        *,
        env: str,
    ) -> ResolvedTarget:
        parsed = urlsplit(url)
        self._check_scheme(parsed, env)
        self._check_credentials(parsed)
        self._check_host_allowlist(parsed)
        self._check_ips(resolved_ips)
        host = parsed.hostname or ""
        default_port = 443 if parsed.scheme == "https" else 80
        return ResolvedTarget(
            host=host,
            port=parsed.port or default_port,
            scheme=parsed.scheme,
            ips=tuple(resolved_ips),
        )

    def check_ip(self, ip: str) -> None:
        """Re-validation hook used by the dispatcher before POST.

        Raises ``InvalidWebhookTarget`` if ``ip`` falls into a blocked
        CIDR range — used as a second line of defence against DNS
        rebinding between subscription-create and dispatch.
        """
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError as exc:
            raise InvalidWebhookTarget(
                InvalidTargetReason.UNRESOLVABLE, f"invalid ip: {ip}"
            ) from exc
        if self._blocked.contains(addr):
            raise InvalidWebhookTarget(InvalidTargetReason.PRIVATE_IP, f"blocked ip: {ip}")

    def _check_scheme(self, p: SplitResult, env: str) -> None:
        if p.scheme not in ("http", "https"):
            raise InvalidWebhookTarget(InvalidTargetReason.BAD_SCHEME, f"bad scheme: {p.scheme}")
        # HTTPS requirement: prod always; dev only when allow_http_in_dev=False.
        is_prod = env not in ("dev", "test", "local", "testnet")
        if p.scheme == "http":
            if is_prod and self._security.require_https_in_prod:
                raise InvalidWebhookTarget(InvalidTargetReason.NOT_HTTPS, "https required in prod")
            if not is_prod and not self._security.allow_http_in_dev:
                raise InvalidWebhookTarget(InvalidTargetReason.NOT_HTTPS, "http disabled")

    def _check_credentials(self, p: SplitResult) -> None:
        if p.username or p.password:
            raise InvalidWebhookTarget(
                InvalidTargetReason.HAS_CREDENTIALS, "URL must not embed credentials"
            )

    def _check_host_allowlist(self, p: SplitResult) -> None:
        allowed = self._security.allowed_hosts
        if allowed is None:
            return
        host = (p.hostname or "").lower()
        if not host:
            raise InvalidWebhookTarget(InvalidTargetReason.BLOCKED_HOST, "missing host")
        for pattern in allowed:
            pat = pattern.lower()
            if pat.startswith("*."):
                if host.endswith(pat[1:]):
                    return
            elif host == pat:
                return
        raise InvalidWebhookTarget(
            InvalidTargetReason.BLOCKED_HOST, f"host not in allow-list: {host}"
        )

    def _check_ips(self, ips: tuple[str, ...]) -> None:
        if not ips:
            raise InvalidWebhookTarget(InvalidTargetReason.UNRESOLVABLE, "no resolved IPs")
        for ip_s in ips:
            self.check_ip(ip_s)
