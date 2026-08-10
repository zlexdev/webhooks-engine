"""Pure URL / IP safety checks. No DNS I/O — the caller resolves the host and
passes the addresses in, so this module stays I/O-free and unit-testable.

Mitigates SSRF via DNS rebinding: the resolved IP is pinned when a delivery is
enqueued and re-validated on every dispatcher send, so a name that flips to a
private address between the two never gets POSTed to.

Escape hatch: ``security.trusted_private_hosts`` exempts named hosts (a compose
service, an internal receiver) from the CIDR deny-list. Empty by default.
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
        host = parsed.hostname or ""
        default_port = 443 if parsed.scheme == "https" else 80
        port = parsed.port or default_port
        self._check_ips(resolved_ips, host=parsed.hostname, port=port)
        return ResolvedTarget(
            host=host,
            port=port,
            scheme=parsed.scheme,
            ips=tuple(resolved_ips),
        )

    def check_ip(self, ip: str, *, host: str | None = None, port: int | None = None) -> None:
        """Re-validation hook used by the dispatcher before POST.

        Raises ``InvalidWebhookTarget`` if ``ip`` falls into a blocked
        CIDR range — used as a second line of defence against DNS
        rebinding between subscription-create and dispatch.

        ``host`` / ``port`` are the name and port the address was resolved
        from. They only ever *relax* the CIDR check, and only for an entry in
        ``security.trusted_private_hosts``; the address must still parse.
        Omitting them (the default) keeps the check at full strength.
        """
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError as exc:
            raise InvalidWebhookTarget(
                InvalidTargetReason.UNRESOLVABLE, f"invalid ip: {ip}"
            ) from exc
        if not self._blocked.contains(addr):
            return
        if host is not None and _is_trusted_private(
            host, port, self._security.trusted_private_hosts
        ):
            return
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
        host = p.hostname or ""
        if not host:
            raise InvalidWebhookTarget(InvalidTargetReason.BLOCKED_HOST, "missing host")
        if _host_matches(host, allowed):
            return
        raise InvalidWebhookTarget(
            InvalidTargetReason.BLOCKED_HOST, f"host not in allow-list: {host.lower()}"
        )

    def _check_ips(
        self, ips: tuple[str, ...], *, host: str | None = None, port: int | None = None
    ) -> None:
        if not ips:
            raise InvalidWebhookTarget(InvalidTargetReason.UNRESOLVABLE, "no resolved IPs")
        for ip_s in ips:
            self.check_ip(ip_s, host=host, port=port)


def _host_matches(host: str, patterns: tuple[str, ...]) -> bool:
    """Exact name or ``*.suffix``, case-insensitive. Used by allowed_hosts."""
    h = _normalise_host(host)
    for pattern in patterns:
        pat = pattern.lower()
        if pat.startswith("*."):
            if h.endswith(pat[1:]):
                return True
        elif h == pat:
            return True
    return False


def _is_trusted_private(host: str, port: int | None, patterns: tuple[str, ...]) -> bool:
    """Exact ``host`` or ``host:port``. No wildcards — see the config docstring.

    A portless pattern matches any port on that host; a pattern with a port
    matches only that port, which is the form to use when the trusted host also
    runs something you do not want reachable (a database, an admin socket).
    """
    h = _normalise_host(host)
    if not h:
        return False
    for pattern in patterns:
        pat = pattern.lower().strip()
        if ":" in pat:
            pat_host, _, pat_port = pat.rpartition(":")
            if h == pat_host and port is not None and str(port) == pat_port:
                return True
        elif h == pat:
            return True
    return False


def _normalise_host(host: str) -> str:
    """Lower-case, drop the FQDN trailing dot, fold IDN to its ASCII form.

    Without the fold, "reсeiver" (Cyrillic с) is a different string here and the
    same name to a resolver — a homograph walks straight past an exact match.
    """
    h = host.strip().rstrip(".").lower()
    try:
        return h.encode("idna").decode("ascii").lower()
    except (UnicodeError, ValueError):
        # Not encodable as IDNA (empty label, over-long, raw underscore) — such
        # a name cannot match a pattern that IS encodable, so leave it as is.
        return h
