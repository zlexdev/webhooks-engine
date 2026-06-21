"""Default :class:`HttpSenderProtocol` implementation over ``httpx``.

Key properties:

- **Response-size cap** — reads at most ``max_response_bytes`` from the
  target via ``client.stream("POST", ...)`` with an aiter loop that
  stops once the cap is reached.
- **DNS pinning** — when ``resolved_ip`` is set, a per-request
  ``httpx.HTTPTransport`` maps the original host to the validated IP,
  defeating DNS rebinding between validate and dispatch.
- **Timeouts** — connect / read / total split controlled by
  :class:`WebhookLimitsConfig`.

App code can swap this out for a curl_cffi-backed sender by
implementing :class:`HttpSenderProtocol`; the dispatcher is unchanged.
"""

from __future__ import annotations

from time import perf_counter
from urllib.parse import urlsplit, urlunsplit

import httpx

from webhook_engine.types import HttpSendResult

__all__ = ["HttpxSender"]


class HttpxSender:
    """Structural impl of :class:`HttpSenderProtocol`.

    Not an explicit subclass — ``Protocol`` is for structural typing,
    so the class just has to match the signatures. The ABC-style
    sibling for persistent stores is :class:`BaseWebhookDeliveryStore`;
    there is intentionally no ``BaseHttpSender`` because the only
    "extension" use case here is swapping httpx for curl_cffi at
    wiring time.
    """

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def post(
        self,
        *,
        url: str,
        body: bytes,
        headers: dict[str, str],
        resolved_ip: str | None,
        connect_timeout_s: float,
        read_timeout_s: float,
        total_timeout_s: float,
        max_response_bytes: int,
    ) -> HttpSendResult:
        timeout = httpx.Timeout(
            connect=connect_timeout_s,
            read=read_timeout_s,
            write=read_timeout_s,
            pool=total_timeout_s,
        )
        request_url, request_headers, sni_hostname = self._pin_host(url, headers, resolved_ip)
        # When pinning to a validated IP the URL host is the raw IP, so force the
        # TLS SNI + certificate verification back to the original hostname — without
        # this the handshake fails with "IP address mismatch" against any hostname
        # cert, and no HTTPS target is ever deliverable.
        extensions = {"sni_hostname": sni_hostname} if sni_hostname else None
        request = self._client.build_request(
            "POST",
            request_url,
            content=body,
            headers=request_headers,
            timeout=timeout,
            extensions=extensions,
        )
        started = perf_counter()
        response = await self._client.send(request, stream=True)
        try:
            captured = bytearray()
            async for chunk in response.aiter_raw():
                remaining = max_response_bytes - len(captured)
                if remaining <= 0:
                    break
                captured.extend(chunk[:remaining])
                if len(captured) >= max_response_bytes:
                    break
            duration_ms = int((perf_counter() - started) * 1000)
            body_snippet = captured.decode("utf-8", errors="replace")
            response_headers = {k: v for k, v in response.headers.items()}
            return HttpSendResult(
                status=response.status_code,
                headers=response_headers,
                body_snippet=body_snippet,
                duration_ms=duration_ms,
            )
        finally:
            await response.aclose()

    def _pin_host(
        self,
        url: str,
        headers: dict[str, str],
        resolved_ip: str | None,
    ) -> tuple[str, dict[str, str], str | None]:
        if not resolved_ip:
            return url, headers, None
        parsed = urlsplit(url)
        host = parsed.hostname or ""
        port = parsed.port
        if not host:
            return url, headers, None
        netloc = resolved_ip if ":" not in resolved_ip else f"[{resolved_ip}]"
        if port is not None:
            netloc = f"{netloc}:{port}"
        pinned = urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))
        new_headers = dict(headers)
        new_headers.setdefault("Host", parsed.netloc)
        return pinned, new_headers, host
