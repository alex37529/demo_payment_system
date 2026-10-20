"""gateway — BFF facade for the browser (phase 6, tz_v2 §2.1/§5).

The browser talks ONLY to the gateway (port 8000). The gateway:

* **routes** ``/v1/*`` to the owning service over the compose DNS names
  (Intake:8001, Profile:8004, Notification:8005) — forwarding the response
  verbatim (BFF adaptation, not business logic);
* **terminates** the SSE feed: ``GET /v1/stream`` proxies the Notification
  SSE stream from :8005 byte-for-byte (``StreamingResponse`` +
  ``text/event-stream``). It does NOT 302-redirect (phase-6 pitfall #10):
  a redirect would break the browser's ``EventSource``;
* **aggregates** ``GET /v1/_status`` — the circuit-breaker state from the
  orchestrator's ``/v1/circuit-breakers`` (the fraud breaker is the one the UI
  badge surfaces), best-effort.

Downstream targets come from ``*_BASE_URL`` env vars (defaults = the compose
service names, so this works in Docker; tests inject fakes). Inside compose the
services listen on 8080 (the 8000-8007 numbers are the *published* host ports,
tz_v2 §4.1); the internal base is ``host:8080``.
"""
from __future__ import annotations

import logging
import os

import httpx
from fastapi import FastAPI, Header, Request
from starlette.responses import JSONResponse, StreamingResponse

log = logging.getLogger(__name__)

_TIMEOUT = 10.0


class GatewayService:
    def __init__(self, *, intake_base: str = "http://intake:8080",
                 profile_base: str = "http://profile:8080",
                 notification_base: str = "http://notification:8080",
                 orchestrator_base: str = "http://orchestrator:8080",
                 ledger_base: str = "http://ledger:8080",
                 timeout: float = _TIMEOUT) -> None:
        self.intake_base = intake_base.rstrip("/")
        self.profile_base = profile_base.rstrip("/")
        self.notification_base = notification_base.rstrip("/")
        self.orchestrator_base = orchestrator_base.rstrip("/")
        self.ledger_base = ledger_base.rstrip("/")
        self.client = httpx.AsyncClient(timeout=timeout)
        # A long-lived client for the SSE proxy: read=None (no read deadline on
        # an idle stream), a generous connect.
        self.stream_client = httpx.AsyncClient(timeout=httpx.Timeout(connect=10.0,
                                                                      read=None,
                                                                      write=10.0,
                                                                      pool=None))

    async def _forward(self, method: str, url: str, *, json_body: dict | None = None,
                       headers: dict | None = None) -> JSONResponse:
        try:
            r = await self.client.request(method, url, json=json_body, headers=headers)
        except httpx.HTTPError as e:
            log.warning("gateway: %s %s failed (%s)", method, url, e.__class__.__name__)
            return JSONResponse(status_code=502,
                                content={"ok": False, "error": "upstream_unreachable",
                                         "upstream": url})
        return JSONResponse(status_code=r.status_code, content=r.json())

    async def _status(self) -> dict:
        out: dict = {"up": True, "circuit_breakers": {}}
        try:
            r = await self.client.get(f"{self.orchestrator_base}/v1/circuit-breakers",
                                      timeout=2.0)
            if r.status_code == 200:
                data = r.json()
                out["circuit_breakers"] = data.get("circuit_breakers", data)
        except httpx.HTTPError as e:
            log.warning("gateway: _status orchestrator unreachable (%s)",
                        e.__class__.__name__)
            out["circuit_breakers"] = {"fraud": {"state": "unknown", "is_open": False}}
        return out

    async def _users(self) -> JSONResponse:
        return await self._forward("GET", f"{self.profile_base}/v1/users")

    async def _user(self, account_id: str) -> JSONResponse:
        return await self._forward("GET", f"{self.profile_base}/v1/users/{account_id}")

    async def _feed(self, limit: int = 20) -> JSONResponse:
        return await self._forward("GET", f"{self.notification_base}/v1/payments?limit={limit}")

    async def _transactions(self, account_id: str, limit: int = 50) -> JSONResponse:
        return await self._forward(
            "GET",
            f"{self.ledger_base}/v1/accounts/{account_id}/transactions?limit={limit}")

    async def _create_payment(self, body: dict, idem: str | None) -> JSONResponse:
        headers = {"Content-Type": "application/json"}
        if idem:
            headers["Idempotency-Key"] = idem
        return await self._forward("POST", f"{self.intake_base}/v1/payments",
                                   json_body=body, headers=headers)

    async def _get_payment(self, payment_id: str) -> JSONResponse:
        return await self._forward("GET", f"{self.intake_base}/v1/payments/{payment_id}")

    async def _open_sse_stream(self):
        """Open an upstream SSE stream to Notification (held until the proxy
        finishes; see ``_sse_proxy_body``)."""
        return await self.stream_client.send(
            self.stream_client.build_request(
                "GET", f"{self.notification_base}/v1/stream",
                headers={"Accept": "text/event-stream", "Cache-Control": "no-cache"}),
            stream=True)

    async def aclose(self) -> None:
        await self.client.aclose()
        await self.stream_client.aclose()


async def _sse_proxy_body(upstream: httpx.Response):
    """Async generator: yield the upstream SSE body byte-for-byte, then close the
    upstream stream. Transparent proxying of ``data: ...`` / ``: keepalive``
    frames (pitfall #10: the gateway *terminates* the feed, not redirect)."""
    try:
        async for chunk in upstream.aiter_bytes():
            if chunk:
                yield chunk
    except Exception as e:  # noqa: BLE001 — an upstream error ends the proxy stream
        log.warning("gateway: SSE proxy stream error (%s)", e.__class__.__name__)
    finally:
        await upstream.aclose()


def attach_gateway(app: FastAPI) -> None:
    from contextlib import asynccontextmanager

    svc = GatewayService(
        intake_base=os.environ.get("INTAKE_BASE_URL", "http://intake:8080"),
        profile_base=os.environ.get("PROFILE_BASE_URL", "http://profile:8080"),
        notification_base=os.environ.get("NOTIFICATION_BASE_URL", "http://notification:8080"),
        orchestrator_base=os.environ.get("ORCHESTRATOR_BASE_URL", "http://orchestrator:8080"),
        ledger_base=os.environ.get("LEDGER_BASE_URL", "http://ledger:8080"),
        timeout=float(os.environ.get("GATEWAY_TIMEOUT_S", "10")),
    )
    app.state.gateway = svc
    app.state.gateway_intake = svc.intake_base
    app.state.gateway_profile = svc.profile_base
    app.state.gateway_notification = svc.notification_base
    app.state.gateway_orchestrator = svc.orchestrator_base
    app.state.gateway_ledger = svc.ledger_base

    @app.post("/v1/payments")
    async def create_payment(
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> JSONResponse:
        body = await request.json()
        return await svc._create_payment(body, idempotency_key)

    @app.get("/v1/payments/{payment_id}")
    async def get_payment(payment_id: str) -> JSONResponse:
        return await svc._get_payment(payment_id)

    @app.get("/v1/users")
    async def users() -> JSONResponse:
        return await svc._users()

    @app.get("/v1/users/{account_id}")
    async def user(account_id: str) -> JSONResponse:
        return await svc._user(account_id)

    @app.get("/v1/payments-feed")
    async def payments_feed(limit: int = 100) -> JSONResponse:
        return await svc._feed(limit)

    @app.get("/v1/users/{account_id}/transactions")
    async def user_transactions(account_id: str, limit: int = 50) -> JSONResponse:
        """BFF route for the per-account balance journal (debit/refund)."""
        return await svc._transactions(account_id, limit=limit)

    @app.get("/v1/stream")
    async def sse_stream() -> StreamingResponse:
        upstream = await svc._open_sse_stream()
        if upstream.status_code >= 400:
            await upstream.aclose()
            return JSONResponse(status_code=upstream.status_code,
                                content={"ok": False, "error": "upstream_unreachable",
                                         "upstream": "notification /v1/stream"})
        return StreamingResponse(
            _sse_proxy_body(upstream),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/v1/_status")
    async def status() -> JSONResponse:
        return JSONResponse(await svc._status())

    @asynccontextmanager
    async def _gw_lifespan(_app: FastAPI):
        log.info("gateway BFF ready (intake=%s profile=%s noti=%s orch=%s)",
                 svc.intake_base, svc.profile_base, svc.notification_base,
                 svc.orchestrator_base)
        try:
            yield
        finally:
            await svc.aclose()
            log.info("gateway shutdown complete")

    app.router.lifespan_context = _gw_lifespan
