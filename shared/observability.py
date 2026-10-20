"""Observability kit: OpenTelemetry -> Tempo, Prometheus /metrics, JSON logs -> Loki.

Called once per service: `app = create_app(...)`.
- OTel spans exported to Tempo over OTLP/gRPC (endpoint default tempo:4317).
- prometheus-fastapi-instrumentator exposes /metrics.
- A Starlette middleware emits one JSON log line per request to stdout (Loki reads via promtail).

Pitfalls: logs MUST be JSON on stdout (not a file); /metrics path is fixed
(Prometheus default scrape path); OTel init happens inside setup() before any
request so trace_id is present in logs.
"""
from __future__ import annotations

import json
import logging
import time

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_fastapi_instrumentator import Instrumentator, metrics
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from shared.metrics import set_sse_connections  # noqa: F401  (re-export convenience)

log = logging.getLogger(__name__)


class JsonLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        ctx = trace.get_current_span().get_span_context()
        trace_id = format(ctx.trace_id, "032x") if ctx and ctx.trace_id else "-"
        svc = getattr(request.app.state, "service", "unknown")
        rec = {
            "ts": round(time.time(), 3),
            "level": "INFO",
            "service": svc,
            "method": request.method,
            "path": request.url.path,
            "status": response.status_code,
            "duration_ms": int((time.perf_counter() - start) * 1000),
            "trace_id": trace_id,
        }
        print(json.dumps(rec, default=str), flush=True)  # stdout -> Loki
        return response


def _setup_otel(app: FastAPI, service: str, otel_endpoint: str) -> None:
    if not otel_endpoint:
        return
    try:
        exporter = OTLPSpanExporter(endpoint=otel_endpoint, insecure=True)
        provider = TracerProvider(
            resource=Resource.create({"service.name": service})
        )
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        FastAPIInstrumentor.instrument_app(app)
        # Instrument httpx so the OUTBOUND calls (gateway -> intake,
        # orchestrator -> fraud) carry the traceparent header and produce
        # client spans. Without it the HTTP hop breaks the trace exactly like
        # the Kafka hop did (phase-7 spec §2.2).
        HTTPXClientInstrumentor().instrument()
    except Exception as e:  # noqa: BLE001 — never let tracing break the service
        log.warning("OTel init skipped: %s", e)


def create_app(
    title: str,
    *,
    otel_endpoint: str = "tempo:4317",
    cors_origins: list[str] | None = None,
) -> FastAPI:
    """App factory shared by all placeholder services (phase 1)."""
    app = FastAPI(title=title)
    app.state.service = title

    _setup_otel(app, title, otel_endpoint)
    if cors_origins:
        app.add_middleware(CORSMiddleware, allow_origins=cors_origins, allow_methods=["*"], allow_headers=["*"])

    app.add_middleware(JsonLogMiddleware)

    def _metrics() -> None:
        # The `service` label makes the metric queries self-describing
        # (`http_requests_total{service="intake"}`) instead of relying on the
        # scrape target's `instance` label (`intake:8080`) — the dashboard and
        # the alert rules are written against `service=...` (phase-7 spec §6.1).
        # The instrumentator has no `custom_labels` knob of its own, but it
        # passes its `instrumentations` list straight to the middleware, which
        # uses it instead of the default set when it is non-empty.
        #
        # NOTE: `metrics.default()` returns None when the collectors already
        # exist in the registry (a second app in the same process — e.g. two
        # services' test suites in one pytest run). The middleware treats an
        # EMPTY list as "use the default set", so we must leave the list empty
        # in that case rather than injecting a None that it would call.
        instrumentor = Instrumentator()
        default_set = metrics.default(custom_labels={"service": title})
        if default_set is not None:
            instrumentor.instrumentations = [default_set]
        instrumentor.instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)

    _metrics()

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok", "service": title})

    @app.get("/_status")
    def status() -> JSONResponse:
        return JSONResponse({"service": title, "up": True, "circuit_breakers": {}})

    return app
