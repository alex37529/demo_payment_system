"""Shared app factory used by every placeholder service."""
from __future__ import annotations

import os

from fastapi import FastAPI

from shared.observability import create_app


def build_app(title: str) -> FastAPI:
    cors = os.environ.get("CORS_ORIGINS")
    origins = [o for o in (cors or "").split(",") if o] or None
    return create_app(title, otel_endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", ""), cors_origins=origins)
