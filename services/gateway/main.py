"""gateway — BFF + SSE terminal (phase 1: placeholder, /healthz only)."""
from services.shared_app import build_app

app = build_app("gateway")
