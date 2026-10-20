"""gateway — BFF + SSE-term entrypoint (phase 6).

Loads `gateway.py` by file path (the folder has no hyphen, so a direct import
would work, but we match the mock-* pattern for consistency and because
`services/gateway` is not yet a package with `__init__.py`).
"""
from __future__ import annotations

import importlib.util
import os

from services.shared_app import build_app

_HERE = os.path.dirname(os.path.abspath(__file__))
_GW_MODULE = os.path.join(_HERE, "gateway.py")


def _load_gateway():
    spec = importlib.util.spec_from_file_location("gateway_bff", _GW_MODULE)
    assert spec is not None and spec.loader is not None, f"cannot load {_GW_MODULE}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


app = build_app("gateway")
_load_gateway().attach_gateway(app)
