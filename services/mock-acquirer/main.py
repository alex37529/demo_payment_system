"""mock-acquirer — load generator entrypoint (phase 6).

The folder name contains a hyphen (`mock-acquirer`), which is not a valid Python
identifier, so we load the `acquirer_service` module by file path (same pattern
as `services/mock-fraud/main.py`).
"""
from __future__ import annotations

import importlib.util
import os

from services.shared_app import build_app

_HERE = os.path.dirname(os.path.abspath(__file__))
_ACQ_MODULE = os.path.join(_HERE, "acquirer_service.py")


def _load_acquirer_service():
    spec = importlib.util.spec_from_file_location("mock_acquirer_service", _ACQ_MODULE)
    assert spec is not None and spec.loader is not None, f"cannot load {_ACQ_MODULE}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


app = build_app("mock-acquirer")
_load_acquirer_service().attach_acquirer(app)
