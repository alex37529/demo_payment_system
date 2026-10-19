"""mock-fraud — fraud-check stub + failure injection (tz_v2 §2.3, phase 3).

The folder name contains a hyphen (`mock-fraud`) which is not a valid Python
identifier, so we import the `fraud_service` module by file path rather than a
`from services.mock_fraud ...` statement.
"""
from __future__ import annotations

import importlib.util
import os

from services.shared_app import build_app

_HERE = os.path.dirname(os.path.abspath(__file__))
_FRAUD_MODULE = os.path.join(_HERE, "fraud_service.py")


def _load_fraud_service():
    spec = importlib.util.spec_from_file_location("mock_fraud_service", _FRAUD_MODULE)
    assert spec is not None and spec.loader is not None, f"cannot load {_FRAUD_MODULE}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


app = build_app("mock-fraud")
_load_fraud_service().attach_fraud(app)
