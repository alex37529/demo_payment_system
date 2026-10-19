"""Service entrypoint: select the per-service app by the SERVICE_NAME arg.

All 8 placeholder services run `uvicorn services.main:app`. SERVICE_NAME may be
dashed (e.g. `mock-fraud`), so we resolve the module by file path (hyphens are
invalid in Python identifiers).
"""
from __future__ import annotations

import importlib.util
import os
import sys

SERVICE_NAME = os.environ.get("SERVICE_NAME", "gateway")
_HERE = os.path.dirname(os.path.abspath(__file__))


def _load() -> object:
    module_name = SERVICE_NAME.replace("-", "_")
    module_path = os.path.join(_HERE, SERVICE_NAME, "main.py")
    if not os.path.exists(module_path):
        raise RuntimeError(f"SERVICE_NAME={SERVICE_NAME!r} not found under services/")
    if module_name not in sys.modules:
        spec = importlib.util.spec_from_file_location(f"services_{module_name}", module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)  # type: ignore[union-attr]
    else:
        module = sys.modules[module_name]
    return module.app


app = _load()
