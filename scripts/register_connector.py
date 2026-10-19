"""Register the Debezium ledger-outbox connector with Kafka Connect (stdlib only).

Runs as a sidecar after kafka-connect is healthy. Waits for the REST API, then
POSTs the connector config. `restart: no` + `service_completed_successfully`
dependency keeps it out of the `docker compose up` "running" count.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

CONNECT_URL = os.environ.get("CONNECT_URL", "http://kafka-connect:8083").rstrip("/")
CONFIG_PATH = "/app/config/debezium-ledger.json"
CONNECTOR = "ledger-outbox"
TIMEOUT = 120


def _get(path: str) -> bytes:
    with urllib.request.urlopen(CONNECT_URL + path, timeout=10) as r:
        return r.read()


def _post(path: str, payload: bytes) -> bytes:
    req = urllib.request.Request(CONNECT_URL + path, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.read()


def main() -> int:
    deadline = time.time() + TIMEOUT
    while time.time() < deadline:
        try:
            _get("/connectors")
            break
        except (urllib.error.URLError, ConnectionError):
            time.sleep(2)
    else:
        print("ERROR: Kafka Connect REST never came up", flush=True)
        return 1

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        payload = f.read().encode()

    # Idempotent: drop if it exists, then create.
    try:
        _get(f"/connectors/{CONNECTOR}")
        req = urllib.request.Request(f"{CONNECT_URL}/connectors/{CONNECTOR}", method="DELETE")
        urllib.request.urlopen(req, timeout=10).read()
        print(f"removed existing connector {CONNECTOR}", flush=True)
    except (urllib.error.HTTPError, urllib.error.URLError):
        pass

    body = _post("/connectors", payload)
    print(f"registered connector: {body.decode()}", flush=True)

    # Wait for RUNNING (best effort) so the compose dependency is meaningful.
    for _ in range(60):
        try:
            status = json.loads(_get(f"/connectors/{CONNECTOR}/status").decode())
            state = (status.get("connector") or {}).get("state")
            if state == "RUNNING":
                print(f"connector state: {state}", flush=True)
                return 0
        except (urllib.error.URLError, ConnectionError, json.JSONDecodeError):
            pass
        time.sleep(2)
    print("WARNING: connector not RUNNING within 120s (check logs)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
