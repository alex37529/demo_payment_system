#!/usr/bin/env python
"""Set the Grafana admin password — in `.env` AND in the running Grafana.

Why both? Grafana's `GF_SECURITY_ADMIN_PASSWORD` is a **bootstrap** value: it
is read only when Grafana creates its database (the first start, or after
`make clean` wiped the volume). Afterwards the password lives in Grafana's own
SQLite database and the environment variable is ignored.

So a password changed only in the UI is silently reverted by the next
`make clean` — and a password changed only in `.env` does not apply until the
volume is recreated. This script changes BOTH, so the two never disagree:

  1. `.env`  — what the next fresh Grafana will bootstrap with;
  2. the running Grafana — what applies right now.

**How the live change is made.** The Grafana API would need the CURRENT
password to authorise it, and the current password is exactly what we may not
know (it was changed in the UI). The API is also a poor fit here: with
`GF_AUTH_ANONYMOUS_ENABLED=true` it accepts ANY request as the anonymous
Admin, so Basic auth cannot even prove which password is current. The
**Grafana CLI** can: `grafana cli admin reset-admin-password <pw>` sets the
password directly in the database — no current password, no API, no lockout.

Usage:
    python scripts/set_grafana_password.py 'my-password'
    make grafana-password PASSWORD=my-password
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT / ".env"
ENV_EXAMPLE = ROOT / ".env.example"
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000")
GRAFANA_CONTAINER = os.environ.get("GRAFANA_CONTAINER", "fintech_grafana")


def read_env() -> dict[str, str]:
    """The current values of .env (empty if it does not exist yet)."""
    values: dict[str, str] = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            values[key.strip()] = val.strip()
    return values


def write_env(values: dict[str, str]) -> None:
    """Rewrite .env, preserving the comments of the template."""
    template = ENV_EXAMPLE.read_text(encoding="utf-8") if ENV_EXAMPLE.exists() else ""
    if not template:
        template = "GRAFANA_ADMIN_PASSWORD={GRAFANA_ADMIN_PASSWORD}\n"
    out = []
    for line in template.splitlines():
        key = line.split("=", 1)[0].strip() if "=" in line and not line.strip().startswith("#") else None
        if key and key in values:
            out.append(f"{key}={values[key]}")
        else:
            out.append(line)
    ENV_FILE.write_text("\n".join(out) + "\n", encoding="utf-8")


def grafana_cli(*args: str) -> tuple[int, str]:
    """Run `grafana cli` inside the running container."""
    cmd = ["docker", "exec", GRAFANA_CONTAINER, "grafana", "cli",
           "--homepath", "/usr/share/grafana", *args]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except FileNotFoundError:
        return 127, "docker not found on PATH"
    except subprocess.TimeoutExpired:
        return 124, "timeout"
    return p.returncode, (p.stdout + p.stderr).strip()


def login(password: str) -> bool:
    """Sign in through /login — the endpoint that really checks the password."""
    req = urllib.request.Request(
        f"{GRAFANA_URL}/login",
        data=json.dumps({"user": "admin", "password": password}).encode(),
        method="POST", headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except urllib.error.HTTPError:
        return False
    except urllib.error.URLError:
        return False


def main(argv: list[str]) -> int:
    if len(argv) < 2 or not argv[1].strip():
        print("usage: python scripts/set_grafana_password.py <password>", file=sys.stderr)
        return 2
    new_password = argv[1].strip()

    env = read_env()
    old_password = env.get("GRAFANA_ADMIN_PASSWORD") or os.environ.get("GRAFANA_ADMIN_PASSWORD") or "admin"

    # 1) the .env — what the next fresh Grafana will bootstrap with
    env["GRAFANA_ADMIN_PASSWORD"] = new_password
    if not ENV_FILE.exists() and ENV_EXAMPLE.exists():
        ENV_FILE.write_text(ENV_EXAMPLE.read_text(encoding="utf-8"), encoding="utf-8")
    write_env(env)
    print(".env      : GRAFANA_ADMIN_PASSWORD updated "
          "(the next `make up` on a fresh volume will use it)")

    # 2) the running Grafana — what applies right now
    code, out = grafana_cli("admin", "reset-admin-password", new_password)
    if code != 0:
        print(f"live      : could not reset the password (exit {code}) {out[:300]}",
              file=sys.stderr)
        print("            The .env value still applies after `make clean && make up`.",
              file=sys.stderr)
        return 1
    print("live      : the running Grafana admin password was changed")

    if login(new_password):
        print("verify    : the new password signs in")
        return 0
    print(f"verify    : the new password does NOT sign in — "
          f"the previous one {'does' if login(old_password) else 'does not'} "
          f"(a lockout may still be active)", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
