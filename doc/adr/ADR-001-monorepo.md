# ADR-001 — Monorepo layout

- Status: accepted
- Date: 2026-10-19
- Deciders: CTO

## Context
The PoC has 8 Python services + a shared abstraction layer. Options: (a) one repo
with a shared package + per-service apps, (b) one repo with per-service
`pyproject.toml`, (c) multi-repo. `doc/implementation_plan.md §0` fixes a single
`pyproject.toml`, single `docker-compose.yml`, single `Makefile`.

## Decision
One monorepo root (`fintech_demo/`) with:
- one `pyproject.toml` (project `fintech_demo`) that installs `shared` + `services`;
- `shared/` = the DDD/infra abstractions (importable by all services);
- `services/<name>/main.py` = 8 thin FastAPI apps; a single `services/main.py`
  resolves the target app from the `SERVICE_NAME` env (handles dashed names like
  `mock-fraud`);
- one shared `Dockerfile` (build arg `SERVICE_NAME`) reused by all 8 services;
- one `docker-compose.yml` (18 containers) + one `Makefile`.

## Consequences
- One `uv.lock`/install, consistent versions, fast local dev via `pip install -e .`.
- Dashed service names work via path-based entrypoint resolution (no hyphenated
  Python packages).
- Trade-off: no per-service independent deploy images; acceptable for a PoC.
