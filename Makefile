# fintech_demo — Makefile
# Phase 1 targets. Uses docker compose + bash. On Windows run via Git Bash.
SHELL := /bin/bash
COMPOSE ?= docker compose

.PHONY: up down ps logs seed test load load-slo fe e2e acceptance demo smoke clean setup grafana-password help

help:
	@echo "Targets: up down ps logs <svc> seed test load load-slo fe e2e acceptance demo smoke clean setup grafana-password"

setup: ## Create .env from .env.example (local config: Grafana password etc.)
	@if [ -f .env ]; then \
		echo ".env already exists — leaving it alone."; \
	else \
		cp .env.example .env && echo "created .env (edit it to taste)"; \
	fi

grafana-password: ## Set the Grafana admin password (make grafana-password PASSWORD=...)
	@test -n "$(PASSWORD)" || (echo "usage: make grafana-password PASSWORD=..." && exit 1)
	@$(MAKE) --no-print-directory setup
	@python scripts/set_grafana_password.py "$(PASSWORD)"

up: ## Build + start the full stack, wait for healthy
	$(COMPOSE) up -d --build
	$(COMPOSE) ps

ps: ## Show container status
	$(COMPOSE) ps

logs: ## Follow logs: make logs svc=gateway
	@test -n "$(svc)" || (echo "usage: make logs svc=<name>" && exit 1)
	$(COMPOSE) logs -f "$(svc)"

down: ## Stop + remove the stack (keeps volumes)
	$(COMPOSE) down

seed: ## Run the init/*.sql seed (20 users dual ledger + profile) against the live postgres
	bash scripts/seed_users.sh

test: ## Run shared unit tests
	python -m pytest -q

load: ## Load test @ 50 TPS (default 60s) via mock-acquirer, then poll + report
	@TPS=$${TPS:-50}; DURATION=$${DURATION:-60}; \
	echo "starting burst: tps=$$TPS duration=$$DURATIONs"; \
	curl -fsS -X POST http://localhost:8006/acquirer/burst \
	     -H 'Content-Type: application/json' \
	     -d "{\"tps\":$$TPS,\"duration\":$$DURATION,\"include_invalids\":true}"; echo; \
	i=0; \
	while :; do \
	  sleep 2; i=$$((i+1)); \
	  st=$$(curl -fsS http://localhost:8006/acquirer/burst/status); \
	  echo "  t+$$i*2s  status=$$st" | tr -d '\n'; echo; \
	  running=$$(echo "$$st" | grep -o '"burst_running":[a-z]*'); \
	  if echo "$$running" | grep -q "false"; then break; fi; \
	  if [ $$i -gt 300 ]; then echo "  (timeout polling)"; break; fi; \
	done; \
	echo "final: $$(curl -fsS http://localhost:8006/acquirer/burst/status)"; \
	echo "fraud CB: $$(curl -fsS http://localhost:8000/v1/_status)"

fe: ## Build the React frontend (frontend/): npm ci && npm run build
	cd frontend && npm ci && npm run build

load-slo: ## Load harness with percentiles (50 TPS / 60s, fails if p99 > 300ms)
	python scripts/load_test.py --tps 50 --duration 60 --slo 300

e2e: ## E2E suite against the LIVE stack (needs `make up` first)
	RUN_E2E=1 python -m pytest tests/e2e -v

acceptance: ## Acceptance: 7 checks §7 TЗ v2 (phase 8)
	bash scripts/acceptance.sh

demo: ## One-shot demo: clean stack + smoke + seed + load + failure-injection + acceptance
	@echo "=== DEMO: 1/5 clean ==="
	$(COMPOSE) down -v
	@echo "=== DEMO: 2/5 up ==="
	$(COMPOSE) up -d --build
	@echo "=== DEMO: 3/5 smoke ==="
	bash scripts/smoke_env.sh
	@echo "=== DEMO: 4/5 seed ==="
	bash scripts/seed_users.sh
	@echo "=== DEMO: 5/5 acceptance (7 criteria) ==="
	bash scripts/acceptance.sh --skip-setup --skip-teardown
	@echo ""
	@echo "=== DEMO: the stack is still up (UI: http://localhost:5173, Grafana: http://localhost:3000) ==="

smoke: ## Phase 1 smoke: healthz x8 + topics + PG + redis + Debezium + Prometheus
	bash scripts/smoke_env.sh

clean: ## Stop + remove volumes
	$(COMPOSE) down -v --remove-orphans
