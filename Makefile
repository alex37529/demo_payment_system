# fintech_demo — Makefile
# Phase 1 targets. Uses docker compose + bash. On Windows run via Git Bash.
SHELL := /bin/bash
COMPOSE ?= docker compose

.PHONY: up down ps logs seed test load acceptance smoke clean help

help:
	@echo "Targets: up down ps logs <svc> seed test load acceptance smoke clean"

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

seed: ## Seed 20 users + reset their balances (idempotent; needs postgres up)
	$(COMPOSE) run --rm seed
	@echo "seed OK — 20 users in ledger.accounts (A2..A21); A1 (test) untouched"

test: ## Run shared unit tests
	python -m pytest -q

load: ## Load test @ 50 TPS / 60s (phase 6)
	@echo "load: implemented in phase 6 (mock-acquirer burst)."

acceptance: ## Acceptance: 7 checks §7 TЗ v2 (phase 8)
	bash scripts/acceptance.sh

smoke: ## Phase 1 smoke: healthz x8 + topics + PG + redis + Debezium + Prometheus
	bash scripts/smoke_env.sh

clean: ## Stop + remove volumes
	$(COMPOSE) down -v --remove-orphans
