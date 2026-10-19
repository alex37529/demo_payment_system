#!/usr/bin/env bash
# Phase 5 seed (read-side) — re-runnable, idempotent.
#
#   Fresh `make up` runs init/01_databases.sql at first postgres boot and has
#   already seeded the 20 users into ledger.accounts + profile.users. This
#   script only *verifies* that (or reports where the seed went).
set -euo pipefail

echo "seed: verifying 20 seeded users (init/01_databases.sql runs on first boot)"
L=$(docker compose exec -T postgres psql -U fintech -d ledger -tAc \
     "SELECT count(*) FROM accounts WHERE id LIKE 'U0%'")
P=$(docker compose exec -T postgres psql -U fintech -d profile -tAc \
     "SELECT count(*) FROM profile.users")
echo "  ledger.accounts : $L"
echo "  profile.users   : $P"
if [[ "${L:-0}" != "20" || "${P:-0}" != "20" ]]; then
  echo "seed: WARN — expected 20 in both. If fresh, re-run with a clean volume:" >&2
  echo "         docker compose down -v  # removes the pgdata volume" >&2
  echo "         docker compose up -d --build" >&2
  exit 1
fi
echo "seed OK — 20 users seeded consistently on write- and read-side."
