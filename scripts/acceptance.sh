#!/usr/bin/env bash
# Acceptance entry point (phase 8) — the 7 criteria of §7 TЗ v2.
#
# The implementation lives in scripts/acceptance.py: the checks need JSON/DB/
# Kafka inspection and POLLING with a timeout, which is far more readable in
# Python than in a shell pipeline (and the machine may not have `jq`).
#
#   ./scripts/acceptance.sh              # recreate the stack, run all checks
#   ./scripts/acceptance.sh --skip-setup # use the running stack
set -uo pipefail   # no -e: we want to collect ALL results, not stop at the first failure

cd "$(dirname "$0")/.."

if command -v python >/dev/null 2>&1; then
  PY=python
elif command -v python3 >/dev/null 2>&1; then
  PY=python3
else
  echo "acceptance: python is required" >&2
  exit 2
fi

exec "$PY" scripts/acceptance.py "$@"
