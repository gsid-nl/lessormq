#!/usr/bin/env bash
# Runs every example against a server from compose.yaml, each on its own queue with a
# zero backoff so the failed attempt comes back at once.
set -euo pipefail
cd "$(dirname "$0")/.."

export LESSORMQ_PORT=${LESSORMQ_PORT:-18080} LEASE_SWEEP_SECONDS=1
export LESSORMQ_URL="http://127.0.0.1:$LESSORMQ_PORT"
compose() { docker compose -p lessormq-examples -f compose.yaml "$@"; }
trap 'compose down -v --remove-orphans >/dev/null 2>&1' EXIT
compose up -d --wait --quiet-pull --build >/dev/null 2>&1

operator=$(compose exec -T app /usr/local/bin/lessormq operator-key:create --name=examples | tail -n 1)
export LESSORMQ_KEY=$(curl -sS -X POST "$LESSORMQ_URL/v1/tenants" -H "Authorization: Bearer $operator" \
  -H 'Content-Type: application/json' -d '{"name": "Examples"}' | jq -r .admin_key.secret)

failed=0
run() {
  local name=$1; shift
  export LESSORMQ_QUEUE="examples-$name"
  curl -sS -o /dev/null -X PATCH "$LESSORMQ_URL/v1/queues/$LESSORMQ_QUEUE" -H "Authorization: Bearer $LESSORMQ_KEY" \
    -H 'Content-Type: application/json' -d '{"backoff": {"initial_seconds": 0, "jitter": 0}}'
  if output=$("$@" 2>&1); then
    echo "ok - $name: $(echo "$output" | tail -n 1)"
  else
    failed=1
    echo "not ok - $name"
    echo "$output" | sed 's/^/  # /'
  fi
}

run curl env BACKOFF_WAIT=1 examples/curl/quickstart.sh
(cd examples/php && composer install -q --no-interaction 2>/dev/null)
run php php examples/php/worker.php
python3 -m venv examples/python/.venv && examples/python/.venv/bin/pip install -q -e clients/python
run python examples/python/.venv/bin/python examples/python/worker.py
(cd clients/typescript && npm install -s --no-audit --no-fund && npm run -s build) >/dev/null
(cd examples/typescript && npm install -s --no-audit --no-fund --install-links) >/dev/null
run typescript node examples/typescript/worker.ts
run go sh -c 'cd examples/go && go run .'
exit $failed
