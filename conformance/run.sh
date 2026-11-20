#!/usr/bin/env bash
# Starts a LessorMQ server with MariaDB and Redis (compose.yaml), runs the conformance
# scenarios against it through the reference HTTP adapter, and tears it all down.
#
#   conformance/run.sh [filter]     e.g. conformance/run.sh lease
#   LESSORMQ_ADAPTER=clients/php/tests/Conformance/adapter.php conformance/run.sh
#                                   the same scenarios through the PHP client
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -f vendor/autoload.php ]; then
    echo "Run composer install first." >&2
    exit 2
fi

project=lessormq-conformance
export LESSORMQ_PORT=${LESSORMQ_PORT:-18080}
# Lease scenarios wait for expired leases to be ended; do that every second.
export LEASE_SWEEP_SECONDS=1

compose() { docker compose -p "$project" -f compose.yaml "$@"; }
trap 'compose down -v --remove-orphans >/dev/null 2>&1' EXIT

compose up -d --wait --quiet-pull
key=$(compose exec -T app bin/console lessormq:operator-key:create --name=conformance | tail -n 1)

LESSORMQ_URL="http://127.0.0.1:$LESSORMQ_PORT" LESSORMQ_OPERATOR_KEY="$key" php conformance/php/run.php "$@"
