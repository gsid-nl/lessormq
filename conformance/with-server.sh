#!/usr/bin/env bash
# Starts a LessorMQ server of this working tree with MariaDB and Redis (compose.yaml), runs
# a command with LESSORMQ_URL and LESSORMQ_OPERATOR_KEY set, and removes it all again.
# The conformance suite runs this way (run.sh), and so do the end-to-end tests of the
# framework integrations; the command runs in the current directory.
#
#   conformance/with-server.sh php conformance/php/run.php
#   (cd clients/rails && ../../conformance/with-server.sh bundle exec rake e2e)
#   LESSORMQ_DATABASE=postgresql conformance/with-server.sh ...
#                       the server on another database: mariadb (the default), mysql,
#                       postgresql or sqlite
#   LESSORMQ_PORT=18081 the port on 127.0.0.1 (default 18080)
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
project=lessormq-conformance
database=${LESSORMQ_DATABASE:-mariadb}
files=(-f "$root/compose.yaml")

case $database in
    mariadb) ;;
    mysql | postgresql | sqlite) files+=(-f "$root/compose.$database.yaml") ;;
    *) echo "LESSORMQ_DATABASE is mariadb, mysql, postgresql or sqlite, not $database." >&2; exit 2 ;;
esac

export LESSORMQ_PORT=${LESSORMQ_PORT:-18080}
# Lease scenarios wait for expired leases to be ended; do that every second.
export LEASE_SWEEP_SECONDS=1

compose() { docker compose -p "$project" "${files[@]}" "$@"; }
trap 'compose down -v --remove-orphans >/dev/null 2>&1' EXIT

# --build: the image is the server of this working tree.
compose up -d --wait --quiet-pull --build
key=$(compose exec -T app /usr/local/bin/lessormq operator-key:create --name=conformance | tail -n 1)

LESSORMQ_URL="http://127.0.0.1:$LESSORMQ_PORT" LESSORMQ_OPERATOR_KEY="$key" "$@"
