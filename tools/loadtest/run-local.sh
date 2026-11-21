#!/usr/bin/env bash
# Loads a local installation shaped like gsid's (compose.yaml) with the scenarios of
# tools/loadtest/README.md and removes it afterwards.
#
#   tools/loadtest/run-local.sh [scenario ...]    normal, tenfold, burst, waiting, overload
set -euo pipefail
cd "$(dirname "$0")"

export LOADTEST_PORT=${LOADTEST_PORT:-18690}
c() { docker compose -p lessormq-loadtest -f compose.yaml "$@"; }
# KEEP=1 leaves the installation running, to look at its logs.
[ "${KEEP:-}" = 1 ] || trap 'c down -v --remove-orphans >/dev/null 2>&1' EXIT

c up -d --wait --build --quiet-pull >/dev/null
c exec -T app1 bin/console doctrine:migrations:migrate --no-interaction --allow-no-migration >/dev/null
operator=$(c exec -T app1 bin/console lessormq:operator-key:create --name=loadtest | tail -n 1)
url=http://127.0.0.1:$LOADTEST_PORT
key=$(curl -sf -X POST "$url/v1/tenants" -H "Authorization: Bearer $operator" -H 'Content-Type: application/json' \
    -d '{"id":"loadtest","name":"Load test"}' | php -r 'echo json_decode(stream_get_contents(STDIN))->admin_key->secret;')

go build -o "${TMPDIR:-/tmp}/lessormq-loadtest" .
lt() { "${TMPDIR:-/tmp}/lessormq-loadtest" -url "$url" -key "$key" "$@"; }

# Today's peak (FOS and the hosted tenants together, 2026-11-21): about 2,000 messages in
# a minute, 33 a second; FOS runs 21 consumers. DURATION shortens or lengthens each run.
duration=${DURATION:-2m}

for scenario in "${@:-normal tenfold burst waiting overload}"; do
  for s in $scenario; do
    # The waiting claims of the scenario before end on the server only when their wait does.
    sleep 12
    echo
    echo "=== $s"
    case $s in
      normal)   lt -rate 33 -workers 21 -wait 2 -duration "$duration" || true ;;
      tenfold)  lt -rate 330 -publishers 40 -workers 40 -wait 2 -duration "$duration" || true ;;
      burst)    lt -burst 10000 -publishers 50 -workers 40 -wait 2 || true ;;
      waiting)  lt -rate 33 -workers 60 -wait 10 -duration "$duration" || true ;;
      overload) lt -rate 0 -publishers 100 -workers 100 -wait 2 -duration "$duration" || true ;;
      *) echo "unknown scenario $s" >&2; exit 2 ;;
    esac
  done
done
