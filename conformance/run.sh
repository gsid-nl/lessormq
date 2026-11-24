#!/usr/bin/env bash
# Starts a LessorMQ server with MariaDB and Redis (compose.yaml), runs the conformance
# scenarios against it through the reference HTTP adapter, and tears it all down
# (with-server.sh).
#
#   conformance/run.sh [filter]     e.g. conformance/run.sh lease
#   LESSORMQ_ADAPTER=clients/php/tests/Conformance/adapter.php conformance/run.sh
#                                   the same scenarios through the PHP client
#   LESSORMQ_DATABASE=postgresql conformance/run.sh
#                                   the server on another database: mariadb (the default),
#                                   mysql, postgresql or sqlite
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -f vendor/autoload.php ]; then
    echo "Run composer install first (the runner reads the scenarios with symfony/yaml)." >&2
    exit 2
fi

exec conformance/with-server.sh php conformance/php/run.php "$@"
