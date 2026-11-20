#!/bin/sh
# Without arguments: PHP-FPM. With arguments, that command instead, such as
# `bin/console doctrine:migrations:migrate --no-interaction` before a new version starts.
if [ "$#" -eq 0 ]; then
    exec php-fpm --nodaemonize
fi
exec "$@"
