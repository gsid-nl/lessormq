# LessorMQ: the server as php-fpm, for nginx (or any FastCGI server) in front: claims on
# port 9001, every other call on port 9000 (docker/claims.conf, docs/self-hosting.md).
#
#   docker build -f docker/Dockerfile -t lessormq .
#
# The base stage is PHP with the extensions LessorMQ needs and Composer; compose.yaml
# runs the working tree on it for local use and the conformance suite. The PDO drivers
# cover every database LessorMQ runs on: MariaDB and MySQL, PostgreSQL, and SQLite (which
# the PHP image has built in).

FROM php:8.4-fpm-alpine AS base

RUN apk add --no-cache libpq \
    && apk add --no-cache --virtual .build-deps postgresql-dev \
    && docker-php-ext-install pdo_mysql pdo_pgsql opcache \
    && apk del .build-deps \
    && rm -rf /tmp/* /usr/src/php*

COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

FROM base AS server

LABEL org.opencontainers.image.title="LessorMQ" \
      org.opencontainers.image.description="Message queue over HTTP where the server decides what happens to every message" \
      org.opencontainers.image.source="https://github.com/gsid-nl/lessormq" \
      org.opencontainers.image.licenses="Apache-2.0"

ENV APP_ENV=prod \
    APP_DEBUG=0

COPY ./docker/queue.ini /usr/local/etc/php/conf.d/zz-queue.ini
COPY ./docker/preload.php /usr/local/etc/php/preload.php
COPY ./docker/www.conf /usr/local/etc/php-fpm.d/www.conf
COPY ./docker/claims.conf /usr/local/etc/php-fpm.d/zz-claims.conf
COPY ./docker/docker-entrypoint.sh /

WORKDIR /app
COPY . /app

RUN chmod +x /docker-entrypoint.sh \
    && composer install --no-scripts --no-dev --optimize-autoloader --no-interaction \
    && composer dump-env prod \
    && php bin/console cache:warmup --env=prod --no-debug \
    && mkdir -p /var/www/html \
    && chown -R 1000:1000 /app /var/www/html

USER 1000
EXPOSE 9000
ENTRYPOINT ["/docker-entrypoint.sh"]
