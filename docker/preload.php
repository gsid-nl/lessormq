<?php

/**
 * opcache.preload entry point.
 *
 * The deployment does not run the app from the image path: an init container copies
 * /app into a shared emptyDir at /var/www/html and re-runs cache:clear there, so the
 * compiled container that actually serves requests lives under /var/www/html. Preloading
 * the /app copy would warm a second set of the same class names from different files.
 *
 * Prefer the mounted root, fall back to the image root for plain `docker run` use.
 */
foreach (['/var/www/html', '/app'] as $root) {
    $container = $root . '/var/cache/prod/App_KernelProdContainer.preload.php';

    if (file_exists($container)) {
        require $container;

        return;
    }
}
