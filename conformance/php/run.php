<?php

declare(strict_types=1);

/*
 * Runs the conformance scenarios against a LessorMQ server through the reference
 * HTTP adapter.
 *
 *   LESSORMQ_URL=http://127.0.0.1:18080 LESSORMQ_OPERATOR_KEY=lmq_... php conformance/php/run.php [filter]
 *
 * LESSORMQ_ADAPTER=<file> runs the scenarios through a PHP client library instead: the
 * file returns fn(string $url): Adapter (see clients/php/tests/Conformance/adapter.php).
 * LESSORMQ_ADAPTER_CMD=<command> runs them through a library in any other language: the
 * command answers one JSON request per line (see src/ProcessAdapter.php).
 *
 * conformance/run.sh starts a server in docker compose and does all of this for you.
 */

require __DIR__ . '/../../vendor/autoload.php';

foreach (glob(__DIR__ . '/src/*.php') as $file) {
    require_once $file;
}

use LessorMQ\Conformance\Http;
use LessorMQ\Conformance\HttpAdapter;
use LessorMQ\Conformance\ProcessAdapter;
use LessorMQ\Conformance\Runner;

$url = getenv('LESSORMQ_URL') ?: 'http://127.0.0.1:18080';
$operatorKey = getenv('LESSORMQ_OPERATOR_KEY');

if (!$operatorKey) {
    fwrite(STDERR, "Set LESSORMQ_OPERATOR_KEY to an operator key of the server under test.\n");
    exit(2);
}

$http = new Http($url);
$adapterFile = getenv('LESSORMQ_ADAPTER');
$adapterCommand = getenv('LESSORMQ_ADAPTER_CMD');
$adapter = match (true) {
    (bool) $adapterCommand => new ProcessAdapter($adapterCommand),
    (bool) $adapterFile => (require $adapterFile)($url),
    default => new HttpAdapter($http),
};
fwrite(STDERR, sprintf(
    "# adapter: %s\n",
    $adapter instanceof ProcessAdapter ? 'process ' . $adapterCommand : $adapter::class,
));
$runner = new Runner($http, $operatorKey, $adapter, $url);
$files = glob(__DIR__ . '/../scenarios/*.yaml');
sort($files);

exit($runner->run($files, $argv[1] ?? null) ? 0 : 1);
