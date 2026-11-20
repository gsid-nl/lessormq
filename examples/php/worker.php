<?php

// Publish three messages and work them off with the LessorMQ worker.
// LESSORMQ_URL and LESSORMQ_KEY (publish and consume scopes); composer install first.

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use LessorMQ\Client;
use LessorMQ\Job;
use LessorMQ\Message;
use LessorMQ\Worker;
use LessorMQ\WorkerOptions;

$client = new Client(getenv('LESSORMQ_URL') ?: 'http://127.0.0.1:18080', getenv('LESSORMQ_KEY'));
$queue = getenv('LESSORMQ_QUEUE') ?: 'examples';

foreach ([['n' => 1], ['n' => 2, 'fail_once' => true], ['n' => 3]] as $body) {
    printf("published %s\n", $client->publish($queue, $body)->id);
}

$handled = 0;
$worker = null;
$worker = new Worker($client, $queue, function (Message $message, Job $job) use (&$handled, &$worker): void {
    $n = $message->body['n'];

    if (($message->body['fail_once'] ?? false) && $message->attempt === 1) {
        printf("n=%d attempt=%d failed\n", $n, $message->attempt);
        throw new RuntimeException('temporarily unavailable');
    }

    printf("n=%d attempt=%d handled\n", $n, $message->attempt);

    if (++$handled === 3) {
        $worker->stop();
    }
}, new WorkerOptions(consumer: 'php-example', waitSeconds: 5, maxSeconds: 60));

$worker->run();
printf("done: %d handled\n", $handled);
exit($handled === 3 ? 0 : 1);
