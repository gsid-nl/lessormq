<?php

declare(strict_types=1);

namespace LessorMQ\Conformance;

/**
 * An adapter in another language: a process that reads one JSON request per line on
 * stdin and writes one JSON result per line on stdout, so a library in any language
 * passes the suite without a runner of its own.
 *
 *   in:  {"call": "claim", "api_key": "lmq_...", "args": {"queue": "...", "wait_seconds": 1}}
 *   out: {"status": 200, "body": {...}}
 *
 * The process runs for the whole suite, with this runner's environment (LESSORMQ_URL
 * included). Anything it writes to stderr shows up in the runner's output.
 */
final class ProcessAdapter implements Adapter
{
    /** @var resource */
    private $process;

    /** @var resource */
    private $stdin;

    /** @var resource */
    private $stdout;

    public function __construct(private readonly string $command)
    {
        $pipes = [];
        $process = proc_open($command, [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => STDERR], $pipes);

        if (!is_resource($process)) {
            throw new \RuntimeException('could not start the adapter: ' . $command);
        }

        $this->process = $process;
        $this->stdin = $pipes[0];
        $this->stdout = $pipes[1];
    }

    public function __destruct()
    {
        fclose($this->stdin);
        fclose($this->stdout);
        proc_close($this->process);
    }

    public function call(string $call, string $apiKey, array $args): array
    {
        $request = json_encode(
            ['call' => $call, 'api_key' => $apiKey, 'args' => (object) $args],
            JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR,
        );
        fwrite($this->stdin, $request . "\n");
        fflush($this->stdin);

        $line = fgets($this->stdout);

        if ($line === false) {
            throw new \RuntimeException(sprintf('the adapter (%s) stopped answering', $this->command));
        }

        $result = json_decode($line, true);

        if (!is_array($result) || !is_int($result['status'] ?? null)) {
            throw new \RuntimeException('the adapter answered something else than {status, body}: ' . trim($line));
        }

        return ['status' => $result['status'], 'body' => $result['body'] ?? null];
    }

    public function __toString(): string
    {
        return $this->command;
    }
}
