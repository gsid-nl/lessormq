<?php

declare(strict_types=1);

namespace App\Tests\Support;

use Predis\Client;
use Predis\Command\FactoryInterface;
use Predis\Command\RedisFactory;

/**
 * In-memory stand-in for the few Redis commands the app uses, so services can be tested
 * against real behaviour (expiry aside) instead of a mock per call.
 */
final class FakeRedis extends Client
{
    /** @var array<string, string> */
    public array $data = [];

    /** @var array<string, int> key => TTL in seconds */
    public array $ttl = [];

    /** @var list<string> commands in the order they ran */
    public array $log = [];

    public bool $down = false;

    public function __construct()
    {
    }

    public function getCommandFactory(): FactoryInterface
    {
        return new RedisFactory();
    }

    public function __call($commandID, $arguments)
    {
        $command = strtolower($commandID);
        $this->log[] = $command;

        if ($this->down) {
            throw new \RuntimeException('Connection refused');
        }

        switch ($command) {
            case 'get':
                return $this->data[$arguments[0]] ?? null;
            case 'mget':
                return array_map(fn(string $key) => $this->data[$key] ?? null, $arguments[0]);
            case 'set':
                [$key, $value] = $arguments;
                $options = array_map('strtoupper', array_filter(array_slice($arguments, 2), 'is_string'));

                if (in_array('NX', $options, true) && isset($this->data[$key])) {
                    return null;
                }

                $this->data[$key] = (string) $value;
                $exAt = array_search('EX', array_map(
                    static fn($argument) => is_string($argument) ? strtoupper($argument) : $argument,
                    $arguments,
                ), true);

                if ($exAt !== false) {
                    $this->ttl[$key] = (int) $arguments[$exAt + 1];
                }

                return 'OK';
            case 'del':
                $keys = is_array($arguments[0]) ? $arguments[0] : $arguments;
                $deleted = 0;

                foreach ($keys as $key) {
                    if (isset($this->data[$key])) {
                        unset($this->data[$key], $this->ttl[$key]);
                        $deleted++;
                    }
                }

                return $deleted;
            case 'scan':
                $pattern = $arguments[1]['MATCH'] ?? '*';

                return [0, array_values(array_filter(
                    array_keys($this->data),
                    static fn(string $key) => fnmatch($pattern, $key),
                ))];
            case 'hincrby':
                [$key, $field, $by] = $arguments;
                $hash = json_decode($this->data[$key] ?? '{}', true);
                $hash[$field] = ($hash[$field] ?? 0) + $by;
                $this->data[$key] = json_encode($hash);

                return $hash[$field];
            case 'hgetall':
                return json_decode($this->data[$arguments[0]] ?? '{}', true);
            case 'ping':
                return 'PONG';
        }

        throw new \LogicException('FakeRedis does not implement ' . $commandID);
    }
}
