<?php

declare(strict_types=1);

namespace App\Service;

use App\Api\ApiProblem;
use App\Api\Input;
use App\Model\QueueConfig;
use App\Model\QueueSettings;
use App\Model\Quotas;
use App\Repository\QueueRepository;
use App\Time\Utc;
use Predis\Client as RedisClient;
use Predis\Collection\Iterator\Keyspace;
use Psr\Log\LoggerInterface;

/**
 * The effective settings of a queue, as the server applies them.
 *
 * Read on every claim, so they are cached in Redis for a minute and memoised per
 * request: a poll costs one Redis round trip, as the pause check before it did, and no
 * extra database query. Every change goes through here and drops the cached copy.
 */
class QueueSettingsService
{
    private const CACHE_PREFIX = 'queue_settings:';
    private const CACHE_TTL = 60;

    /** @var array<string, QueueConfig> keyed by tenant and queue */
    private array $memo = [];

    public function __construct(
        private readonly QueueRepository $queues,
        private readonly RedisClient $redis,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function forQueue(string $tenant, string $queue): QueueConfig
    {
        return $this->forQueues($tenant, [$queue])[$queue];
    }

    /**
     * @param list<string> $queues
     * @return array<string, QueueConfig> keyed by queue name
     */
    public function forQueues(string $tenant, array $queues): array
    {
        $result = [];
        $missing = [];

        foreach (array_unique($queues) as $queue) {
            if (isset($this->memo[$this->memoKey($tenant, $queue)])) {
                $result[$queue] = $this->memo[$this->memoKey($tenant, $queue)];
            } else {
                $missing[] = $queue;
            }
        }

        if ($missing === []) {
            return $result;
        }

        $keys = array_map(fn(string $queue) => $this->cacheKey($tenant, $queue), $missing);

        $cacheAvailable = true;

        try {
            $values = $this->redis->mget($keys);
        } catch (\Throwable $e) {
            $this->logger->warning('Queue settings cache unavailable', ['error' => $e->getMessage()]);
            $values = [];
            $cacheAvailable = false;
        }

        foreach ($missing as $index => $queue) {
            $entry = isset($values[$index]) ? json_decode($values[$index], true) : null;

            if (!is_array($entry)) {
                $entry = $this->resolve($tenant, $queue);

                if ($cacheAvailable) {
                    $this->store($tenant, $queue, $entry);
                }
            }

            $pausedAt = $entry['paused_at'] === null ? null : Utc::parse($entry['paused_at']);

            // An entry cached before quotas were part of it has none until it expires.
            $config = new QueueConfig($entry['settings'], $pausedAt, $entry['exists'], $entry['quotas'] ?? []);
            $this->memo[$this->memoKey($tenant, $queue)] = $config;
            $result[$queue] = $config;
        }

        return $result;
    }

    /**
     * The queue's settings as they are now, not as this request first read them: for a
     * claim that waited and was woken, perhaps by a resume.
     */
    public function reload(string $tenant, string $queue): QueueConfig
    {
        unset($this->memo[$this->memoKey($tenant, $queue)]);

        return $this->forQueue($tenant, $queue);
    }

    /**
     * Called on publish: a queue exists from its first message on, with the tenant's
     * defaults (and paused, if the tenant's defaults say new queues start paused).
     */
    public function ensureExists(string $tenant, string $queue): void
    {
        $config = $this->forQueue($tenant, $queue);

        if ($config->exists) {
            return;
        }

        $this->assertRoomForQueue($tenant, $config);
        $this->queues->createIfMissing(
            $tenant,
            $queue,
            $config->settings['paused'] ? Utc::sql(Utc::now()) : null,
        );
        $this->invalidate($tenant, $queue);
    }

    public function pause(string $tenant, string $queue): QueueConfig
    {
        return $this->apply($tenant, $queue, [], true);
    }

    public function resume(string $tenant, string $queue): QueueConfig
    {
        return $this->apply($tenant, $queue, [], false);
    }

    /**
     * PATCH /v1/queues/{queue}: any subset of the settings, `paused` included. Creates
     * the queue when it does not exist yet.
     */
    public function updateFromInput(string $tenant, string $queue, Input $input): QueueConfig
    {
        return $this->update($tenant, $queue, $input->all());
    }

    /**
     * @param array<string, mixed> $patch validated here
     */
    public function update(string $tenant, string $queue, array $patch): QueueConfig
    {
        $current = $this->forQueue($tenant, $queue);
        $errors = QueueSettings::validatePatch($patch, $current->settings);

        if ($errors === []) {
            $errors = Quotas::settingErrors($patch, $current->quotas);
        }

        if ($errors !== []) {
            throw ApiProblem::validation($errors);
        }

        if (!$current->exists) {
            $this->assertRoomForQueue($tenant, $current);
        }

        $paused = array_key_exists('paused', $patch) ? (bool) $patch['paused'] : null;
        unset($patch['paused']);

        return $this->apply($tenant, $queue, $patch, $paused);
    }

    /**
     * Drop every cached queue of a tenant, after its defaults changed.
     */
    public function invalidateTenant(string $tenant): void
    {
        foreach (array_keys($this->memo) as $key) {
            if (str_starts_with($key, $tenant . "\0")) {
                unset($this->memo[$key]);
            }
        }

        try {
            $keys = iterator_to_array(new Keyspace($this->redis, self::CACHE_PREFIX . $tenant . ':*', 500), false);

            if ($keys !== []) {
                $this->redis->del($keys);
            }
        } catch (\Throwable $e) {
            $this->logger->warning('Could not drop cached queue settings', [
                'tenant' => $tenant,
                'error' => $e->getMessage(),
            ]);
        }
    }

    /**
     * @param array<string, mixed> $patch settings to merge into the queue's own
     * @param bool|null            $paused null leaves the pause state as it is
     */
    private function apply(string $tenant, string $queue, array $patch, ?bool $paused): QueueConfig
    {
        $stored = $this->queues->load($tenant, $queue);
        $current = $this->forQueue($tenant, $queue);

        $pausedAt = match ($paused) {
            true => $current->pausedAt === null ? Utc::sql(Utc::now()) : Utc::sql($current->pausedAt),
            false => null,
            null => $current->pausedAt === null ? null : Utc::sql($current->pausedAt),
        };

        $this->queues->save($tenant, $queue, QueueSettings::merge($stored['settings'], $patch), $pausedAt);

        $this->invalidate($tenant, $queue);
        $this->logger->info('Queue settings changed', [
            'tenant' => $tenant,
            'queue' => $queue,
            'changed' => array_keys($patch),
            'paused' => $pausedAt !== null,
        ]);

        return $this->forQueue($tenant, $queue);
    }

    /**
     * A new queue only while the tenant has fewer than its max_queues. Counted in the
     * database: a queue is new only once.
     */
    private function assertRoomForQueue(string $tenant, QueueConfig $config): void
    {
        $max = $config->quota('max_queues');

        if ($max !== null && $this->queues->count($tenant) >= $max) {
            throw ApiProblem::quotaExceeded(sprintf('This tenant may have at most %d queues.', $max));
        }
    }

    /**
     * @return array{
     *     settings: array<string, mixed>, quotas: array<string, int|null>, paused_at: ?string, exists: bool
     * }
     */
    private function resolve(string $tenant, string $queue): array
    {
        $stored = $this->queues->load($tenant, $queue);
        $quotas = Quotas::resolve($stored['quotas'] ?? []);

        return [
            'settings' => Quotas::cap(QueueSettings::resolve($stored['defaults'], $stored['settings']), $quotas),
            'quotas' => $quotas,
            'paused_at' => $stored['paused_at'],
            'exists' => $stored['exists'],
        ];
    }

    /**
     * @param array{
     *     settings: array<string, mixed>, quotas: array<string, int|null>, paused_at: ?string, exists: bool
     * } $entry
     */
    private function store(string $tenant, string $queue, array $entry): void
    {
        try {
            $this->redis->set($this->cacheKey($tenant, $queue), json_encode($entry), 'EX', self::CACHE_TTL);
        } catch (\Throwable $e) {
            $this->logger->warning('Could not cache queue settings', ['error' => $e->getMessage()]);
        }
    }

    private function invalidate(string $tenant, string $queue): void
    {
        unset($this->memo[$this->memoKey($tenant, $queue)]);

        try {
            $this->redis->del([$this->cacheKey($tenant, $queue)]);
        } catch (\Throwable $e) {
            $this->logger->warning('Could not drop cached queue settings', ['error' => $e->getMessage()]);
        }
    }

    private function cacheKey(string $tenant, string $queue): string
    {
        return self::CACHE_PREFIX . $tenant . ':' . $queue;
    }

    private function memoKey(string $tenant, string $queue): string
    {
        return $tenant . "\0" . $queue;
    }
}
