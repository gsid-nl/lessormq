<?php

declare(strict_types=1);

namespace App\Model;

use DateTimeImmutable;

/**
 * A queue's effective settings at this moment: product defaults, then the tenant's
 * defaults, then the queue's own settings. Pausing is a state with a time, not a plain
 * setting, so it lives beside them.
 */
final class QueueConfig
{
    /**
     * @param array<string, mixed> $settings resolved, see QueueSettings::DEFAULTS
     */
    public function __construct(
        public readonly array $settings,
        public readonly ?DateTimeImmutable $pausedAt,
        public readonly bool $exists,
    ) {
    }

    public function isPaused(): bool
    {
        return $this->pausedAt !== null;
    }

    public function int(string $name): int
    {
        return (int) $this->settings[$name];
    }

    /**
     * @return array<string, mixed> the settings as the API shows them
     */
    public function view(): array
    {
        $settings = $this->settings;
        $settings['paused'] = $this->isPaused();

        return $settings;
    }
}
