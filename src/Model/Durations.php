<?php

declare(strict_types=1);

namespace App\Model;

/**
 * How long something took for a set of messages (waiting for a worker, or running), kept
 * as a histogram over fixed classes rather than every value, so the database can group
 * them and hand over a few rows instead of a row per message.
 *
 * The average and the maximum are exact. The median and the 95th percentile are read from
 * the classes, interpolated within the class they fall in, so they are as precise as the
 * classes are narrow: to the second below 2 seconds, within the class above.
 *
 * Times are stored to the second, so a duration is a whole number of seconds and work that
 * takes less than a second mostly reads 0. The average stays true all the same: a start
 * and an end each fall at a random point within their second.
 */
final class Durations
{
    /** The upper bounds (exclusive) of the classes, in seconds; the last class has none. */
    public const BOUNDS = [1, 2, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 7200, 21600, 86400];

    /** @var array<int, int> class => count */
    private array $counts = [];

    private int $count = 0;

    private float $sum = 0.0;

    private ?int $max = null;

    /**
     * The class of the whole seconds $seconds (an SQL expression), as an SQL expression:
     * 0 below the first bound, count(BOUNDS) from the last on.
     */
    public static function classSql(string $seconds): string
    {
        $cases = '';

        foreach (self::BOUNDS as $class => $bound) {
            $cases .= " WHEN {$seconds} < {$bound} THEN {$class}";
        }

        return 'CASE' . $cases . ' ELSE ' . count(self::BOUNDS) . ' END';
    }

    /** Take in $count durations of one class, with their sum and their maximum. */
    public function add(int $class, int $count, float $sum, int $max): void
    {
        if ($count <= 0) {
            return;
        }

        $class = max(0, min(count(self::BOUNDS), $class));
        $this->counts[$class] = ($this->counts[$class] ?? 0) + $count;
        $this->count += $count;
        $this->sum += $sum;
        $this->max = $this->max === null ? $max : max($this->max, $max);
    }

    public function merge(self $other): void
    {
        foreach ($other->counts as $class => $count) {
            $this->counts[$class] = ($this->counts[$class] ?? 0) + $count;
        }

        $this->count += $other->count;
        $this->sum += $other->sum;

        if ($other->max !== null) {
            $this->max = $this->max === null ? $other->max : max($this->max, $other->max);
        }
    }

    public function count(): int
    {
        return $this->count;
    }

    /**
     * Seconds at which a share $q (0 to 1) of the durations is done, interpolated within its
     * class and never above the maximum; null without durations.
     */
    public function percentile(float $q): ?float
    {
        if ($this->count === 0 || $this->max === null) {
            return null;
        }

        $target = $q * $this->count;
        $before = 0;
        ksort($this->counts);

        foreach ($this->counts as $class => $count) {
            if ($before + $count >= $target) {
                $lower = $class === 0 ? 0 : self::BOUNDS[$class - 1];
                $upper = self::BOUNDS[$class] ?? max($lower, $this->max);
                $value = $lower + ($upper - $lower) * (($target - $before) / $count);

                return round(max(0, min($value, $this->max)), 1);
            }

            $before += $count;
        }

        return (float) $this->max;
    }

    /**
     * @return array{avg: float, p50: float, p95: float, max: int}|null null without durations
     */
    public function view(): ?array
    {
        if ($this->count === 0 || $this->max === null) {
            return null;
        }

        return [
            'avg' => round(max(0, $this->sum / $this->count), 2),
            'p50' => (float) $this->percentile(0.5),
            'p95' => (float) $this->percentile(0.95),
            'max' => max(0, $this->max),
        ];
    }
}
