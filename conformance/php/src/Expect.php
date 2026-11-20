<?php

declare(strict_types=1);

namespace LessorMQ\Conformance;

/**
 * Checks a step's result against its `expect` block. Keys are dotted paths into the
 * result ({status, body, elapsed}); values are literals or one matcher object:
 *
 *   not: <value>                  present, and anything but this value
 *   between: [low, high]          a number in the range, inclusive
 *   seconds_from_now: [low, high] an RFC 3339 time that many seconds from now
 *   matches: <regex>              a string matching the pattern
 *   exists: true|false            the path is (not) there
 *   keys: [a, b]                  an object with exactly these keys, in any order
 */
final class Expect
{
    private function __construct()
    {
    }

    /**
     * @param array<string, mixed> $expectations
     * @param array<string, mixed> $result
     * @return list<string> one line per failed expectation
     */
    public static function check(array $expectations, array $result): array
    {
        $failures = [];

        foreach ($expectations as $path => $expected) {
            [$found, $actual] = self::dig($result, (string) $path);
            $problem = self::compare($expected, $found, $actual);

            if ($problem !== null) {
                $failures[] = sprintf('%s: %s', $path, $problem);
            }
        }

        return $failures;
    }

    /**
     * @return array{0: bool, 1: mixed} whether the path exists, and its value
     */
    public static function dig(mixed $value, string $path): array
    {
        foreach (explode('.', $path) as $segment) {
            if (!is_array($value) || !array_key_exists($segment, $value)) {
                return [false, null];
            }

            $value = $value[$segment];
        }

        return [true, $value];
    }

    private static function compare(mixed $expected, bool $found, mixed $actual): ?string
    {
        if (is_array($expected) && count($expected) === 1 && !array_is_list($expected)) {
            $matcher = array_key_first($expected);
            $argument = $expected[$matcher];

            switch ($matcher) {
                case 'not':
                    return $found && $actual != $argument
                        ? null
                        : sprintf('expected anything but %s', self::show($argument));
                case 'exists':
                    return $found === (bool) $argument ? null : ($found ? 'present' : 'missing');
                case 'between':
                    return $found && is_numeric($actual) && $actual >= $argument[0] && $actual <= $argument[1]
                        ? null
                        : sprintf(
                            'expected between %s and %s, got %s',
                            $argument[0],
                            $argument[1],
                            self::show($actual),
                        );
                case 'seconds_from_now':
                    $time = is_string($actual) ? strtotime($actual) : false;
                    $offset = $time === false ? null : $time - time();

                    return $offset !== null && $offset >= $argument[0] && $offset <= $argument[1]
                        ? null
                        : sprintf(
                            'expected %s to %s s from now, got %s',
                            $argument[0],
                            $argument[1],
                            self::show($actual),
                        );
                case 'matches':
                    return is_string($actual) && preg_match('/' . $argument . '/', $actual) === 1
                        ? null
                        : sprintf('expected to match /%s/, got %s', $argument, self::show($actual));
                case 'keys':
                    $keys = is_array($actual) ? array_map('strval', array_keys($actual)) : null;
                    $want = array_map('strval', $argument);
                    sort($want);

                    if ($keys !== null) {
                        sort($keys);
                    }

                    return $keys === $want
                        ? null
                        : sprintf('expected keys %s, got %s', self::show($want), self::show($keys));
            }
        }

        if (!$found) {
            return 'missing, expected ' . self::show($expected);
        }

        return $actual == $expected && gettype($actual) === gettype($expected)
            || (is_int($actual) && is_float($expected) && (float) $actual === $expected)
            ? null
            : sprintf('expected %s, got %s', self::show($expected), self::show($actual));
    }

    private static function show(mixed $value): string
    {
        return json_encode($value, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) ?: var_export($value, true);
    }
}
