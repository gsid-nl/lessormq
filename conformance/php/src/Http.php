<?php

declare(strict_types=1);

namespace LessorMQ\Conformance;

/**
 * Just enough HTTP for the suite: JSON in, JSON out, every status returned as data.
 */
final class Http
{
    public function __construct(private readonly string $baseUrl)
    {
    }

    /**
     * @param array<string, string> $headers
     * @return array{status: int, body: mixed, headers: array<string, string>}
     */
    public function request(string $method, string $path, mixed $body = null, array $headers = []): array
    {
        $curl = curl_init(rtrim($this->baseUrl, '/') . $path);
        $responseHeaders = [];
        $lines = ['Accept: application/json'];

        foreach ($headers as $name => $value) {
            $lines[] = $name . ': ' . $value;
        }

        if ($body !== null) {
            $lines[] = 'Content-Type: application/json';
            curl_setopt($curl, CURLOPT_POSTFIELDS, json_encode($body, JSON_UNESCAPED_SLASHES));
        }

        curl_setopt_array($curl, [
            CURLOPT_CUSTOMREQUEST => $method,
            CURLOPT_HTTPHEADER => $lines,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT => 60,
            CURLOPT_HEADERFUNCTION => static function ($curl, string $line) use (&$responseHeaders): int {
                $parts = explode(':', $line, 2);

                if (count($parts) === 2) {
                    $responseHeaders[strtolower(trim($parts[0]))] = trim($parts[1]);
                }

                return strlen($line);
            },
        ]);

        $raw = curl_exec($curl);

        if ($raw === false) {
            throw new \RuntimeException(sprintf('%s %s failed: %s', $method, $path, curl_error($curl)));
        }

        $status = (int) curl_getinfo($curl, CURLINFO_RESPONSE_CODE);
        $decoded = $raw === '' ? null : json_decode((string) $raw, true);

        return ['status' => $status, 'body' => $decoded ?? $raw, 'headers' => $responseHeaders];
    }
}
