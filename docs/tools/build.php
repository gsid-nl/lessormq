<?php

/*
 * Builds the two generated pages of the documentation:
 *
 *   api.md       from openapi.yaml
 *   examples.md  from tools/examples.template.md and the tested files in examples/
 *
 *   php docs/tools/build.php           write them
 *   php docs/tools/build.php --check   exit 1 when either is out of date (for CI)
 *
 * tools/ is not published: the CeeMS docs sync skips it.
 */

declare(strict_types=1);

require __DIR__ . '/../../vendor/autoload.php';

use Symfony\Component\Yaml\Yaml;

const SOURCE = 'https://github.com/gsid-nl/lessorMQ/blob/main/';

$docs = dirname(__DIR__);
$root = dirname($docs);
$pages = [
    'api.md' => apiReference(Yaml::parseFile($docs . '/openapi.yaml')),
    'examples.md' => examples((string) file_get_contents(__DIR__ . '/examples.template.md'), $root),
];
$check = in_array('--check', $argv, true);
$stale = [];

foreach ($pages as $name => $content) {
    $path = $docs . '/' . $name;

    if ((is_file($path) ? file_get_contents($path) : null) === $content) {
        continue;
    }

    if ($check) {
        $stale[] = $name;
    } else {
        file_put_contents($path, $content);
        echo "wrote docs/$name\n";
    }
}

if ($stale !== []) {
    fwrite(STDERR, 'Out of date: ' . implode(', ', $stale) . ". Run php docs/tools/build.php.\n");
    exit(1);
}

function examples(string $template, string $root): string
{
    $languages = ['sh' => 'bash', 'php' => 'php', 'py' => 'python', 'ts' => 'typescript', 'go' => 'go'];

    $include = static function (array $match) use ($root, $languages): string {
        $file = $root . '/' . $match[1];

        if (!is_file($file)) {
            throw new RuntimeException('examples.template.md includes a missing file: ' . $match[1]);
        }

        $language = $languages[pathinfo($file, PATHINFO_EXTENSION)] ?? '';

        return sprintf(
            "```%s\n%s```\n\n[%s](%s)",
            $language,
            rtrim((string) file_get_contents($file)) . "\n",
            $match[1],
            SOURCE . $match[1],
        );
    };

    return (string) preg_replace_callback('/^\{\{ (\S+) \}\}$/m', $include, $template);
}

/**
 * @param array<string, mixed> $spec
 */
function apiReference(array $spec): string
{
    $lines = [
        '---',
        'title: API reference',
        'description: "Every endpoint of the LessorMQ v1 API, with its requests, answers and errors."',
        '---',
        '',
        '# API reference',
        '',
        sprintf(
            '_Generated from [openapi.yaml](%sdocs/openapi.yaml), which is the source of truth._',
            SOURCE,
        ),
        '',
        str_replace('See `docs/protocol.md`', 'See [the protocol](protocol.md)', trim($spec['info']['description'])),
        '',
        '## Authentication',
        '',
        trim($spec['components']['securitySchemes']['apiKey']['description']),
        '',
    ];

    foreach ($spec['tags'] as $tag) {
        $lines[] = '## ' . $tag['name'];
        $lines[] = '';
        $lines[] = trim($tag['description']);
        $lines[] = '';

        foreach ($spec['paths'] as $path => $item) {
            foreach (['get', 'post', 'patch', 'put', 'delete'] as $method) {
                if (isset($item[$method]) && in_array($tag['name'], $item[$method]['tags'], true)) {
                    array_push($lines, ...operation($spec, $path, $method, $item[$method], $item['parameters'] ?? []));
                }
            }
        }
    }

    $lines[] = '## Schemas';
    $lines[] = '';

    foreach ($spec['components']['schemas'] as $name => $schema) {
        array_push($lines, ...schema($spec, $name, $schema));
    }

    return implode("\n", $lines) . "\n";
}

/**
 * @param array<string, mixed>       $spec
 * @param array<string, mixed>       $operation
 * @param list<array<string, mixed>> $shared parameters on the path
 * @return list<string>
 */
function operation(array $spec, string $path, string $method, array $operation, array $shared): array
{
    $lines = [
        '### ' . $operation['summary'],
        '',
        sprintf('`%s %s`', strtoupper($method), $path),
        '',
    ];

    if (isset($operation['description'])) {
        $lines[] = trim($operation['description']);
        $lines[] = '';
    }

    $parameters = array_map(
        static fn(array $p) => resolve($spec, $p),
        [...$shared, ...($operation['parameters'] ?? [])],
    );

    if ($parameters !== []) {
        $lines[] = '| Parameter | In | Type | Description |';
        $lines[] = '|---|---|---|---|';

        foreach ($parameters as $parameter) {
            $lines[] = sprintf(
                '| `%s`%s | %s | %s | %s |',
                $parameter['name'],
                ($parameter['required'] ?? false) ? ' (required)' : '',
                $parameter['in'],
                type($parameter['schema'] ?? []),
                cell($parameter['description'] ?? ''),
            );
        }

        $lines[] = '';
    }

    if (isset($operation['requestBody'])) {
        $body = $operation['requestBody']['content']['application/json']['schema'];
        $lines[] = sprintf(
            'Request body%s: %s',
            ($operation['requestBody']['required'] ?? false) ? '' : ' (optional)',
            type($body),
        );
        $lines[] = '';
    }

    $lines[] = '| Status | Meaning | Body |';
    $lines[] = '|---|---|---|';

    foreach ($operation['responses'] as $status => $response) {
        $response = resolve($spec, $response);
        $content = $response['content'] ?? [];
        $schema = ($content['application/json'] ?? $content['application/problem+json'] ?? [])['schema'] ?? null;
        $lines[] = sprintf(
            '| %s | %s | %s |',
            $status,
            cell($response['description']),
            $schema === null ? '' : type($schema),
        );
    }

    $lines[] = '';

    return $lines;
}

/**
 * @param array<string, mixed> $spec
 * @param array<string, mixed> $schema
 * @return list<string>
 */
function schema(array $spec, string $name, array $schema): array
{
    $lines = ['### ' . $name, ''];

    if (isset($schema['description'])) {
        $lines[] = trim($schema['description']);
        $lines[] = '';
    }

    $properties = $schema['properties'] ?? [];
    $required = $schema['required'] ?? [];

    foreach ($schema['allOf'] ?? [] as $part) {
        if (isset($part['$ref'])) {
            $lines[] = 'All fields of ' . type($part) . ', and:';
            $lines[] = '';
        } else {
            $properties += $part['properties'] ?? [];
            $required = [...$required, ...($part['required'] ?? [])];
        }
    }

    if ($properties === []) {
        $lines[] = 'Type: ' . type($schema);
        $lines[] = '';

        return $lines;
    }

    $lines[] = '| Field | Type | Description |';
    $lines[] = '|---|---|---|';

    foreach ($properties as $field => $property) {
        $default = array_key_exists('default', $property)
            ? ' Default: `' . json_encode($property['default'], JSON_UNESCAPED_SLASHES) . '`.'
            : '';
        $lines[] = sprintf(
            '| `%s`%s | %s | %s |',
            $field,
            in_array($field, $required, true) ? ' (required)' : '',
            type($property),
            cell(trim(($property['description'] ?? '') . $default)),
        );
    }

    $lines[] = '';

    return $lines;
}

/**
 * @param array<string, mixed> $schema
 */
function type(array $schema): string
{
    if (isset($schema['$ref'])) {
        $name = basename($schema['$ref']);

        return sprintf('[%s](#%s)', $name, strtolower($name));
    }

    if (isset($schema['oneOf'])) {
        return implode(' or ', array_map('type', $schema['oneOf']));
    }

    if (isset($schema['allOf'])) {
        return implode(' + ', array_map('type', $schema['allOf']));
    }

    if ($schema === [] || !isset($schema['type'])) {
        return 'any JSON value';
    }

    $type = is_array($schema['type']) ? implode(' or ', $schema['type']) : $schema['type'];

    if ($type === 'array') {
        $type = 'array of ' . type($schema['items'] ?? []);
    }

    if ($type === 'object' && isset($schema['additionalProperties']) && is_array($schema['additionalProperties'])) {
        $type = 'map of ' . type($schema['additionalProperties']);
    }

    if (isset($schema['format'])) {
        $type .= ' (' . $schema['format'] . ')';
    }

    if (isset($schema['enum'])) {
        $type .= ': ' . implode(', ', array_map(static fn($value) => '`' . $value . '`', $schema['enum']));
    }

    if (isset($schema['const'])) {
        $type .= ': `' . $schema['const'] . '`';
    }

    $limits = array_filter([
        isset($schema['minimum']) ? 'min ' . $schema['minimum'] : null,
        isset($schema['maximum']) ? 'max ' . $schema['maximum'] : null,
        isset($schema['maxLength']) ? 'up to ' . $schema['maxLength'] . ' characters' : null,
    ]);

    return $limits === [] ? $type : $type . ' (' . implode(', ', $limits) . ')';
}

/**
 * @param array<string, mixed> $spec
 * @param array<string, mixed> $node
 * @return array<string, mixed>
 */
function resolve(array $spec, array $node): array
{
    while (isset($node['$ref'])) {
        $target = $spec;

        foreach (explode('/', substr($node['$ref'], 2)) as $part) {
            $target = $target[$part];
        }

        $node = $target;
    }

    return $node;
}

function cell(string $text): string
{
    return str_replace(['|', "\n"], ['\|', ' '], trim($text));
}
