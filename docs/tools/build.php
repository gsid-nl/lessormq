<?php

/*
 * Builds the generated parts of the documentation:
 *
 *   api.md           from openapi.yaml
 *   examples.md      from tools/examples.template.md and the tested files in examples/
 *   self-hosting.md  its blocks that show a file the release tarball installs (the first
 *                    line names where it goes, such as # /etc/lessormq/lessormq.conf),
 *                    from that file in server/packaging
 *   clients/         a page per library from its README (which registries show too),
 *                    and the overview from tools/clients.template.md; every README
 *                    follows the outline of a language library or of a framework
 *                    integration (CLIENTS below)
 *
 *   php docs/tools/build.php           write them
 *   php docs/tools/build.php --check   exit 1 when one is out of date (for CI)
 *
 * tools/ is not published: the CeeMS docs sync skips it.
 */

declare(strict_types=1);

require __DIR__ . '/../../vendor/autoload.php';

use Symfony\Component\Yaml\Yaml;

const SOURCE = 'https://github.com/gsid-nl/lessormq/blob/main/';

/** The sections of a README, in this order; ### subsections are free. */
const OUTLINES = [
    'language' => ['Install', 'Connect', 'Publish', 'Work', 'Without the worker', 'Errors', 'Development'],
    'framework' => [
        'Install',
        'Configure',
        'Dispatch',
        'Run workers',
        'How it behaves',
        'Moving from another queue',
        'Development',
    ],
];

/** Every directory in clients/, with its outline, in the order of the overview. */
const CLIENTS = [
    'php' => 'language',
    'python' => 'language',
    'typescript' => 'language',
    'go' => 'language',
    'java' => 'language',
    'dotnet' => 'language',
    'ruby' => 'language',
    'symfony-messenger' => 'framework',
    'laravel' => 'framework',
    'spring-boot' => 'framework',
    'django' => 'framework',
    'celery' => 'framework',
    'rails' => 'framework',
];

$docs = dirname(__DIR__);
$root = dirname($docs);
$pages = [
    'api.md' => apiReference(Yaml::parseFile($docs . '/openapi.yaml')),
    'examples.md' => examples((string) file_get_contents(__DIR__ . '/examples.template.md'), $root),
    'self-hosting.md' => packaged((string) file_get_contents($docs . '/self-hosting.md'), $root),
] + clientPages((string) file_get_contents(__DIR__ . '/clients.template.md'), $root);
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
        is_dir(dirname($path)) || mkdir(dirname($path), 0777, true);
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
 * The pages of docs/clients/: one per library from its README, and the overview.
 *
 * @return array<string, string> page => content
 */
function clientPages(string $template, string $root): array
{
    $found = array_map('basename', array_map('dirname', glob($root . '/clients/*/README.md') ?: []));
    $unlisted = array_diff($found, array_keys(CLIENTS));

    if ($unlisted !== []) {
        throw new RuntimeException(
            'Add to CLIENTS in docs/tools/build.php: clients/' . implode(', clients/', $unlisted),
        );
    }

    $pages = [];
    $rows = ['language' => [], 'framework' => []];

    foreach (CLIENTS as $dir => $kind) {
        $readme = (string) file_get_contents($root . '/clients/' . $dir . '/README.md');
        [$title, $description, $install] = readme($dir, $readme, OUTLINES[$kind]);
        $pages['clients/' . $dir . '.md'] = clientPage($dir, $readme, $title, $description);
        $name = preg_replace('/^LessorMQ for /', '', $title);
        $rows[$kind][] = sprintf('| [%s](%s.md) | `%s` |', $name, $dir, $install);
    }

    $table = static fn (string $head, array $rows): string => "| $head | Install |\n|---|---|\n" . implode("\n", $rows);
    $pages['clients/index.md'] = strtr($template, [
        '{{ languages }}' => $table('Language', $rows['language']),
        '{{ frameworks }}' => $table('Framework', $rows['framework']),
    ]);

    return $pages;
}

/**
 * Checks a README against its outline.
 *
 * @param list<string> $outline
 * @return array{0: string, 1: string, 2: string} title, description, install command
 */
function readme(string $dir, string $readme, array $outline): array
{
    $where = "clients/$dir/README.md";

    if (!preg_match('/\A# (.+)\n\n((?:.+\n)+)/', $readme, $head)) {
        throw new RuntimeException("$where: start with '# LessorMQ for ...' and a paragraph.");
    }

    preg_match_all('/^## (.+)$/m', $readme, $sections);

    if ($sections[1] !== $outline) {
        throw new RuntimeException(sprintf(
            "%s: the sections must be %s, not %s.",
            $where,
            implode(', ', $outline),
            implode(', ', $sections[1]),
        ));
    }

    if (preg_match('/\]\((?!https?:\/\/|#)([^)]*)\)/', $readme, $relative)) {
        throw new RuntimeException("$where: link to $relative[1] with a full URL; registries show this file too.");
    }

    // The first line of the first block: the install command, or the package's
    // coordinates where there is none (Maven).
    if (!preg_match('/^## Install\n(?:.*\n)*?```\w*\n(.+)\n/m', $readme, $install)) {
        throw new RuntimeException("$where: '## Install' needs a block with the install command first.");
    }

    $paragraph = preg_replace(['/\[([^\]]+)\]\([^)]+\)/', '/`/', '/\s+/'], ['$1', '', ' '], trim($head[2]));
    $sentence = preg_match('/^.+?\.(?= |$)/', (string) $paragraph, $first) ? $first[0] : (string) $paragraph;

    return [$head[1], $sentence, $install[1]];
}

/**
 * A README as a page of the site: links into the documentation made relative.
 *
 * A link to the site itself (https://lessormq.dev, or /docs without a page)
 * stays absolute. Made relative it would point at a README.md outside docs/,
 * which the CeeMS docs sync does not mount: it refuses the whole run over it
 * ("README.md of GSID/lessormq@0.4.10 is not mounted"), and lessormq.dev
 * stopped updating on 24 November 2026. The site's home is not a page of
 * docs/ (docs/README.md is excluded there), so there is nothing relative to
 * point at.
 */
function clientPage(string $dir, string $readme, string $title, string $description): string
{
    $body = preg_replace_callback(
        '/\]\(https:\/\/lessormq\.dev(\/docs(?:\/([^)#]*))?)?(#[^)]*)?\)/',
        static function (array $match): string {
            $page = $match[2] ?? '';
            $anchor = $match[3] ?? '';
            $target = match (true) {
                $page === '' => 'https://lessormq.dev/',
                str_starts_with($page, 'clients/') => substr($page, 8) . '.md',
                default => '../' . $page . '.md',
            };

            return '](' . $target . $anchor . ')';
        },
        $readme,
    );

    return sprintf(
        "---\ntitle: %s\ndescription: %s\n---\n\n%s\n_From [clients/%s/README.md](%sclients/%s/README.md)._\n",
        json_encode($title, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
        json_encode($description, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
        rtrim((string) $body) . "\n",
        $dir,
        SOURCE,
        $dir,
    );
}

/**
 * Fills each block whose first line names where a packaged file is installed with that file.
 */
function packaged(string $page, string $root): string
{
    $installed = [
        '/etc/lessormq/' => 'server/packaging/',
        '/etc/systemd/system/' => 'server/packaging/systemd/',
    ];

    $fill = static function (array $match) use ($root, $installed): string {
        $dir = dirname($match[2]) . '/';

        if (!isset($installed[$dir])) {
            return $match[0];
        }

        $file = $installed[$dir] . basename($match[2]);

        if (!is_file($root . '/' . $file)) {
            throw new RuntimeException('self-hosting.md shows a file that is not packaged: ' . $file);
        }

        return $match[1] . rtrim((string) file_get_contents($root . '/' . $file)) . "\n```";
    };

    return (string) preg_replace_callback('/^(```\w*\n# (\/etc\/\S+)\n).*?^```$/ms', $fill, $page);
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
