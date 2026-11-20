<?php

declare(strict_types=1);

namespace App\Api;

use Symfony\Component\HttpFoundation\Request;

/**
 * Reads a JSON request body field by field and collects every problem, so a client gets
 * all invalid fields in one 400 instead of one per round trip.
 *
 * Accessors return null for an absent optional field and for an invalid one; call
 * validate() before using any value.
 */
final class Input
{
    /** @var list<array{field: string, message: string}> */
    private array $errors = [];

    /**
     * @param array<string, mixed> $data
     */
    private function __construct(private readonly array $data)
    {
    }

    public static function fromRequest(Request $request, bool $allowEmpty = false): self
    {
        $content = trim($request->getContent());

        if ($content === '' && $allowEmpty) {
            return new self([]);
        }

        try {
            $data = json_decode($content, true, 64, JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            throw ApiProblem::validation([['field' => '(body)', 'message' => 'must be valid JSON']]);
        }

        if (!is_array($data) || ($data !== [] && array_is_list($data))) {
            throw ApiProblem::validation([['field' => '(body)', 'message' => 'must be a JSON object']]);
        }

        return new self($data);
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self($data);
    }

    /**
     * @return array<string, mixed>
     */
    public function all(): array
    {
        return $this->data;
    }

    public function has(string $field): bool
    {
        return array_key_exists($field, $this->data);
    }

    public function raw(string $field): mixed
    {
        return $this->data[$field] ?? null;
    }

    public function allowOnly(string ...$fields): void
    {
        foreach (array_keys($this->data) as $field) {
            if (!in_array($field, $fields, true)) {
                $this->addError((string) $field, 'unknown field');
            }
        }
    }

    public function string(string $field, bool $required = false, int $max = 255, ?string $pattern = null): ?string
    {
        if (!$this->has($field)) {
            if ($required) {
                $this->addError($field, 'is required');
            }

            return null;
        }

        $value = $this->data[$field];

        if (!is_string($value) || $value === '') {
            $this->addError($field, 'must be a non-empty string');

            return null;
        }

        if (mb_strlen($value) > $max) {
            $this->addError($field, 'must be at most ' . $max . ' characters');

            return null;
        }

        if ($pattern !== null && preg_match('/^(?:' . $pattern . ')$/D', $value) !== 1) {
            $this->addError($field, 'must match ' . $pattern);

            return null;
        }

        return $value;
    }

    public function bool(string $field): ?bool
    {
        if (!$this->has($field)) {
            return null;
        }

        if (!is_bool($this->data[$field])) {
            $this->addError($field, 'must be a boolean');

            return null;
        }

        return $this->data[$field];
    }

    /**
     * @param list<string> $allowed
     */
    public function choice(string $field, array $allowed, bool $required = false): ?string
    {
        if (!$this->has($field)) {
            if ($required) {
                $this->addError($field, 'is required');
            }

            return null;
        }

        if (!in_array($this->data[$field], $allowed, true)) {
            $this->addError($field, 'must be one of: ' . implode(', ', $allowed));

            return null;
        }

        return $this->data[$field];
    }

    /**
     * @return list<string>|null
     */
    public function stringList(
        string $field,
        bool $required = false,
        int $minItems = 0,
        int $maxLength = 255,
        ?string $itemPattern = null,
    ): ?array {
        if (!$this->has($field)) {
            if ($required) {
                $this->addError($field, 'is required');
            }

            return null;
        }

        $value = $this->data[$field];

        if (!is_array($value) || !array_is_list($value)) {
            $this->addError($field, 'must be an array of strings');

            return null;
        }

        if (count($value) < $minItems) {
            $this->addError($field, 'must have at least ' . $minItems . ' item(s)');

            return null;
        }

        foreach ($value as $index => $item) {
            $valid = is_string($item) && $item !== '' && mb_strlen($item) <= $maxLength
                && ($itemPattern === null || preg_match('/^(?:' . $itemPattern . ')$/D', $item) === 1);

            if (!$valid) {
                $this->addError($field . '[' . $index . ']', 'is not valid');

                return null;
            }
        }

        return array_values(array_unique($value));
    }

    /**
     * @param list<array{field: string, message: string}> $errors
     */
    public function addErrors(array $errors): void
    {
        array_push($this->errors, ...$errors);
    }

    public function addError(string $field, string $message): void
    {
        $this->errors[] = ['field' => $field, 'message' => $message];
    }

    public function validate(): void
    {
        if ($this->errors !== []) {
            throw ApiProblem::validation($this->errors);
        }
    }
}
