<?php

declare(strict_types=1);

namespace App\Service;

use App\Api\ApiProblem;
use App\Api\Input;
use App\Api\Time;
use App\Entity\ApiKey;
use App\Entity\Enum\Scope;
use App\Repository\ApiKeyRepository;
use App\Security\ApiKeyFactory;

/**
 * Tenant keys, as managed by a tenant admin (/v1/api-keys) or an operator
 * (/v1/tenants/{id}/api-keys). Operator keys are created on the command line only.
 */
class ApiKeyService
{
    /** Queue name globs: queue name characters plus * and ? */
    private const QUEUE_PATTERN = '[A-Za-z0-9._*?-]{1,200}';

    public function __construct(private readonly ApiKeyRepository $apiKeys)
    {
    }

    /**
     * @return array<string, mixed> the created key including its secret, shown once
     */
    public function createFromInput(string $tenantId, Input $input): array
    {
        $input->allowOnly('name', 'scopes', 'queues');
        $name = $input->string('name', required: true);
        $scopes = $input->stringList(
            'scopes',
            required: true,
            minItems: 1,
            itemPattern: implode('|', array_map(static fn(Scope $scope) => $scope->value, Scope::cases())),
        );
        $queues = $input->stringList('queues', itemPattern: self::QUEUE_PATTERN) ?? [];
        $input->validate();

        [$key, $secret] = ApiKeyFactory::create(
            $tenantId,
            $name,
            array_map(static fn(string $scope) => Scope::from($scope), $scopes),
            $queues,
        );
        $this->apiKeys->save($key);

        return $this->view($key) + ['secret' => $secret];
    }

    /**
     * @return list<array<string, mixed>>
     */
    public function listForTenant(string $tenantId): array
    {
        return array_map(fn(ApiKey $key) => $this->view($key), $this->apiKeys->forTenant($tenantId));
    }

    /**
     * @param string|null $callerKeyId the key making the request; it may not revoke itself
     */
    public function revoke(string $tenantId, string $keyId, ?string $callerKeyId = null): void
    {
        $key = $this->apiKeys->findForTenant($tenantId, $keyId);

        if ($key === null) {
            throw ApiProblem::notFound(sprintf('No API key "%s".', $keyId));
        }

        if ($key->getId() === $callerKeyId) {
            throw ApiProblem::invalidState('A key cannot revoke itself; use another admin key.');
        }

        $key->revoke();
        $this->apiKeys->save($key);
    }

    /**
     * @return array<string, mixed>
     */
    public function view(ApiKey $key): array
    {
        return [
            'id' => $key->getId(),
            'name' => $key->getName(),
            'prefix' => $key->getPrefix(),
            'scopes' => array_map(static fn(Scope $scope) => $scope->value, $key->getScopes()),
            'queues' => $key->getQueues(),
            'created_at' => Time::format($key->getCreatedAt()),
            'last_used_at' => Time::format($key->getLastUsedAt()),
        ];
    }
}
