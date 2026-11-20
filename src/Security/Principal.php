<?php

declare(strict_types=1);

namespace App\Security;

use App\Api\ApiProblem;
use App\Entity\Enum\Scope;

/**
 * Who is calling: the key, the tenant it acts in and what it may do there.
 *
 * A tenant key acts in its own tenant with its own scopes. An operator key acts in no
 * tenant, or, with the LessorMQ-Tenant header, in that tenant as an admin.
 */
final class Principal
{
    public const ATTRIBUTE = '_lessormq_principal';

    /**
     * @param list<Scope>  $scopes
     * @param list<string> $queuePatterns glob patterns; empty means every queue
     */
    public function __construct(
        public readonly string $keyId,
        public readonly ?string $tenantId,
        public readonly array $scopes,
        public readonly array $queuePatterns,
        public readonly bool $operator,
    ) {
    }

    public function has(Scope $scope): bool
    {
        return in_array($scope, $this->scopes, true) || in_array(Scope::ADMIN, $this->scopes, true);
    }

    public function mayUseQueue(string $queue): bool
    {
        if ($this->queuePatterns === []) {
            return true;
        }

        foreach ($this->queuePatterns as $pattern) {
            if (fnmatch($pattern, $queue, FNM_NOESCAPE)) {
                return true;
            }
        }

        return false;
    }

    /**
     * The tenant to act in; an operator key without the LessorMQ-Tenant header has none.
     */
    public function tenant(): string
    {
        if ($this->tenantId === null) {
            throw ApiProblem::forbidden('An operator key acts in a tenant only with the LessorMQ-Tenant header.');
        }

        return $this->tenantId;
    }
}
