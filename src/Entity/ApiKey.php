<?php

declare(strict_types=1);

namespace App\Entity;

use App\Doctrine\UtcDateTimeImmutableType;
use App\Entity\Enum\Scope;
use App\Repository\ApiKeyRepository;
use DateTimeImmutable;
use Doctrine\ORM\Mapping as ORM;

/**
 * An API key. Only a hash of the secret is stored; the secret itself is shown once, when
 * the key is created.
 *
 * A key without a tenant is an operator key: it manages tenants and is never created
 * over HTTP.
 */
#[ORM\Entity(repositoryClass: ApiKeyRepository::class)]
#[ORM\Table(name: 'api_keys')]
#[ORM\UniqueConstraint(name: 'uniq_api_keys_secret_hash', columns: ['secret_hash'])]
#[ORM\Index(columns: ['tenant_id'], name: 'idx_api_keys_tenant')]
class ApiKey
{
    #[ORM\Column(type: UtcDateTimeImmutableType::NAME)]
    private DateTimeImmutable $createdAt;

    #[ORM\Column(type: UtcDateTimeImmutableType::NAME, nullable: true)]
    private ?DateTimeImmutable $lastUsedAt = null;

    #[ORM\Column(type: UtcDateTimeImmutableType::NAME, nullable: true)]
    private ?DateTimeImmutable $revokedAt = null;

    /**
     * @param list<string> $scopes  Scope values; empty for an operator key
     * @param list<string> $queues  glob patterns of queue names; empty means all
     */
    public function __construct(
        #[ORM\Id]
        #[ORM\Column(type: 'string', length: 32)]
        private string $id,
        #[ORM\Column(type: 'string', length: 63, nullable: true)]
        private ?string $tenantId,
        #[ORM\Column(type: 'string', length: 255)]
        private string $name,
        #[ORM\Column(type: 'string', length: 16)]
        private string $prefix,
        #[ORM\Column(type: 'string', length: 64, options: ['fixed' => true])]
        private string $secretHash,
        #[ORM\Column(type: 'json')]
        private array $scopes,
        #[ORM\Column(type: 'json')]
        private array $queues = [],
    ) {
        $this->createdAt = new DateTimeImmutable();
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getTenantId(): ?string
    {
        return $this->tenantId;
    }

    public function isOperatorKey(): bool
    {
        return $this->tenantId === null;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getPrefix(): string
    {
        return $this->prefix;
    }

    /**
     * @return list<Scope>
     */
    public function getScopes(): array
    {
        return array_map(static fn(string $scope) => Scope::from($scope), $this->scopes);
    }

    /**
     * @return list<string>
     */
    public function getQueues(): array
    {
        return $this->queues;
    }

    public function getCreatedAt(): DateTimeImmutable
    {
        return $this->createdAt;
    }

    public function getLastUsedAt(): ?DateTimeImmutable
    {
        return $this->lastUsedAt;
    }

    public function isRevoked(): bool
    {
        return $this->revokedAt !== null;
    }

    public function revoke(): void
    {
        $this->revokedAt ??= new DateTimeImmutable();
    }
}
