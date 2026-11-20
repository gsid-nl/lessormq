<?php

declare(strict_types=1);

namespace App\Repository;

use App\Doctrine\UtcDateTimeImmutableType;
use App\Entity\ApiKey;
use DateTimeImmutable;
use Doctrine\Bundle\DoctrineBundle\Repository\ServiceEntityRepository;
use Doctrine\Persistence\ManagerRegistry;

/**
 * @extends ServiceEntityRepository<ApiKey>
 */
class ApiKeyRepository extends ServiceEntityRepository
{
    public function __construct(ManagerRegistry $registry)
    {
        parent::__construct($registry, ApiKey::class);
    }

    public function save(ApiKey $key): void
    {
        $this->getEntityManager()->persist($key);
        $this->getEntityManager()->flush();
    }

    /**
     * Everything the authenticator needs for one request, in one indexed query and without
     * hydrating entities: this runs on every API call.
     *
     * @return array{
     *     id: string,
     *     tenant_id: ?string,
     *     scopes: string,
     *     queues: string,
     *     last_used_at: ?string,
     *     tenant_status: ?string
     * }|null
     */
    public function findCredential(string $secretHash): ?array
    {
        $row = $this->getEntityManager()->getConnection()->fetchAssociative(
            <<<'SQL'
                SELECT k.id, k.tenant_id, k.scopes, k.queues, k.last_used_at, t.status AS tenant_status
                FROM api_keys k
                LEFT JOIN tenants t ON t.id = k.tenant_id
                WHERE k.secret_hash = ?
                  AND k.revoked_at IS NULL
                SQL,
            [$secretHash],
        );

        return $row === false ? null : $row;
    }

    /**
     * Record use, guarded so that concurrent requests of one key write at most once.
     */
    public function touch(string $id, DateTimeImmutable $now, DateTimeImmutable $staleBefore): void
    {
        $utc = UtcDateTimeImmutableType::utc();

        $this->getEntityManager()->getConnection()->executeStatement(
            'UPDATE api_keys SET last_used_at = ? WHERE id = ? AND (last_used_at IS NULL OR last_used_at < ?)',
            [
                $now->setTimezone($utc)->format('Y-m-d H:i:s'),
                $id,
                $staleBefore->setTimezone($utc)->format('Y-m-d H:i:s'),
            ],
        );
    }

    /**
     * @return list<ApiKey> the tenant's keys that are not revoked, oldest first
     */
    public function forTenant(string $tenantId): array
    {
        return $this->createQueryBuilder('k')
            ->where('k.tenantId = :tenant')
            ->andWhere('k.revokedAt IS NULL')
            ->setParameter('tenant', $tenantId)
            ->orderBy('k.createdAt', 'ASC')
            ->getQuery()
            ->getResult();
    }

    public function findForTenant(string $tenantId, string $keyId): ?ApiKey
    {
        $key = $this->find($keyId);

        if ($key === null || $key->getTenantId() !== $tenantId || $key->isRevoked()) {
            return null;
        }

        return $key;
    }

    public function deleteForTenant(string $tenantId): int
    {
        return (int) $this->getEntityManager()->getConnection()
            ->executeStatement('DELETE FROM api_keys WHERE tenant_id = ?', [$tenantId]);
    }
}
