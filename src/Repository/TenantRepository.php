<?php

declare(strict_types=1);

namespace App\Repository;

use App\Entity\Enum\TenantStatus;
use App\Entity\Tenant;
use Doctrine\Bundle\DoctrineBundle\Repository\ServiceEntityRepository;
use Doctrine\Persistence\ManagerRegistry;

/**
 * @extends ServiceEntityRepository<Tenant>
 */
class TenantRepository extends ServiceEntityRepository
{
    public function __construct(ManagerRegistry $registry)
    {
        parent::__construct($registry, Tenant::class);
    }

    public function save(Tenant $tenant): void
    {
        $this->getEntityManager()->persist($tenant);
        $this->getEntityManager()->flush();
    }

    public function remove(Tenant $tenant): void
    {
        $this->getEntityManager()->remove($tenant);
        $this->getEntityManager()->flush();
    }

    /**
     * One page in id order, starting after $afterId.
     *
     * @return list<Tenant>
     */
    public function page(?TenantStatus $status, int $limit, ?string $afterId): array
    {
        $query = $this->createQueryBuilder('t')
            ->orderBy('t.id', 'ASC')
            ->setMaxResults($limit);

        if ($status !== null) {
            $query->andWhere('t.status = :status')->setParameter('status', $status);
        }

        if ($afterId !== null) {
            $query->andWhere('t.id > :after')->setParameter('after', $afterId);
        }

        return $query->getQuery()->getResult();
    }

    /**
     * Status without hydrating the entity: the authenticator asks this on every operator
     * request that names a tenant.
     */
    public function statusOf(string $id): ?TenantStatus
    {
        $status = $this->getEntityManager()->getConnection()
            ->fetchOne('SELECT status FROM tenants WHERE id = ?', [$id]);

        return $status === false ? null : TenantStatus::from($status);
    }

    /**
     * @return list<Tenant>
     */
    public function findDeleting(): array
    {
        return $this->findBy(['status' => TenantStatus::DELETING], ['id' => 'ASC']);
    }
}
