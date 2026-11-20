<?php

declare(strict_types=1);

namespace App\Entity;

use App\Doctrine\UtcDateTimeImmutableType;
use App\Entity\Enum\TenantStatus;
use App\Model\QueueSettings;
use App\Model\Quotas;
use App\Repository\TenantRepository;
use DateTimeImmutable;
use Doctrine\ORM\Mapping as ORM;

#[ORM\Entity(repositoryClass: TenantRepository::class)]
#[ORM\Table(name: 'tenants')]
class Tenant
{
    public const ID_PATTERN = '[a-z0-9][a-z0-9-]{1,62}';

    #[ORM\Column(type: 'string', length: 20, enumType: TenantStatus::class)]
    private TenantStatus $status = TenantStatus::ACTIVE;

    /** @var array<string, int|null> only the limits that are set */
    #[ORM\Column(type: 'json')]
    private array $quotas = [];

    /** @var array<string, mixed> a patch over QueueSettings::DEFAULTS */
    #[ORM\Column(type: 'json')]
    private array $queueDefaults = [];

    #[ORM\Column(type: UtcDateTimeImmutableType::NAME)]
    private DateTimeImmutable $createdAt;

    #[ORM\Column(type: UtcDateTimeImmutableType::NAME)]
    private DateTimeImmutable $updatedAt;

    #[ORM\Column(type: UtcDateTimeImmutableType::NAME, nullable: true)]
    private ?DateTimeImmutable $suspendedAt = null;

    public function __construct(
        #[ORM\Id]
        #[ORM\Column(type: 'string', length: 63)]
        private string $id,
        #[ORM\Column(type: 'string', length: 255)]
        private string $name,
    ) {
        $this->createdAt = new DateTimeImmutable();
        $this->updatedAt = $this->createdAt;
    }

    public static function isValidId(string $id): bool
    {
        return preg_match('/^' . self::ID_PATTERN . '$/D', $id) === 1;
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function rename(string $name): void
    {
        $this->name = $name;
        $this->touch();
    }

    public function getStatus(): TenantStatus
    {
        return $this->status;
    }

    public function isActive(): bool
    {
        return $this->status === TenantStatus::ACTIVE;
    }

    public function suspend(): void
    {
        if ($this->status === TenantStatus::SUSPENDED) {
            return;
        }

        $this->status = TenantStatus::SUSPENDED;
        $this->suspendedAt = new DateTimeImmutable();
        $this->touch();
    }

    public function activate(): void
    {
        $this->status = TenantStatus::ACTIVE;
        $this->suspendedAt = null;
        $this->touch();
    }

    public function markDeleting(): void
    {
        $this->status = TenantStatus::DELETING;
        $this->touch();
    }

    /**
     * @return array<string, int|null> every quota, null where unlimited
     */
    public function getQuotas(): array
    {
        return Quotas::resolve($this->quotas);
    }

    /**
     * @param array<string, int|null> $patch validated with Quotas::validate()
     */
    public function patchQuotas(array $patch): void
    {
        foreach ($patch as $field => $value) {
            if ($value === null) {
                unset($this->quotas[$field]);
            } else {
                $this->quotas[$field] = $value;
            }
        }

        $this->touch();
    }

    /**
     * @return array<string, mixed> the tenant's own overrides only
     */
    public function getQueueDefaultsPatch(): array
    {
        return $this->queueDefaults;
    }

    /**
     * @return array<string, mixed> complete settings a new queue of this tenant starts with
     */
    public function getQueueDefaults(): array
    {
        return QueueSettings::resolve($this->queueDefaults);
    }

    /**
     * @param array<string, mixed> $patch validated with QueueSettings::validatePatch()
     */
    public function patchQueueDefaults(array $patch): void
    {
        $this->queueDefaults = QueueSettings::merge($this->queueDefaults, $patch);
        $this->touch();
    }

    public function getCreatedAt(): DateTimeImmutable
    {
        return $this->createdAt;
    }

    public function getUpdatedAt(): DateTimeImmutable
    {
        return $this->updatedAt;
    }

    public function getSuspendedAt(): ?DateTimeImmutable
    {
        return $this->suspendedAt;
    }

    private function touch(): void
    {
        $this->updatedAt = new DateTimeImmutable();
    }
}
