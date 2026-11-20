<?php

declare(strict_types=1);

namespace App\Entity;

use App\Entity\Enum\MessageStatus;
use App\Repository\MessageRepository;
use App\Time\Utc;
use DateTime;
use Doctrine\ORM\Mapping as ORM;
use Ramsey\Uuid\Doctrine\UuidOrderedTimeGenerator;
use Ramsey\Uuid\UuidInterface;

#[ORM\Entity(repositoryClass: MessageRepository::class)]
#[ORM\Table(name: 'messages')]
#[ORM\Index(columns: ['tenant_id', 'queue', 'status', 'id', 'processing_after'], name: 'idx_tenant_claim')]
#[ORM\Index(columns: ['tenant_id', 'queue', 'status', 'processing_started_at'], name: 'idx_tenant_stale')]
#[ORM\Index(columns: ['queue', 'status', 'id', 'processing_after'], name: 'idx_claim_ordered')]
#[ORM\Index(columns: ['queue', 'status', 'processing_started_at'], name: 'idx_stale_recovery')]
#[ORM\Index(columns: ['status', 'updated_at'], name: 'idx_purge')]
#[ORM\HasLifecycleCallbacks]
class Message
{
    #[ORM\Id]
    #[ORM\Column(type: 'uuid_binary_ordered_time', unique: true, nullable: false)]
    #[ORM\GeneratedValue(strategy: 'CUSTOM')]
    #[ORM\CustomIdGenerator(class: UuidOrderedTimeGenerator::class)]
    private UuidInterface $id;

    #[ORM\Column(type: 'string', length: 63)]
    private string $tenantId;

    #[ORM\Column(type: 'string', length: 255)]
    private string $queue;

    /** Any JSON value */
    #[ORM\Column(type: 'json')]
    private mixed $body;

    /** @var array<string, string>|null */
    #[ORM\Column(type: 'json', nullable: true)]
    private ?array $headers = null;

    /**
     * While the message is open, no other open message of its queue has this key: a
     * unique index on dedup_open, which holds the key only while the message is pending
     * or processing. MessageRepository clears it when a message completes or fails and
     * sets it again on replay.
     */
    #[ORM\Column(type: 'string', length: 255, nullable: true)]
    private ?string $dedupKey = null;

    #[ORM\Column(type: 'string', length: 255, nullable: true)]
    private ?string $dedupOpen = null;

    /** What the publisher asked for; null means the queue's max_attempts */
    #[ORM\Column(type: 'integer', nullable: true)]
    private ?int $maxAttempts = null;

    /** Proof of the current (or last) lease; kept after it ends so ack and fail are idempotent */
    #[ORM\Column(type: 'string', length: 64, nullable: true)]
    private ?string $receipt = null;

    #[ORM\Column(type: 'datetime', nullable: true)]
    private ?DateTime $leaseUntil = null;

    /** The route of the current (or last) attempt */
    #[ORM\Column(type: 'string', length: 255, nullable: true)]
    private ?string $route = null;

    /** @var array<string, string>|null route => when an attempt last failed on it (UTC) */
    #[ORM\Column(type: 'json', nullable: true)]
    private ?array $failedRoutes = null;

    #[ORM\Column(type: 'string', length: 20, enumType: MessageStatus::class, options: ['default' => 'pending'])]
    private MessageStatus $status = MessageStatus::PENDING;

    #[ORM\Column(type: 'datetime', nullable: true)]
    private ?DateTime $processingStartedAt = null;

    #[ORM\Column(type: 'datetime', nullable: true)]
    private ?DateTime $processingAfter = null;

    #[ORM\Column(type: 'datetime', nullable: true)]
    private ?DateTime $processedAt = null;

    #[ORM\Column(type: 'datetime', nullable: true)]
    private ?DateTime $failedAt = null;

    #[ORM\Column(type: 'integer', nullable: false)]
    private int $retryCount = 0;

    #[ORM\Column(type: 'string', length: 255, nullable: true)]
    private ?string $consumedBy = null;

    #[ORM\Column(type: 'text', nullable: true)]
    private ?string $errorMessage = null;

    #[ORM\Column(type: 'text', nullable: true)]
    private ?string $errorDetails = null;


    #[ORM\Column(type: 'datetime', nullable: false)]
    private DateTime $createdAt;

    #[ORM\Column(type: 'datetime', nullable: false)]
    private DateTime $updatedAt;

    public function __construct(string $tenantId, string $queue, mixed $body)
    {
        $this->tenantId = $tenantId;
        $this->queue = $queue;
        $this->body = $body;
        $this->createdAt = Utc::now();
        $this->updatedAt = Utc::now();
    }

    #[ORM\PrePersist]
    public function onPrePersist(): void
    {
        $this->createdAt ??= Utc::now();
        $this->updatedAt ??= Utc::now();
    }

    #[ORM\PreUpdate]
    public function onPreUpdate(): void
    {
        $this->updatedAt = Utc::now();
    }

    public function getId(): ?UuidInterface
    {
        return $this->id;
    }

    public function getTenantId(): string
    {
        return $this->tenantId;
    }

    public function getQueue(): string
    {
        return $this->queue;
    }

    public function setQueue(string $queue): static
    {
        $this->queue = $queue;
        return $this;
    }

    public function getBody(): mixed
    {
        return $this->body;
    }

    /**
     * @return array<string, string>
     */
    public function getHeaders(): array
    {
        return $this->headers ?? [];
    }

    /**
     * @param array<string, string> $headers
     */
    public function setHeaders(array $headers): static
    {
        $this->headers = $headers === [] ? null : $headers;
        return $this;
    }

    public function getDedupKey(): ?string
    {
        return $this->dedupKey;
    }

    /**
     * Only for a new (pending) message: the key is reserved from the insert on.
     */
    public function setDedupKey(?string $dedupKey): static
    {
        $this->dedupKey = $dedupKey;
        $this->dedupOpen = $dedupKey;
        return $this;
    }

    public function getMaxAttempts(): ?int
    {
        return $this->maxAttempts;
    }

    public function setMaxAttempts(?int $maxAttempts): static
    {
        $this->maxAttempts = $maxAttempts;
        return $this;
    }

    public function getReceipt(): ?string
    {
        return $this->receipt;
    }

    public function getLeaseUntil(): ?DateTime
    {
        return $this->leaseUntil;
    }

    public function getRoute(): ?string
    {
        return $this->route;
    }

    /**
     * @return array<string, string> route => when an attempt last failed on it, as stored (UTC)
     */
    public function getFailedRoutes(): array
    {
        return $this->failedRoutes ?? [];
    }

    /**
     * failed_routes with the current attempt's route marked as failed now, as the JSON to
     * store; null when the attempt had no route and nothing changes.
     */
    public function failedRoutesWithCurrent(DateTime $now): ?string
    {
        if ($this->route === null) {
            return null;
        }

        $routes = $this->getFailedRoutes();
        $routes[$this->route] = $now->format('Y-m-d H:i:s');
        $this->failedRoutes = $routes;

        return json_encode($routes);
    }

    /**
     * Attempts started so far. retry_count counts the failed attempts that led to another
     * one (an expired lease included, a release not); the attempt that is running, that
     * succeeded or that was the last to fail comes on top.
     */
    public function getAttempts(): int
    {
        return $this->retryCount + ($this->status === MessageStatus::PENDING ? 0 : 1);
    }

    public function setBody(mixed $body): static
    {
        $this->body = $body;
        return $this;
    }

    public function getStatus(): MessageStatus
    {
        return $this->status;
    }

    public function setStatus(MessageStatus $status): static
    {
        $this->status = $status;
        return $this;
    }

    public function getProcessingStartedAt(): ?DateTime
    {
        return $this->processingStartedAt;
    }

    public function setProcessingStartedAt(?DateTime $processingStartedAt): static
    {
        $this->processingStartedAt = $processingStartedAt;
        return $this;
    }

    public function getProcessingAfter(): ?DateTime
    {
        return $this->processingAfter;
    }

    public function setProcessingAfter(?DateTime $processingAfter): static
    {
        $this->processingAfter = $processingAfter;
        return $this;
    }

    public function getProcessedAt(): ?DateTime
    {
        return $this->processedAt;
    }

    public function setProcessedAt(?DateTime $processedAt): static
    {
        $this->processedAt = $processedAt;
        return $this;
    }

    public function getFailedAt(): ?DateTime
    {
        return $this->failedAt;
    }

    public function setFailedAt(?DateTime $failedAt): static
    {
        $this->failedAt = $failedAt;
        return $this;
    }

    public function getRetryCount(): int
    {
        return $this->retryCount;
    }

    public function setRetryCount(int $retryCount): static
    {
        $this->retryCount = $retryCount;
        return $this;
    }

    public function incrementRetryCount(): int
    {
        ++$this->retryCount;
        return $this->retryCount;
    }

    public function getConsumedBy(): ?string
    {
        return $this->consumedBy;
    }

    public function setConsumedBy(?string $consumedBy): static
    {
        $this->consumedBy = $consumedBy;
        return $this;
    }

    public function getErrorMessage(): ?string
    {
        return $this->errorMessage;
    }

    public function setErrorMessage(?string $errorMessage): static
    {
        $this->errorMessage = $errorMessage;
        return $this;
    }

    public function getErrorDetails(): ?string
    {
        return $this->errorDetails;
    }

    public function setErrorDetails(?string $errorDetails): static
    {
        $this->errorDetails = $errorDetails;
        return $this;
    }

    public function getCreatedAt(): DateTime
    {
        return $this->createdAt;
    }

    public function setCreatedAt(DateTime $createdAt): static
    {
        $this->createdAt = $createdAt;
        return $this;
    }

    public function getUpdatedAt(): DateTime
    {
        return $this->updatedAt;
    }

    public function setUpdatedAt(DateTime $updatedAt): static
    {
        $this->updatedAt = $updatedAt;
        return $this;
    }
}
