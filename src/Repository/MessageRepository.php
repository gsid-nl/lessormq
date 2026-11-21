<?php

declare(strict_types=1);

namespace App\Repository;

use App\Database\Dialect;
use App\Doctrine\OrderedTimeUuidType;
use App\Entity\Enum\MessageStatus;
use App\Entity\Message;
use App\Exception\DuplicateOpenMessageException;
use App\Time\Utc;
use DateTimeInterface;
use Doctrine\Bundle\DoctrineBundle\Repository\ServiceEntityRepository;
use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Types\Type;
use Doctrine\Persistence\ManagerRegistry;
use Ramsey\Uuid\Rfc4122\FieldsInterface;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;

/**
 * @extends ServiceEntityRepository<Message>
 */
class MessageRepository extends ServiceEntityRepository
{
    use DeadlockRetryTrait;

    private const REPLAY_ASSIGNMENTS = <<<'SQL'
        status = :pending, retry_count = 0, processing_after = NULL, processing_started_at = NULL,
        processed_at = NULL, failed_at = NULL, consumed_by = NULL, receipt = NULL, lease_until = NULL,
        route = NULL, failed_routes = NULL, error_message = NULL, error_details = NULL, updated_at = :now,
        dedup_open = dedup_key
        SQL;

    public function __construct(
        private readonly ManagerRegistry $managerRegistry,
        private readonly Dialect $dialect,
    ) {
        parent::__construct($managerRegistry, Message::class);
    }

    /** The error type of a message's last failure. */
    private function errorType(): string
    {
        return $this->dialect->jsonValue('error_details', 'type');
    }

    public function add(Message $entity, bool $flush = false): void
    {
        $this->getEntityManager()->persist($entity);

        if ($flush) {
            $this->getEntityManager()->flush();
        }
    }

    /**
     * Replace the EntityManager after a failed flush closed it. The manager service is
     * lazy, so it is reset in place and this repository keeps working. Anything the old
     * manager tracked is detached, so only call this between independent writes.
     */
    public function resetManager(): void
    {
        $this->managerRegistry->resetManager();
    }

    /**
     * Atomically claim the next pending message.
     *
     * Strategy:
     * 1. Look for a claimable id with a plain read, no transaction; stop if there is none
     * 2. In one READ COMMITTED transaction, SELECT the first claimable id
     *    FOR UPDATE SKIP LOCKED and UPDATE that single row by primary key, setting
     *    status=processing and consumed_by to a fresh claim token
     * 3. Load the claimed row by primary key
     *
     * The single-statement `UPDATE ... ORDER BY id LIMIT 1` this replaces deadlocked
     * under load: every claim scanned the same (queue, status) index range, and under
     * REPEATABLE READ that scan takes next-key locks over the whole range, empty polls
     * included. Two workers that lock the range in opposite order relative to the
     * primary key produce a lock cycle, so InnoDB kills one with 1213.
     *
     * READ COMMITTED drops the gap locks, so a claim only ever holds the one row it is
     * taking, and SKIP LOCKED makes a worker walk past a row another worker already
     * holds instead of queueing behind it. Both statements touch a single row in
     * primary key order, which leaves no cycle to detect.
     *
     * PostgreSQL does the same. SQLite has no row locks and one writer at a time, so there
     * step 2 is a single UPDATE of the first claimable row, RETURNING its id.
     */
    public function claimNextMessage(
        string $tenant,
        string $queue,
        string $consumer,
        ?string $route = null,
        ?int $avoidRouteSeconds = null,
        int $leaseSeconds = 900,
    ): ?Message {
        $conn = $this->getEntityManager()->getConnection();
        $nowTime = Utc::now();
        $now = Utc::sql($nowTime);

        $routeCondition = '';
        $selectParams = [
            'tenant' => $tenant,
            'queue' => $queue,
            'status' => MessageStatus::PENDING->value,
            'now' => $now,
        ];

        // Skip a message that failed on this route within the cooldown. A row lookup per
        // candidate, but only on queues that avoid failed routes and only for claims that
        // name one.
        if ($route !== null && $avoidRouteSeconds !== null) {
            $routeCondition = 'AND (failed_routes IS NULL'
                . " OR COALESCE({$this->dialect->jsonMember('failed_routes', ':route_path')}, '0') < :avoid_before)";
            $selectParams['route_path'] = $this->dialect->jsonMemberArgument($route);
            $selectParams['avoid_before'] = Utc::sql((clone $nowTime)->modify('-' . $avoidRouteSeconds . ' seconds'));
        }

        // Served by idx_tenant_claim (tenant_id, queue, status, id, processing_after):
        // the scan stops at the first claimable row instead of sorting the backlog.
        $candidateSql = <<<SQL
            SELECT id
            FROM messages
            WHERE tenant_id = :tenant
              AND queue = :queue
              AND status = :status
              AND (processing_after IS NULL OR processing_after <= :now)
              {$routeCondition}
            ORDER BY id ASC
            LIMIT 1
        SQL;

        // Nearly every poll finds the queue empty. A plain consistent read answers that
        // in autocommit, without the isolation switch, the transaction or any lock, so an
        // empty poll costs one statement instead of four. A row inserted right after this
        // read is picked up by the next poll, exactly as if it had arrived after the claim.
        if ($conn->fetchOne($candidateSql, $selectParams) === false) {
            return null;
        }

        $selectSql = $candidateSql . $this->dialect->skipLocked();

        $updateSql = <<<SQL
            UPDATE messages
            SET status = 'processing',
                consumed_by = :consumer,
                receipt = :receipt,
                lease_until = :lease_until,
                route = :route,
                processing_started_at = :now,
                updated_at = :now
            WHERE id = :id
              AND status = :status
        SQL;

        $updateParams = [
            'consumer' => $consumer,
            'receipt' => bin2hex(random_bytes(16)),
            'lease_until' => Utc::sql((clone $nowTime)->modify('+' . $leaseSeconds . ' seconds')),
            'route' => $route,
            'now' => $now,
            'status' => MessageStatus::PENDING->value,
        ];

        // Retained as a backstop only: a deadlocked claim rolled back without taking a
        // row, so re-running it is the same operation rather than a duplicate claim.
        $claimedId = $this->retryOnDeadlock(fn() => $this->dialect->claimsInOneStatement()
            ? $this->claimInOneStatement($conn, $candidateSql, $selectParams, $updateSql, $updateParams)
            : $this->claimOneRow($conn, $selectSql, $selectParams, $updateSql, $updateParams));

        if ($claimedId === null) {
            return null;
        }

        // Fetch the claimed message by primary key. The identity map cannot hold a stale
        // copy of this row: nothing earlier in a claim request loads messages, so no
        // clear() is needed (and clearing would only force extra hydration work).
        return $this->find(
            Type::getType('uuid_binary_ordered_time')->convertToPHPValue($claimedId, $conn->getDatabasePlatform())
        );
    }

    /**
     * Lock one claimable row and mark it processing, in a single READ COMMITTED
     * transaction. Returns the claimed row's raw primary key, or null when another
     * worker took the candidate first and nothing else is claimable.
     */
    private function claimOneRow(
        Connection $conn,
        string $selectSql,
        array $selectParams,
        string $updateSql,
        array $updateParams,
    ): ?string {
        // Scoped to the next transaction only, so the connection keeps its configured
        // isolation level afterwards. fpm workers hold their connection across requests.
        $this->dialect->readCommitted($conn);
        $conn->beginTransaction();

        try {
            $id = $conn->fetchOne($selectSql, $selectParams);

            if ($id === false) {
                $conn->commit();

                return null;
            }

            $id = OrderedTimeUuidType::bytes($id);
            $affected = $conn->executeStatement(
                $updateSql,
                $updateParams + ['id' => $id],
                ['id' => ParameterType::BINARY],
            );
            $conn->commit();

            return $affected > 0 ? $id : null;
        } catch (\Throwable $e) {
            if ($conn->isTransactionActive()) {
                $conn->rollBack();
            }

            throw $e;
        }
    }

    /**
     * The claim as one statement: the first claimable row, chosen and taken by one UPDATE.
     * For SQLite, whose single writer makes a statement atomic and which has no row locks
     * to skip; a read in a transaction that then writes could not wait for the writer.
     */
    private function claimInOneStatement(
        Connection $conn,
        string $candidateSql,
        array $selectParams,
        string $updateSql,
        array $updateParams,
    ): ?string {
        $sql = str_replace('WHERE id = :id', "WHERE id = ({$candidateSql})", $updateSql) . ' RETURNING id';
        $id = $conn->fetchOne($sql, $updateParams + $selectParams);

        return $id === false ? null : OrderedTimeUuidType::bytes($id);
    }

    /**
     * Write one message's state change straight to its row, by primary key, and only if
     * the row is still in the status the caller read.
     *
     * A single autocommit UPDATE: a primary key equality match locks only that record at
     * any isolation level, so a transaction around it would add three round trips and no
     * protection. The row's secondary index entries (idx_claim_ordered, idx_purge,
     * idx_stale_recovery) still change, which can close a lock cycle with the range
     * writers (the stale sweep and the purge DELETE); the deadlock retry covers that.
     *
     * The status guard makes the read-then-write in ack/reject atomic: if the row moved
     * on after the caller loaded it (a stale sweep, a second ack), nothing is written.
     *
     * This bypasses the UnitOfWork, so the entity is detached afterwards: the caller
     * keeps it for the response, and no later flush can re-issue the same UPDATE.
     *
     * @param array<string, scalar|null> $values column => value; updated_at is set here
     * @return bool false when the row was no longer in $expectedStatus
     */
    public function updateMessageRow(
        Message $message,
        array $values,
        MessageStatus $expectedStatus,
        ?string $receipt = null,
    ): bool {
        $conn = $this->getEntityManager()->getConnection();
        $now = Utc::now();

        // The PreUpdate callback does not fire without a flush, so the timestamp is
        // maintained here, on the row and on the entity the caller returns.
        $values['updated_at'] = Utc::sql($now);

        // A finished message frees its dedup_key for the next publish.
        $finished = [MessageStatus::COMPLETED->value, MessageStatus::FAILED->value];

        if (in_array($values['status'] ?? null, $finished, true)) {
            $values['dedup_open'] = null;
        }

        $assignments = [];

        foreach (array_keys($values) as $column) {
            $assignments[] = "{$column} = :{$column}";
        }

        $sql = 'UPDATE messages SET ' . implode(', ', $assignments)
            . ' WHERE id = :id AND status = :expected_status';
        $params = $values + ['id' => $message->getId(), 'expected_status' => $expectedStatus->value];

        // v1: only the holder of the current, unexpired lease may end it.
        if ($receipt !== null) {
            $sql .= ' AND receipt = :guard_receipt AND lease_until > :guard_now';
            $params += ['guard_receipt' => $receipt, 'guard_now' => $values['updated_at']];
        }

        // The primary key is stored ordered-time encoded, so it is bound through the
        // Doctrine type rather than as raw UUID bytes.
        $types = ['id' => 'uuid_binary_ordered_time'];

        // MariaDB reports changed rows, not matched rows. Every caller moves the row out
        // of $expectedStatus, so a matched row is always a changed row.
        $affected = $this->retryOnDeadlock(fn() => $conn->executeStatement($sql, $params, $types));

        $message->setUpdatedAt($now);
        $this->getEntityManager()->detach($message);

        return $affected > 0;
    }

    /**
     * The open message of a queue with this dedup_key, if there is one; served by
     * uniq_open_dedup (tenant_id, queue, dedup_open). dedup_open is kept by this
     * repository: cleared when a message completes or fails, set again on replay.
     */
    public function findOpenByDedupKey(string $tenant, string $queue, string $dedupKey): ?Message
    {
        $id = $this->getEntityManager()->getConnection()->fetchOne(
            'SELECT id FROM messages WHERE tenant_id = ? AND queue = ? AND dedup_open = ?',
            [$tenant, $queue, $dedupKey],
        );

        if ($id === false) {
            return null;
        }

        $conn = $this->getEntityManager()->getConnection();

        return $this->find(
            Type::getType('uuid_binary_ordered_time')->convertToPHPValue($id, $conn->getDatabasePlatform())
        );
    }

    /**
     * Message ids are v1 UUIDs; the ordered-time type cannot encode any other version.
     */
    private function isMessageId(string $value): bool
    {
        if (!Uuid::isValid($value)) {
            return false;
        }

        $fields = Uuid::fromString($value)->getFields();

        return $fields instanceof FieldsInterface && $fields->getVersion() === 1;
    }

    /**
     * A message by id, only if it belongs to the tenant: ids are not secret, so every
     * lookup by id must be scoped.
     */
    public function findInTenant(string $tenant, UuidInterface|string $id): ?Message
    {
        $message = $this->find($id);

        return $message !== null && $message->getTenantId() === $tenant ? $message : null;
    }

    /**
     * A tenant's open and dead-letter messages over its queues. Only those rows are read,
     * through ranges of (tenant_id, queue, status): the finished messages, which are most
     * of them, are not touched.
     *
     * @param list<string> $queues the tenant's queues
     * @return array{queues: int, open_messages: int, failed_messages: int}
     */
    public function tenantUsage(string $tenant, array $queues): array
    {
        $usage = ['queues' => count($queues), 'open_messages' => 0, 'failed_messages' => 0];

        if ($queues === []) {
            return $usage;
        }

        $rows = $this->getEntityManager()->getConnection()->fetchAllAssociative(
            <<<'SQL'
                SELECT status, COUNT(*) AS n
                FROM messages
                WHERE tenant_id = :tenant AND queue IN (:queues) AND status IN (:pending, :processing, :failed)
                GROUP BY status
                SQL,
            [
                'tenant' => $tenant,
                'queues' => $queues,
                'pending' => MessageStatus::PENDING->value,
                'processing' => MessageStatus::PROCESSING->value,
                'failed' => MessageStatus::FAILED->value,
            ],
            ['queues' => ArrayParameterType::STRING],
        );

        foreach ($rows as $row) {
            $field = $row['status'] === MessageStatus::FAILED->value ? 'failed_messages' : 'open_messages';
            $usage[$field] += (int) $row['n'];
        }

        return $usage;
    }

    public function deleteTenantBatch(string $tenant, int $limit): int
    {
        return (int) $this->getEntityManager()->getConnection()->executeStatement(
            $this->dialect->deleteMessages('tenant_id = :tenant'),
            ['tenant' => $tenant, 'limit' => $limit],
            ['limit' => \Doctrine\DBAL\ParameterType::INTEGER],
        );
    }

    /**
     * Delete a message only if it is still in the status the caller read, like
     * updateMessageRow() for a transition that ends in deletion.
     */
    public function deleteIfStatus(Message $message, MessageStatus $expectedStatus, ?string $receipt = null): bool
    {
        $conn = $this->getEntityManager()->getConnection();
        $sql = 'DELETE FROM messages WHERE id = :id AND status = :status';
        $params = ['id' => $message->getId(), 'status' => $expectedStatus->value];

        if ($receipt !== null) {
            $sql .= ' AND receipt = :receipt AND lease_until > :now';
            $params += ['receipt' => $receipt, 'now' => Utc::sql(Utc::now())];
        }

        $deleted = $this->retryOnDeadlock(fn() => $conn->executeStatement(
            $sql,
            $params,
            ['id' => 'uuid_binary_ordered_time'],
        ));

        $this->getEntityManager()->detach($message);

        return $deleted > 0;
    }

    /**
     * Every (tenant, queue) that has messages; a loose scan of idx_tenant_claim.
     *
     * @return list<array{tenant_id: string, queue: string}>
     */
    public function distinctQueues(?string $tenant = null, ?string $queue = null): array
    {
        $where = [];
        $params = [];

        if ($tenant !== null) {
            $where[] = 'tenant_id = :tenant';
            $params['tenant'] = $tenant;
        }

        if ($queue !== null) {
            $where[] = 'queue = :queue';
            $params['queue'] = $queue;
        }

        $sql = 'SELECT DISTINCT tenant_id, queue FROM messages'
            . ($where === [] ? '' : ' WHERE ' . implode(' AND ', $where))
            . ' ORDER BY tenant_id, queue';

        return $this->getEntityManager()->getConnection()->fetchAllAssociative($sql, $params);
    }

    /**
     * One batch of the retention purge for one queue and status; served by
     * idx_tenant_purge (tenant_id, queue, status, updated_at).
     */
    public function purgeBatch(string $tenant, string $queue, MessageStatus $status, string $before, int $limit): int
    {
        return (int) $this->getEntityManager()->getConnection()->executeStatement(
            $this->dialect->deleteMessages(
                'tenant_id = :tenant AND queue = :queue AND status = :status AND updated_at < :before',
            ),
            [
                'tenant' => $tenant,
                'queue' => $queue,
                'status' => $status->value,
                'before' => $before,
                'limit' => $limit,
            ],
            ['limit' => \Doctrine\DBAL\ParameterType::INTEGER],
        );
    }

    /**
     * Statistics per queue of a tenant (QueueStats in the spec), all but the number of
     * completed messages (completedCount(); QueueStatsService caches it). Only open and
     * dead-letter rows are read, through ranges of (tenant_id, queue, status): the
     * completed messages, which are most of them, are not touched.
     *
     * @param list<string> $queues
     * @return array<string, array{
     *     ready: int, delayed: int, processing: int, completed: int, failed: int,
     *     oldest_ready_seconds: int, oldest_processing_seconds: int, max_retry: int
     * }> completed is 0
     */
    public function queueStats(string $tenant, array $queues, \DateTimeInterface $now): array
    {
        if ($queues === []) {
            return [];
        }

        $conn = $this->getEntityManager()->getConnection();
        $filter = 'AND queue IN (:queues)';
        $params = ['tenant' => $tenant, 'queues' => $queues];
        $types = ['queues' => ArrayParameterType::STRING];

        $empty = [
            'ready' => 0,
            'delayed' => 0,
            'processing' => 0,
            'completed' => 0,
            'failed' => 0,
            'oldest_ready_seconds' => 0,
            'oldest_processing_seconds' => 0,
            'max_retry' => 0,
        ];
        $stats = [];

        foreach ($queues as $queue) {
            $stats[$queue] = $empty;
        }

        $failed = $conn->fetchAllAssociative(
            "SELECT queue, COUNT(*) AS n FROM messages
             WHERE tenant_id = :tenant {$filter} AND status = 'failed'
             GROUP BY queue",
            $params,
            $types,
        );

        foreach ($failed as $row) {
            $stats[$row['queue']]['failed'] = (int) $row['n'];
        }

        $nowSql = Utc::sql($now);
        $open = $conn->fetchAllAssociative(
            <<<SQL
                SELECT queue,
                       SUM(CASE WHEN status = 'pending' AND (processing_after IS NULL OR processing_after <= :now)
                           THEN 1 ELSE 0 END) AS ready,
                       SUM(CASE WHEN status = 'pending' AND processing_after > :now THEN 1 ELSE 0 END) AS delayed_count,
                       SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) AS processing,
                       MIN(CASE WHEN status = 'pending' AND (processing_after IS NULL OR processing_after <= :now)
                           THEN COALESCE(processing_after, created_at) END) AS oldest_ready,
                       MIN(CASE WHEN status = 'processing' THEN processing_started_at END) AS oldest_processing,
                       MAX(retry_count) AS max_retry
                FROM messages
                WHERE tenant_id = :tenant {$filter}
                  AND status IN ('pending', 'processing')
                GROUP BY queue
                SQL,
            $params + ['now' => $nowSql],
            $types,
        );

        $age = static fn(?string $time): int => $time === null
            ? 0
            : max(0, $now->getTimestamp() - Utc::parse($time)->getTimestamp());

        foreach ($open as $row) {
            $stats[$row['queue']]['ready'] = (int) $row['ready'];
            $stats[$row['queue']]['delayed'] = (int) $row['delayed_count'];
            $stats[$row['queue']]['processing'] = (int) $row['processing'];
            $stats[$row['queue']]['oldest_ready_seconds'] = $age($row['oldest_ready']);
            $stats[$row['queue']]['oldest_processing_seconds'] = $age($row['oldest_processing']);
            $stats[$row['queue']]['max_retry'] = (int) $row['max_retry'];
        }

        ksort($stats);

        return $stats;
    }

    /**
     * Which consumers hold messages of these queues now, and how many: the processing
     * rows of the queues, through idx_tenant_claim. A message claimed without a consumer
     * name counts under WorkerRegistry::name('').
     *
     * @param list<string> $queues
     * @return array<string, array<string, int>> queue => consumer => messages
     */
    public function processingByConsumer(string $tenant, array $queues): array
    {
        $holding = array_fill_keys($queues, []);

        if ($queues === []) {
            return $holding;
        }

        $rows = $this->getEntityManager()->getConnection()->fetchAllAssociative(
            <<<'SQL'
                SELECT queue, consumed_by, COUNT(*) AS n FROM messages
                WHERE tenant_id = :tenant AND queue IN (:queues) AND status = 'processing'
                GROUP BY queue, consumed_by
                SQL,
            ['tenant' => $tenant, 'queues' => $queues],
            ['queues' => ArrayParameterType::STRING],
        );

        foreach ($rows as $row) {
            $name = \App\Service\WorkerRegistry::name((string) $row['consumed_by']);
            $holding[$row['queue']][$name] = ($holding[$row['queue']][$name] ?? 0) + (int) $row['n'];
        }

        return $holding;
    }

    /**
     * When the next pending message of a queue that is not ready yet will be (a retry
     * after its backoff, a delayed publish), or null when none is waiting for a time: what
     * a waiting claim wakes up for when nothing announces it. Reads the queue's pending
     * part of idx_tenant_claim, which holds processing_after.
     */
    public function nextAvailableAt(string $tenant, string $queue, \DateTimeInterface $now): ?\DateTimeImmutable
    {
        $next = $this->getEntityManager()->getConnection()->fetchOne(
            <<<'SQL'
                SELECT MIN(processing_after) FROM messages
                WHERE tenant_id = :tenant AND queue = :queue AND status = 'pending' AND processing_after > :now
                SQL,
            ['tenant' => $tenant, 'queue' => $queue, 'now' => Utc::sql($now)],
        );

        return is_string($next) ? Utc::parse($next) : null;
    }

    /**
     * The completed messages of one queue: a count over the queue's completed range of the
     * index, so it takes time in proportion to their number (about 10 ms per 50,000).
     */
    public function completedCount(string $tenant, string $queue): int
    {
        return (int) $this->getEntityManager()->getConnection()->fetchOne(
            "SELECT COUNT(*) FROM messages WHERE tenant_id = :tenant AND queue = :queue AND status = 'completed'",
            ['tenant' => $tenant, 'queue' => $queue],
        );
    }

    /**
     * Put a finished message back in line: attempts to zero, no error, no lease, available
     * now. Only from failed or completed, so a message in flight is never run twice.
     *
     * @return bool false when the message was not failed or completed (any more)
     */
    public function replay(Message $message): bool
    {
        $conn = $this->getEntityManager()->getConnection();
        $now = Utc::now();

        try {
            $replayed = $this->replayRow($conn, $message, $now);
        } catch (UniqueConstraintViolationException) {
            throw new DuplicateOpenMessageException(sprintf(
                'Another open message in queue "%s" has dedup_key "%s".',
                $message->getQueue(),
                $message->getDedupKey(),
            ));
        } finally {
            $this->getEntityManager()->detach($message);
        }

        return $replayed;
    }

    private function replayRow(Connection $conn, Message $message, \DateTimeInterface $now): bool
    {
        return $this->retryOnDeadlock(fn() => $conn->executeStatement(
            'UPDATE messages SET ' . self::REPLAY_ASSIGNMENTS . ' WHERE id = :id AND status IN (:failed, :completed)',
            [
                'id' => $message->getId(),
                'now' => Utc::sql($now),
                'pending' => MessageStatus::PENDING->value,
                'failed' => MessageStatus::FAILED->value,
                'completed' => MessageStatus::COMPLETED->value,
            ],
            ['id' => 'uuid_binary_ordered_time'],
        )) > 0;
    }

    /**
     * Delete a message unless it is in flight; false when it was processing (or gone).
     */
    public function deleteUnlessProcessing(Message $message): bool
    {
        $conn = $this->getEntityManager()->getConnection();

        $deleted = $this->retryOnDeadlock(fn() => $conn->executeStatement(
            'DELETE FROM messages WHERE id = :id AND status <> :processing',
            ['id' => $message->getId(), 'processing' => MessageStatus::PROCESSING->value],
            ['id' => 'uuid_binary_ordered_time'],
        )) > 0;

        $this->getEntityManager()->detach($message);

        return $deleted;
    }

    /**
     * Replay or delete the finished messages of a queue that match a filter, oldest
     * first, at most $limit. READ COMMITTED like the other range writers, so the scan
     * takes no gap locks next to claims and publishes.
     *
     * @param string $action 'replay' or 'purge'
     */
    public function bulk(
        string $action,
        string $tenant,
        string $queue,
        MessageStatus $status,
        ?string $errorType,
        ?\DateTimeInterface $before,
        int $limit,
    ): int {
        $conn = $this->getEntityManager()->getConnection();
        $where = 'tenant_id = :tenant AND queue = :queue AND status = :status';
        $params = [
            'tenant' => $tenant,
            'queue' => $queue,
            'status' => $status->value,
            'limit' => $limit,
        ];

        if ($errorType !== null) {
            $where .= ' AND ' . $this->errorType() . ' = :error_type';
            $params['error_type'] = $errorType;
        }

        if ($before !== null) {
            $where .= ' AND updated_at < :before';
            $params['before'] = Utc::sql($before);
        }

        if ($action === 'replay' && !$this->dialect->replaysInOneStatement()) {
            return $this->replayEach($conn, $where, $params);
        }

        if ($action === 'replay') {
            // IGNORE: a row whose dedup_key an open message already holds stays where it is.
            $sql = 'UPDATE IGNORE messages SET ' . self::REPLAY_ASSIGNMENTS
                . " WHERE {$where} ORDER BY id LIMIT :limit";
            $params += ['now' => Utc::sql(Utc::now()), 'pending' => MessageStatus::PENDING->value];
        } else {
            $sql = $this->dialect->deleteMessages($where, 'id');
        }

        return $this->retryOnDeadlock(function () use ($conn, $sql, $params): int {
            $this->dialect->readCommitted($conn);

            return (int) $conn->executeStatement($sql, $params, ['limit' => ParameterType::INTEGER]);
        });
    }

    /**
     * A bulk replay one message at a time, oldest first, where no UPDATE can skip the
     * rows that would break uniq_open_dedup: a message whose dedup_key an open message
     * already holds (or one replayed just before it) stays where it is.
     *
     * @param array<string, mixed> $params
     */
    private function replayEach(Connection $conn, string $where, array $params): int
    {
        $ids = $conn->fetchFirstColumn(
            "SELECT id FROM messages WHERE {$where} ORDER BY id LIMIT :limit",
            $params,
            ['limit' => ParameterType::INTEGER],
        );
        $now = Utc::sql(Utc::now());
        $replayed = 0;

        foreach ($ids as $id) {
            try {
                $replayed += $this->retryOnDeadlock(fn() => $conn->executeStatement(
                    'UPDATE messages SET ' . self::REPLAY_ASSIGNMENTS . ' WHERE id = :id AND status = :status',
                    [
                        'id' => OrderedTimeUuidType::bytes($id),
                        'status' => $params['status'],
                        'now' => $now,
                        'pending' => MessageStatus::PENDING->value,
                    ],
                    ['id' => ParameterType::BINARY],
                ));
            } catch (UniqueConstraintViolationException) {
                // Its dedup_key is taken by an open message: it stays failed or completed.
            }
        }

        return $replayed;
    }

    /**
     * One page of a queue's messages, newest first, after the message $afterId.
     *
     * $search is a message id or text in the body. An id finds that message, and the
     * messages whose body names it (a body often carries the id of what it is about).
     * Text is matched without regard to case, as stored and as JSON escapes it (a slash,
     * a quote or an accented letter is stored escaped). Either way it reads the bodies of
     * the queue's messages, newest first, until the page is full: a search that finds
     * little reads the whole queue, which is what an admin search may do.
     *
     * @return list<Message>
     */
    public function page(
        string $tenant,
        string $queue,
        ?MessageStatus $status,
        ?string $errorType,
        int $limit,
        ?string $afterId,
        ?string $search = null,
    ): array {
        $conn = $this->getEntityManager()->getConnection();
        $where = 'tenant_id = :tenant AND queue = :queue';
        $params = ['tenant' => $tenant, 'queue' => $queue];
        $types = [];

        if ($status !== null) {
            $where .= ' AND status = :status';
            $params['status'] = $status->value;
        }

        if ($errorType !== null) {
            $where .= ' AND ' . $this->errorType() . ' = :error_type';
            $params['error_type'] = $errorType;
        }

        if ($afterId !== null) {
            $where .= ' AND id < :after';
            $params['after'] = $afterId;
            $types['after'] = 'uuid_binary_ordered_time';
        }

        if ($search !== null) {
            // The text as typed, and as it reads inside a JSON string (DBAL stores bodies
            // with json_encode's default escaping).
            $needles = [mb_strtolower($search)];
            $encoded = json_encode($search);

            if ($encoded !== false) {
                $needles[] = mb_strtolower(substr($encoded, 1, -1));
            }

            $needles = array_values(array_unique($needles));
            $matches = [];

            foreach ($needles as $i => $needle) {
                $matches[] = 'LOWER(' . $this->dialect->jsonText('body') . ") LIKE :search_{$i} ESCAPE '!'";
                $params["search_{$i}"] = '%' . strtr($needle, ['!' => '!!', '%' => '!%', '_' => '!_']) . '%';
            }

            if ($this->isMessageId($search)) {
                $matches[] = 'id = :search_id';
                $params['search_id'] = $search;
                $types['search_id'] = 'uuid_binary_ordered_time';
            }

            $where .= ' AND (' . implode(' OR ', $matches) . ')';
        }

        $ids = array_map(
            OrderedTimeUuidType::bytes(...),
            $conn->fetchFirstColumn(
                "SELECT id FROM messages WHERE {$where} ORDER BY id DESC LIMIT " . (int) $limit,
                $params,
                $types,
            ),
        );

        if ($ids === []) {
            return [];
        }

        $byId = [];

        $messages = $this->createQueryBuilder('m')
            ->where('m.id IN (:ids)')
            ->setParameter('ids', $ids, ArrayParameterType::BINARY)
            ->getQuery()
            ->getResult();

        foreach ($messages as $message) {
            $byId[$message->getId()->getBytes()] = $message;
        }

        $platform = $conn->getDatabasePlatform();
        $type = Type::getType('uuid_binary_ordered_time');
        $ordered = [];

        foreach ($ids as $id) {
            $key = $type->convertToPHPValue($id, $platform)->getBytes();

            if (isset($byId[$key])) {
                $ordered[] = $byId[$key];
            }
        }

        return $ordered;
    }

    /**
     * The lease state of one message, without hydrating it: what v1 extend, ack, fail and
     * release check the receipt against, and what makes a repeated ack or fail answer the
     * same as the first.
     *
     * `expired` tells whether the last attempt ended by its lease running out rather than
     * by a call with the receipt.
     *
     * @return array{
     *     queue: string, status: string, receipt: ?string, lease_until: ?string,
     *     processing_after: ?string, retry_count: int, max_attempts: ?int, expired: bool
     * }|null
     */
    public function leaseState(string $tenant, string $id): ?array
    {
        if (!Uuid::isValid($id) || Uuid::fromString($id)->getFields()->getVersion() !== 1) {
            return null;
        }

        $row = $this->getEntityManager()->getConnection()->fetchAssociative(
            <<<SQL
                SELECT queue, status, receipt, lease_until, processing_after, retry_count, max_attempts,
                       {$this->dialect->jsonValue('error_details', 'expired')} AS expired
                FROM messages
                WHERE id = :id AND tenant_id = :tenant
                SQL,
            ['id' => $id, 'tenant' => $tenant],
            ['id' => 'uuid_binary_ordered_time'],
        );

        if ($row === false) {
            return null;
        }

        $row['retry_count'] = (int) $row['retry_count'];
        $row['max_attempts'] = $row['max_attempts'] === null ? null : (int) $row['max_attempts'];
        // A JSON true reads as 1 on MariaDB and SQLite, as true on PostgreSQL and MySQL.
        $row['expired'] = in_array((string) $row['expired'], ['1', 'true'], true);

        return $row;
    }

    /**
     * Move a current lease's end; false when the receipt is not the current lease.
     */
    public function extendLease(string $tenant, string $id, string $receipt, \DateTimeInterface $until): bool
    {
        $conn = $this->getEntityManager()->getConnection();
        $now = Utc::sql(Utc::now());

        return $this->retryOnDeadlock(fn() => $conn->executeStatement(
            <<<'SQL'
                UPDATE messages
                SET lease_until = :until, updated_at = :now
                WHERE id = :id AND tenant_id = :tenant AND status = 'processing'
                  AND receipt = :receipt AND lease_until > :now
                SQL,
            ['until' => Utc::sql($until), 'now' => $now, 'id' => $id, 'tenant' => $tenant, 'receipt' => $receipt],
            ['id' => 'uuid_binary_ordered_time'],
        )) > 0;
    }

    /**
     * End every expired lease of a queue. An expired lease is a failed attempt: the
     * message goes back to pending after the queue's backoff, or, when that was its last
     * attempt, to the dead-letter set or away (on_exhausted).
     *
     * Leases from the release before leases existed have no lease_until; they expire by
     * processing_started_at plus the queue's lease, as before.
     *
     * Runs READ COMMITTED: it updates a (queue, processing) range while claims move rows
     * into that range, which under REPEATABLE READ closed a lock cycle with them.
     *
     * @return array{retried: int, dead_lettered: int, discarded: int}
     */
    public function expireLeases(
        string $tenant,
        string $queue,
        \App\Model\QueueConfig $config,
        \DateTimeInterface $now,
    ): array {
        $conn = $this->getEntityManager()->getConnection();
        $settings = $config->settings;
        $params = [
            'tenant' => $tenant,
            'queue' => $queue,
            'now' => Utc::sql($now),
            'max_attempts' => $config->int('max_attempts'),
            'limit' => $config->int('max_attempts_limit'),
        ];
        $expired = <<<'SQL'
            tenant_id = :tenant AND queue = :queue AND status = 'processing'
            AND lease_until < :now
            SQL;
        // Numbers are bound as numbers: SQLite compares a number bound as text as text.
        $types = [
            'max_attempts' => ParameterType::INTEGER,
            'limit' => ParameterType::INTEGER,
            'initial' => ParameterType::INTEGER,
            'max_delay' => ParameterType::INTEGER,
        ];
        $d = $this->dialect;
        $exhausted = 'retry_count + 1 >= ' . $d->least('COALESCE(max_attempts, :max_attempts)', ':limit');
        $errorDetails = $d->jsonObject([
            'message' => "'Lease expired'",
            'expired' => $d->jsonBool(true),
            'at' => $d->jsonString(':at'),
            'attempt' => 'retry_count + 1',
            'consumer' => 'consumed_by',
            'route' => 'route',
            'permanent' => $d->jsonBool(false),
        ]);
        // The attempt's route failed now; a route is a plain JSON key (Route::PATTERN).
        $failedRoutes = 'CASE WHEN route IS NULL THEN failed_routes ELSE '
            . $d->jsonWith('failed_routes', 'route', $d->jsonString(':now')) . ' END';
        $delay = $d->least(':initial * ' . $d->power(':multiplier', 'retry_count'), ':max_delay');
        $backoff = $d->plusSeconds(':now', "FLOOR({$delay})");
        $at = \DateTimeImmutable::createFromInterface($now)->setTimezone(Utc::zone())->format('Y-m-d\\TH:i:s\\Z');

        $sweep = function () use (
            $conn,
            $settings,
            $params,
            $expired,
            $exhausted,
            $errorDetails,
            $failedRoutes,
            $backoff,
            $at,
            $types,
        ): array {
            $this->dialect->readCommitted($conn);
            $conn->beginTransaction();

            try {
                $result = ['retried' => 0, 'dead_lettered' => 0, 'discarded' => 0];

                if ($settings['on_exhausted'] === 'discard') {
                    $result['discarded'] = (int) $conn->executeStatement(
                        "DELETE FROM messages WHERE {$expired} AND {$exhausted}",
                        $params,
                        $types,
                    );
                } else {
                    // error_details first: MariaDB and MySQL assign left to right, so it still
                    // sees the retry_count and consumed_by of the attempt that expired
                    // (PostgreSQL and SQLite read the old row throughout).
                    $result['dead_lettered'] = (int) $conn->executeStatement(
                        <<<SQL
                            UPDATE messages
                            SET error_message = 'Lease expired',
                                error_details = {$errorDetails},
                                failed_routes = {$failedRoutes},
                                dedup_open = NULL,
                                status = 'failed',
                                failed_at = :now,
                                lease_until = NULL,
                                updated_at = :now
                            WHERE {$expired} AND {$exhausted}
                            SQL,
                        $params + ['at' => $at],
                        $types,
                    );
                }

                $result['retried'] = (int) $conn->executeStatement(
                    <<<SQL
                        UPDATE messages
                        SET error_message = 'Lease expired',
                            error_details = {$errorDetails},
                            failed_routes = {$failedRoutes},
                            processing_after = {$backoff},
                            retry_count = retry_count + 1,
                            status = 'pending',
                            consumed_by = NULL,
                            processing_started_at = NULL,
                            lease_until = NULL,
                            updated_at = :now
                        WHERE {$expired}
                        SQL,
                    $params + [
                        'at' => $at,
                        'initial' => $settings['backoff']['initial_seconds'],
                        'multiplier' => $settings['backoff']['multiplier'],
                        'max_delay' => $settings['backoff']['max_seconds'],
                    ],
                    $types,
                );

                $conn->commit();

                return $result;
            } catch (\Throwable $e) {
                if ($conn->isTransactionActive()) {
                    $conn->rollBack();
                }

                throw $e;
            }
        };

        return $this->retryOnDeadlock($sweep);
    }
}
