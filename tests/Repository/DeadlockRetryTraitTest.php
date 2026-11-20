<?php

declare(strict_types=1);

namespace App\Tests\Repository;

use App\Repository\DeadlockRetryTrait;
use Doctrine\DBAL\Driver\Exception as DriverException;
use Doctrine\DBAL\Exception\DeadlockException;
use Doctrine\DBAL\Exception\LockWaitTimeoutException;
use PHPUnit\Framework\TestCase;

class DeadlockRetryTraitTest extends TestCase
{
    private object $subject;

    protected function setUp(): void
    {
        $this->subject = new class {
            use DeadlockRetryTrait;

            public function run(callable $operation): mixed
            {
                return $this->retryOnDeadlock($operation);
            }
        };
    }

    private function deadlock(): DeadlockException
    {
        return new DeadlockException($this->createStub(DriverException::class), null);
    }

    private function lockWaitTimeout(): LockWaitTimeoutException
    {
        return new LockWaitTimeoutException($this->createStub(DriverException::class), null);
    }

    public function testReturnsResultWithoutRetryingWhenOperationSucceeds(): void
    {
        $calls = 0;

        $result = $this->subject->run(function () use (&$calls) {
            $calls++;
            return 'ok';
        });

        $this->assertSame('ok', $result);
        $this->assertSame(1, $calls);
    }

    public function testRetriesAfterDeadlockAndReturnsLaterSuccess(): void
    {
        $calls = 0;

        $result = $this->subject->run(function () use (&$calls) {
            if (++$calls < 3) {
                throw $this->deadlock();
            }
            return 'claimed';
        });

        $this->assertSame('claimed', $result);
        $this->assertSame(3, $calls);
    }

    public function testRetriesOnLockWaitTimeout(): void
    {
        $calls = 0;

        $result = $this->subject->run(function () use (&$calls) {
            if (++$calls < 2) {
                throw $this->lockWaitTimeout();
            }
            return 'claimed';
        });

        $this->assertSame('claimed', $result);
        $this->assertSame(2, $calls);
    }

    public function testGivesUpAfterThreeAttempts(): void
    {
        $calls = 0;

        $this->expectException(DeadlockException::class);

        try {
            $this->subject->run(function () use (&$calls) {
                $calls++;
                throw $this->deadlock();
            });
        } finally {
            $this->assertSame(3, $calls);
        }
    }

    public function testDoesNotRetryOtherExceptions(): void
    {
        $calls = 0;

        $this->expectException(\RuntimeException::class);

        try {
            $this->subject->run(function () use (&$calls) {
                $calls++;
                throw new \RuntimeException('unrelated');
            });
        } finally {
            $this->assertSame(1, $calls);
        }
    }
}
