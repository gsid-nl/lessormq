<?php

declare(strict_types=1);

namespace App\Tests\Service;

use App\Entity\Message;
use App\Repository\MessageRepository;
use App\Service\MessageCreator;
use Doctrine\DBAL\Driver\Exception as DriverException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Ramsey\Uuid\Uuid;

#[\PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations]
class MessageCreatorTest extends TestCase
{
    private MessageRepository&MockObject $messageRepository;
    private MessageCreator $creator;

    protected function setUp(): void
    {
        $this->messageRepository = $this->createMock(MessageRepository::class);
        $this->creator = new MessageCreator($this->messageRepository, $this->createMock(LoggerInterface::class));
    }

    public function testCreatesANewMessage(): void
    {
        $this->messageRepository->expects($this->once())->method('add');

        [$message, $duplicate] = $this->creator->create('acme', 'mail', ['to' => 'ada'], null, null);

        $this->assertFalse($duplicate);
        $this->assertSame(['to' => 'ada'], $message->getBody());
        $this->assertSame('acme', $message->getTenantId());
    }

    public function testAnOpenMessageWithTheSameKeyIsReturned(): void
    {
        $open = $this->message();
        $this->messageRepository->method('findOpenByDedupKey')->willReturn($open);
        $this->messageRepository->expects($this->never())->method('add');

        [$message, $duplicate] = $this->creator->create('acme', 'mail', ['n' => 2], null, 'order-7');

        $this->assertSame($open, $message);
        $this->assertTrue($duplicate);
    }

    public function testRetriesWithAFreshMessageWhenTheIdCollides(): void
    {
        $attempts = [];
        $this->messageRepository->expects($this->exactly(2))
            ->method('add')
            ->willReturnCallback(function (Message $message) use (&$attempts): void {
                $attempts[] = $message;
                if (count($attempts) === 1) {
                    throw $this->collision();
                }
            });
        $this->messageRepository->expects($this->once())->method('resetManager');

        [$message] = $this->creator->create('acme', 'mail', ['key' => 'value']);

        $this->assertNotSame($attempts[0], $attempts[1]);
        $this->assertSame($attempts[1], $message);
    }

    public function testGivesUpAfterRepeatedIdCollisions(): void
    {
        $this->messageRepository->expects($this->exactly(3))->method('add')->willThrowException($this->collision());

        $this->expectException(UniqueConstraintViolationException::class);

        $this->creator->create('acme', 'mail', ['key' => 'value']);
    }

    public function testConcurrentPublishesWithTheSameKeyReturnTheWinner(): void
    {
        $winner = $this->message();
        // Nothing open yet when checked, then the insert loses on uniq_open_dedup.
        $this->messageRepository->expects($this->exactly(2))
            ->method('findOpenByDedupKey')
            ->willReturnOnConsecutiveCalls(null, $winner);
        $this->messageRepository->expects($this->once())->method('add')->willThrowException($this->collision());

        [$message, $duplicate] = $this->creator->create('acme', 'mail', ['n' => 2], null, 'order-7');

        $this->assertSame($winner, $message);
        $this->assertTrue($duplicate);
    }

    private function message(): Message
    {
        $message = new Message('acme', 'mail', ['n' => 1]);
        (new \ReflectionProperty(Message::class, 'id'))->setValue($message, Uuid::uuid1());

        return $message;
    }

    private function collision(): UniqueConstraintViolationException
    {
        $driver = new class ('Duplicate entry', 1062) extends \Exception implements DriverException {
            public function getSQLState(): ?string
            {
                return '23000';
            }
        };

        return new UniqueConstraintViolationException($driver, null);
    }
}
