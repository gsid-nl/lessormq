<?php

declare(strict_types=1);

namespace App\Tests\Service;

use App\Entity\Enum\MessageStatus;
use App\Entity\Message;
use App\Model\QueueConfig;
use App\Model\QueueSettings;
use App\Service\MessageView;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;

class MessageViewTest extends TestCase
{
    public function testV1FailIsShownAsRecorded(): void
    {
        $message = $this->message(MessageStatus::PENDING, 1, [
            'type' => 'Timeout',
            'message' => 'slow',
            'details' => ['ms' => 3000],
            'at' => '2026-11-19T10:00:00Z',
            'attempt' => 1,
            'permanent' => false,
            'expired' => false,
            'consumer' => 'w1',
        ]);

        $error = MessageView::lastError($message);

        $this->assertSame('Timeout', $error['type']);
        $this->assertSame(['ms' => 3000], $error['details']);
        $this->assertSame('2026-11-19T10:00:00Z', $error['at']);
        $this->assertSame(1, $error['attempt']);
        $this->assertSame('w1', $error['consumer']);
    }

    public function testAttemptsCountTheFinalAttemptOfFailedAndCompletedMessages(): void
    {
        $config = new QueueConfig(QueueSettings::DEFAULTS, null, true);

        $view = fn(MessageStatus $status, int $retries) => MessageView::render(
            $this->message($status, $retries, null),
            $config,
        );

        $this->assertSame(2, $view(MessageStatus::PENDING, 2)['attempts']);
        $this->assertSame(3, $view(MessageStatus::PROCESSING, 2)['attempts']);
        $this->assertSame(3, $view(MessageStatus::FAILED, 2)['attempts']);
        $this->assertSame(1, $view(MessageStatus::COMPLETED, 0)['attempts']);
        $this->assertNull($view(MessageStatus::COMPLETED, 0)['last_error']);
    }

    /**
     * @param array<string, mixed>|null $details
     */
    private function message(MessageStatus $status, int $retries, ?array $details): Message
    {
        $message = new Message('acme', 'mail', ['n' => 1]);
        (new \ReflectionProperty(Message::class, 'id'))->setValue($message, Uuid::uuid1());
        $message->setStatus($status);
        $message->setRetryCount($retries);

        if ($details !== null) {
            $message->setErrorMessage($details['message']);
            $message->setErrorDetails(json_encode($details));
        }

        return $message;
    }
}
