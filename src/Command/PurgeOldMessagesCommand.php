<?php

declare(strict_types=1);

namespace App\Command;

use App\Entity\Enum\MessageStatus;
use App\Repository\MessageRepository;
use App\Service\QueueSettingsService;
use App\Service\QueueStatsService;
use App\Time\Utc;
use Psr\Log\LoggerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Removes finished messages once their queue's retention has passed: retention.completed_seconds
 * for completed messages, retention.failed_seconds for the dead-letter set.
 */
#[AsCommand(
    name: 'app:purge-old-messages',
    description: 'Purge completed and failed messages past their queue\'s retention',
)]
class PurgeOldMessagesCommand extends Command
{
    private const BATCH_SIZE = 1000;

    public function __construct(
        private readonly MessageRepository $messages,
        private readonly QueueSettingsService $queueSettings,
        private readonly QueueStatsService $queueStats,
        private readonly LoggerInterface $logger,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption(
                'hours',
                null,
                InputOption::VALUE_REQUIRED,
                'Ignore the queue settings and purge everything finished more than this many hours ago',
            )
            ->addOption('batch-size', null, InputOption::VALUE_REQUIRED, 'Rows deleted per statement', self::BATCH_SIZE)
            ->addOption('tenant', null, InputOption::VALUE_REQUIRED, 'Only purge this tenant')
            ->addOption('queue', null, InputOption::VALUE_REQUIRED, 'Only purge this queue');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $hours = $input->getOption('hours');
        $batchSize = max(1, (int) $input->getOption('batch-size'));
        $total = 0;

        foreach ($this->messages->distinctQueues($input->getOption('tenant'), $input->getOption('queue')) as $row) {
            $tenant = $row['tenant_id'];
            $queue = $row['queue'];
            $retention = $this->queueSettings->forQueue($tenant, $queue)->settings['retention'];

            foreach (
                [
                    [MessageStatus::COMPLETED, $retention['completed_seconds']],
                    [MessageStatus::FAILED, $retention['failed_seconds']],
                ] as [$status, $seconds]
            ) {
                $keep = $hours !== null ? (int) $hours * 3600 : (int) $seconds;
                $before = Utc::sql(Utc::now('-' . $keep . ' seconds'));
                $deleted = 0;

                do {
                    $batch = $this->messages->purgeBatch($tenant, $queue, $status, $before, $batchSize);
                    $deleted += $batch;
                } while ($batch >= $batchSize);

                if ($deleted > 0 && $status === MessageStatus::COMPLETED) {
                    $this->queueStats->forget($tenant, $queue);
                }

                if ($deleted > 0) {
                    $output->writeln(sprintf(
                        '%s/%s: deleted %d %s messages',
                        $tenant,
                        $queue,
                        $deleted,
                        $status->value,
                    ));
                }

                $total += $deleted;
            }
        }

        $this->logger->info('Purged old messages', ['total_deleted' => $total, 'hours_override' => $hours]);
        $output->writeln(sprintf('Done. Purged %d messages.', $total));

        return Command::SUCCESS;
    }
}
