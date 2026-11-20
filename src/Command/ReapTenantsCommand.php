<?php

declare(strict_types=1);

namespace App\Command;

use App\Repository\ApiKeyRepository;
use App\Repository\MessageRepository;
use App\Repository\QueueRepository;
use App\Repository\TenantRepository;
use App\Service\QueueSettingsService;
use Psr\Log\LoggerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Removes tenants marked for deletion, with all their data, in small batches so the
 * shared messages table is never locked for long. Run it on a schedule.
 */
#[AsCommand(
    name: 'lessormq:tenants:reap',
    description: 'Delete the data of tenants marked for deletion',
)]
final class ReapTenantsCommand extends Command
{
    public function __construct(
        private readonly TenantRepository $tenants,
        private readonly ApiKeyRepository $apiKeys,
        private readonly MessageRepository $messages,
        private readonly QueueRepository $queues,
        private readonly QueueSettingsService $queueSettings,
        private readonly LoggerInterface $logger,
    ) {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('batch-size', null, InputOption::VALUE_REQUIRED, 'Messages deleted per statement', 1000);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $batchSize = max(1, (int) $input->getOption('batch-size'));

        foreach ($this->tenants->findDeleting() as $tenant) {
            $id = $tenant->getId();
            $deleted = 0;

            do {
                $batch = $this->messages->deleteTenantBatch($id, $batchSize);
                $deleted += $batch;
            } while ($batch >= $batchSize);

            $keys = $this->apiKeys->deleteForTenant($id);
            $this->queues->deleteForTenant($id);
            $this->queueSettings->invalidateTenant($id);
            $this->tenants->remove($tenant);

            $this->logger->info('Tenant deleted', ['tenant' => $id, 'messages' => $deleted, 'keys' => $keys]);
            $output->writeln(sprintf('Deleted tenant %s: %d messages, %d keys.', $id, $deleted, $keys));
        }

        return Command::SUCCESS;
    }
}
