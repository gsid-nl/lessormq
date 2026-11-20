<?php

declare(strict_types=1);

namespace App\Command;

use App\Repository\ApiKeyRepository;
use App\Security\ApiKeyFactory;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Operator keys manage tenants. They are created here and nowhere else, so no API key,
 * leaked or not, can mint one.
 */
#[AsCommand(
    name: 'lessormq:operator-key:create',
    description: 'Create an operator key (manages tenants); prints the secret once',
)]
final class CreateOperatorKeyCommand extends Command
{
    public function __construct(private readonly ApiKeyRepository $apiKeys)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this->addOption('name', null, InputOption::VALUE_REQUIRED, 'What the key is for', 'operator');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        [$key, $secret] = ApiKeyFactory::create(null, (string) $input->getOption('name'), []);
        $this->apiKeys->save($key);

        $output->writeln(sprintf(
            'Operator key %s (%s) created. Store the secret now; it is not shown again:',
            $key->getId(),
            $key->getName(),
        ));
        $output->writeln($secret);

        return Command::SUCCESS;
    }
}
