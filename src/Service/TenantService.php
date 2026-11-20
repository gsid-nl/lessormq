<?php

declare(strict_types=1);

namespace App\Service;

use App\Api\ApiProblem;
use App\Api\Input;
use App\Api\Time;
use App\Entity\Enum\Scope;
use App\Entity\Enum\TenantStatus;
use App\Entity\Tenant;
use App\Model\QueueSettings;
use App\Model\Quotas;
use App\Repository\ApiKeyRepository;
use App\Repository\MessageRepository;
use App\Repository\TenantRepository;
use App\Security\ApiKeyFactory;

class TenantService
{
    public function __construct(
        private readonly TenantRepository $tenants,
        private readonly ApiKeyRepository $apiKeys,
        private readonly MessageRepository $messages,
        private readonly ApiKeyService $apiKeyService,
        private readonly QueueSettingsService $queueSettings,
    ) {
    }

    public function get(string $id): Tenant
    {
        $tenant = $this->tenants->find($id);

        if ($tenant === null) {
            throw ApiProblem::notFound(sprintf('No tenant "%s".', $id));
        }

        return $tenant;
    }

    /**
     * @return array<string, mixed> the tenant, plus its first admin key unless asked not to
     */
    public function createFromInput(Input $input): array
    {
        $input->allowOnly('id', 'name', 'quotas', 'queue_defaults', 'create_admin_key');
        $name = $input->string('name', required: true);
        $id = $input->string('id', max: 63, pattern: Tenant::ID_PATTERN);
        $createKey = $input->bool('create_admin_key') ?? true;

        if ($input->has('quotas')) {
            $input->addErrors(Quotas::validate($input->raw('quotas')));
        }

        if ($input->has('queue_defaults')) {
            $input->addErrors(QueueSettings::validatePatch($input->raw('queue_defaults'), prefix: 'queue_defaults.'));
        }

        $input->validate();

        if ($id !== null && $this->tenants->find($id) !== null) {
            throw ApiProblem::invalidState(sprintf('Tenant "%s" already exists.', $id));
        }

        $tenant = new Tenant($id ?? $this->generateId($name), $name);

        if ($input->has('quotas')) {
            $tenant->patchQuotas($input->raw('quotas'));
        }

        if ($input->has('queue_defaults')) {
            $tenant->patchQueueDefaults($input->raw('queue_defaults'));
        }

        $this->tenants->save($tenant);

        $view = $this->view($tenant);
        $view['admin_key'] = null;

        if ($createKey) {
            [$key, $secret] = ApiKeyFactory::create(
                $tenant->getId(),
                'admin',
                [Scope::PUBLISH, Scope::CONSUME, Scope::ADMIN],
            );
            $this->apiKeys->save($key);
            $view['admin_key'] = $this->apiKeyService->view($key) + ['secret' => $secret];
        }

        return $view;
    }

    /**
     * Operator changes: name, status, quotas, queue defaults.
     */
    public function updateFromInput(Tenant $tenant, Input $input): void
    {
        $input->allowOnly('name', 'status', 'quotas', 'queue_defaults');
        $name = $input->string('name');
        $status = $input->choice('status', [TenantStatus::ACTIVE->value, TenantStatus::SUSPENDED->value]);

        if ($input->has('quotas')) {
            $input->addErrors(Quotas::validate($input->raw('quotas')));
        }

        if ($input->has('queue_defaults')) {
            $input->addErrors(QueueSettings::validatePatch(
                $input->raw('queue_defaults'),
                $tenant->getQueueDefaults(),
                'queue_defaults.',
            ));
        }

        $input->validate();

        if ($tenant->getStatus() === TenantStatus::DELETING) {
            throw ApiProblem::invalidState('The tenant is being deleted.');
        }

        if ($name !== null) {
            $tenant->rename($name);
        }

        if ($status === TenantStatus::SUSPENDED->value) {
            $tenant->suspend();
        } elseif ($status === TenantStatus::ACTIVE->value) {
            $tenant->activate();
        }

        if ($input->has('quotas')) {
            $tenant->patchQuotas($input->raw('quotas'));
        }

        if ($input->has('queue_defaults')) {
            $tenant->patchQueueDefaults($input->raw('queue_defaults'));
        }

        $this->tenants->save($tenant);

        if ($input->has('queue_defaults')) {
            $this->queueSettings->invalidateTenant($tenant->getId());
        }
    }

    /**
     * A tenant admin may only change the defaults for its own new queues.
     */
    public function updateOwnFromInput(Tenant $tenant, Input $input): void
    {
        $input->allowOnly('queue_defaults');

        if ($input->has('queue_defaults')) {
            $input->addErrors(QueueSettings::validatePatch(
                $input->raw('queue_defaults'),
                $tenant->getQueueDefaults(),
                'queue_defaults.',
            ));
        }

        $input->validate();

        if ($input->has('queue_defaults')) {
            $tenant->patchQueueDefaults($input->raw('queue_defaults'));
            $this->tenants->save($tenant);
            $this->queueSettings->invalidateTenant($tenant->getId());
        }
    }

    /**
     * Deleting takes two steps on purpose: suspend, then delete. The data is removed by
     * `lessormq:tenants:reap`, not in this request.
     */
    public function requestDeletion(Tenant $tenant): void
    {
        if ($tenant->getStatus() === TenantStatus::ACTIVE) {
            throw ApiProblem::invalidState('Suspend the tenant before deleting it.');
        }

        $tenant->markDeleting();
        $this->tenants->save($tenant);
    }

    /**
     * @return array<string, mixed>
     */
    public function view(Tenant $tenant): array
    {
        return [
            'id' => $tenant->getId(),
            'name' => $tenant->getName(),
            'status' => $tenant->getStatus()->value,
            'quotas' => $tenant->getQuotas(),
            'queue_defaults' => $tenant->getQueueDefaults(),
            'usage' => $this->messages->tenantUsage($tenant->getId()),
            'created_at' => Time::format($tenant->getCreatedAt()),
            'suspended_at' => Time::format($tenant->getSuspendedAt()),
        ];
    }

    /**
     * A readable id from the name, with a random suffix when that one is taken.
     */
    private function generateId(string $name): string
    {
        $base = trim((string) preg_replace('/[^a-z0-9]+/', '-', strtolower($name)), '-');
        $base = substr($base, 0, 54);

        if (Tenant::isValidId($base) && $this->tenants->find($base) === null) {
            return $base;
        }

        do {
            $candidate = ltrim($base . '-' . bin2hex(random_bytes(4)), '-');
        } while ($this->tenants->find($candidate) !== null);

        return $candidate;
    }
}
