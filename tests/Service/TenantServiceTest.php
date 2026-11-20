<?php

declare(strict_types=1);

namespace App\Tests\Service;

use App\Api\ApiProblem;
use App\Api\Input;
use App\Entity\ApiKey;
use App\Entity\Enum\TenantStatus;
use App\Entity\Tenant;
use App\Repository\ApiKeyRepository;
use App\Repository\MessageRepository;
use App\Repository\TenantRepository;
use App\Security\ApiKeyFactory;
use App\Service\ApiKeyService;
use App\Service\QueueSettingsService;
use App\Service\TenantService;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

#[AllowMockObjectsWithoutExpectations]
class TenantServiceTest extends TestCase
{
    private TenantRepository&MockObject $tenants;
    private ApiKeyRepository&MockObject $apiKeys;
    private TenantService $service;

    protected function setUp(): void
    {
        $this->tenants = $this->createMock(TenantRepository::class);
        $this->apiKeys = $this->createMock(ApiKeyRepository::class);
        $messages = $this->createMock(MessageRepository::class);
        $messages->method('tenantUsage')->willReturn(['queues' => 0, 'open_messages' => 0, 'failed_messages' => 0]);

        $this->service = new TenantService(
            $this->tenants,
            $this->apiKeys,
            $messages,
            new ApiKeyService($this->apiKeys),
            $this->createMock(QueueSettingsService::class),
        );
    }

    public function testCreateDerivesTheIdFromTheNameAndReturnsAnAdminKeyOnce(): void
    {
        $this->tenants->method('find')->willReturn(null);
        $this->tenants->expects($this->once())->method('save');

        $savedKey = null;
        $this->apiKeys->expects($this->once())->method('save')
            ->willReturnCallback(function (ApiKey $key) use (&$savedKey): void {
                $savedKey = $key;
            });

        $view = $this->service->createFromInput(Input::fromArray([
            'name' => 'Acme Corp.',
            'queue_defaults' => ['lease_seconds' => 120],
        ]));

        $this->assertSame('acme-corp', $view['id']);
        $this->assertSame(120, $view['queue_defaults']['lease_seconds']);
        $this->assertSame(['publish', 'consume', 'admin'], $view['admin_key']['scopes']);
        $this->assertStringStartsWith(ApiKeyFactory::SECRET_PREFIX, $view['admin_key']['secret']);
        // Only the hash is stored
        $this->assertSame('acme-corp', $savedKey->getTenantId());
        $this->assertStringStartsWith($savedKey->getPrefix(), $view['admin_key']['secret']);
    }

    public function testCreateWithoutAdminKey(): void
    {
        $this->tenants->method('find')->willReturn(null);
        $this->apiKeys->expects($this->never())->method('save');

        $view = $this->service->createFromInput(Input::fromArray([
            'id' => 'acme',
            'name' => 'Acme',
            'create_admin_key' => false,
        ]));

        $this->assertSame('acme', $view['id']);
        $this->assertNull($view['admin_key']);
    }

    public function testCreateRefusesATakenId(): void
    {
        $this->tenants->method('find')->willReturn(new Tenant('acme', 'Acme'));

        $this->expectExceptionObject(ApiProblem::invalidState('Tenant "acme" already exists.'));
        $this->service->createFromInput(Input::fromArray(['id' => 'acme', 'name' => 'Acme']));
    }

    public function testCreateValidatesQuotasAndDefaults(): void
    {
        try {
            $this->service->createFromInput(Input::fromArray([
                'name' => 'Acme',
                'quotas' => ['max_queues' => 0],
                'queue_defaults' => ['lease_seconds' => 'long'],
            ]));
            $this->fail('Expected a validation problem');
        } catch (ApiProblem $problem) {
            $this->assertSame(
                ['quotas.max_queues', 'queue_defaults.lease_seconds'],
                array_column($problem->errors, 'field'),
            );
        }
    }

    public function testAnActiveTenantMustBeSuspendedBeforeDeletion(): void
    {
        $tenant = new Tenant('acme', 'Acme');

        try {
            $this->service->requestDeletion($tenant);
            $this->fail('Expected an invalid_state problem');
        } catch (ApiProblem $problem) {
            $this->assertSame('invalid_state', $problem->problemCode);
        }

        $tenant->suspend();
        $this->service->requestDeletion($tenant);
        $this->assertSame(TenantStatus::DELETING, $tenant->getStatus());
    }

    public function testSuspendAndResume(): void
    {
        $tenant = new Tenant('acme', 'Acme');

        $this->service->updateFromInput($tenant, Input::fromArray(['status' => 'suspended']));
        $this->assertSame(TenantStatus::SUSPENDED, $tenant->getStatus());
        $this->assertNotNull($tenant->getSuspendedAt());

        $this->service->updateFromInput(
            $tenant,
            Input::fromArray(['status' => 'active', 'quotas' => ['max_queues' => 3]]),
        );
        $this->assertTrue($tenant->isActive());
        $this->assertNull($tenant->getSuspendedAt());
        $this->assertSame(3, $tenant->getQuotas()['max_queues']);

        $this->service->updateFromInput($tenant, Input::fromArray(['quotas' => ['max_queues' => null]]));
        $this->assertNull($tenant->getQuotas()['max_queues']);
    }

    public function testATenantOnlyChangesItsOwnQueueDefaults(): void
    {
        $tenant = new Tenant('acme', 'Acme');

        try {
            $this->service->updateOwnFromInput($tenant, Input::fromArray(['quotas' => ['max_queues' => 1000]]));
            $this->fail('Expected a validation problem');
        } catch (ApiProblem $problem) {
            $this->assertSame('quotas', $problem->errors[0]['field']);
        }

        $this->service->updateOwnFromInput($tenant, Input::fromArray(['queue_defaults' => ['max_attempts' => 3]]));
        $this->assertSame(3, $tenant->getQueueDefaults()['max_attempts']);
    }

    public function testAKeyCannotRevokeItself(): void
    {
        [$key] = ApiKeyFactory::create('acme', 'admin', []);
        $this->apiKeys->method('findForTenant')->willReturn($key);

        $service = new ApiKeyService($this->apiKeys);

        try {
            $service->revoke('acme', $key->getId(), $key->getId());
            $this->fail('Expected an invalid_state problem');
        } catch (ApiProblem $problem) {
            $this->assertSame('invalid_state', $problem->problemCode);
        }

        $service->revoke('acme', $key->getId(), 'another-key');
        $this->assertTrue($key->isRevoked());
    }
}
