<?php

declare(strict_types=1);

namespace App\Tests\Security;

use App\Api\ApiProblem;
use App\Entity\Enum\Scope;
use App\Entity\Enum\TenantStatus;
use App\Repository\ApiKeyRepository;
use App\Repository\TenantRepository;
use App\Security\ApiKeyAuthenticator;
use App\Security\ApiKeyFactory;
use App\Security\Principal;
use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Event\RequestEvent;
use Symfony\Component\HttpKernel\HttpKernelInterface;

#[AllowMockObjectsWithoutExpectations]
class ApiKeyAuthenticatorTest extends TestCase
{
    private ApiKeyRepository&MockObject $apiKeys;
    private TenantRepository&MockObject $tenants;
    private ApiKeyAuthenticator $authenticator;

    protected function setUp(): void
    {
        $this->apiKeys = $this->createMock(ApiKeyRepository::class);
        $this->tenants = $this->createMock(TenantRepository::class);
        $this->authenticator = new ApiKeyAuthenticator($this->apiKeys, $this->tenants);
    }

    public function testTenantKeyActsInItsTenantWithItsScopes(): void
    {
        $this->apiKeys->expects($this->once())
            ->method('findCredential')
            ->with(ApiKeyFactory::hash('lmq_secret'))
            ->willReturn($this->credential('acme', ['publish', 'consume'], ['import-*']));

        $principal = $this->authenticator->authenticate($this->request('lmq_secret'));

        $this->assertSame('acme', $principal->tenant());
        $this->assertFalse($principal->operator);
        $this->assertTrue($principal->has(Scope::PUBLISH));
        $this->assertFalse($principal->has(Scope::ADMIN));
        $this->assertTrue($principal->mayUseQueue('import-images'));
        $this->assertFalse($principal->mayUseQueue('mail'));
    }

    public function testMissingOrMalformedHeaderIsUnauthorized(): void
    {
        $this->assertProblem(401, 'unauthorized', fn() => $this->authenticator->authenticate(new Request()));
        $this->assertProblem(401, 'unauthorized', fn() => $this->authenticator->authenticate(
            Request::create('/v1/tenant', server: ['HTTP_AUTHORIZATION' => 'Basic abc']),
        ));
    }

    public function testUnknownKeyIsUnauthorized(): void
    {
        $this->apiKeys->method('findCredential')->willReturn(null);

        $this->assertProblem(401, 'unauthorized', fn() => $this->authenticator->authenticate($this->request('nope')));
    }

    public function testKeyOfASuspendedTenantIsRefused(): void
    {
        $this->apiKeys->method('findCredential')
            ->willReturn($this->credential('acme', ['admin'], [], TenantStatus::SUSPENDED->value));

        $this->assertProblem(403, 'tenant_suspended', fn() => $this->authenticator->authenticate($this->request('k')));
    }

    public function testOperatorKeyWithoutHeaderActsInNoTenant(): void
    {
        $this->apiKeys->method('findCredential')->willReturn($this->credential(null, [], [], null));

        $principal = $this->authenticator->authenticate($this->request('op'));

        $this->assertTrue($principal->operator);
        $this->assertNull($principal->tenantId);
        $this->assertProblem(403, 'forbidden', fn() => $principal->tenant());
    }

    public function testOperatorKeyWithHeaderIsAdminOfThatTenantEvenWhenSuspended(): void
    {
        $this->apiKeys->method('findCredential')->willReturn($this->credential(null, [], [], null));
        $this->tenants->expects($this->once())->method('statusOf')->with('acme')->willReturn(TenantStatus::SUSPENDED);

        $request = $this->request('op');
        $request->headers->set(ApiKeyAuthenticator::TENANT_HEADER, 'acme');
        $principal = $this->authenticator->authenticate($request);

        $this->assertSame('acme', $principal->tenant());
        $this->assertTrue($principal->has(Scope::CONSUME));
    }

    public function testOperatorHeaderNamingAnUnknownTenantIsNotFound(): void
    {
        $this->apiKeys->method('findCredential')->willReturn($this->credential(null, [], [], null));
        $this->tenants->method('statusOf')->willReturn(null);

        $request = $this->request('op');
        $request->headers->set(ApiKeyAuthenticator::TENANT_HEADER, 'ghost');

        $this->assertProblem(404, 'not_found', fn() => $this->authenticator->authenticate($request));
    }

    public function testRecordsUseAtMostOncePerMinute(): void
    {
        $recent = gmdate('Y-m-d H:i:s', time() - 10);
        $old = gmdate('Y-m-d H:i:s', time() - 600);

        $this->apiKeys->method('findCredential')->willReturnOnConsecutiveCalls(
            $this->credential('acme', ['admin'], [], 'active', $recent),
            $this->credential('acme', ['admin'], [], 'active', $old),
        );
        $this->apiKeys->expects($this->once())->method('touch');

        $this->authenticator->authenticate($this->request('k'));
        $this->authenticator->authenticate($this->request('k'));
    }

    public function testOnlyRequestsUnderV1AreAuthenticated(): void
    {
        $this->apiKeys->expects($this->never())->method('findCredential');

        $request = Request::create('/queues/mail/next');
        $kernel = $this->createStub(HttpKernelInterface::class);
        ($this->authenticator)(new RequestEvent($kernel, $request, HttpKernelInterface::MAIN_REQUEST));

        $this->assertFalse($request->attributes->has(Principal::ATTRIBUTE));
    }

    /**
     * @param list<string> $scopes
     * @param list<string> $queues
     */
    private function credential(
        ?string $tenant,
        array $scopes,
        array $queues,
        ?string $tenantStatus = 'active',
        ?string $lastUsedAt = null,
    ): array {
        return [
            'id' => 'key1',
            'tenant_id' => $tenant,
            'scopes' => json_encode($scopes),
            'queues' => json_encode($queues),
            'last_used_at' => $lastUsedAt,
            'tenant_status' => $tenantStatus,
        ];
    }

    private function request(string $secret): Request
    {
        return Request::create('/v1/tenant', server: ['HTTP_AUTHORIZATION' => 'Bearer ' . $secret]);
    }

    private function assertProblem(int $status, string $code, callable $call): void
    {
        try {
            $call();
            $this->fail('Expected an ApiProblem');
        } catch (ApiProblem $problem) {
            $this->assertSame($status, $problem->status);
            $this->assertSame($code, $problem->problemCode);
        }
    }
}
