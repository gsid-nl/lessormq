<?php

declare(strict_types=1);

namespace App\Tests\Security;

use App\Api\ApiProblem;
use App\Entity\Enum\Scope;
use App\Security\OperatorOnly;
use App\Security\Principal;
use App\Security\RequireScope;
use App\Security\ScopeListener;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Event\ControllerArgumentsEvent;
use Symfony\Component\HttpKernel\HttpKernelInterface;

class ScopeListenerTest extends TestCase
{
    public function testTenantEndpointNeedsTheScope(): void
    {
        $this->check(new ScopedController(), 'publish', $this->tenantKey([Scope::PUBLISH]));

        $this->assertProblem(403, 'forbidden', fn() => $this->check(
            new ScopedController(),
            'publish',
            $this->tenantKey([Scope::CONSUME]),
        ));
    }

    public function testAdminSatisfiesEveryScope(): void
    {
        $this->check(new ScopedController(), 'publish', $this->tenantKey([Scope::ADMIN]));
        $this->addToAssertionCount(1);
    }

    public function testOperatorEndpointRefusesTenantKeys(): void
    {
        $this->assertProblem(403, 'forbidden', fn() => $this->check(
            new OperatorController(),
            'list',
            $this->tenantKey([Scope::ADMIN]),
        ));

        $this->check(new OperatorController(), 'list', new Principal('op', null, [], [], true));
    }

    public function testOperatorNeedsTheTenantHeaderForTenantEndpoints(): void
    {
        $this->assertProblem(403, 'forbidden', fn() => $this->check(
            new ScopedController(),
            'publish',
            new Principal('op', null, [], [], true),
        ));

        $this->check(new ScopedController(), 'publish', new Principal('op', 'acme', [Scope::ADMIN], [], true));
    }

    public function testUnauthenticatedRequestToAGuardedEndpointIsUnauthorized(): void
    {
        $this->assertProblem(401, 'unauthorized', fn() => $this->check(new ScopedController(), 'publish', null));
    }

    /**
     * @param list<Scope> $scopes
     */
    private function tenantKey(array $scopes): Principal
    {
        return new Principal('key', 'acme', $scopes, [], false);
    }

    private function check(object $controller, string $method, ?Principal $principal): void
    {
        $request = new Request();

        if ($principal !== null) {
            $request->attributes->set(Principal::ATTRIBUTE, $principal);
        }

        $event = new ControllerArgumentsEvent(
            $this->createStub(HttpKernelInterface::class),
            [$controller, $method],
            [],
            $request,
            HttpKernelInterface::MAIN_REQUEST,
        );

        (new ScopeListener())($event);
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

// phpcs:disable PSR1.Classes.ClassDeclaration.MultipleClasses
final class ScopedController
{
    #[RequireScope(Scope::PUBLISH)]
    public function publish(): void
    {
    }
}

#[OperatorOnly]
final class OperatorController
{
    public function list(): void
    {
    }
}
