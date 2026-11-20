<?php

declare(strict_types=1);

namespace App\Security;

use App\Api\ApiProblem;
use App\Doctrine\UtcDateTimeImmutableType;
use App\Entity\Enum\Scope;
use App\Entity\Enum\TenantStatus;
use App\Repository\ApiKeyRepository;
use App\Repository\TenantRepository;
use DateTimeImmutable;
use Symfony\Component\EventDispatcher\Attribute\AsEventListener;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Event\RequestEvent;
use Symfony\Component\HttpKernel\KernelEvents;

/**
 * Turns the bearer key of every /v1 request into a Principal on the request.
 *
 * Runs after routing, so an unknown path is a 404 rather than a 401. Endpoints declare
 * what they need with RequireScope or OperatorOnly; ScopeListener checks that.
 */
#[AsEventListener(event: KernelEvents::REQUEST, priority: 7)]
final class ApiKeyAuthenticator
{
    public const TENANT_HEADER = 'LessorMQ-Tenant';

    /** last_used_at is written at most this often per key */
    private const TOUCH_INTERVAL = 60;

    public function __construct(
        private readonly ApiKeyRepository $apiKeys,
        private readonly TenantRepository $tenants,
    ) {
    }

    public function __invoke(RequestEvent $event): void
    {
        $request = $event->getRequest();

        if (!$event->isMainRequest() || !str_starts_with($request->getPathInfo(), '/v1/')) {
            return;
        }

        $request->attributes->set(Principal::ATTRIBUTE, $this->authenticate($request));
    }

    public function authenticate(Request $request): Principal
    {
        $header = (string) $request->headers->get('Authorization', '');

        if (preg_match('/^Bearer\s+(\S+)$/i', $header, $match) !== 1) {
            throw ApiProblem::unauthorized('Send the API key as "Authorization: Bearer <key>".');
        }

        $credential = $this->apiKeys->findCredential(ApiKeyFactory::hash($match[1]));

        if ($credential === null) {
            throw ApiProblem::unauthorized('Unknown or revoked API key.');
        }

        $principal = $credential['tenant_id'] === null
            ? $this->operator($credential['id'], $request)
            : $this->tenantKey($credential);

        $this->recordUse($credential['id'], $credential['last_used_at']);

        return $principal;
    }

    private function operator(string $keyId, Request $request): Principal
    {
        $tenantId = $request->headers->get(self::TENANT_HEADER);

        if ($tenantId === null || $tenantId === '') {
            return new Principal($keyId, null, [], [], true);
        }

        // An operator may look into a suspended tenant; that is what suspending is for.
        if ($this->tenants->statusOf($tenantId) === null) {
            throw ApiProblem::notFound(sprintf('No tenant "%s".', $tenantId));
        }

        return new Principal($keyId, $tenantId, [Scope::ADMIN], [], true);
    }

    /**
     * @param array{id: string, tenant_id: ?string, scopes: string, queues: string, tenant_status: ?string} $credential
     */
    private function tenantKey(array $credential): Principal
    {
        if ($credential['tenant_status'] === null) {
            throw ApiProblem::unauthorized('Unknown or revoked API key.');
        }

        if ($credential['tenant_status'] !== TenantStatus::ACTIVE->value) {
            throw ApiProblem::tenantSuspended();
        }

        return new Principal(
            $credential['id'],
            $credential['tenant_id'],
            array_map(static fn(string $scope) => Scope::from($scope), json_decode($credential['scopes'], true)),
            json_decode($credential['queues'], true),
            false,
        );
    }

    private function recordUse(string $keyId, ?string $lastUsedAt): void
    {
        $now = new DateTimeImmutable();
        $staleBefore = $now->modify('-' . self::TOUCH_INTERVAL . ' seconds');

        if ($lastUsedAt !== null) {
            $last = new DateTimeImmutable($lastUsedAt, UtcDateTimeImmutableType::utc());

            if ($last >= $staleBefore) {
                return;
            }
        }

        $this->apiKeys->touch($keyId, $now, $staleBefore);
    }
}
