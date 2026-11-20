<?php

declare(strict_types=1);

namespace App\Security;

use App\Api\ApiProblem;
use App\Entity\Enum\Scope;
use Symfony\Component\EventDispatcher\Attribute\AsEventListener;
use Symfony\Component\HttpKernel\Event\ControllerArgumentsEvent;
use Symfony\Component\HttpKernel\KernelEvents;

/**
 * Enforces RequireScope and OperatorOnly on the controller about to run.
 */
#[AsEventListener(event: KernelEvents::CONTROLLER_ARGUMENTS)]
final class ScopeListener
{
    public function __invoke(ControllerArgumentsEvent $event): void
    {
        $operatorOnly = $event->getAttributes(OperatorOnly::class);
        $required = $event->getAttributes(RequireScope::class);

        if ($operatorOnly === [] && $required === []) {
            return;
        }

        $principal = $event->getRequest()->attributes->get(Principal::ATTRIBUTE);

        if (!$principal instanceof Principal) {
            throw ApiProblem::unauthorized('Send the API key as "Authorization: Bearer <key>".');
        }

        if ($operatorOnly !== []) {
            if (!$principal->operator) {
                throw ApiProblem::forbidden('Only an operator key may manage tenants.');
            }

            return;
        }

        $principal->tenant();

        foreach ($required as $attribute) {
            if (!$this->hasAny($principal, $attribute->scopes)) {
                throw ApiProblem::forbidden(sprintf(
                    'This key needs the %s scope.',
                    implode(' or ', array_map(static fn(Scope $scope) => $scope->value, $attribute->scopes)),
                ));
            }
        }
    }

    /**
     * @param list<Scope> $scopes
     */
    private function hasAny(Principal $principal, array $scopes): bool
    {
        foreach ($scopes as $scope) {
            if ($principal->has($scope)) {
                return true;
            }
        }

        return false;
    }
}
