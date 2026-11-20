<?php

declare(strict_types=1);

namespace App\Security;

use App\Api\ApiProblem;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Controller\ValueResolverInterface;
use Symfony\Component\HttpKernel\ControllerMetadata\ArgumentMetadata;

/**
 * Lets a controller take the caller as a `Principal $principal` argument.
 */
final class PrincipalValueResolver implements ValueResolverInterface
{
    /**
     * @return iterable<Principal>
     */
    public function resolve(Request $request, ArgumentMetadata $argument): iterable
    {
        if ($argument->getType() !== Principal::class) {
            return [];
        }

        $principal = $request->attributes->get(Principal::ATTRIBUTE);

        if (!$principal instanceof Principal) {
            throw ApiProblem::unauthorized('Send the API key as "Authorization: Bearer <key>".');
        }

        return [$principal];
    }
}
