<?php

declare(strict_types=1);

namespace App\Security;

use App\Entity\Enum\Scope;

/**
 * The endpoint acts in a tenant and needs one of these scopes. `admin` satisfies any.
 */
#[\Attribute(\Attribute::TARGET_CLASS | \Attribute::TARGET_METHOD)]
final class RequireScope
{
    /** @var list<Scope> */
    public readonly array $scopes;

    public function __construct(Scope ...$scopes)
    {
        $this->scopes = array_values($scopes);
    }
}
