<?php

declare(strict_types=1);

namespace App\Entity\Enum;

/**
 * What a tenant API key may do. `admin` includes the other two.
 */
enum Scope: string
{
    case PUBLISH = 'publish';
    case CONSUME = 'consume';
    case ADMIN = 'admin';
}
