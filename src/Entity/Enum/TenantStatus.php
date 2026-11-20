<?php

declare(strict_types=1);

namespace App\Entity\Enum;

enum TenantStatus: string
{
    case ACTIVE = 'active';
    case SUSPENDED = 'suspended';
    case DELETING = 'deleting';
}
