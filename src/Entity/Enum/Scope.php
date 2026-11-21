<?php

declare(strict_types=1);

namespace App\Entity\Enum;

/**
 * What a tenant API key may do. `admin` includes the others. `metrics` only reads the
 * figures of queues, for a monitoring system to hold.
 */
enum Scope: string
{
    case PUBLISH = 'publish';
    case CONSUME = 'consume';
    case METRICS = 'metrics';
    case ADMIN = 'admin';
}
