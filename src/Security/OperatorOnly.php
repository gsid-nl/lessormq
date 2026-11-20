<?php

declare(strict_types=1);

namespace App\Security;

/**
 * The endpoint manages the installation (tenants) and only accepts an operator key.
 */
#[\Attribute(\Attribute::TARGET_CLASS | \Attribute::TARGET_METHOD)]
final class OperatorOnly
{
}
