<?php

declare(strict_types=1);

namespace App;

use Symfony\Bundle\FrameworkBundle\Kernel\MicroKernelTrait;
use Symfony\Component\HttpKernel\Kernel as BaseKernel;

class Kernel extends BaseKernel
{
    use MicroKernelTrait;

    public function boot(): void
    {
        // Stored and compared times are UTC (see App\Time\Utc). The PHP default zone of
        // the image is Europe/Amsterdam, which made every stored time ambiguous for an
        // hour each October.
        date_default_timezone_set('UTC');

        parent::boot();
    }
}
