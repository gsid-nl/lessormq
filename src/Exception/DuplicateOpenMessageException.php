<?php

declare(strict_types=1);

namespace App\Exception;

/**
 * A finished message cannot be put back in line: another open message in its queue
 * already has the same dedup_key.
 */
final class DuplicateOpenMessageException extends \RuntimeException
{
}
