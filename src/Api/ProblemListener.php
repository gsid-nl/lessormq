<?php

declare(strict_types=1);

namespace App\Api;

use Psr\Log\LoggerInterface;
use Symfony\Component\EventDispatcher\Attribute\AsEventListener;
use Symfony\Component\HttpKernel\Event\ExceptionEvent;
use Symfony\Component\HttpKernel\Exception\HttpExceptionInterface;
use Symfony\Component\HttpKernel\KernelEvents;

/**
 * Renders every error under /v1 (and /healthz) as problem+json, so clients in any
 * language branch on one `code` field instead of parsing Symfony's error pages.
 */
#[AsEventListener(event: KernelEvents::EXCEPTION)]
final class ProblemListener
{
    public function __construct(private readonly LoggerInterface $logger)
    {
    }

    public function __invoke(ExceptionEvent $event): void
    {
        if (!self::isApiPath($event->getRequest()->getPathInfo())) {
            return;
        }

        $event->setResponse($this->toProblem($event->getThrowable())->toResponse());
    }

    public static function isApiPath(string $path): bool
    {
        return $path === '/healthz' || str_starts_with($path, '/v1/');
    }

    private function toProblem(\Throwable $throwable): ApiProblem
    {
        if ($throwable instanceof ApiProblem) {
            return $throwable;
        }

        if ($throwable instanceof HttpExceptionInterface) {
            $status = $throwable->getStatusCode();

            return match (true) {
                $status === 404 => ApiProblem::notFound('No such endpoint.'),
                $status === 413 => new ApiProblem(413, 'payload_too_large', 'The request body is too large.'),
                $status >= 500 => ApiProblem::internal(),
                default => new ApiProblem(
                    $status,
                    'validation_failed',
                    $throwable->getMessage() !== '' ? $throwable->getMessage() : 'The request is not valid.',
                    [],
                    $throwable->getHeaders(),
                ),
            };
        }

        $this->logger->error('Unhandled exception in API request', ['exception' => $throwable]);

        return ApiProblem::internal();
    }
}
