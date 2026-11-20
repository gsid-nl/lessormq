<?php

declare(strict_types=1);

namespace App\Tests\Api;

use App\Api\ApiProblem;
use App\Api\Input;
use App\Api\Pagination;
use App\Api\ProblemListener;
use App\Api\Time;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Event\ExceptionEvent;
use Symfony\Component\HttpKernel\Exception\MethodNotAllowedHttpException;
use Symfony\Component\HttpKernel\Exception\NotFoundHttpException;
use Symfony\Component\HttpKernel\HttpKernelInterface;

class ApiProblemTest extends TestCase
{
    public function testRendersProblemDetails(): void
    {
        $response = ApiProblem::validation([['field' => 'name', 'message' => 'is required']])->toResponse();
        $body = json_decode((string) $response->getContent(), true);

        $this->assertSame(400, $response->getStatusCode());
        $this->assertSame('application/problem+json', $response->headers->get('Content-Type'));
        $this->assertSame('urn:lessormq:problem:validation_failed', $body['type']);
        $this->assertSame('validation_failed', $body['code']);
        $this->assertSame([['field' => 'name', 'message' => 'is required']], $body['errors']);
        $this->assertSame('Bearer', ApiProblem::unauthorized('x')->toResponse()->headers->get('WWW-Authenticate'));
    }

    public function testListenerRendersOnlyApiPaths(): void
    {
        $listener = new ProblemListener(new NullLogger());

        $legacy = $this->exceptionEvent('/queues/mail/next', new \RuntimeException('boom'));
        $listener($legacy);
        $this->assertNull($legacy->getResponse());

        $api = $this->exceptionEvent('/v1/tenants', new \RuntimeException('secret detail'));
        $listener($api);
        $body = json_decode((string) $api->getResponse()->getContent(), true);
        $this->assertSame(500, $api->getResponse()->getStatusCode());
        $this->assertSame('internal', $body['code']);
        $this->assertStringNotContainsString('secret detail', (string) $api->getResponse()->getContent());
    }

    public function testListenerMapsHttpExceptions(): void
    {
        $listener = new ProblemListener(new NullLogger());

        $notFound = $this->exceptionEvent('/v1/nothing', new NotFoundHttpException());
        $listener($notFound);
        $this->assertSame(404, $notFound->getResponse()->getStatusCode());

        $method = $this->exceptionEvent('/v1/tenants', new MethodNotAllowedHttpException(['GET', 'POST']));
        $listener($method);
        $this->assertSame(405, $method->getResponse()->getStatusCode());
        $this->assertSame('GET, POST', $method->getResponse()->headers->get('Allow'));
    }

    public function testInputCollectsAllErrors(): void
    {
        $input = Input::fromArray(['name' => '', 'scopes' => ['publish', 'root'], 'extra' => 1]);
        $input->allowOnly('name', 'scopes');
        $input->string('name', required: true);
        $input->stringList('scopes', itemPattern: 'publish|consume|admin');
        $input->string('id', required: true);

        try {
            $input->validate();
            $this->fail('Expected a validation problem');
        } catch (ApiProblem $problem) {
            $this->assertSame(['extra', 'name', 'scopes[1]', 'id'], array_column($problem->errors, 'field'));
        }
    }

    public function testInputPatternsAreAnchoredAsAWhole(): void
    {
        $input = Input::fromArray(['scopes' => ['publishing']]);
        $this->assertNull($input->stringList('scopes', itemPattern: 'publish|consume'));
    }

    public function testInputRejectsNonObjectBodies(): void
    {
        foreach (['not json', '[1,2]', '"text"'] as $body) {
            try {
                Input::fromRequest(new Request(content: $body));
                $this->fail('Expected a validation problem for ' . $body);
            } catch (ApiProblem $problem) {
                $this->assertSame('validation_failed', $problem->problemCode);
            }
        }

        $this->assertFalse(Input::fromRequest(new Request(content: ''), allowEmpty: true)->has('x'));
    }

    public function testPaginationCursorRoundTrips(): void
    {
        $cursor = Pagination::cursor('acme-corp');
        $request = new Request(['cursor' => $cursor, 'limit' => '50']);

        $this->assertSame('acme-corp', Pagination::after($request, '[a-z-]+'));
        $this->assertSame(50, Pagination::limit($request));

        $this->expectException(ApiProblem::class);
        Pagination::after(new Request(['cursor' => Pagination::cursor('../etc')]), '[a-z-]+');
    }

    public function testTimesAreUtcWithZ(): void
    {
        $amsterdam = new \DateTimeImmutable('2026-07-01 12:00:00', new \DateTimeZone('Europe/Amsterdam'));

        $this->assertSame('2026-07-01T10:00:00Z', Time::format($amsterdam));
        $this->assertNull(Time::format(null));
    }

    private function exceptionEvent(string $path, \Throwable $throwable): ExceptionEvent
    {
        return new ExceptionEvent(
            $this->createStub(HttpKernelInterface::class),
            Request::create($path),
            HttpKernelInterface::MAIN_REQUEST,
            $throwable,
        );
    }
}
