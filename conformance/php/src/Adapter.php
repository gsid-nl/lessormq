<?php

declare(strict_types=1);

namespace LessorMQ\Conformance;

/**
 * What a client library plugs into the suite: the six protocol calls, each answering
 * with the HTTP status and body the call ended in.
 *
 * A library adapter maps each call onto the library's own API and turns the library's
 * results and exceptions back into {status, body}: a lease_lost exception becomes
 * ['status' => 409, 'body' => ['code' => 'lease_lost', ...]], an empty claim
 * ['status' => 204, 'body' => null]. HttpAdapter is the reference: the protocol itself.
 */
interface Adapter
{
    /**
     * @param string               $call   publish, claim, extend, ack, fail or release
     * @param string               $apiKey the key to act with
     * @param array<string, mixed> $args   the step's arguments; queue names are final
     * @return array{status: int, body: mixed}
     */
    public function call(string $call, string $apiKey, array $args): array;
}
