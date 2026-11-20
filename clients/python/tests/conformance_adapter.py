"""Runs the conformance scenarios through this library (conformance/php/src/ProcessAdapter.php).

One JSON request per line on stdin, one JSON result per line on stdout. Every call goes
through lessormq.Client, and its results and exceptions are turned back into the
{status, body} the scenarios check:

    LESSORMQ_ADAPTER_CMD="clients/python/.venv/bin/python clients/python/tests/conformance_adapter.py" \\
        conformance/run.sh
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

from lessormq import ApiError, Client, Lease, Message
from lessormq._models import format_time, parse_time

URL = os.environ.get("LESSORMQ_URL", "http://127.0.0.1:18080")
clients: dict[str, Client] = {}


def call(name: str, api_key: str, args: dict[str, Any]) -> dict[str, Any]:
    client = clients.get(api_key)
    if client is None:
        client = clients[api_key] = Client(URL, api_key, retry_delay=0.05)
    try:
        if name == "publish":
            return publish(client, args)
        if name == "claim":
            return claim(client, args)
        if name == "extend":
            lease = client.extend(lease_of(args), args.get("lease_seconds"))
            return {"status": 200, "body": {"lease_until": format_time(lease.lease_until)}}
        if name == "ack":
            client.ack(lease_of(args))
            return {"status": 204, "body": None}
        if name == "release":
            client.release(lease_of(args), args.get("delay_seconds") or 0)
            return {"status": 204, "body": None}
        if name == "fail":
            return fail(client, args)
        raise ValueError(f"unknown call {name}")
    except ApiError as e:
        return {"status": e.status, "body": {"code": e.code, "detail": e.detail, "errors": e.errors}}


def publish(client: Client, args: dict[str, Any]) -> dict[str, Any]:
    available_at = args.get("available_at")
    published = client.publish(
        args["queue"],
        args.get("body"),
        headers=args.get("headers") or None,
        dedup_key=args.get("dedup_key"),
        delay_seconds=args.get("delay_seconds"),
        available_at=parse_time(available_at) if available_at else None,
        max_attempts=args.get("max_attempts"),
    )
    return {
        "status": 200 if published.duplicate else 201,
        "body": {
            "id": published.id,
            "queue": published.queue,
            "status": published.status,
            "created_at": format_time(published.created_at),
            "available_at": format_time(published.available_at),
            "duplicate": published.duplicate,
        },
    }


def claim(client: Client, args: dict[str, Any]) -> dict[str, Any]:
    lease = client.claim(
        args["queue"],
        consumer=args.get("consumer"),
        route=args.get("route"),
        wait_seconds=args.get("wait_seconds") or 0,
        lease_seconds=args.get("lease_seconds"),
    )
    if lease is None:
        return {"status": 204, "body": None}
    message = lease.message
    return {
        "status": 200,
        "body": {
            "receipt": lease.receipt,
            "lease_until": format_time(lease.lease_until),
            "message": {
                "id": message.id,
                "queue": message.queue,
                "body": message.body,
                "headers": message.headers,
                "attempt": message.attempt,
                "max_attempts": message.max_attempts,
                "created_at": format_time(message.created_at),
            },
        },
    }


def fail(client: Client, args: dict[str, Any]) -> dict[str, Any]:
    error = args.get("error") or {}
    result = client.fail(
        lease_of(args),
        str(error.get("message", "")),
        permanent=bool(args.get("permanent", False)),
        details=error.get("details"),
        type=error.get("type"),
    )
    return {
        "status": 200,
        "body": {
            "outcome": result.outcome,
            "available_at": format_time(result.available_at) if result.available_at else None,
            "attempt": result.attempt,
            "max_attempts": result.max_attempts,
        },
    }


def lease_of(args: dict[str, Any]) -> Lease:
    """A lease as the scenario names it: the message id and the receipt, which may be
    stale or someone else's on purpose."""
    now = datetime.now(timezone.utc)
    return Lease(str(args["receipt"]), now, Message(str(args["message"]), "", None, {}, 0, 0, now))


def main() -> None:
    for line in sys.stdin:
        if not line.strip():
            continue
        request = json.loads(line)
        args = request["args"] if isinstance(request["args"], dict) else {}
        result = call(request["call"], request["api_key"], args)
        sys.stdout.write(json.dumps(result, ensure_ascii=False) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
