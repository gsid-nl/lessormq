#!/usr/bin/env bash
# The whole protocol with curl: publish, claim, fail, claim again, ack.
# Needs LESSORMQ_URL and LESSORMQ_KEY (a key with the publish and consume scopes).
set -euo pipefail

url=${LESSORMQ_URL:-http://127.0.0.1:18080}
queue=${LESSORMQ_QUEUE:-examples}
auth="Authorization: Bearer $LESSORMQ_KEY"
json="Content-Type: application/json"

# Publish
curl -sS -X POST "$url/v1/queues/$queue/messages" -H "$auth" -H "$json" \
  -d '{"body": {"to": "ada@example.com"}, "dedup_key": "welcome-ada"}' | jq .

# Claim, waiting up to 5 seconds for a message
lease=$(curl -sS -X POST "$url/v1/queues/$queue/claims" -H "$auth" -H "$json" \
  -d '{"consumer": "curl", "wait_seconds": 5}')
echo "$lease" | jq .
id=$(echo "$lease" | jq -r .message.id)
receipt=$(echo "$lease" | jq -r .receipt)

# The job failed: report it, and let the queue decide what happens next
curl -sS -X POST "$url/v1/messages/$id/fail" -H "$auth" -H "$json" \
  -d "{\"receipt\": \"$receipt\", \"error\": {\"type\": \"SmtpTimeout\", \"message\": \"mail server did not answer\"}}" | jq .

# Claim again once the backoff has passed, and report success this time
sleep "${BACKOFF_WAIT:-11}"
lease=$(curl -sS -X POST "$url/v1/queues/$queue/claims" -H "$auth" -H "$json" -d '{"wait_seconds": 5}')
echo "$lease" | jq '{receipt, attempt: .message.attempt}'
curl -sS -o /dev/null -w "ack: %{http_code}\n" -X POST "$url/v1/messages/$(echo "$lease" | jq -r .message.id)/ack" \
  -H "$auth" -H "$json" -d "{\"receipt\": \"$(echo "$lease" | jq -r .receipt)\"}"
