# Claude durable billing hotfix

This patch is based on deployment revision `f2669c8cf62555cd92389b3f55920e9e6e7c6ff2`.

## Enablement

- Set `SUB2API_SHARED_CONCURRENCY=true` during blue/green overlap. This disables destructive process-prefix startup cleanup; existing slot TTL cleanup remains active.
- Set `CLAUDE_BILLING_WORKER=true` on the settlement worker instance.
- Configure `CLAUDE_BILLING_NEWAPI_URL`, `CLAUDE_BILLING_NEWAPI_USER`, and `CLAUDE_BILLING_NEWAPI_TOKEN` using local secret configuration, never Git or build arguments.
- Opt individual Anthropic API-key accounts in with `extra.claude_durable_billing=true`.
- Standard balance-billed text calls are supported. Subscription and fast-tier requests retain their original billing path.

## Data and settlement

Each actual HTTP attempt is persisted before sending upstream. Response headers immediately capture `X-Oneapi-Request-Id`. A response-body observer persists Anthropic usage independently of downstream disconnects and the existing gateway parser. A missing usage/terminal event remains pending.

The worker settles completed valid usage, and fetches New API logs in fixed, overlapping completion-time windows for pending attempts. New API consumption records supply actual tokens; error-only zero-token records settle as zero. Calls without an upstream request identifier stay pending for manual reconciliation. They must never be automatically assigned by time proximity.

Settlement locks the attempt row, claims a stable per-revision billing key, deducts balances/key quotas, inserts a usage row and settlement audit row, and updates journal state in one PostgreSQL transaction. Existing cache/quota finalization runs after commit. Replayed worker batches cannot deduct twice.

## Operational checks

```sql
SELECT state, error_reason, count(*)
FROM claude_billing_attempts GROUP BY 1,2;

SELECT count(*), sum(amount) FROM claude_billing_settlements;

SELECT id, upstream_request_id, settled_cost, started_at, finished_at
FROM claude_billing_attempts
WHERE state <> 'settled' ORDER BY started_at;
```

Use an independent application data/log directory and inherit the existing JWT, encryption and gateway configuration. Limit the combined database pool to the actual PostgreSQL connection budget. Before replacing any application container, stop its new ingress and verify established inbound calls and outstanding upstream work are drained. Nginx reload alone does not prove a container has drained.

Historical bills without an exact request-ID mapping require an explicit reconciliation report. The patch does not assign historical costs by approximate timing.
