CREATE TABLE IF NOT EXISTS claude_billing_attempts (
    id VARCHAR(64) PRIMARY KEY,
    account_id BIGINT NOT NULL,
    api_key_id BIGINT NOT NULL,
    local_request_id TEXT NOT NULL,
    upstream_request_id VARCHAR(128) NOT NULL DEFAULT '',
    snapshot JSONB NOT NULL,
    observed_usage JSONB NOT NULL DEFAULT '{}',
    authoritative_usage JSONB,
    billed_usage JSONB NOT NULL DEFAULT '{}',
    settled_cost NUMERIC(20,8) NOT NULL DEFAULT 0,
    settled_base NUMERIC(20,10) NOT NULL DEFAULT 0,
    revision INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'running',
    http_status INTEGER NOT NULL DEFAULT 0,
    error_reason TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    checked_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_claude_billing_pending ON claude_billing_attempts(state, checked_at, started_at);
CREATE INDEX IF NOT EXISTS idx_claude_billing_upstream ON claude_billing_attempts(account_id, upstream_request_id) WHERE upstream_request_id <> '';
CREATE TABLE IF NOT EXISTS claude_billing_settlements (
    attempt_id VARCHAR(64) NOT NULL REFERENCES claude_billing_attempts(id),
    revision INTEGER NOT NULL,
    amount NUMERIC(20,8) NOT NULL,
    usage_log_id BIGINT NOT NULL REFERENCES usage_logs(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(attempt_id, revision)
);
