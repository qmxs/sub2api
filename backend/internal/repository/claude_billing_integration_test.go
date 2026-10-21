//go:build integration

package repository

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// Exercise the real PostgreSQL journal JSON encoding, row lock and idempotent zero settlement.
func TestClaudeBillingPostgresJournal(t *testing.T) {
	dsn := os.Getenv("CLAUDE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("CLAUDE_TEST_DATABASE_URL not configured")
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	_, err = db.ExecContext(ctx, `CREATE TABLE usage_logs(id BIGSERIAL PRIMARY KEY)`)
	require.NoError(t, err)
	migration, err := os.ReadFile("../../migrations/239_claude_billing_journal.sql")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(migration))
	require.NoError(t, err)
	r := &usageBillingRepository{db: db}
	a := &service.ClaudeBillingAttempt{ID: "pg-attempt", Snapshot: service.ClaudeBillingSnapshot{AccountID: 4, APIKeyID: 2, StartedAt: time.Now()}, State: "pending", UpstreamRequestID: "upstream-id"}
	require.NoError(t, r.SaveClaudeAttempt(ctx, a))
	rows, err := r.PendingClaudeAttempts(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "upstream-id", rows[0].UpstreamRequestID)
	u := service.ClaudeUsage{}
	a.Authoritative = &u
	result, err := r.SettleClaudeAttempt(ctx, a, &service.UsageLog{}, &service.UsageBillingCommand{}, true)
	require.NoError(t, err)
	require.True(t, result.Applied)
	result, err = r.SettleClaudeAttempt(ctx, a, &service.UsageLog{}, &service.UsageBillingCommand{}, true)
	require.NoError(t, err)
	require.False(t, result.Applied)
	rows, err = r.PendingClaudeAttempts(ctx)
	require.NoError(t, err)
	require.Empty(t, rows)
}
