package repository

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestClaudeBillingSettlementIsAtomic(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	r := &usageBillingRepository{db: db}
	u := service.ClaudeUsage{InputTokens: 82654}
	a := &service.ClaudeBillingAttempt{ID: "attempt", Authoritative: &u}
	log := &service.UsageLog{RequestID: "claude:attempt:1", APIKeyID: 2, UserID: 1, AccountID: 4, Model: "claude-opus-4-6", InputTokens: 82654, TotalCost: 0.41327, ActualCost: 0.01198483, CreatedAt: time.Now()}
	cmd := &service.UsageBillingCommand{RequestID: log.RequestID, APIKeyID: 2, UserID: 1, AccountID: 4, BalanceCost: 0.01198483}
	cmd.Normalize()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT revision,state").WillReturnRows(sqlmock.NewRows([]string{"revision", "state"}).AddRow(0, "pending"))
	mock.ExpectQuery("INSERT INTO usage_billing_dedup").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery("SELECT request_fingerprint.*FROM usage_billing_dedup_archive").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("UPDATE users").WillReturnRows(sqlmock.NewRows([]string{"balance"}).AddRow(10.0))
	mock.ExpectQuery("INSERT INTO usage_logs").WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"}).AddRow(42, time.Now()))
	mock.ExpectExec("INSERT INTO claude_billing_settlements").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("UPDATE claude_billing_attempts").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	result, err := r.SettleClaudeAttempt(context.Background(), a, log, cmd, true)
	require.NoError(t, err)
	require.True(t, result.Applied)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClaudeBillingSettlementRejectsConcurrentReplay(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	r := &usageBillingRepository{db: db}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT revision,state").WillReturnRows(sqlmock.NewRows([]string{"revision", "state"}).AddRow(1, "settled"))
	mock.ExpectRollback()
	result, err := r.SettleClaudeAttempt(context.Background(), &service.ClaudeBillingAttempt{ID: "attempt"}, &service.UsageLog{}, &service.UsageBillingCommand{}, true)
	require.NoError(t, err)
	require.False(t, result.Applied)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestClaudeBillingSettlementRollsBackDeductionOnLogFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	r := &usageBillingRepository{db: db}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT revision,state").WillReturnRows(sqlmock.NewRows([]string{"revision", "state"}).AddRow(0, "pending"))
	mock.ExpectQuery("INSERT INTO usage_billing_dedup").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery("SELECT request_fingerprint.*FROM usage_billing_dedup_archive").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("UPDATE users").WillReturnRows(sqlmock.NewRows([]string{"balance"}).AddRow(10.0))
	mock.ExpectQuery("INSERT INTO usage_logs").WillReturnError(errors.New("database unavailable"))
	mock.ExpectRollback()
	_, err = r.SettleClaudeAttempt(context.Background(), &service.ClaudeBillingAttempt{ID: "attempt"}, &service.UsageLog{TotalCost: 1}, &service.UsageBillingCommand{BalanceCost: 0.03}, true)
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
