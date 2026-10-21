package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

func (r *usageBillingRepository) SaveClaudeAttempt(ctx context.Context, a *service.ClaudeBillingAttempt) error {
	snapshot, err := json.Marshal(a.Snapshot)
	if err != nil {
		return err
	}
	usage, err := json.Marshal(a.Observed)
	if err != nil {
		return err
	}
	_, err = r.db.ExecContext(ctx, `INSERT INTO claude_billing_attempts
	(id,account_id,api_key_id,local_request_id,upstream_request_id,snapshot,observed_usage,state,http_status,error_reason,started_at,finished_at)
	VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
	ON CONFLICT(id) DO UPDATE SET upstream_request_id=EXCLUDED.upstream_request_id,
	observed_usage=EXCLUDED.observed_usage,state=CASE WHEN claude_billing_attempts.state='settled' THEN 'settled' ELSE EXCLUDED.state END,
	http_status=EXCLUDED.http_status,error_reason=EXCLUDED.error_reason,finished_at=EXCLUDED.finished_at,updated_at=now()`, a.ID, a.Snapshot.AccountID, a.Snapshot.APIKeyID, a.LocalRequestID, a.UpstreamRequestID, snapshot, usage, a.State, a.HTTPStatus, a.ErrorReason, a.Snapshot.StartedAt, a.FinishedAt)
	return err
}

func (r *usageBillingRepository) PendingClaudeAttempts(ctx context.Context) ([]service.ClaudeBillingAttempt, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT id,local_request_id,upstream_request_id,snapshot,observed_usage,billed_usage,settled_cost,settled_base,revision,state,http_status,finished_at
	FROM claude_billing_attempts WHERE (state IN('pending','ready') OR (state='running' AND started_at<now()-interval '16 minutes'))
	AND (checked_at IS NULL OR checked_at<now()-interval '2 minutes') ORDER BY (state='ready') DESC, checked_at NULLS FIRST,started_at LIMIT 1000`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var attempts []service.ClaudeBillingAttempt
	for rows.Next() {
		var a service.ClaudeBillingAttempt
		var snapshot, observed, billed []byte
		if err := rows.Scan(&a.ID, &a.LocalRequestID, &a.UpstreamRequestID, &snapshot, &observed, &billed, &a.SettledCost, &a.SettledBase, &a.Revision, &a.State, &a.HTTPStatus, &a.FinishedAt); err != nil {
			return nil, err
		}
		if json.Unmarshal(snapshot, &a.Snapshot) != nil || json.Unmarshal(observed, &a.Observed) != nil || json.Unmarshal(billed, &a.Billed) != nil {
			return nil, errors.New("invalid persisted billing attempt")
		}
		attempts = append(attempts, a)
	}
	return attempts, rows.Err()
}

func (r *usageBillingRepository) CheckClaudeAttempt(ctx context.Context, id, reason string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE claude_billing_attempts SET checked_at=now(),error_reason=$2 WHERE id=$1`, id, reason)
	return err
}

func (r *usageBillingRepository) SettleClaudeAttempt(ctx context.Context, a *service.ClaudeBillingAttempt, log *service.UsageLog, cmd *service.UsageBillingCommand, complete bool) (*service.UsageBillingApplyResult, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	var revision int
	var state string
	if err = tx.QueryRowContext(ctx, `SELECT revision,state FROM claude_billing_attempts WHERE id=$1 FOR UPDATE`, a.ID).Scan(&revision, &state); err != nil {
		return nil, err
	}
	if revision != a.Revision || state == "settled" {
		return &service.UsageBillingApplyResult{}, nil
	}
	if cmd.BalanceCost < 0 || log.TotalCost < 0 {
		return nil, errors.New("negative billing adjustment")
	}
	result := &service.UsageBillingApplyResult{Applied: true}
	if cmd.BalanceCost > 0 || log.TotalCost > 0 {
		claimed, err := r.claimUsageBillingKey(ctx, tx, cmd)
		if err != nil {
			return nil, err
		}
		if !claimed {
			return nil, fmt.Errorf("settlement key already claimed for attempt %s", a.ID)
		}
		if err = r.applyUsageBillingEffects(ctx, tx, cmd, result); err != nil {
			return nil, err
		}
		logs := &usageLogRepository{sql: tx}
		if _, err = logs.createSingle(ctx, tx, log); err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO claude_billing_settlements(attempt_id,revision,amount,usage_log_id) VALUES($1,$2,$3,$4)`, a.ID, revision+1, cmd.BalanceCost, log.ID); err != nil {
			return nil, err
		}
	}
	usage, err := json.Marshal(a.Authoritative)
	if err != nil {
		return nil, err
	}
	next := "pending"
	if complete {
		next = "settled"
	}
	_, err = tx.ExecContext(ctx, `UPDATE claude_billing_attempts SET revision=revision+1,state=$2,billed_usage=$3,authoritative_usage=CASE WHEN $4 THEN $3::jsonb ELSE authoritative_usage END,
	settled_cost=settled_cost+$5,settled_base=settled_base+$6,checked_at=now(),updated_at=now() WHERE id=$1`, a.ID, next, usage, complete, cmd.BalanceCost, log.TotalCost)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

var _ service.ClaudeBillingJournal = (*usageBillingRepository)(nil)
