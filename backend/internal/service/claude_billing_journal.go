package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

type claudeIdentityKey struct{}
type claudeSnapshotKey struct{}

type claudeBillingIdentity struct {
	Key          *APIKey
	Subscription *UserSubscription
	At           time.Time
}

// WithClaudeBillingIdentity captures the actual selected group, including fallback routing.
func WithClaudeBillingIdentity(ctx context.Context, key *APIKey, sub *UserSubscription, at time.Time) context.Context {
	return context.WithValue(ctx, claudeIdentityKey{}, claudeBillingIdentity{key, sub, at})
}

type ClaudeBillingSnapshot struct {
	UserID            int64
	APIKeyID          int64
	GroupID           int64
	AccountID         int64
	Model             string
	Multiplier        float64
	AccountMultiplier float64
	InputPrice        float64
	OutputPrice       float64
	WritePrice        float64
	Write1hPrice      float64
	ReadPrice         float64
	KeyQuota          bool
	KeyWindows        bool
	AccountQuota      bool
	StartedAt         time.Time
	Stream            bool
	started           bool
}

type ClaudeBillingAttempt struct {
	ID                string
	LocalRequestID    string
	UpstreamRequestID string
	Snapshot          ClaudeBillingSnapshot
	Observed          ClaudeUsage
	Authoritative     *ClaudeUsage
	Billed            ClaudeUsage
	SettledCost       float64
	SettledBase       float64
	Revision          int
	State             string
	HTTPStatus        int
	ErrorReason       string
	FinishedAt        *time.Time
}

type ClaudeBillingJournal interface {
	SaveClaudeAttempt(context.Context, *ClaudeBillingAttempt) error
	PendingClaudeAttempts(context.Context) ([]ClaudeBillingAttempt, error)
	SettleClaudeAttempt(context.Context, *ClaudeBillingAttempt, *UsageLog, *UsageBillingCommand, bool) (*UsageBillingApplyResult, error)
	CheckClaudeAttempt(context.Context, string, string) error
}

func claudeBillingEnabled(ctx context.Context) bool {
	snapshot, ok := ctx.Value(claudeSnapshotKey{}).(*ClaudeBillingSnapshot)
	return ok && snapshot.started
}

func (s *GatewayService) claudeBillingContext(ctx context.Context, account *Account, parsed *ParsedRequest) context.Context {
	if account == nil || parsed == nil || account.Extra["claude_durable_billing"] != true || account.Platform != PlatformAnthropic || account.Type != AccountTypeAPIKey || parsed.Speed == "fast" {
		return ctx
	}
	if _, ok := s.usageBillingRepo.(ClaudeBillingJournal); !ok {
		return ctx
	}
	i, ok := ctx.Value(claudeIdentityKey{}).(claudeBillingIdentity)
	if !ok || i.Key == nil || i.Key.User == nil || i.Key.Group == nil || i.Subscription != nil || i.Key.Group.IsSubscriptionType() {
		return ctx
	}
	if i.At.IsZero() {
		i.At = time.Now()
	}
	model := account.GetMappedModel(parsed.Model)
	multiplier := s.ResolveUserGroupRateMultiplier(ctx, i.Key.UserID, i.Key.Group.ID, i.Key.Group.RateMultiplier)
	multiplier, _ = computePeakAwareMultipliers(i.Key, multiplier, i.At)
	price := func(u ClaudeUsage) *CostBreakdown {
		return s.calculateTokenCost(ctx, &ForwardResult{Model: model, UpstreamModel: model, Usage: u}, i.Key, model, 1, i.At)
	}
	ip := price(ClaudeUsage{InputTokens: 1})
	op := price(ClaudeUsage{OutputTokens: 1})
	wp := price(ClaudeUsage{CacheCreationInputTokens: 1, CacheCreation5mTokens: 1})
	w1 := price(ClaudeUsage{CacheCreationInputTokens: 1, CacheCreation1hTokens: 1})
	rp := price(ClaudeUsage{CacheReadInputTokens: 1})
	if ip.TotalCost <= 0 || op.TotalCost <= 0 {
		return ctx
	}
	snapshot := &ClaudeBillingSnapshot{UserID: i.Key.UserID, APIKeyID: i.Key.ID, GroupID: i.Key.Group.ID, AccountID: account.ID, Model: model, Multiplier: multiplier, AccountMultiplier: account.BillingRateMultiplier(), InputPrice: ip.TotalCost, OutputPrice: op.TotalCost, WritePrice: wp.TotalCost, Write1hPrice: w1.TotalCost, ReadPrice: rp.TotalCost, KeyQuota: i.Key.Quota > 0, KeyWindows: i.Key.HasRateLimits(), AccountQuota: account.HasAnyQuotaLimit(), StartedAt: i.At}
	snapshot.Stream = parsed.Stream
	return context.WithValue(ctx, claudeSnapshotKey{}, snapshot)
}

// Every HTTP attempt is durable before any upstream work is performed. Only opted-in accounts use this path.
func (s *GatewayService) doClaudeBillingUpstream(ctx context.Context, req *http.Request, account *Account, send func() (*http.Response, error)) (*http.Response, error) {
	snap, ok := ctx.Value(claudeSnapshotKey{}).(*ClaudeBillingSnapshot)
	if !ok {
		return send()
	}
	j, ok := s.usageBillingRepo.(ClaudeBillingJournal)
	if !ok {
		return nil, errors.New("durable billing repository unavailable")
	}
	a := &ClaudeBillingAttempt{ID: uuid.NewString(), LocalRequestID: resolveUsageBillingRequestID(ctx, ""), Snapshot: *snap, State: "running"}
	a.Snapshot.StartedAt = time.Now()
	dbctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	err := j.SaveClaudeAttempt(dbctx, a)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("persist upstream billing attempt: %w", err)
	}
	snap.started = true
	upctx, stop := context.WithTimeout(context.WithoutCancel(req.Context()), 15*time.Minute)
	*req = *req.WithContext(upctx)
	resp, err := send()
	if err != nil {
		stop()
		a.State = "pending"
		a.ErrorReason = "transport_error"
		now := time.Now()
		a.FinishedAt = &now
		s.saveClaudeAttempt(a)
		return resp, err
	}
	a.HTTPStatus = resp.StatusCode
	a.UpstreamRequestID = strings.TrimSpace(resp.Header.Get("X-Oneapi-Request-Id"))
	if a.UpstreamRequestID == "" {
		a.UpstreamRequestID = UpstreamRequestIDFromHeaders(account, resp.Header)
	}
	s.saveClaudeAttempt(a)
	resp.Body = &claudeJournalBody{ReadCloser: resp.Body, attempt: a, gateway: s, stop: stop, stream: snap.Stream}
	return resp, nil
}

func (s *GatewayService) saveClaudeAttempt(a *ClaudeBillingAttempt) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	j, ok := s.usageBillingRepo.(ClaudeBillingJournal)
	if !ok {
		return
	}
	if err := j.SaveClaudeAttempt(ctx, a); err != nil {
		logger.LegacyPrintf("service.gateway", "ALERT claude billing journal persist failed attempt=%s: %v", a.ID, err)
	}
}

type claudeJournalBody struct {
	io.ReadCloser
	attempt     *ClaudeBillingAttempt
	gateway     *GatewayService
	stop        context.CancelFunc
	stream      bool
	buffer      []byte
	lastPersist time.Time
	terminal    bool
	once        sync.Once
	mu          sync.Mutex
}

func (b *claudeJournalBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > 0 {
		if len(b.buffer)+n <= 32<<20 {
			b.buffer = append(b.buffer, p[:n]...)
		} else {
			b.attempt.ErrorReason = "usage_capture_limit"
		}
		if b.stream {
			for {
				idx := strings.IndexByte(string(b.buffer), '\n')
				if idx < 0 {
					break
				}
				b.observeLine(string(b.buffer[:idx]))
				b.buffer = b.buffer[idx+1:]
			}
		}
		if b.attempt.Observed.hasObservedTokens() && time.Since(b.lastPersist) > 5*time.Second {
			b.gateway.saveClaudeAttempt(b.attempt)
			b.lastPersist = time.Now()
		}
	}
	if err != nil {
		b.finish(err)
	}
	return n, err
}

func (b *claudeJournalBody) observeLine(line string) {
	data, ok := extractAnthropicSSEDataLine(strings.TrimSpace(line))
	if !ok {
		return
	}
	if anthropicStreamEventIsTerminal("", data) {
		b.terminal = true
	}
	parseSSEUsagePassthrough(data, &b.attempt.Observed)
	// Some compatible relays include the initial output count only in message_start.
	if gjson.Get(data, "type").String() == "message_start" {
		if v := int(gjson.Get(data, "message.usage.output_tokens").Int()); v > b.attempt.Observed.OutputTokens {
			b.attempt.Observed.OutputTokens = v
		}
	}
}

func (b *claudeJournalBody) finish(readErr error) {
	b.once.Do(func() {
		if b.stream && len(b.buffer) > 0 {
			b.observeLine(string(b.buffer))
		}
		if !b.stream && readErr == io.EOF && json.Valid(b.buffer) {
			if usage := parseClaudeUsageFromResponseBody(b.buffer); usage != nil {
				b.attempt.Observed = *usage
			}
			b.terminal = gjson.GetBytes(b.buffer, "usage").Exists()
		}
		now := time.Now()
		b.attempt.FinishedAt = &now
		b.attempt.State = "pending"
		if b.terminal && b.attempt.Observed.hasObservedTokens() && b.attempt.HTTPStatus < 400 {
			b.attempt.State = "ready"
		}
		if !b.terminal {
			b.attempt.ErrorReason = "missing_terminal_or_usage"
		}
		b.gateway.saveClaudeAttempt(b.attempt)
		b.stop()
	})
}
func (b *claudeJournalBody) Close() error {
	err := b.ReadCloser.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.finish(errors.New("body_closed"))
	return err
}

// New API uses mutually exclusive Anthropic input/cache buckets. No request text is retained.
type newAPIBillingLog struct {
	RequestID string `json:"request_id"`
	Model     string `json:"model_name"`
	Input     int    `json:"prompt_tokens"`
	Output    int    `json:"completion_tokens"`
	Other     string `json:"other"`
}

func decodeNewAPIBillingUsage(log newAPIBillingLog) (ClaudeUsage, error) {
	if log.Input < 0 || log.Output < 0 || !gjson.Valid(log.Other) {
		return ClaudeUsage{}, errors.New("invalid upstream usage")
	}
	o := gjson.Parse(log.Other)
	if o.Get("usage_semantic").String() != "anthropic" && !o.Get("claude").Bool() {
		return ClaudeUsage{}, errors.New("unsupported usage semantic")
	}
	u := ClaudeUsage{InputTokens: log.Input, OutputTokens: log.Output, CacheReadInputTokens: int(o.Get("cache_tokens").Int()), CacheCreationInputTokens: int(o.Get("cache_creation_tokens").Int()), CacheCreation5mTokens: int(o.Get("cache_creation_tokens_5m").Int()), CacheCreation1hTokens: int(o.Get("cache_creation_tokens_1h").Int())}
	if u.CacheCreationInputTokens == 0 {
		u.CacheCreationInputTokens = int(o.Get("cache_write_tokens").Int())
	}
	if u.CacheCreationInputTokens == 0 {
		u.CacheCreationInputTokens = u.CacheCreation5mTokens + u.CacheCreation1hTokens
	}
	if u.CacheReadInputTokens < 0 || u.CacheCreationInputTokens < 0 || u.CacheCreation5mTokens < 0 || u.CacheCreation1hTokens < 0 {
		return ClaudeUsage{}, errors.New("negative upstream cache tokens")
	}
	return u, nil
}

func (s *GatewayService) queryClaudeBill(ctx context.Context, a *ClaudeBillingAttempt) (*ClaudeUsage, error) {
	if a.UpstreamRequestID == "" {
		return nil, errors.New("missing upstream request id")
	}
	base := strings.TrimRight(os.Getenv("CLAUDE_BILLING_NEWAPI_URL"), "/")
	token := os.Getenv("CLAUDE_BILLING_NEWAPI_TOKEN")
	uid := os.Getenv("CLAUDE_BILLING_NEWAPI_USER")
	if base == "" || token == "" || uid == "" {
		return nil, errors.New("upstream billing not configured")
	}
	// curl is an explicit deployment dependency: this upstream rejects Go/Python TLS fingerprints.
	return queryNewAPIBillingWithCurl(ctx, base, token, uid, a.UpstreamRequestID, a.Snapshot.Model)
}

func claudeTargetCost(s ClaudeBillingSnapshot, u ClaudeUsage) (float64, float64) {
	w5, w1 := normalizeCacheCreationBreakdown(UsageTokens{CacheCreationTokens: u.CacheCreationInputTokens, CacheCreation5mTokens: u.CacheCreation5mTokens, CacheCreation1hTokens: u.CacheCreation1hTokens})
	remaining := u.CacheCreationInputTokens - w5 - w1
	if remaining < 0 {
		remaining = 0
	}
	base := float64(u.InputTokens)*s.InputPrice + float64(u.OutputTokens)*s.OutputPrice + float64(u.CacheReadInputTokens)*s.ReadPrice + float64(w5+remaining)*s.WritePrice + float64(w1)*s.Write1hPrice
	return base, QuantizeUsageBillingAmount(base * s.Multiplier)
}

func (s *GatewayService) startClaudeBillingWorker() {
	if os.Getenv("CLAUDE_BILLING_WORKER") != "true" {
		return
	}
	j, ok := s.usageBillingRepo.(ClaudeBillingJournal)
	if !ok {
		return
	}
	go func() {
		for {
			time.Sleep(10 * time.Second)
			func() {
				defer func() {
					if r := recover(); r != nil {
						logger.LegacyPrintf("service.gateway", "ALERT claude billing worker panic: %v", r)
					}
				}()
				ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
				defer cancel()
				attempts, err := j.PendingClaudeAttempts(ctx)
				if err != nil {
					logger.LegacyPrintf("service.gateway", "claude billing poll failed: %v", err)
					return
				}
				queried := false
				for i := range attempts {
					if ctx.Err() != nil {
						return
					}
					if attempts[i].State != "ready" {
						if queried {
							continue
						}
						queried = true // Upstream user search limit: at most six requests/minute.
					}
					s.reconcileClaudeAttempt(ctx, &attempts[i], j)
				}
			}()
		}
	}()
}

func (s *GatewayService) reconcileClaudeAttempt(ctx context.Context, a *ClaudeBillingAttempt, j ClaudeBillingJournal) {
	u := a.Observed
	complete := a.State == "ready"
	if !complete {
		actual, err := s.queryClaudeBill(ctx, a)
		if err != nil {
			_ = j.CheckClaudeAttempt(ctx, a.ID, err.Error())
			// Save observed usage durably but wait for a complete authoritative bill before
			// charging ambiguous streams: early deltas from compatible relays can change buckets.
			return
		} else {
			u = *actual
			a.Authoritative = actual
			complete = true
		}
	}
	if !u.hasObservedTokens() {
		if complete {
			a.Authoritative = &u
			_, _ = j.SettleClaudeAttempt(ctx, a, &UsageLog{}, &UsageBillingCommand{}, true)
		}
		return
	}
	if u.InputTokens < a.Billed.InputTokens || u.OutputTokens < a.Billed.OutputTokens || u.CacheCreationInputTokens < a.Billed.CacheCreationInputTokens || u.CacheReadInputTokens < a.Billed.CacheReadInputTokens {
		_ = j.CheckClaudeAttempt(ctx, a.ID, "upstream token buckets below settled usage")
		return
	}
	base, target := claudeTargetCost(a.Snapshot, u)
	if target+1e-8 < a.SettledCost || base+1e-9 < a.SettledBase {
		_ = j.CheckClaudeAttempt(ctx, a.ID, "upstream usage below settled amount")
		return
	}
	delta := QuantizeUsageBillingAmount(target - a.SettledCost)
	baseDelta := base - a.SettledBase
	snap := a.Snapshot
	requestID := "claude:" + a.ID + ":" + strconv.Itoa(a.Revision+1)
	duration := 0
	if a.FinishedAt != nil {
		duration = int(a.FinishedAt.Sub(snap.StartedAt).Milliseconds())
	}
	mode := "token"
	log := &UsageLog{UserID: snap.UserID, APIKeyID: snap.APIKeyID, AccountID: snap.AccountID, GroupID: &snap.GroupID, RequestID: requestID, Model: snap.Model, RequestedModel: snap.Model, UpstreamRequestID: optionalTrimmedStringPtr(a.UpstreamRequestID), RateMultiplier: snap.Multiplier, AccountRateMultiplier: &snap.AccountMultiplier, Stream: snap.Stream, DurationMs: &duration, BillingMode: &mode, CreatedAt: time.Now(), TotalCost: baseDelta, ActualCost: delta}
	log.InputTokens = max(0, u.InputTokens-a.Billed.InputTokens)
	log.OutputTokens = max(0, u.OutputTokens-a.Billed.OutputTokens)
	log.CacheCreationTokens = max(0, u.CacheCreationInputTokens-a.Billed.CacheCreationInputTokens)
	log.CacheReadTokens = max(0, u.CacheReadInputTokens-a.Billed.CacheReadInputTokens)
	log.CacheCreation5mTokens = max(0, u.CacheCreation5mTokens-a.Billed.CacheCreation5mTokens)
	log.CacheCreation1hTokens = max(0, u.CacheCreation1hTokens-a.Billed.CacheCreation1hTokens)
	log.InputCost = float64(log.InputTokens) * snap.InputPrice
	log.OutputCost = float64(log.OutputTokens) * snap.OutputPrice
	log.CacheReadCost = float64(log.CacheReadTokens) * snap.ReadPrice
	log.CacheCreationCost = baseDelta - log.InputCost - log.OutputCost - log.CacheReadCost
	cmd := &UsageBillingCommand{RequestID: requestID, APIKeyID: snap.APIKeyID, UserID: snap.UserID, AccountID: snap.AccountID, AccountType: AccountTypeAPIKey, Model: snap.Model, BalanceCost: delta}
	if snap.KeyQuota {
		cmd.APIKeyQuotaCost = delta
	}
	if snap.KeyWindows {
		cmd.APIKeyRateLimitCost = delta
	}
	if snap.AccountQuota {
		cmd.AccountQuotaCost = baseDelta * snap.AccountMultiplier
	}
	cmd.Normalize()
	a.Authoritative = &u
	result, err := j.SettleClaudeAttempt(ctx, a, log, cmd, complete)
	if err != nil {
		logger.LegacyPrintf("service.gateway", "claude settlement failed attempt=%s: %v", a.ID, err)
		return
	}
	if result == nil || !result.Applied {
		return
	}
	key := &APIKey{ID: snap.APIKeyID, GroupID: &snap.GroupID}
	if snap.KeyWindows {
		key.RateLimit1d = 1
	}
	user := &User{ID: snap.UserID}
	account := &Account{ID: snap.AccountID, Type: AccountTypeAPIKey}
	p := &postUsageBillingParams{Cost: &CostBreakdown{TotalCost: baseDelta, ActualCost: delta}, User: user, APIKey: key, Account: account, Platform: PlatformAnthropic}
	finalizePostUsageBilling(ctx, p, s.billingDeps(), result)
	if s.billingCacheService != nil {
		_ = s.billingCacheService.InvalidateUserBalance(ctx, snap.UserID)
	}
}

func queryNewAPIBillingWithCurl(ctx context.Context, base, token, user, requestID, model string) (*ClaudeUsage, error) {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, errors.New("invalid billing origin")
	}
	u.Path = "/api/log/self"
	u.RawQuery = url.Values{"request_id": {requestID}, "p": {"1"}, "page_size": {"10"}}.Encode()
	// Credentials travel on stdin rather than command-line arguments, and redirects are not followed.
	escape := func(s string) string {
		return strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "", "\r", "").Replace(s)
	}
	config := fmt.Sprintf("header = \"Authorization: Bearer %s\"\nheader = \"New-Api-User: %s\"\n", escape(token), escape(user))
	// No shell is involved; the only variable argument is a validated HTTPS URL.
	cmd := exec.CommandContext(ctx, "curl", "--silent", "--show-error", "--fail", "--max-time", "12", "--max-filesize", "1048576", "--user-agent", "Sub2API-Billing-Reconcile/1.0", "--config", "-", "--", u.String()) // #nosec G702 -- fixed executable/flags, HTTPS URL encoded with net/url, secrets on stdin.
	cmd.Stdin = strings.NewReader(config)
	body, err := cmd.Output()
	if err != nil {
		return nil, errors.New("upstream billing transport unavailable")
	}
	var response struct {
		Success bool `json:"success"`
		Data    struct {
			Total int                `json:"total"`
			Items []newAPIBillingLog `json:"items"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &response) != nil || !response.Success {
		return nil, errors.New("upstream billing authentication or format error")
	}
	if response.Data.Total != 1 || len(response.Data.Items) != 1 {
		return nil, errors.New("upstream bill missing or ambiguous")
	}
	entry := response.Data.Items[0]
	if entry.RequestID != requestID || entry.Model != model {
		return nil, errors.New("upstream bill identity mismatch")
	}
	usage, err := decodeNewAPIBillingUsage(entry)
	if err != nil {
		return nil, err
	}
	return &usage, nil
}
