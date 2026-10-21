package service

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClaudeBillingMetadataSurvivesSnapshotSerialization(t *testing.T) {
	requested, effective := "max", "high"
	ctx := WithClaudeBillingMetadata(context.Background(), "/v1/messages", "/v1/messages", &requested, &effective)
	metadata, ok := ctx.Value(claudeBillingMetadataKey{}).(claudeBillingMetadata)
	require.True(t, ok)
	snapshot := ClaudeBillingSnapshot{InboundEndpoint: metadata.InboundEndpoint, UpstreamEndpoint: metadata.UpstreamEndpoint, RequestedReasoningEffort: metadata.RequestedReasoningEffort, ReasoningEffort: metadata.ReasoningEffort}
	encoded, err := json.Marshal(snapshot)
	require.NoError(t, err)
	var restored ClaudeBillingSnapshot
	require.NoError(t, json.Unmarshal(encoded, &restored))
	log := &UsageLog{}
	applyClaudeBillingMetadata(log, restored)
	require.Equal(t, "/v1/messages", *log.InboundEndpoint)
	require.Equal(t, "/v1/messages", *log.UpstreamEndpoint)
	require.Equal(t, "max", *log.RequestedReasoningEffort)
	require.Equal(t, "high", *log.ReasoningEffort)
}

func TestClaudeBillingMetadataDoesNotInventAbsentEffort(t *testing.T) {
	ctx := WithClaudeBillingMetadata(context.Background(), "/v1/messages", "/v1/messages", nil, nil)
	metadata, ok := ctx.Value(claudeBillingMetadataKey{}).(claudeBillingMetadata)
	require.True(t, ok)
	log := &UsageLog{}
	applyClaudeBillingMetadata(log, ClaudeBillingSnapshot{InboundEndpoint: metadata.InboundEndpoint, UpstreamEndpoint: metadata.UpstreamEndpoint})
	require.NotNil(t, log.InboundEndpoint)
	require.Nil(t, log.RequestedReasoningEffort)
	require.Nil(t, log.ReasoningEffort)
}

func TestClaudeBillingInputWithoutOutput(t *testing.T) {
	u, err := decodeNewAPIBillingUsage(newAPIBillingLog{Type: 2, Input: 82654, Other: `{"claude":true,"usage_semantic":"anthropic","cache_tokens":0,"cache_creation_tokens":0}`})
	require.NoError(t, err)
	base, actual := claudeTargetCost(ClaudeBillingSnapshot{InputPrice: 0.000005, OutputPrice: 0.000025, Multiplier: 0.029}, u)
	require.InDelta(t, 0.41327, base, 1e-12)
	require.Equal(t, 0.01198483, actual)
}

func TestNewAPIBillingCacheBucketsNotDoubleCounted(t *testing.T) {
	u, err := decodeNewAPIBillingUsage(newAPIBillingLog{Type: 2, Input: 100, Output: 10, Other: `{"usage_semantic":"anthropic","cache_tokens":300,"cache_creation_tokens":200,"cache_creation_tokens_5m":150,"cache_creation_tokens_1h":50}`})
	require.NoError(t, err)
	base, _ := claudeTargetCost(ClaudeBillingSnapshot{InputPrice: 5e-6, OutputPrice: 25e-6, WritePrice: 10e-6, Write1hPrice: 10e-6, ReadPrice: 0.5e-6, Multiplier: 1}, u)
	require.InDelta(t, 0.0029, base, 1e-12)
}

func TestNewAPIBillingRejectsAmbiguousSemantics(t *testing.T) {
	_, err := decodeNewAPIBillingUsage(newAPIBillingLog{Type: 2, Input: 100, Other: `{}`})
	require.Error(t, err)
}

func TestNewAPIBillingFailedCallHasNoCharge(t *testing.T) {
	u, err := decodeNewAPIBillingUsage(newAPIBillingLog{Type: 5, Other: `{"status_code":502}`})
	require.NoError(t, err)
	require.False(t, u.hasObservedTokens())
}

func TestNewAPIBillingRejectsNonConsumptionRecord(t *testing.T) {
	_, err := decodeNewAPIBillingUsage(newAPIBillingLog{Type: 5, Input: 82654, Other: `{"claude":true}`})
	require.NoError(t, err)
}

func TestClaudeBillingObserverInitialOutputAndTail(t *testing.T) {
	b := &claudeJournalBody{ReadCloser: io.NopCloser(strings.NewReader("")), attempt: &ClaudeBillingAttempt{Snapshot: ClaudeBillingSnapshot{StartedAt: time.Now().Add(-1250 * time.Millisecond)}}, stream: true}
	b.observeLine(`data: {"type":"message_start","message":{"usage":{"input_tokens":82654,"output_tokens":3}}}`)
	require.Equal(t, 82654, b.attempt.Observed.InputTokens)
	require.Equal(t, 3, b.attempt.Observed.OutputTokens)
	require.NotNil(t, b.attempt.FirstTokenMs)
	require.GreaterOrEqual(t, *b.attempt.FirstTokenMs, 1250)
	b.observeLine(`data: {"type":"message_delta","usage":{"output_tokens":15}}`)
	require.Equal(t, 15, b.attempt.Observed.OutputTokens)
}

func TestClaudeBillingObserverIgnoresSSECommentsForFirstToken(t *testing.T) {
	b := &claudeJournalBody{ReadCloser: io.NopCloser(strings.NewReader("")), attempt: &ClaudeBillingAttempt{Snapshot: ClaudeBillingSnapshot{StartedAt: time.Now().Add(-time.Second)}}, stream: true}
	b.observeLine(": keepalive")
	require.Nil(t, b.attempt.FirstTokenMs)
	b.observeLine("data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}")
	require.NotNil(t, b.attempt.FirstTokenMs)
}
