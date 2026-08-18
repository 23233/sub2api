package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func newOpenAICacheCostSchedulerTestService(accounts []Account, cacheMinRate string) *OpenAIGatewayService {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	repo := &openAIAdvancedSchedulerSettingRepoStub{values: map[string]string{
		openAIAdvancedSchedulerSettingKey: "true",
	}}
	if cacheMinRate != "" {
		repo.values[SettingKeyOpenAIAdvancedSchedulerCacheMinRate] = cacheMinRate
	}
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.LBTopK = 1
	cfg.Gateway.OpenAIWS.SchedulerScoreWeights.Priority = 1
	cfg.Gateway.OpenAIWS.SchedulerScoreWeights.Load = 1
	cfg.Gateway.OpenAIWS.SchedulerScoreWeights.Queue = 0.7
	cfg.Gateway.OpenAIWS.SchedulerScoreWeights.ErrorRate = 0.8
	cfg.Gateway.OpenAIWS.SchedulerScoreWeights.TTFT = 0.5
	return &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: accounts},
		cache:              &schedulerTestGatewayCache{},
		cfg:                cfg,
		rateLimitService:   &RateLimitService{settingService: NewSettingService(repo, cfg)},
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{}),
	}
}

func newOpenAICacheCostSchedulerTestServiceWithSettings(accounts []Account, values map[string]string) *OpenAIGatewayService {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	repoValues := map[string]string{openAIAdvancedSchedulerSettingKey: "true"}
	for key, value := range values {
		repoValues[key] = value
	}
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.LBTopK = 1
	cfg.Gateway.OpenAIWS.SchedulerScoreWeights.Priority = 1
	cfg.Gateway.OpenAIWS.SchedulerScoreWeights.Load = 1
	cfg.Gateway.OpenAIWS.SchedulerScoreWeights.Queue = 0.7
	cfg.Gateway.OpenAIWS.SchedulerScoreWeights.ErrorRate = 0.8
	cfg.Gateway.OpenAIWS.SchedulerScoreWeights.TTFT = 0.5
	return &OpenAIGatewayService{
		accountRepo: schedulerTestOpenAIAccountRepo{accounts: accounts},
		cache:       &schedulerTestGatewayCache{},
		cfg:         cfg,
		rateLimitService: &RateLimitService{settingService: NewSettingService(
			&openAIAdvancedSchedulerSettingRepoStub{values: repoValues}, cfg,
		)},
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{}),
	}
}

func openAICacheCostTestAccount(id int64, priority int) Account {
	return Account{
		ID:          id,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Priority:    priority,
	}
}

func reportOpenAICacheCostTestUsage(
	svc *OpenAIGatewayService,
	accountID int64,
	model string,
	inputTokens int,
	cacheReadTokens int,
	accountCost float64,
) {
	svc.ReportOpenAIAccountScheduleUsage(accountID, model, OpenAIAccountScheduleUsage{
		InputTokens:     inputTokens - cacheReadTokens,
		CacheReadTokens: cacheReadTokens,
		AccountCost:     accountCost,
	})
}

func TestOpenAIAdvancedSchedulerCacheMinRateDefaultsTo80PercentAndIsConfigurable(t *testing.T) {
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

	defaultSvc := newOpenAICacheCostSchedulerTestService(nil, "")
	require.InDelta(t, 0.80, defaultSvc.openAIAdvancedSchedulerCacheMinRate(context.Background()), 1e-9)

	overriddenSvc := newOpenAICacheCostSchedulerTestService(nil, "85")
	require.InDelta(t, 0.85, overriddenSvc.openAIAdvancedSchedulerCacheMinRate(context.Background()), 1e-9)

	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	recoverySvc := newOpenAICacheCostSchedulerTestServiceWithSettings(nil, map[string]string{
		SettingKeyOpenAIAdvancedSchedulerCacheRecoveryMinutes: "7",
	})
	require.Equal(t, 7*time.Minute, recoverySvc.openAIAdvancedSchedulerCacheRecoveryInterval(context.Background()))
}

func TestOpenAIAdvancedSchedulerCacheCircuitHalfOpenRecovery(t *testing.T) {
	stats := newOpenAIAccountRuntimeStats()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	usage := OpenAIAccountScheduleUsage{InputTokens: 170_000, AccountCost: 0.10}
	stats.reportUsageAt(51000, "gpt-5.4", usage, 0.80, 15*time.Minute, now)

	decision := stats.cacheDecisionAt(51000, "gpt-5.4", 0.80, now.Add(14*time.Minute))
	require.Equal(t, openAIAccountCacheBlocked, decision.state)

	decision = stats.cacheDecisionAt(51000, "gpt-5.4", 0.80, now.Add(15*time.Minute))
	require.Equal(t, openAIAccountCacheHalfOpen, decision.state)
	require.True(t, stats.beginCacheProbeAt(51000, "gpt-5.4", now.Add(15*time.Minute)))
	require.False(t, stats.beginCacheProbeAt(51000, "gpt-5.4", now.Add(15*time.Minute)),
		"only one request may probe a half-open account")

	stats.reportUsageAt(51000, "gpt-5.4", OpenAIAccountScheduleUsage{
		InputTokens:     10_000,
		CacheReadTokens: 90_000,
		AccountCost:     0.02,
	}, 0.80, 15*time.Minute, now.Add(16*time.Minute))
	decision = stats.cacheDecisionAt(51000, "gpt-5.4", 0.80, now.Add(16*time.Minute))
	require.Equal(t, openAIAccountCacheHealthy, decision.state)
	require.InDelta(t, 0.90, decision.cacheRate, 1e-9)
}

func TestOpenAIAdvancedSchedulerCacheCircuitFailedProbeBacksOff(t *testing.T) {
	stats := newOpenAIAccountRuntimeStats()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	miss := OpenAIAccountScheduleUsage{InputTokens: 170_000, AccountCost: 0.10}
	stats.reportUsageAt(51000, "gpt-5.4", miss, 0.80, 15*time.Minute, now)
	require.True(t, stats.beginCacheProbeAt(51000, "gpt-5.4", now.Add(15*time.Minute)))
	stats.reportUsageAt(51000, "gpt-5.4", miss, 0.80, 15*time.Minute, now.Add(16*time.Minute))

	decision := stats.cacheDecisionAt(51000, "gpt-5.4", 0.80, now.Add(45*time.Minute))
	require.Equal(t, openAIAccountCacheBlocked, decision.state, "a failed probe doubles the next cooldown to 30 minutes")
	decision = stats.cacheDecisionAt(51000, "gpt-5.4", 0.80, now.Add(46*time.Minute))
	require.Equal(t, openAIAccountCacheHalfOpen, decision.state)
}

func TestOpenAIAdvancedSchedulerCacheCircuitDoesNotJudgeTinyProbe(t *testing.T) {
	stats := newOpenAIAccountRuntimeStats()
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	stats.reportUsageAt(51005, "gpt-5.4", OpenAIAccountScheduleUsage{
		InputTokens: 170_000,
		AccountCost: 0.10,
	}, 0.80, 15*time.Minute, now)
	require.True(t, stats.beginCacheProbeAtWithPolicy(51005, "gpt-5.4", 0.80, 15*time.Minute, now.Add(15*time.Minute)))

	// A tiny probe is not enough evidence to classify cache health. Keep the
	// account cooled down for another base interval instead of immediately
	// allowing every small request to probe it again.
	stats.reportUsageAt(51005, "gpt-5.4", OpenAIAccountScheduleUsage{
		InputTokens:     100,
		CacheReadTokens: 0,
		AccountCost:     0.001,
	}, 0.80, 15*time.Minute, now.Add(16*time.Minute))

	decision := stats.cacheDecisionAtWithRecovery(51005, "gpt-5.4", 0.80, 15*time.Minute, now.Add(16*time.Minute))
	require.Equal(t, openAIAccountCacheBlocked, decision.state)
	decision = stats.cacheDecisionAtWithRecovery(51005, "gpt-5.4", 0.80, 15*time.Minute, now.Add(31*time.Minute))
	require.Equal(t, openAIAccountCacheHalfOpen, decision.state)
}

func TestOpenAIAdvancedSchedulerCachePolicySkipsNonPromptCacheRequests(t *testing.T) {
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)
	svc := newOpenAICacheCostSchedulerTestService(nil, "80")
	scheduler, ok := newDefaultOpenAIAccountScheduler(svc, newOpenAIAccountRuntimeStats()).(*defaultOpenAIAccountScheduler)
	require.True(t, ok)

	_, _, cacheAware := scheduler.cachePolicyForRequest(context.Background(), OpenAIAccountScheduleRequest{
		Platform:           PlatformOpenAI,
		RequiredCapability: OpenAIEndpointCapabilityEmbeddings,
	})
	require.False(t, cacheAware)

	_, _, cacheAware = scheduler.cachePolicyForRequest(context.Background(), OpenAIAccountScheduleRequest{
		Platform:                PlatformOpenAI,
		RequiredImageCapability: OpenAIImagesCapabilityBasic,
	})
	require.False(t, cacheAware)

	_, _, cacheAware = scheduler.cachePolicyForRequest(context.Background(), OpenAIAccountScheduleRequest{
		Platform:           PlatformOpenAI,
		RequiredCapability: OpenAIEndpointCapabilityChatCompletions,
	})
	require.True(t, cacheAware)
}

func TestOpenAIAdvancedSchedulerCacheGateFiltersLargeMissAndReturnsNoAvailable(t *testing.T) {
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)
	account := openAICacheCostTestAccount(51001, 0)
	svc := newOpenAICacheCostSchedulerTestService([]Account{account}, "80")

	// One large uncached request is enough evidence to stop the next expensive request.
	reportOpenAICacheCostTestUsage(svc, account.ID, "gpt-5.4", 170_000, 0, 0.10)

	selection, _, err := svc.SelectAccountWithScheduler(
		context.Background(), nil, "", "", "gpt-5.4", nil, OpenAIUpstreamTransportAny, false,
	)
	require.Nil(t, selection)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
	require.Contains(t, err.Error(), "cache_rate_below_threshold=1")
}

func TestOpenAIAdvancedSchedulerCacheGateAllowsUnknownAndIsolatesModels(t *testing.T) {
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)
	account := openAICacheCostTestAccount(51011, 0)
	svc := newOpenAICacheCostSchedulerTestService([]Account{account}, "80")

	// Unknown accounts must get an initial chance; otherwise a fresh process can never learn.
	selection, _, err := svc.SelectAccountWithScheduler(
		context.Background(), nil, "", "", "gpt-5.4", nil, OpenAIUpstreamTransportAny, false,
	)
	require.NoError(t, err)
	require.Equal(t, account.ID, selection.Account.ID)
	selection.ReleaseFunc()

	reportOpenAICacheCostTestUsage(svc, account.ID, "gpt-5.4", 170_000, 0, 0.10)
	selection, _, err = svc.SelectAccountWithScheduler(
		context.Background(), nil, "", "", "gpt-5.5", nil, OpenAIUpstreamTransportAny, false,
	)
	require.NoError(t, err)
	require.Equal(t, account.ID, selection.Account.ID, "cache health must be scoped by upstream model")
	selection.ReleaseFunc()
}

func TestOpenAIAdvancedSchedulerCacheGateEscapesUnhealthyStickyAccount(t *testing.T) {
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)
	bad := openAICacheCostTestAccount(51021, 0)
	good := openAICacheCostTestAccount(51022, 10)
	svc := newOpenAICacheCostSchedulerTestService([]Account{bad, good}, "80")
	cache := svc.cache.(*schedulerTestGatewayCache)
	cache.sessionBindings = map[string]int64{"openai:cache-cost-sticky": bad.ID}

	reportOpenAICacheCostTestUsage(svc, bad.ID, "gpt-5.4", 170_000, 0, 0.10)
	reportOpenAICacheCostTestUsage(svc, good.ID, "gpt-5.4", 100_000, 90_000, 0.03)

	selection, decision, err := svc.SelectAccountWithScheduler(
		context.Background(), nil, "", "cache-cost-sticky", "gpt-5.4", nil, OpenAIUpstreamTransportAny, false,
	)
	require.NoError(t, err)
	require.Equal(t, good.ID, selection.Account.ID)
	require.Equal(t, openAIAccountScheduleLayerLoadBalance, decision.Layer)
	require.False(t, decision.StickySessionHit)
	selection.ReleaseFunc()
}

func TestOpenAIAdvancedSchedulerPrefersObservedLowerCostWhenTTFTIsClose(t *testing.T) {
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)
	fastExpensive := openAICacheCostTestAccount(51031, 0)
	slightlySlowerCheap := openAICacheCostTestAccount(51032, 10)
	svc := newOpenAICacheCostSchedulerTestService([]Account{fastExpensive, slightlySlowerCheap}, "80")

	reportOpenAICacheCostTestUsage(svc, fastExpensive.ID, "gpt-5.4", 100_000, 90_000, 0.10)
	reportOpenAICacheCostTestUsage(svc, slightlySlowerCheap.ID, "gpt-5.4", 100_000, 90_000, 0.03)
	fastTTFT, cheapTTFT := 1_000, 1_100
	svc.ReportOpenAIAccountScheduleResult(fastExpensive.ID, "gpt-5.4", true, &fastTTFT)
	svc.ReportOpenAIAccountScheduleResult(slightlySlowerCheap.ID, "gpt-5.4", true, &cheapTTFT)

	selection, _, err := svc.SelectAccountWithScheduler(
		context.Background(), nil, "", "", "gpt-5.4", nil, OpenAIUpstreamTransportAny, false,
	)
	require.NoError(t, err)
	require.Equal(t, slightlySlowerCheap.ID, selection.Account.ID,
		"within the TTFT similarity band, observed account cost must win over declared priority")
	selection.ReleaseFunc()
}

func forceOpenAICacheHalfOpenForTest(t *testing.T, svc *OpenAIGatewayService, accountID int64, model string) {
	t.Helper()
	stat := svc.openaiAccountStats.loadOrCreateModel(accountID, model)
	require.NotNil(t, stat)
	stat.mu.Lock()
	stat.blockedUntil = time.Now().Add(-time.Minute)
	stat.probeInFlight = false
	stat.probeLeaseUntil = time.Time{}
	stat.mu.Unlock()
}

func TestOpenAIAdvancedSchedulerCacheGatePreviousResponseDoesNotBypassBlockedAccount(t *testing.T) {
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)
	bad := openAICacheCostTestAccount(51041, 0)
	bad.Extra = map[string]any{"openai_apikey_responses_websockets_v2_enabled": true}
	good := openAICacheCostTestAccount(51042, 10)
	good.Extra = map[string]any{"openai_apikey_responses_websockets_v2_enabled": true}
	svc := newOpenAICacheCostSchedulerTestService([]Account{bad, good}, "80")
	svc.cfg.Gateway.OpenAIWS.Enabled = true
	svc.cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	svc.cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	ctx := context.Background()
	groupID := int64(5104)
	store := svc.getOpenAIWSStateStore()
	require.NoError(t, store.BindResponseAccount(ctx, groupID, "resp_cache_blocked", bad.ID, time.Hour))
	reportOpenAICacheCostTestUsage(svc, bad.ID, "gpt-5.4", 170_000, 0, 0.10)

	selection, _, err := svc.SelectAccountWithScheduler(
		ctx, &groupID, "resp_cache_blocked", "", "gpt-5.4", nil, OpenAIUpstreamTransportAny, false,
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, good.ID, selection.Account.ID,
		"previous_response_id must fall back to a healthy account while the sticky account is cache-blocked")
	selection.ReleaseFunc()
}

func TestOpenAIAdvancedSchedulerCacheGateHalfOpenStickyRequiresAcquiredProbe(t *testing.T) {
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)
	bad := openAICacheCostTestAccount(51051, 0)
	good := openAICacheCostTestAccount(51052, 10)
	svc := newOpenAICacheCostSchedulerTestServiceWithSettings([]Account{bad, good}, map[string]string{
		SettingKeyOpenAIAdvancedSchedulerCacheMinRate: "80",
	})
	svc.concurrencyService = NewConcurrencyService(schedulerTestConcurrencyCache{
		acquireResults: map[int64]bool{bad.ID: false, good.ID: true},
	})
	cache := svc.cache.(*schedulerTestGatewayCache)
	cache.sessionBindings = map[string]int64{"openai:half-open-sticky": bad.ID}
	reportOpenAICacheCostTestUsage(svc, bad.ID, "gpt-5.4", 170_000, 0, 0.10)
	forceOpenAICacheHalfOpenForTest(t, svc, bad.ID, "gpt-5.4")

	selection, _, err := svc.SelectAccountWithScheduler(
		context.Background(), nil, "", "half-open-sticky", "gpt-5.4", nil, OpenAIUpstreamTransportAny, false,
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, good.ID, selection.Account.ID,
		"a half-open sticky account that cannot acquire a slot must not produce a wait plan")
	require.Nil(t, selection.WaitPlan)
	selection.ReleaseFunc()
}

func TestOpenAIAdvancedSchedulerCacheGateHalfOpenStickyAllowsOnlyOneProbe(t *testing.T) {
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)
	bad := openAICacheCostTestAccount(51061, 0)
	good := openAICacheCostTestAccount(51062, 10)
	svc := newOpenAICacheCostSchedulerTestService([]Account{bad, good}, "80")
	cache := svc.cache.(*schedulerTestGatewayCache)
	cache.sessionBindings = map[string]int64{"openai:half-open-single-probe": bad.ID}
	reportOpenAICacheCostTestUsage(svc, bad.ID, "gpt-5.4", 170_000, 0, 0.10)
	forceOpenAICacheHalfOpenForTest(t, svc, bad.ID, "gpt-5.4")

	first, _, err := svc.SelectAccountWithScheduler(
		context.Background(), nil, "", "half-open-single-probe", "gpt-5.4", nil, OpenAIUpstreamTransportAny, false,
	)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, bad.ID, first.Account.ID)
	first.ReleaseFunc()

	second, _, err := svc.SelectAccountWithScheduler(
		context.Background(), nil, "", "half-open-single-probe", "gpt-5.4", nil, OpenAIUpstreamTransportAny, false,
	)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, good.ID, second.Account.ID,
		"while a probe is in flight, another request must not reuse the half-open account")
	second.ReleaseFunc()
}
