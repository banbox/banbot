package runtimeplan

import (
	"slices"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	ta "github.com/banbox/banta"
)

func TestInspectCollectsCanonicalRuntimePlanWithoutDataAccess(t *testing.T) {
	const strategyName = "runtime_plan_fixture"
	startupCalls := 0
	strat.StratMake[strategyName] = func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		readSID := func(job *strat.StratJob) int64 {
			value, ok := job.Env.Data.Load("sid")
			sid, typed := value.(int64)
			if !ok || !typed || sid <= 0 {
				t.Fatalf("callback did not receive physical sid: value=%v ok=%v", value, ok)
			}
			return sid
		}
		return &strat.TradeStrat{
			WarmupNum:     12,
			RunTimeFrames: []string{"5m", "1h", "5m"},
			RefineTF:      "15m",
			OnSymbols: func(_ []string) []string {
				return []string{"ETH/USDT:USDT", "BTC/USDT:USDT", "BTC/USDT:USDT"}
			},
			OnStartUp: func(job *strat.StratJob) {
				if job.IsWarmUp || job.MaxOpenLong != 0 || job.MaxOpenShort != 0 || job.OrderNum != 0 {
					t.Fatalf("inspection job did not match real OnStartUp initial state: %+v", job)
				}
				startupCalls++
				job.TPMaxs[readSID(job)] = 1
			},
			OnPairInfos: func(job *strat.StratJob) []*strat.PairSub {
				sid := readSID(job)
				if job.TPMaxs[sid] != 1 {
					t.Fatalf("OnPairInfos ran before OnStartUp")
				}
				if sid == 1 {
					return []*strat.PairSub{{Pair: "ETH/USDT:USDT", TimeFrame: "15m", WarmupNum: 20}}
				}
				return []*strat.PairSub{{Pair: "BTC/USDT:USDT", TimeFrame: "30m", WarmupNum: 21}}
			},
			OnDataSubs: func(job *strat.StratJob) []*strat.DataSub {
				sid := readSID(job)
				if job.TPMaxs[sid] != 1 {
					t.Fatalf("OnDataSubs ran before OnStartUp")
				}
				tf := "1d"
				if sid == 2 {
					tf = "4h"
				}
				return []*strat.DataSub{{Source: "", TimeFrame: tf, WarmupNum: 3, Fields: []string{"close", "close"}, SeriesFields: []string{"close"}}}
			},
		}
	}
	t.Cleanup(func() { delete(strat.StratMake, strategyName) })

	req := validRequest(t, strategyName)
	first, err := Inspect(req)
	if err != nil {
		t.Fatalf("Inspect returned error: %v", err)
	}
	firstBytes, err := MarshalOutput(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Inspect(req)
	if err != nil {
		t.Fatalf("second Inspect returned error: %v", err)
	}
	secondBytes, _ := MarshalOutput(second)
	if string(firstBytes) != string(secondBytes) {
		t.Fatalf("runtime plan is nondeterministic:\n%s\n%s", firstBytes, secondBytes)
	}
	if startupCalls != 8 {
		t.Fatalf("OnStartUp calls = %d, want 8 for two runs x two symbols x two timeframes", startupCalls)
	}
	if first.SelectionMode != SelectionMode || len(first.Policies) != 1 {
		t.Fatalf("unexpected policy output: %+v", first)
	}
	policy := first.Policies[0]
	if !slices.Equal(policy.SelectedSymbols, []string{"ETH/USDT:USDT", "BTC/USDT:USDT"}) {
		t.Fatalf("selected symbols = %v", policy.SelectedSymbols)
	}
	if !slices.Equal(policy.CoverageSymbols, []string{"BTC/USDT:USDT", "ETH/USDT:USDT"}) {
		t.Fatalf("coverage symbols = %v", policy.CoverageSymbols)
	}
	if !slices.Equal(policy.AllowedRunTimeframes, []string{"1h", "5m"}) {
		t.Fatalf("allowed timeframes = %v", policy.AllowedRunTimeframes)
	}
	if len(first.Requirements) != 22 {
		t.Fatalf("requirements = %d, want 22: %+v", len(first.Requirements), first.Requirements)
	}
	if len(first.Unsupported) != 0 || len(first.SemanticPlanSHA256) != 64 || len(first.RequestSHA256) != 64 {
		t.Fatalf("unexpected hashes or unsupported output: %+v", first)
	}
	for _, requirement := range first.Requirements {
		if requirement.Reason != "primary" && requirement.Reason != "pair_info" && requirement.Reason != "data_sub" &&
			requirement.Reason != "pair_list" && requirement.Reason != "pair_score" {
			t.Fatalf("unexpected requirement reason: %s", requirement.Reason)
		}
	}
}

func TestInspectIncludesFrameworkPairListAndScoreKlines(t *testing.T) {
	const strategyName = "runtime_plan_framework_fixture"
	strat.StratMake[strategyName] = func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{RunTimeFrames: []string{"3d"}, WarmupNum: 300}
	}
	t.Cleanup(func() { delete(strat.StratMake, strategyName) })
	req := validRequest(t, strategyName)
	req.ConfigYAML = strings.Replace(req.ConfigYAML, "exchange:\n", "run_timeframes: [15m]\nexchange:\n", 1)
	req.ConfigSHA256 = rawHash([]byte(req.ConfigYAML))
	output, err := Inspect(req)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"pair_list\x001h": 1, "pair_score\x0015m": 600, "pair_score\x003d": 600}
	for _, requirement := range output.Requirements {
		if requirement.PolicyID != "__framework__" || requirement.JobSymbol != "BTC/USDT:USDT" {
			continue
		}
		key := requirement.Reason + "\x00" + requirement.Timeframe
		if warmup, ok := want[key]; ok {
			if requirement.WarmupBars != warmup {
				t.Fatalf("%s warmup = %d, want %d", key, requirement.WarmupBars, warmup)
			}
			delete(want, key)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing framework requirements: %v", want)
	}
}

func TestInspectAllowsForcedPairFiltersWithoutConfiguredFilters(t *testing.T) {
	const strategyName = "runtime_plan_forced_filters_fixture"
	strat.StratMake[strategyName] = func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{RunTimeFrames: []string{"1h"}}
	}
	t.Cleanup(func() { delete(strat.StratMake, strategyName) })
	req := validRequest(t, strategyName)
	req.ConfigYAML = strings.Replace(req.ConfigYAML, "exchange:\n", "pairmgr:\n  force_filters: true\nexchange:\n", 1)
	req.ConfigSHA256 = rawHash([]byte(req.ConfigYAML))
	output, err := Inspect(req)
	if err != nil || output == nil || len(output.Unsupported) != 0 {
		t.Fatalf("empty forced pair filters were rejected: output=%+v err=%v", output, err)
	}
}

func TestInspectRejectsConfiguredForcedPairFiltersWithoutRuntimeData(t *testing.T) {
	const strategyName = "runtime_plan_configured_forced_filters_fixture"
	strat.StratMake[strategyName] = func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{RunTimeFrames: []string{"1h"}}
	}
	t.Cleanup(func() { delete(strat.StratMake, strategyName) })
	req := validRequest(t, strategyName)
	req.ConfigYAML = strings.Replace(req.ConfigYAML, "exchange:\n",
		"pairmgr:\n  force_filters: true\npairlists:\n  - name: producer\n  - name: filter\nexchange:\n", 1)
	req.ConfigSHA256 = rawHash([]byte(req.ConfigYAML))
	output, err := Inspect(req)
	if err == nil || output == nil || !hasUnsupportedCode(output.Unsupported, "forced_pair_filters_require_runtime_data") {
		t.Fatalf("configured forced pair filters were not rejected: output=%+v err=%v", output, err)
	}
}

func TestInspectFrameworkRequirementsCoverPolicyAndPreMaxPairSymbols(t *testing.T) {
	const strategyName = "runtime_plan_framework_symbols_fixture"
	strat.StratMake[strategyName] = func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			RunTimeFrames: []string{"1h"},
			OnSymbols: func([]string) []string {
				return []string{"SOL/USDT:USDT", "ETH/USDT:USDT", "BTC/USDT:USDT"}
			},
		}
	}
	t.Cleanup(func() { delete(strat.StratMake, strategyName) })
	req := validRequest(t, strategyName)
	req.MarketUniverse = append(req.MarketUniverse, MarketSymbolV1{
		SID: 3, Exchange: "binance", Market: "linear", Symbol: "SOL/USDT:USDT", ListMS: 1_600_000_000_000,
	})
	req.MarketUniverseSHA256 = mustMarketUniverseSHA256(t, req.MarketUniverse)
	req.InitialSymbols = []string{"BTC/USDT:USDT"}
	req.InputPairsSHA256 = mustInputPairsSHA256(t, req.InitialSymbols)
	req.ConfigYAML = strings.Replace(req.ConfigYAML, "  - name: "+strategyName+"\n",
		"  - name: "+strategyName+"\n    pairs: [ETH/USDT:USDT]\n    max_pair: 1\n", 1)
	req.ConfigSHA256 = rawHash([]byte(req.ConfigYAML))
	output, err := Inspect(req)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{
		"pair_list\x00BTC/USDT:USDT\x001h":  1,
		"pair_list\x00ETH/USDT:USDT\x001h":  1,
		"pair_score\x00BTC/USDT:USDT\x001h": 600,
		"pair_score\x00ETH/USDT:USDT\x001h": 600,
		"pair_score\x00SOL/USDT:USDT\x001h": 600,
	}
	for _, requirement := range output.Requirements {
		if requirement.PolicyID != "__framework__" {
			continue
		}
		key := requirement.Reason + "\x00" + requirement.JobSymbol + "\x00" + requirement.Timeframe
		warmup, ok := want[key]
		if !ok || requirement.WarmupBars != warmup {
			t.Fatalf("unexpected framework requirement: %+v", requirement)
		}
		delete(want, key)
	}
	if len(want) != 0 {
		t.Fatalf("missing framework requirements: %v", want)
	}
	if !slices.Equal(output.Policies[0].SelectedSymbols, []string{"SOL/USDT:USDT"}) {
		t.Fatalf("MaxPair did not remain limited to strategy jobs: %+v", output.Policies[0])
	}
}

func TestInspectFrameworkPairScoresMirrorSubMinuteShortCircuit(t *testing.T) {
	const strategyName = "runtime_plan_framework_subminute_fixture"
	strat.StratMake[strategyName] = func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{RunTimeFrames: []string{"30s", "1h"}}
	}
	t.Cleanup(func() { delete(strat.StratMake, strategyName) })
	output, err := Inspect(validRequest(t, strategyName))
	if err != nil {
		t.Fatal(err)
	}
	for _, requirement := range output.Requirements {
		if requirement.PolicyID == "__framework__" && requirement.Reason == "pair_score" {
			t.Fatalf("sub-minute score path must not require K-lines: %+v", requirement)
		}
	}
}

func TestInspectionBarEnvMatchesRuntimeSIDAndReuse(t *testing.T) {
	envs := make(map[string]*ta.BarEnv)
	symbol := &orm.ExSymbol{ID: 42, Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT"}
	oldExchange, oldMarket := core.ExgName, core.Market
	core.ExgName, core.Market = symbol.Exchange, symbol.Market
	defer func() { core.ExgName, core.Market = oldExchange, oldMarket }()

	first, err := inspectionBarEnv(envs, symbol, "1h")
	if err != nil {
		t.Fatal(err)
	}
	second, err := inspectionBarEnv(envs, symbol, "1h")
	if err != nil {
		t.Fatal(err)
	}
	if first != second || len(envs) != 1 {
		t.Fatalf("symbol/timeframe environment was not reused: first=%p second=%p count=%d", first, second, len(envs))
	}
	value, ok := first.Data.Load("sid")
	if !ok || value != int64(symbol.ID) || first.MaxCache != core.NumTaCache {
		t.Fatalf("runtime environment metadata mismatch: sid=%v ok=%v max_cache=%d", value, ok, first.MaxCache)
	}
}

func TestInspectFailsClosedAndPersistsCanonicalUnsupportedItems(t *testing.T) {
	const strategyName = "runtime_plan_unsupported_fixture"
	strat.StratMake[strategyName] = func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			RunTimeFrames: []string{"1h"},
			OnSymbols: func(_ []string) []string {
				return []string{"BTC/USDT:USDT", "XRP/USDT:USDT"}
			},
			OnDataSubs: func(_ *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{
					{Source: "macro", ExSymbol: &orm.ExSymbol{Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT"}, TimeFrame: "1d"},
					{Source: "kline", ExSymbol: &orm.ExSymbol{Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT"}, TimeFrame: "1d", Fields: []string{"signal"}},
					{Source: "kline", ExSymbol: &orm.ExSymbol{Exchange: "binance", Market: "linear", Symbol: "DOGE/USDT:USDT"}, TimeFrame: "1d"},
					{Source: "kline", ExSymbol: &orm.ExSymbol{ID: 999, Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT"}, TimeFrame: "4h"},
				}
			},
		}
	}
	t.Cleanup(func() { delete(strat.StratMake, strategyName) })

	output, err := Inspect(validRequest(t, strategyName))
	if err == nil || output == nil {
		t.Fatalf("Inspect should fail with a persisted output, output=%+v err=%v", output, err)
	}
	var codes []string
	for _, item := range output.Unsupported {
		codes = append(codes, item.Code)
	}
	for _, want := range []string{"symbol_outside_snapshot", "unsupported_field", "unsupported_source"} {
		if !slices.Contains(codes, want) {
			t.Fatalf("unsupported codes %v do not contain %s", codes, want)
		}
	}
}

func TestInspectCanonicalizesNondeterministicCallbackOrder(t *testing.T) {
	const strategyName = "runtime_plan_map_order_fixture"
	strat.StratMake[strategyName] = func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			RunTimeFrames: []string{"1h"},
			OnDataSubs: func(_ *strat.StratJob) []*strat.DataSub {
				byTimeframe := map[string]int{"1d": 4, "15m": 9, "5m": 2, "4h": 7}
				items := make([]*strat.DataSub, 0, len(byTimeframe))
				for tf, warmup := range byTimeframe {
					items = append(items, &strat.DataSub{Source: "kline", TimeFrame: tf, WarmupNum: warmup})
				}
				return items
			},
		}
	}
	t.Cleanup(func() { delete(strat.StratMake, strategyName) })

	req := validRequest(t, strategyName)
	var expected string
	for i := 0; i < 20; i++ {
		output, err := Inspect(req)
		if err != nil {
			t.Fatal(err)
		}
		data, marshalErr := MarshalOutput(output)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if i == 0 {
			expected = string(data)
		} else if string(data) != expected {
			t.Fatalf("map iteration changed canonical output on run %d", i)
		}
	}
}

func TestInspectFailsClosedWithoutTimeframeScoresOrPolicyFilterData(t *testing.T) {
	tests := []struct {
		name       string
		strategy   string
		configTail string
		make       strat.FuncMakeStrat
		wantCode   string
	}{
		{
			name: "pick timeframe", strategy: "runtime_plan_pick_tf_fixture",
			make: func(_ *config.RunPolicyConfig) *strat.TradeStrat {
				return &strat.TradeStrat{RunTimeFrames: []string{"1h"}, PickTimeFrame: func(string, []*core.TfScore) string { return "1h" }}
			},
			wantCode: "pick_timeframe_requires_scores",
		},
		{
			name: "policy filters", strategy: "runtime_plan_filter_fixture",
			make: func(_ *config.RunPolicyConfig) *strat.TradeStrat {
				return &strat.TradeStrat{RunTimeFrames: []string{"1h"}}
			},
			configTail: "    filters:\n      - name: VolumeFilter\n",
			wantCode:   "policy_filters_require_runtime_data",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			strat.StratMake[test.strategy] = test.make
			t.Cleanup(func() { delete(strat.StratMake, test.strategy) })
			req := validRequest(t, test.strategy)
			if test.configTail != "" {
				req.ConfigYAML += test.configTail
				req.ConfigSHA256 = rawHash([]byte(req.ConfigYAML))
			}
			output, err := Inspect(req)
			if err == nil || output == nil || !hasUnsupportedCode(output.Unsupported, test.wantCode) {
				t.Fatalf("output=%+v err=%v, want unsupported %s", output, err, test.wantCode)
			}
		})
	}
}

func TestInspectPreservesSelectionOrderAndAppliesMaxPairBeforeCoverage(t *testing.T) {
	const strategyName = "runtime_plan_max_pair_fixture"
	selectedOrder := []string{"ETH/USDT:USDT", "BTC/USDT:USDT", "ETH/USDT:USDT"}
	strat.StratMake[strategyName] = func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			RunTimeFrames: []string{"1h"},
			OnSymbols: func([]string) []string {
				return slices.Clone(selectedOrder)
			},
		}
	}
	t.Cleanup(func() { delete(strat.StratMake, strategyName) })
	req := validRequest(t, strategyName)
	req.ConfigYAML = strings.Replace(req.ConfigYAML, "  - name: "+strategyName+"\n", "  - name: "+strategyName+"\n    max_pair: 1\n", 1)
	req.ConfigSHA256 = rawHash([]byte(req.ConfigYAML))
	output, err := Inspect(req)
	if err != nil {
		t.Fatal(err)
	}
	policy := output.Policies[0]
	if !slices.Equal(policy.SelectedSymbols, []string{"ETH/USDT:USDT"}) || !slices.Equal(policy.CoverageSymbols, []string{"ETH/USDT:USDT"}) {
		t.Fatalf("MaxPair/order not preserved: %+v", policy)
	}
	for _, requirement := range output.Requirements {
		if requirement.PolicyID != "__framework__" && requirement.JobSymbol != "ETH/USDT:USDT" {
			t.Fatalf("requirement escaped MaxPair cutoff: %+v", requirement)
		}
	}
	req.ConfigYAML = strings.Replace(req.ConfigYAML, "max_pair: 1", "max_pair: 2", 1)
	req.ConfigSHA256 = rawHash([]byte(req.ConfigYAML))
	ordered, err := Inspect(req)
	if err != nil {
		t.Fatal(err)
	}
	selectedOrder = []string{"BTC/USDT:USDT", "ETH/USDT:USDT", "BTC/USDT:USDT"}
	reversed, err := Inspect(req)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ordered.Policies[0].CoverageSymbols, reversed.Policies[0].CoverageSymbols) ||
		slices.Equal(ordered.Policies[0].SelectedSymbols, reversed.Policies[0].SelectedSymbols) ||
		ordered.SemanticPlanSHA256 == reversed.SemanticPlanSHA256 {
		t.Fatalf("semantic hash did not bind selection order: ordered=%+v reversed=%+v", ordered.Policies[0], reversed.Policies[0])
	}
}

func TestInspectPreservesInitialSymbolOrderBeforeMaxPair(t *testing.T) {
	const strategyName = "runtime_plan_initial_order_fixture"
	strat.StratMake[strategyName] = func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{RunTimeFrames: []string{"1h"}}
	}
	t.Cleanup(func() { delete(strat.StratMake, strategyName) })
	req := validRequest(t, strategyName)
	req.InitialSymbols = []string{"ETH/USDT:USDT", "BTC/USDT:USDT"}
	req.InputPairsSHA256, _ = InputPairsSHA256(req.InitialSymbols)
	req.ConfigYAML = strings.Replace(req.ConfigYAML, "  - name: "+strategyName+"\n",
		"  - name: "+strategyName+"\n    max_pair: 1\n", 1)
	req.ConfigSHA256 = rawHash([]byte(req.ConfigYAML))

	output, err := Inspect(req)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(output.InitialSymbols, req.InitialSymbols) ||
		!slices.Equal(output.Policies[0].SelectedSymbols, []string{"ETH/USDT:USDT"}) {
		t.Fatalf("initial order was not preserved before MaxPair: %+v", output)
	}
}

func TestInspectRecordsOrderAPICallsWithRealStartupState(t *testing.T) {
	const strategyName = "runtime_plan_order_effect_fixture"
	strat.StratMake[strategyName] = func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			RunTimeFrames: []string{"1h"},
			OnStartUp: func(job *strat.StratJob) {
				if job.IsWarmUp || job.MaxOpenLong != 0 || job.MaxOpenShort != 0 {
					t.Fatalf("unexpected inspection startup state")
				}
				_ = job.OpenOrder(&strat.EnterReq{Tag: "forbidden"})
			},
		}
	}
	t.Cleanup(func() { delete(strat.StratMake, strategyName) })
	output, err := Inspect(validRequest(t, strategyName))
	if err == nil || output == nil || !hasUnsupportedCode(output.Unsupported, "startup_order_effect") {
		t.Fatalf("order API effect was not rejected: output=%+v err=%v", output, err)
	}
	if !strings.Contains(output.Unsupported[0].Message, "OpenOrder") {
		t.Fatalf("order API audit detail missing: %+v", output.Unsupported)
	}
}

func TestEffectivePolicyMaxPairMatchesBacktestAccountFallback(t *testing.T) {
	policy := &config.RunPolicyConfig{MaxPair: 0}
	cfg := &config.Config{Accounts: map[string]*config.AccountConfig{
		"zeta":  {MaxPair: 4},
		"alpha": {MaxPair: 2},
	}}
	if got, errText := effectivePolicyMaxPair(policy, cfg); got != 2 || errText != "" {
		t.Fatalf("fallback max pair = %d, %q; want lexicographic backtest default limit 2", got, errText)
	}
	cfg.Accounts[config.DefAcc] = &config.AccountConfig{MaxPair: 3}
	if got, errText := effectivePolicyMaxPair(policy, cfg); got != 3 || errText != "" {
		t.Fatalf("explicit default max pair = %d, %q; want 3", got, errText)
	}
	policy.MaxPair = 1
	if got, errText := effectivePolicyMaxPair(policy, cfg); got != 1 || errText != "" {
		t.Fatalf("policy max pair = %d, %q; want 1", got, errText)
	}
}

func hasUnsupportedCode(items []UnsupportedV1, code string) bool {
	for _, item := range items {
		if item.Code == code {
			return true
		}
	}
	return false
}

func TestDecodeAndValidateRequestRejectAmbiguousInput(t *testing.T) {
	if _, err := DecodeRequest([]byte(`{"version":1,"unknown":true}`)); err == nil {
		t.Fatal("unknown request field was accepted")
	}
	if _, err := DecodeRequest([]byte(`{} {}`)); err == nil {
		t.Fatal("trailing JSON value was accepted")
	}
	req := validRequest(t, "missing_strategy_is_validated_after_hashes")
	req.InitialSymbols = []string{"BTC/USDT:USDT", "BTC/USDT:USDT"}
	if _, _, err := validateRequest(req); err == nil || !strings.Contains(err.Error(), "initial_symbols") {
		t.Fatalf("duplicate initial_symbols error = %v", err)
	}
}

func TestCanonicalHashAPICompatibility(t *testing.T) {
	request := RequestV1{Version: Version, CompileKey: "fixture", InitialSymbols: []string{"ETH", "BTC"}}
	semantic := SemanticPlanV1{Version: Version, SelectionMode: SelectionMode,
		InitialSymbols: []string{"ETH", "BTC"}, Policies: []PolicyV1{}, Requirements: []RequirementV1{}, Unsupported: []UnsupportedV1{}}
	markets := []MarketSymbolV1{{SID: 7, Exchange: "binance", Market: "linear", Symbol: "ETH/USDT:USDT", ListMS: 123}}

	requestHash, err := RequestSHA256(request)
	if err != nil {
		t.Fatal(err)
	}
	semanticHash, err := SemanticPlanSHA256(semantic)
	if err != nil {
		t.Fatal(err)
	}
	universeHash, err := MarketUniverseSHA256(markets)
	if err != nil {
		t.Fatal(err)
	}
	pairsHash, err := InputPairsSHA256(request.InitialSymbols)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{
		"request": requestHash, "semantic": semanticHash, "universe": universeHash, "pairs": pairsHash,
	}
	want := map[string]string{
		"request":  "c9e19e2dedac2793b5a74b67afbcdb8590c9b4012b46f3f7d89d3fb490173a6b",
		"semantic": "cdc42a1795298234ec06fa8c1dff06e2c2346f33c472b86fa493e4e14d72cc73",
		"universe": "47b53877ce5f71bc9dd4cfbf0687567f2085546419aaa3c0e7639d949812f02c",
		"pairs":    "594f0ab4dc2bd3f0ba61e386e73605da320b94c402a6c47651d22af2a8e1042b",
	}
	for name, hash := range got {
		if hash != want[name] {
			t.Errorf("%s hash = %s, want %s", name, hash, want[name])
		}
	}

	reversed := request
	reversed.InitialSymbols = []string{"BTC", "ETH"}
	reversedRequestHash, err := RequestSHA256(reversed)
	if err != nil {
		t.Fatal(err)
	}
	reversedPairsHash, err := InputPairsSHA256(reversed.InitialSymbols)
	if err != nil {
		t.Fatal(err)
	}
	if reversedRequestHash == requestHash || reversedPairsHash == pairsHash {
		t.Fatal("canonical hashes did not bind input pair order")
	}
}

func validRequest(t *testing.T, strategyName string) *RequestV1 {
	t.Helper()
	configYAML := "market_type: linear\n" +
		"stake_currency: [USDT]\n" +
		"exchange:\n  name: binance\n" +
		"run_policy:\n  - name: " + strategyName + "\n"
	universe := []MarketSymbolV1{
		{SID: 1, Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT", ListMS: 1_600_000_000_000},
		{SID: 2, Exchange: "binance", Market: "linear", Symbol: "ETH/USDT:USDT", ListMS: 1_600_000_000_000},
	}
	initial := []string{"BTC/USDT:USDT", "ETH/USDT:USDT"}
	hash := strings.Repeat("a", 64)
	return &RequestV1{
		Version: Version, CompileKey: hash, CompileBaseName: "fixture", CompileVersion: "v1",
		StrategyInputSHA256: hash, StrategySourceSHA256: hash, CompileManifestSHA256: hash,
		CompiledBinarySHA256: hash, BanbotCommit: strings.Repeat("b", 40), BanbotSourceManifestSHA256: hash,
		ConfigYAML: configYAML, ConfigSHA256: rawHash([]byte(configYAML)), ConfigSemanticSHA256: hash,
		MarketSnapshotIdentity: "fixture-snapshot", MarketSnapshotSHA256: hash,
		MarketUniverseSHA256: mustMarketUniverseSHA256(t, universe), MarketUniverse: universe,
		InitialSymbols: initial, InputPairsSHA256: mustInputPairsSHA256(t, initial),
		TimeStartMS: 1_700_000_000_000, TimeEndMS: 1_710_000_000_000,
	}
}

func mustMarketUniverseSHA256(t *testing.T, markets []MarketSymbolV1) string {
	t.Helper()
	hash, err := MarketUniverseSHA256(markets)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func mustInputPairsSHA256(t *testing.T, symbols []string) string {
	t.Helper()
	hash, err := InputPairsSHA256(symbols)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}
