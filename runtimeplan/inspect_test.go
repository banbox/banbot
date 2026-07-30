package runtimeplan

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
)

func TestInspectCollectsCanonicalRuntimePlanWithoutDataAccess(t *testing.T) {
	const strategyName = "runtime_plan_fixture"
	startupCalls := 0
	strat.StratMake[strategyName] = func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			WarmupNum:     12,
			RunTimeFrames: []string{"5m", "1h", "5m"},
			RefineTF:      "15m",
			OnSymbols: func(_ []string) []string {
				return []string{"ETH/USDT:USDT", "BTC/USDT:USDT", "BTC/USDT:USDT"}
			},
			OnStartUp: func(job *strat.StratJob) {
				startupCalls++
				job.TPMaxs[1] = 1
			},
			OnPairInfos: func(job *strat.StratJob) []*strat.PairSub {
				if job.TPMaxs[1] != 1 {
					t.Fatalf("OnPairInfos ran before OnStartUp")
				}
				return []*strat.PairSub{{Pair: "ETH/USDT:USDT", TimeFrame: "15m", WarmupNum: 20}}
			},
			OnDataSubs: func(job *strat.StratJob) []*strat.DataSub {
				if job.TPMaxs[1] != 1 {
					t.Fatalf("OnDataSubs ran before OnStartUp")
				}
				return []*strat.DataSub{{Source: "", TimeFrame: "1d", WarmupNum: 3, Fields: []string{"close", "close"}, SeriesFields: []string{"close"}}}
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
	if !slices.Equal(policy.SelectedSymbols, []string{"BTC/USDT:USDT", "ETH/USDT:USDT"}) {
		t.Fatalf("selected symbols = %v", policy.SelectedSymbols)
	}
	if !slices.Equal(policy.AllowedRunTimeframes, []string{"1h", "5m"}) {
		t.Fatalf("allowed timeframes = %v", policy.AllowedRunTimeframes)
	}
	if len(first.Requirements) != 16 {
		t.Fatalf("requirements = %d, want 16: %+v", len(first.Requirements), first.Requirements)
	}
	if len(first.Unsupported) != 0 || len(first.SemanticPlanSHA256) != 64 || len(first.RequestSHA256) != 64 {
		t.Fatalf("unexpected hashes or unsupported output: %+v", first)
	}
	for _, requirement := range first.Requirements {
		if requirement.Reason != "primary" && requirement.Reason != "pair_info" && requirement.Reason != "data_sub" {
			t.Fatalf("unexpected requirement reason: %s", requirement.Reason)
		}
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

func TestDecodeAndValidateRequestRejectAmbiguousInput(t *testing.T) {
	if _, err := DecodeRequest([]byte(`{"version":1,"unknown":true}`)); err == nil {
		t.Fatal("unknown request field was accepted")
	}
	if _, err := DecodeRequest([]byte(`{} {}`)); err == nil {
		t.Fatal("trailing JSON value was accepted")
	}
	req := validRequest(t, "missing_strategy_is_validated_after_hashes")
	req.InitialSymbols = []string{"ETH/USDT:USDT", "BTC/USDT:USDT"}
	if _, _, err := validateRequest(req); err == nil || !strings.Contains(err.Error(), "initial_symbols") {
		t.Fatalf("noncanonical initial_symbols error = %v", err)
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
	universeJSON, _ := json.Marshal(universe)
	initialJSON, _ := json.Marshal(initial)
	hash := strings.Repeat("a", 64)
	return &RequestV1{
		Version: Version, CompileKey: hash, CompileBaseName: "fixture", CompileVersion: "v1",
		StrategyInputSHA256: hash, StrategySourceSHA256: hash, CompileManifestSHA256: hash,
		CompiledBinarySHA256: hash, BanbotCommit: strings.Repeat("b", 40), BanbotSourceManifestSHA256: hash,
		ConfigYAML: configYAML, ConfigSHA256: rawHash([]byte(configYAML)), ConfigSemanticSHA256: hash,
		MarketSnapshotIdentity: "fixture-snapshot", MarketSnapshotSHA256: hash,
		MarketUniverseSHA256: domainHash(universeHashDomain, universeJSON), MarketUniverse: universe,
		InitialSymbols: initial, InputPairsSHA256: domainHash(pairsHashDomain, initialJSON),
		TimeStartMS: 1_700_000_000_000, TimeEndMS: 1_710_000_000_000,
	}
}
