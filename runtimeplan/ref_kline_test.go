package runtimeplan

import (
	"slices"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/strat"
)

func TestInspectResolvesRefKlineSubOutsideInitialTradingPairs(t *testing.T) {
	const strategyName = "runtime_plan_ref_kline_fixture"
	strat.RegisterStrategy(strategyName, func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			WarmupNum:     80,
			RunTimeFrames: []string{"15m"},
			OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{
					s.RefKlineSub("BTC", "1d"),
					s.RefKlineSub("", "4h", 30),
				}
			},
		}
	})
	t.Cleanup(func() { strat.UnregisterStrategy(strategyName) })

	req := validRequest(t, strategyName)
	req.InitialSymbols = []string{"ETH/USDT:USDT"}
	req.InputPairsSHA256 = mustInputPairsSHA256(t, req.InitialSymbols)

	output, err := Inspect(req, t.TempDir())
	if err != nil {
		t.Fatalf("Inspect returned error: %v", err)
	}
	if len(output.Policies) != 1 || !slices.Equal(output.Policies[0].SelectedSymbols, []string{"ETH/USDT:USDT"}) {
		t.Fatalf("selected symbols = %+v, want only ETH/USDT:USDT", output.Policies)
	}

	foundBTC1d := false
	foundETH4h := false
	for _, item := range output.Requirements {
		if item.PolicyID != strategyName || item.Reason != "data_sub" {
			continue
		}
		if item.JobSymbol == "ETH/USDT:USDT" && item.TargetSymbol == "BTC/USDT:USDT" && item.Timeframe == "1d" && item.WarmupBars == 80 {
			foundBTC1d = true
		}
		if item.JobSymbol == "ETH/USDT:USDT" && item.TargetSymbol == "ETH/USDT:USDT" && item.Timeframe == "4h" && item.WarmupBars == 30 {
			foundETH4h = true
		}
	}
	if !foundBTC1d || !foundETH4h {
		t.Fatalf("missing expected RefKlineSub requirements (btc1d=%v eth4h=%v): %+v",
			foundBTC1d, foundETH4h, output.Requirements)
	}
}

func TestInspectReportsRefKlineSubOutsideMarketUniverse(t *testing.T) {
	const strategyName = "runtime_plan_ref_kline_outside_universe"
	strat.RegisterStrategy(strategyName, func(_ *config.RunPolicyConfig) *strat.TradeStrat {
		return &strat.TradeStrat{
			WarmupNum:     20,
			RunTimeFrames: []string{"1h"},
			OnDataSubs: func(s *strat.StratJob) []*strat.DataSub {
				return []*strat.DataSub{s.RefKlineSub("SOL", "1d")}
			},
		}
	})
	t.Cleanup(func() { strat.UnregisterStrategy(strategyName) })

	req := validRequest(t, strategyName)
	req.InitialSymbols = []string{"ETH/USDT:USDT"}
	req.InputPairsSHA256 = mustInputPairsSHA256(t, req.InitialSymbols)

	output, err := Inspect(req, t.TempDir())
	if err == nil || output == nil || len(output.Unsupported) == 0 {
		t.Fatalf("expected unsupported symbol_outside_snapshot, got output=%+v err=%v", output, err)
	}
	found := false
	for _, u := range output.Unsupported {
		if u.Code == "symbol_outside_snapshot" && strings.Contains(u.Message, "SOL/USDT:USDT") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected symbol_outside_snapshot for SOL/USDT:USDT, got %+v", output.Unsupported)
	}
}
