package strat

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
)

func TestNewStrategyRequireReturnsBadConfigAndDoesNotPolluteCache(t *testing.T) {
	const name = "ts08_require_probe"
	RegisterStrategy(name, func(pol *config.RunPolicyConfig) *TradeStrat {
		fast := pol.DefInt("fast", 10, nil)
		slow := pol.DefInt("slow", 30, nil)
		pol.Require(fast < slow, "fast must be less than slow")
		return &TradeStrat{RunTimeFrames: []string{"1h"}}
	})
	t.Cleanup(func() { UnregisterStrategy(name) })

	state := NewState()
	badPol := &config.RunPolicyConfig{
		Name:   name,
		Params: map[string]float64{"fast": 30, "slow": 10},
	}
	if stgy, err := state.NewStrategyChecked(badPol); stgy != nil || err == nil {
		t.Fatalf("NewStrategyChecked(badPol) = (%v, %v), want nil and BadConfig", stgy, err)
	} else if err.Code != core.ErrBadConfig || err.Short() != "[BadConfig] fast must be less than slow" {
		t.Fatalf("NewStrategyChecked error = code %d short %q", err.Code, err.Short())
	}

	if stgy, err := NewChecked(badPol); stgy != nil || err == nil || err.Code != core.ErrBadConfig {
		t.Fatalf("NewChecked(badPol) = (%v, %v), want nil and BadConfig", stgy, err)
	}

	goodPol := &config.RunPolicyConfig{
		Name:   name,
		Params: map[string]float64{"fast": 10, "slow": 30},
	}
	if stgy, err := state.NewStrategyChecked(goodPol); err != nil || stgy == nil {
		t.Fatalf("NewStrategyChecked(goodPol) = (%v, %v), want valid strategy", stgy, err)
	}
}

func TestNewStrategyExplicitConstructorPanicBecomesBadConfig(t *testing.T) {
	const name = "ts08_explicit_panic_probe"
	RegisterStrategy(name, func(pol *config.RunPolicyConfig) *TradeStrat {
		fast := pol.DefInt("fast", 10, nil)
		slow := pol.DefInt("slow", 30, nil)
		if fast >= slow {
			panic("require fast < slow")
		}
		return &TradeStrat{RunTimeFrames: []string{"1h"}}
	})
	t.Cleanup(func() { UnregisterStrategy(name) })

	state := NewState()
	pol := &config.RunPolicyConfig{
		Name:   name,
		Params: map[string]float64{"fast": 20, "slow": 20},
	}
	stgy, err := state.NewStrategyChecked(pol)
	if stgy != nil || err == nil || err.Code != core.ErrBadConfig {
		t.Fatalf("NewStrategyChecked = (%v, %v), want BadConfig", stgy, err)
	}
	if err.Short() != "[BadConfig] require fast < slow" {
		t.Fatalf("Short() = %q, want %q", err.Short(), "[BadConfig] require fast < slow")
	}
}

func TestRuntimeErrorsInConstructorAreNeverClassifiedAsBadConfig(t *testing.T) {
	cases := []struct {
		name string
		fn   FuncMakeStrat
	}{
		{
			name: "nil_pointer_dereference",
			fn: func(*config.RunPolicyConfig) *TradeStrat {
				var ptr *int
				_ = *ptr
				return &TradeStrat{RunTimeFrames: []string{"1h"}}
			},
		},
		{
			name: "index_out_of_range",
			fn: func(*config.RunPolicyConfig) *TradeStrat {
				items := []int{1}
				_ = items[3]
				return &TradeStrat{RunTimeFrames: []string{"1h"}}
			},
		},
		{
			name: "divide_by_zero",
			fn: func(pol *config.RunPolicyConfig) *TradeStrat {
				zero := pol.DefInt("z", 0, nil)
				_ = 100 / zero
				return &TradeStrat{RunTimeFrames: []string{"1h"}}
			},
		},
		{
			name: "type_assertion",
			fn: func(*config.RunPolicyConfig) *TradeStrat {
				var v any = 42
				_ = v.(string)
				return &TradeStrat{RunTimeFrames: []string{"1h"}}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stratName := "ts08_rtbug_" + tc.name
			RegisterStrategy(stratName, tc.fn)
			t.Cleanup(func() { UnregisterStrategy(stratName) })

			state := NewState()
			pol := &config.RunPolicyConfig{Name: stratName}

			for _, label := range []string{"NewStrategyChecked", "NewChecked"} {
				var recovered any
				func() {
					defer func() { recovered = recover() }()
					if label == "NewStrategyChecked" {
						_, _ = state.NewStrategyChecked(pol)
					} else {
						_, _ = NewChecked(pol)
					}
				}()
				if recovered == nil {
					t.Fatalf("%s swallowed genuine runtime panic %s", label, tc.name)
				}
				if _, isBad := config.AsBadParamError(recovered); isBad {
					t.Fatalf("%s misclassified %s as BadParamError: %v", label, tc.name, recovered)
				}
				if _, isRuntime := recovered.(runtime.Error); !isRuntime {
					t.Fatalf("%s recovered %T (%v), want runtime.Error", label, recovered, recovered)
				}
			}
		})
	}
}

func TestLoadStratJobsAndCalcPairTfScoresPropagateBadConfigAndReleaseLocks(t *testing.T) {
	const name = "ts08_load_jobs_bad_config"
	RegisterStrategy(name, func(pol *config.RunPolicyConfig) *TradeStrat {
		fast := pol.DefInt("fast", 10, nil)
		slow := pol.DefInt("slow", 30, nil)
		pol.Requiref(fast < slow, "require fast < slow (got %d >= %d)", fast, slow)
		return &TradeStrat{RunTimeFrames: []string{"1h"}}
	})
	t.Cleanup(func() { UnregisterStrategy(name) })

	runtimeCore, coreErr := core.NewState(nil)
	if coreErr != nil {
		t.Fatal(coreErr)
	}
	defer runtimeCore.Close()
	runtimeCore.SetRunMode(core.RunModeBackTest)
	runtimeCore.ExgName, runtimeCore.Market = "binance", "linear"
	clock := btime.NewClockState(true, nil)
	clock.SetTimeMS(1_700_000_000_000)

	symbols := orm.NewSymbolStateWithIdentity("binance", "linear")
	if err := symbols.SetExSymbols([]*orm.ExSymbol{
		{ID: 1, Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT", ListMs: 1_600_000_000_000},
		{ID: 2, Exchange: "binance", Market: "linear", Symbol: "ETH/USDT:USDT", ListMs: 1_600_000_000_000},
	}); err != nil {
		t.Fatal(err)
	}

	t.Run("policy level Require failure", func(t *testing.T) {
		cfg := &config.Config{
			Exchange:      &config.ExchangeConfig{Name: "binance"},
			MarketType:    "linear",
			StakeCurrency: []string{"USDT"},
			RunTimeframes: []string{"1h"},
			Pairs:         []string{"BTC/USDT:USDT"},
			Accounts:      map[string]*config.AccountConfig{"default": {}},
			RunPolicy: []*config.RunPolicyConfig{{
				Name:   name,
				Params: map[string]float64{"fast": 40, "slow": 20},
			}},
		}
		strategyState := NewStateWithRuntime(runtimeCore, clock, cfg, symbols, nil)
		orders := ormo.NewOrderState()
		strategyState.BindRuntimeOrders(orders)

		if _, err := CalcPairTfScoresWithState(strategyState, symbols, nil, cfg.Pairs); err == nil ||
			err.Code != core.ErrBadConfig || !strings.Contains(err.Short(), "[BadConfig] require fast < slow") {
			t.Fatalf("CalcPairTfScoresWithState error = %v, want BadConfig", err)
		}

		tfScores := map[string]map[string]float64{"BTC/USDT:USDT": {"1h": 1}}
		_, _, err := LoadStratJobsWithState(strategyState, runtimeCore, symbols, cfg.Pairs, tfScores, orders)
		if err == nil || err.Code != core.ErrBadConfig || !strings.Contains(err.Short(), "[BadConfig] require fast < slow (got 40 >= 20)") {
			t.Fatalf("LoadStratJobsWithState error = %v, want BadConfig", err)
		}

		done := make(chan struct{})
		go func() {
			strategyState.Reset()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("LoadStratJobsWithState left jobs lock held after BadConfig")
		}
	})

	t.Run("pair_params override Require failure", func(t *testing.T) {
		cfg := &config.Config{
			Exchange:      &config.ExchangeConfig{Name: "binance"},
			MarketType:    "linear",
			StakeCurrency: []string{"USDT"},
			RunTimeframes: []string{"1h"},
			Pairs:         []string{"BTC/USDT:USDT", "ETH/USDT:USDT"},
			Accounts:      map[string]*config.AccountConfig{"default": {}},
			RunPolicy: []*config.RunPolicyConfig{{
				Name:   name,
				Params: map[string]float64{"fast": 10, "slow": 30},
				PairParams: map[string]map[string]float64{
					"ETH/USDT:USDT": {"fast": 50, "slow": 25},
				},
			}},
		}
		strategyState := NewStateWithRuntime(runtimeCore, clock, cfg, symbols, nil)
		orders := ormo.NewOrderState()
		strategyState.BindRuntimeOrders(orders)

		tfScores := map[string]map[string]float64{
			"BTC/USDT:USDT": {"1h": 1},
			"ETH/USDT:USDT": {"1h": 1},
		}
		_, _, err := LoadStratJobsWithState(strategyState, runtimeCore, symbols, cfg.Pairs, tfScores, orders)
		if err == nil || err.Code != core.ErrBadConfig || !strings.Contains(err.Short(), "[BadConfig] require fast < slow (got 50 >= 25)") {
			t.Fatalf("pair_params LoadStratJobsWithState error = %v, want BadConfig", err)
		}
	})
}
