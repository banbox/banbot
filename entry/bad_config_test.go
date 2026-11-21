package entry

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
	"github.com/spf13/cobra"
)

func TestEntryPropagatesStrategyBadConfigAndPreservesRuntimePanics(t *testing.T) {
	const (
		requireStrat = "ts08_entry_require"
		runtimeStrat = "ts08_entry_nil_deref"
	)
	strat.RegisterStrategy(requireStrat, func(pol *config.RunPolicyConfig) *strat.TradeStrat {
		fast := pol.DefInt("fast", 10, nil)
		slow := pol.DefInt("slow", 30, nil)
		pol.Require(fast < slow, "fast must be less than slow")
		return &strat.TradeStrat{RunTimeFrames: []string{"1h"}}
	})
	strat.RegisterStrategy(runtimeStrat, func(*config.RunPolicyConfig) *strat.TradeStrat {
		var ptr *int
		_ = *ptr
		return &strat.TradeStrat{RunTimeFrames: []string{"1h"}}
	})
	t.Cleanup(func() {
		strat.UnregisterStrategy(requireStrat)
		strat.UnregisterStrategy(runtimeStrat)
	})

	commandRegistryMu.RLock()
	before := len(extraCommands)
	commandRegistryMu.RUnlock()
	t.Cleanup(func() {
		commandRegistryMu.Lock()
		extraCommands = extraCommands[:before]
		commandRegistryMu.Unlock()
	})

	runJobLoad := func(pol *config.RunPolicyConfig, directNew bool) error {
		runtimeCore, err := core.NewState(nil)
		if err != nil {
			return err
		}
		defer runtimeCore.Close()
		runtimeCore.SetRunMode(core.RunModeBackTest)
		runtimeCore.ExgName, runtimeCore.Market = "binance", "linear"
		clock := btime.NewClockState(true, nil)
		clock.SetTimeMS(1_700_000_000_000)
		symbols := orm.NewSymbolStateWithIdentity("binance", "linear")
		if setErr := symbols.SetExSymbols([]*orm.ExSymbol{
			{ID: 1, Exchange: "binance", Market: "linear", Symbol: "BTC/USDT:USDT", ListMs: 1_600_000_000_000},
		}); setErr != nil {
			return setErr
		}
		cfg := &config.Config{
			Exchange:      &config.ExchangeConfig{Name: "binance"},
			MarketType:    "linear",
			StakeCurrency: []string{"USDT"},
			RunTimeframes: []string{"1h"},
			Pairs:         []string{"BTC/USDT:USDT"},
			Accounts:      map[string]*config.AccountConfig{"default": {}},
			RunPolicy:     []*config.RunPolicyConfig{pol},
		}
		state := strat.NewStateWithRuntime(runtimeCore, clock, cfg, symbols, nil)
		if directNew {
			_ = state.NewStrategy(pol)
			return nil
		}
		orders := ormo.NewOrderState()
		state.BindRuntimeOrders(orders)
		tfScores := map[string]map[string]float64{"BTC/USDT:USDT": {"1h": 1}}
		_, _, loadErr := strat.LoadStratJobsWithState(state, runtimeCore, symbols, cfg.Pairs, tfScores, orders)
		if loadErr != nil {
			return loadErr
		}
		return nil
	}

	AddCommandFactory("", func() *cobra.Command {
		var stratName string
		var direct bool
		cmd := &cobra.Command{
			Use: "ts08-probe",
			RunE: func(_ *cobra.Command, _ []string) error {
				return runJobLoad(&config.RunPolicyConfig{
					Name:   stratName,
					Params: map[string]float64{"fast": 30, "slow": 10},
				}, direct)
			},
		}
		cmd.Flags().StringVar(&stratName, "strat", "", "")
		cmd.Flags().BoolVar(&direct, "direct", false, "")
		return cmd
	})

	if mode := os.Getenv("BANBOT_TS08_SUBPROC_MODE"); mode != "" {
		oldArgs := os.Args
		defer func() { os.Args = oldArgs }()
		switch mode {
		case "require_load":
			os.Args = []string{"banbot", "ts08-probe", "--strat", requireStrat, "--direct=false"}
		case "require_direct":
			os.Args = []string{"banbot", "ts08-probe", "--strat", requireStrat, "--direct=true"}
		case "runtime_bug":
			os.Args = []string{"banbot", "ts08-probe", "--strat", runtimeStrat, "--direct=false"}
		}
		RunCmd()
		return
	}

	for _, direct := range []bool{false, true} {
		err := ExecuteContext(context.Background(), []string{"ts08-probe", "--strat", requireStrat, "--direct=" + boolText(direct)})
		if err == nil {
			t.Fatalf("ExecuteContext(direct=%v) succeeded on violated Require", direct)
		}
		var banErr *errs.Error
		if !errors.As(err, &banErr) || banErr.Code != core.ErrBadConfig {
			t.Fatalf("ExecuteContext(direct=%v) err = %T (%v), want *errs.Error with ErrBadConfig", direct, err, err)
		}
		var stderr bytes.Buffer
		writeCommandError(&stderr, err)
		if !strings.HasPrefix(stderr.String(), "Error: [BadConfig] fast must be less than slow\n") {
			t.Fatalf("stderr = %q, want Error: [BadConfig] fast must be less than slow", stderr.String())
		}
	}

	for _, direct := range []bool{false, true} {
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			_ = ExecuteContext(context.Background(), []string{"ts08-probe", "--strat", runtimeStrat, "--direct=" + boolText(direct)})
		}()
		if recovered == nil {
			t.Fatalf("ExecuteContext(direct=%v) swallowed runtime nil-pointer panic", direct)
		}
		if _, isBad := config.AsBadParamError(recovered); isBad {
			t.Fatalf("ExecuteContext(direct=%v) misclassified runtime panic as BadConfig: %v", direct, recovered)
		}
		if _, isRuntime := recovered.(runtime.Error); !isRuntime {
			t.Fatalf("ExecuteContext(direct=%v) recovered %T (%v), want runtime.Error", direct, recovered, recovered)
		}
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"require_load", "require_direct"} {
		cmd := exec.Command(exe, "-test.run=^TestEntryPropagatesStrategyBadConfigAndPreservesRuntimePanics$")
		cmd.Env = append(os.Environ(), "BANBOT_TS08_SUBPROC_MODE="+mode)
		out, runErr := cmd.CombinedOutput()
		exitErr, ok := runErr.(*exec.ExitError)
		if !ok || exitErr.ExitCode() != 1 {
			t.Fatalf("RunCmd subprocess(%s) err=%v out=%q, want exit code 1", mode, runErr, string(out))
		}
		text := string(out)
		if !strings.Contains(text, "Error: [BadConfig] fast must be less than slow") || strings.Contains(text, "banbot panic raw stack:") {
			t.Fatalf("RunCmd subprocess(%s) output = %q, want Error: [BadConfig] without raw panic header", mode, text)
		}
	}

	bugCmd := exec.Command(exe, "-test.run=^TestEntryPropagatesStrategyBadConfigAndPreservesRuntimePanics$")
	bugCmd.Env = append(os.Environ(), "BANBOT_TS08_SUBPROC_MODE=runtime_bug")
	bugOut, bugErr := bugCmd.CombinedOutput()
	if exitErr, ok := bugErr.(*exec.ExitError); !ok || exitErr.ExitCode() != 1 {
		t.Fatalf("RunCmd subprocess(runtime_bug) err=%v out=%q, want exit code 1", bugErr, string(bugOut))
	}
	bugText := string(bugOut)
	if strings.Contains(bugText, "Error: [BadConfig]") || !strings.Contains(bugText, "banbot panic raw stack:") {
		t.Fatalf("RunCmd subprocess(runtime_bug) output = %q, want raw panic stack and not BadConfig", bugText)
	}
}

func boolText(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
