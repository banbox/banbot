package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/banbox/banbot/factor"
	"strings"
	"testing"
)

func TestLifecycleReplayAllBuiltinModes(t *testing.T) {
	for _, transition := range []factor.TransitionConfig{
		{Mode: "direct"},
		{Mode: "linear-exit", ExitSteps: 8, Basis: "quantity"},
		{Mode: "linear-exit", ExitSteps: 8, Basis: "weight"},
		{Mode: "cohort", PeriodBars: 16, Startup: "gradual"},
		{Mode: "cohort", PeriodBars: 16, Startup: "seed-all", Sizing: "current-nav"},
		{Mode: "geometric", Basis: "quantity", Ratio: .5, FinalThreshold: .01},
		{Mode: "target-step", Alpha: .5},
	} {
		t.Run(transition.Mode+transition.Basis+transition.Startup, func(t *testing.T) {
			c := archiveConfig(t, false)
			c.Manifest.Portfolio.Policy = "lifecycle-v1"
			c.Manifest.Portfolio.LongNotional = 1
			c.Manifest.Portfolio.ShortNotional = 0
			c.Manifest.Portfolio.Rebalance = &factor.RebalanceConfig{EveryBars: 2}
			c.Manifest.Portfolio.Transition = &transition
			if transition.Mode != "cohort" {
				c.Manifest.Portfolio.Holding = &factor.HoldingConfig{MinBars: 2, MaxBars: 8}
			}
			var output bytes.Buffer
			result, err := Run(context.Background(), c, nil, &JSONOutput{Writer: &output, SIDs: c.Snapshot.Universe.Evaluation})
			if err != nil {
				t.Fatal(err)
			}
			if result.Decisions != 28 || result.TargetsAccepted == 0 || !strings.Contains(output.String(), "allocation-accepted") {
				t.Fatalf("policy not exercised: %+v", result)
			}
		})
	}
}

func TestLifecycleRefusesOutputLosingQuantityFacts(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Portfolio.Policy = "lifecycle-v1"
	c.Manifest.Portfolio.Transition = &factor.TransitionConfig{Mode: "cohort", PeriodBars: 2}
	_, err := Run(context.Background(), c, nil, &capture{targets: map[int64]map[int32]float64{}})
	if err == nil || !strings.Contains(err.Error(), "output does not support quantity") {
		t.Fatalf("quantity silently converted: %v", err)
	}
}

func TestLifecycleAssetOverridesUseDataSymbols(t *testing.T) {
	c := paperConfig(t, archiveConfig(t, false))
	c.Manifest.Portfolio.Policy = "lifecycle-v1"
	for sid, instrument := range c.Execution.Instruments {
		instrument.ID = "venue-" + instrument.ID
		c.Execution.Instruments[sid] = instrument
	}
	observed := false
	c.PolicyContext = func(_ context.Context, input *factor.PortfolioContext) error {
		observed = true
		for sid, symbol := range c.Snapshot.SIDMap {
			if input.AssetNames[sid] != symbol {
				t.Fatalf("SID %d uses execution ID %q instead of data symbol %q", sid, input.AssetNames[sid], symbol)
			}
		}
		return nil
	}
	if _, err := Run(context.Background(), c, nil, nil); err != nil {
		t.Fatal(err)
	}
	if !observed {
		t.Fatal("policy context not exercised")
	}
}

type independentPortfolioPolicy struct{}

func (independentPortfolioPolicy) Propose(input factor.PortfolioContext, _ json.RawMessage) (factor.PortfolioProposal, error) {
	target, err := factor.NewPortfolioTarget(input.Spec, map[int32]factor.Allocation{1: {Basis: factor.NAVFraction, Value: "0.5"}})
	return factor.PortfolioProposal{Target: target, NextState: json.RawMessage(`{}`)}, err
}

func TestCustomPolicyAndQuantilesDoNotRequireLegacyK(t *testing.T) {
	const custom = "independent-policy-v1"
	if err := RegisterPortfolioPolicy(custom, func(factor.PortfolioPolicyConfig) (factor.PortfolioPolicy, error) {
		return independentPortfolioPolicy{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		portfolioPolicies.Lock()
		delete(portfolioPolicies.factories, custom)
		portfolioPolicies.Unlock()
	})
	for _, name := range []string{"lifecycle-v1", custom} {
		t.Run(name, func(t *testing.T) {
			c := archiveConfig(t, false)
			c.Manifest.Portfolio.Policy = name
			c.Manifest.Portfolio.K = 0
			if name == "lifecycle-v1" {
				c.Manifest.Portfolio.Selection = &factor.SelectionConfig{LongQuantile: .2, ShortQuantile: .2}
			} else {
				c.Manifest.Portfolio.LongNotional, c.Manifest.Portfolio.ShortNotional = 0, 0
			}
			result, err := Run(context.Background(), c, nil, nil)
			if err != nil || result.TargetsAccepted == 0 {
				t.Fatalf("policy forced through legacy builder validation: %+v %v", result, err)
			}
		})
	}
}
