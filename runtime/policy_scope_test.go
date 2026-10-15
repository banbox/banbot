package runtime

import (
	"reflect"
	"slices"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
)

func TestPolicyReplacementRetainsExecutionTailOutsideRankingPool(t *testing.T) {
	for _, kind := range []string{"position", "pending-entry"} {
		t.Run(kind, func(t *testing.T) {
			f := newSharedTriggerFixture(t)
			registerReplacementSources(t, f, &replacementSource{name: "signal"}, &replacementSource{name: "price"}, &replacementSource{name: "funding"})
			oldCfg := replacementConfig(t, f, "signal", []int32{1, 2}, []int32{1, 2})
			oldCfg.Manifest.Portfolio.Policy = "lifecycle-v1"
			oldCfg.Manifest.Portfolio.Transition = &factor.TransitionConfig{Mode: "linear-exit", ExitSteps: 8, Basis: "quantity"}
			oldCfg.Manifest.Costs.FundingPolicy = "required-stream"
			oldSink := &runner.AccountSink{Account: f.rt.SharedExecution(), AccountID: oldCfg.AccountID, StrategyID: oldCfg.StrategyID, Currency: oldCfg.Manifest.Currency, Instruments: oldCfg.Execution.Instruments}
			old, err := runner.NewLive(oldCfg, oldSink, f.rt.Clock.TimeMS, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer old.Stop()
			nextCfg, err := runner.CloneConfig(oldCfg)
			if err != nil {
				t.Fatal(err)
			}
			nextCfg.Snapshot.Universe = factor.Universe{Version: "next", Static: true, Investable: []int32{2}, Reference: []int32{2}, Tradable: []int32{2}, Evaluation: []int32{2}, Tracked: []int32{2}}
			delete(nextCfg.Snapshot.SIDMap, 1)
			delete(nextCfg.Execution.Instruments, 1)
			nextSink := *oldSink
			nextSink.Instruments = nextCfg.Execution.Instruments
			next, err := runner.NewLive(nextCfg, &nextSink, f.rt.Clock.TimeMS, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer next.Stop()
			instrument := oldCfg.Execution.Instruments[1]
			snapshot := execution.AccountSnapshot{}
			if kind == "position" {
				snapshot.Lots = []execution.VirtualLot{{Strategy: execution.StrategyID(oldCfg.StrategyID), ID: "factor:1", Instrument: instrument, SignedSteps: 4}}
			} else {
				snapshot.Orders = []execution.StoredOrder{{State: execution.OrderAcknowledged, Intent: execution.OrderIntent{Instrument: instrument, Allocations: []execution.FillAllocation{{ID: "entry", Strategy: execution.StrategyID(oldCfg.StrategyID), Lot: "factor:1", Kind: execution.EntryIntent, Side: execution.Buy, Steps: 4}}}}}
			}
			retained, err := next.RetainPolicyScope(old, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(next.ExecutionSIDs(), 1) || !slices.Contains(next.FundingSIDs(), 1) || !reflect.DeepEqual(next.DataSIDs(), []int32{2}) || !reflect.DeepEqual(retained.Snapshot.Universe.Investable, []int32{2}) {
				t.Fatal("tail was dropped or leaked into ranking", next.ExecutionSIDs(), next.DataSIDs(), retained.Snapshot.Universe)
			}
			if nextCfg.Snapshot.SIDMap[1] != "" || slices.Contains(nextCfg.Snapshot.Universe.Tracked, 1) {
				t.Fatal("retention mutated caller candidate config")
			}
			plan, err := f.rt.CompileFactorsLivePlan([]*runner.Live{next}, []runner.Config{retained})
			if err != nil {
				t.Fatal(err)
			}
			priceTail, fundingTail, inferenceTail := false, false, false
			for _, stream := range plan.Streams() {
				if stream.Subscription.ExSymbol.ID == 1 {
					switch stream.Subscription.Source {
					case "price":
						priceTail = true
					case "funding":
						fundingTail = true
					case "signal":
						inferenceTail = true
					}
				}
			}
			if !priceTail || !fundingTail || inferenceTail {
				t.Fatal("subscription scopes not separated", priceTail, fundingTail, inferenceTail)
			}
			generation := &factorLiveGeneration{owner: &FactorLiveSubscription{runtime: f.rt}, sink: &factorsLiveSourceSink{consumers: []*factorLiveSourceSink{{engine: next, cfg: retained}}}}
			if err := protectAccountSubscriptions(snapshot, generation); err != nil {
				t.Fatal("retained plan failed final owner protection", err)
			}
			// Once holdings and inflight settle, a fresh replacement may release SID1.
			settledSink := nextSink
			settled, err := runner.NewLive(nextCfg, &settledSink, f.rt.Clock.TimeMS, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer settled.Stop()
			if _, err := settled.RetainPolicyScope(next, execution.AccountSnapshot{}); err != nil {
				t.Fatal(err)
			}
			if slices.Contains(settled.ExecutionSIDs(), 1) {
				t.Fatal("settled tail retained forever")
			}
		})
	}
}
