package entry

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm"
)

type subscriptionPlanSink struct{}

func (subscriptionPlanSink) StrategyNAV(context.Context, int64) (float64, error) { return 10000, nil }
func (subscriptionPlanSink) ProcessSnapshot(context.Context, *factor.TargetPortfolio, map[int32]backtest.Quote, int64) error {
	return nil
}

func TestFactorLiveKlinePlanRetainsFieldsTimeFramesAndAllPools(t *testing.T) {
	raw, err := os.ReadFile("../factor/runner/example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c runner.Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Snapshot.Universe = factor.Universe{Version: "scoped", Static: true, Tracked: []int32{1}, Investable: []int32{2}, Tradable: []int32{2}, Reference: []int32{3}, Evaluation: []int32{4}}
	c.Snapshot.SIDMap[4] = "asset-4"
	plan, err := factor.New().Add("integer", factor.Field("kline", "integer", "1h")).Add("custom", factor.Field("kline", "custom", "1h")).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Plan = plan
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"integer"}, Weights: map[string]float64{"integer": 1}}
	symbols := make(map[int32]*orm.ExSymbol)
	for sid, symbol := range c.Snapshot.SIDMap {
		symbols[sid] = &orm.ExSymbol{ID: sid, Symbol: symbol}
	}
	for _, priceTF := range []string{"1m", "1h"} {
		t.Run(priceTF, func(t *testing.T) {
			c.Prices = runner.PriceStream{Source: "kline", TimeFrame: priceTF, Field: "mark"}
			engine, err := runner.NewLive(c, subscriptionPlanSink{}, func() int64 { return 3600002 }, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				engine.Stop()
				if err := engine.Join(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			if !slices.Equal(engine.DataSIDs(), []int32{2, 3}) || !slices.Equal(engine.ExecutionSIDs(), []int32{1, 2}) {
				t.Fatal("inference and execution pool scopes changed", engine.DataSIDs(), engine.ExecutionSIDs())
			}
			subs, err := factorLiveKlineSubscriptions(engine, c, symbols)
			if err != nil {
				t.Fatal(err)
			}
			subs, err = data.NewDataSourceCatalog().NormalizeSubscriptions(subs)
			if err != nil {
				t.Fatal(err)
			}
			seen := make(map[int32]map[string][]string)
			for _, sub := range subs {
				if seen[sub.ExSymbol.ID] == nil {
					seen[sub.ExSymbol.ID] = make(map[string][]string)
				}
				seen[sub.ExSymbol.ID][sub.TimeFrame] = sub.Fields
			}
			if seen[4] != nil {
				t.Fatal("evaluation-only member acquired a required live subscription", seen[4])
			}
			if len(seen) != 3 || seen[1] == nil || seen[2] == nil || seen[3] == nil {
				t.Fatalf("data pools: %v", seen)
			}
			for sid, tfs := range seen {
				if slices.Contains(tfs["1h"], "integer") != (sid != 1) || slices.Contains(tfs["1h"], "custom") != (sid != 1) {
					t.Fatalf("SID %d DAG projection: %v", sid, tfs)
				}
				if slices.Contains(tfs[priceTF], "mark") != (sid == 1 || sid == 2) {
					t.Fatalf("SID %d execution projection: %v", sid, tfs)
				}
				if priceTF == "1m" && sid == 3 && len(tfs) != 1 {
					t.Fatalf("reference acquired execution timeframe: %v", tfs)
				}
			}
			// Excluding an evaluation-only live input must not erase its research
			// universe, stable SID mapping or typed snapshot observations.
			spec := factor.CloneSnapshotSpec(c.Snapshot)
			spec.GridTime, spec.DecisionTime = 3600000, 3600002
			record := factor.VersionRecord{Series: orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: 4, EndMS: 3600000, Closed: true, Values: map[string]any{"integer": int64(9007199254740993), "nullable": nil}}, EventTime: 3600000, AvailableAt: 3600001, IngestedAt: 3600001, Revision: 1, SourceVersion: "v1"}
			researchSnapshot, err := factor.Freeze(spec, []factor.VersionRecord{record}, []factor.Requirement{{SID: 4, Source: "kline", TimeFrame: "1h", EventTime: 3600000}})
			if err != nil || !researchSnapshot.Status().Ready || !slices.Equal(researchSnapshot.Spec().Universe.Evaluation, []int32{4}) || researchSnapshot.Spec().SIDMap[4] != "asset-4" {
				t.Fatal("live planning lost evaluation research metadata", err)
			}
			observed, ok := researchSnapshot.Row(4, "kline", "1h")
			if !ok || observed.Series.Values["integer"] != int64(9007199254740993) {
				t.Fatal("evaluation research snapshot lost concrete values")
			}
			if nullable, exists := observed.Series.Values["nullable"]; !exists || nullable != nil {
				t.Fatal("evaluation research snapshot lost explicit NULL")
			}
		})
	}
}
