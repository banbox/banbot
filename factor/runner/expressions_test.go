package runner

import (
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/expr"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/orm"
)

func expressionConfig() *expr.Spec {
	return &expr.Spec{SchemaVersion: 1, Frequency: "1h", Bindings: map[string]expr.Binding{"kline": {Source: "kline", Frequency: "1h"}}, Params: map[string]float64{"window": 3}, Lets: map[string]string{"mom": "ts.return(kline.close, param.window)"}, Outputs: map[string]string{"momentum": "cs.zscore(factor.mom)"}, Combine: research.ComboSpec{Method: research.Fixed, Weights: map[string]float64{"momentum": 1}}}
}

func TestExpressionDefinitionAndComboValidation(t *testing.T) {
	c := Config{Expressions: expressionConfig(), DecisionInterval: 3600000}
	plan, combo, err := CompileDefinition(c)
	if err != nil || plan == nil || !reflect.DeepEqual(combo.Columns, []string{"momentum"}) {
		t.Fatalf("expression definition: %v, %+v", err, combo)
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.Definition = "momentum-vol" },
		func(c *Config) { c.Plan = plan },
		func(c *Config) { c.DecisionInterval = 86400000 },
		func(c *Config) { c.Combo = research.ComboSpec{Method: research.Equal, Columns: []string{"absent"}} },
		func(c *Config) {
			c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"momentum"}, Weights: map[string]float64{"momentum": math.NaN()}}
		},
		func(c *Config) { c.Expressions.Combine.Weights["typo"] = 1 },
	} {
		bad := Config{Expressions: expressionConfig(), DecisionInterval: 3600000}
		change(&bad)
		if _, _, err := CompileDefinition(bad); err == nil {
			t.Fatal("invalid expression config accepted")
		}
	}
}

func TestExpressionReplayMatchesGoDefinition(t *testing.T) {
	c := archiveConfig(t, false)
	c.Expressions = expressionConfig()
	dsl := &capture{targets: map[int64]map[int32]float64{}}
	r, err := Run(context.Background(), c, nil, dsl)
	if err != nil {
		t.Fatal(err)
	}
	c.Expressions = nil
	c.Plan, err = factor.New().Add("momentum", factor.ZScore(factor.Return(factor.Field("kline", "close", "1h"), 3))).Compile()
	if err != nil {
		t.Fatal(err)
	}
	c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"momentum"}, Weights: map[string]float64{"momentum": 1}}
	goOutput := &capture{targets: map[int64]map[int32]float64{}}
	other, err := Run(context.Background(), c, nil, goOutput)
	if err != nil {
		t.Fatal(err)
	}
	if r.StrategyHash != other.StrategyHash || !reflect.DeepEqual(r.Book, other.Book) || !reflect.DeepEqual(dsl.targets, goOutput.targets) {
		t.Fatal("expression replay differs from Go builder")
	}
	if r.Decisions != 28 || r.Executions == 0 || len(dsl.reports) == 0 {
		t.Fatal("expression research/trading pipeline not exercised")
	}
}

func TestExpressionLiveDecisionMatchesGo(t *testing.T) {
	c := archiveConfig(t, false)
	c.Manifest.Costs.FundingPolicy = "explicit-zero"
	c.Manifest.Portfolio.K = 1
	sids := []int32{1, 2, 3, 4}
	c.Snapshot.Universe = factor.Universe{Version: "live-expressions", Static: true, Investable: sids, Tradable: sids, Reference: sids, Evaluation: sids}
	c.Prices = PriceStream{Source: "kline", Frequency: "1h", Field: "close"}
	var baseline map[int64]map[int32]float64
	for _, expressions := range []bool{false, true} {
		if expressions {
			c.Plan = nil
			c.Combo = research.ComboSpec{}
			c.Expressions = expressionConfig()
			c.Expressions.Outputs["momentum"] = "cs.zscore(kline.close)"
		} else {
			var err error
			c.Plan, err = factor.New().Add("momentum", factor.ZScore(factor.Field("kline", "close", "1h"))).Compile()
			if err != nil {
				t.Fatal(err)
			}
			c.Combo = research.ComboSpec{Method: research.Fixed, Columns: []string{"momentum"}, Weights: map[string]float64{"momentum": 1}}
		}
		out := &capture{targets: map[int64]map[int32]float64{}}
		live, err := NewLive(c, &scopedQuoteSink{}, func() int64 { return 3600002 }, out)
		if err != nil {
			t.Fatal(err)
		}
		for _, sid := range sids {
			row := factor.Record(orm.DataSeries{Source: "kline", TimeFrame: "1h", Sid: sid, Closed: true, EndMS: 3600000, Values: map[string]any{"close": 100 + float64(sid)}}, 1, 3600001, 3600002, "v1")
			if err := live.Observe(context.Background(), row); err != nil {
				t.Fatal(err)
			}
		}
		if err := live.Flush(context.Background(), 3600000); err != nil {
			t.Fatal(err)
		}
		live.Stop()
		if len(out.targets) != 1 {
			t.Fatal("live expression decision absent")
		}
		if !expressions {
			baseline = out.targets
		} else if !reflect.DeepEqual(baseline, out.targets) {
			t.Fatal("live expression targets differ from Go targets")
		}
	}
}
