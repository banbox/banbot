package expr

import (
	"math"
	"testing"

	"github.com/banbox/banbot/factor"
)

func TestFA01ExpressionGoEquivalenceAndExecution(t *testing.T) {
	c := factor.Field("prices", "close", "1h")
	o := factor.Field("prices", "open", "1h")
	v := factor.Field("prices", "volume", "1h")
	zero := factor.Constant(0, "1h")

	cases := []struct {
		name    string
		formula string
		node    *factor.Node
	}{
		{"ts_corr", "ts.corr(kline.close, kline.volume, 4)", factor.TsCorr(c, v, 4)},
		{"ts_cov", "ts.cov(kline.close, kline.volume, 4)", factor.TsCov(c, v, 4)},
		{"ts_beta", "ts.beta(kline.close, kline.open, 4)", factor.TsBeta(c, o, 4)},
		{"ts_sum", "ts.sum(kline.close, 4)", factor.TsSum(c, 4)},
		{"ts_product", "ts.product(kline.close, 3)", factor.TsProduct(c, 3)},
		{"ts_rank", "ts.rank(kline.close, 4)", factor.TsRank(c, 4)},
		{"ts_zscore", "ts.zscore(kline.close, 4)", factor.TsZScore(c, 4)},
		{"ts_argmax", "ts.argmax(kline.close, 4)", factor.TsArgMax(c, 4)},
		{"ts_argmin", "ts.argmin(kline.close, 4)", factor.TsArgMin(c, 4)},
		{"ts_skew", "ts.skew(kline.close, 3)", factor.TsSkew(c, 3)},
		{"ts_kurt", "ts.kurt(kline.close, 4)", factor.TsKurt(c, 4)},
		{"ts_slope", "ts.slope(kline.close, 4)", factor.TsSlope(c, 4)},
		{"ts_rsquare", "ts.rsquare(kline.close, 4)", factor.TsRSquare(c, 4)},
		{"ts_decay_linear", "ts.decay_linear(kline.close, 4)", factor.DecayLinear(c, 4)},
		{"sign", "sign(kline.close - kline.open)", factor.Sign(factor.Sub(c, o))},
		{"clip", "clip(kline.close, -1.5, 10.5)", factor.Clip(c, -1.5, 10.5)},
		{"gt_infix", "kline.close > kline.open", factor.Gt(c, o)},
		{"gt_fn", "gt(kline.close, kline.open)", factor.Gt(c, o)},
		{"ge_infix", "kline.close >= kline.open", factor.Ge(c, o)},
		{"ge_fn", "ge(kline.close, kline.open)", factor.Ge(c, o)},
		{"lt_infix", "kline.close < kline.open", factor.Lt(c, o)},
		{"lt_fn", "lt(kline.close, kline.open)", factor.Lt(c, o)},
		{"le_infix", "kline.close <= kline.open", factor.Le(c, o)},
		{"le_fn", "le(kline.close, kline.open)", factor.Le(c, o)},
		{"precedence", "kline.close + 1 > kline.open * 2", factor.Gt(factor.Add(c, factor.Constant(1, "1h")), factor.Mul(o, factor.Constant(2, "1h")))},
		{"where_short_circuit", "where(kline.close > 0, log(kline.close), 0)", factor.Where(factor.Gt(c, zero), factor.Log(c), zero)},
		{"if_else_alias", "if_else(kline.close > 0, log(kline.close), 0)", factor.Where(factor.Gt(c, zero), factor.Log(c), zero)},
		{"cs_pct_rank", "cs.pct_rank(kline.close)", factor.PctRank(c)},
	}

	spec := Spec{
		SchemaVersion: 1,
		TimeFrame:     "1h",
		Bindings:      map[string]Binding{"kline": {Source: "prices", TimeFrame: "1h"}},
		Outputs:       map[string]string{},
	}
	goOutputs := map[string]*factor.Node{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsl := mustCompile(t, specFor(tc.formula))
			goPlan, err := factor.Compile(map[string]*factor.Node{"score": tc.node})
			if err != nil {
				t.Fatal(err)
			}
			if dsl.Hash() != goPlan.Hash() || dsl.WarmupLength() != goPlan.WarmupLength() || dsl.StateRetention() != goPlan.StateRetention() {
				t.Fatalf("%s hash/lookback mismatch: dsl=%s(%d/%d) go=%s(%d/%d)", tc.name, dsl.Hash(), dsl.WarmupLength(), dsl.StateRetention(), goPlan.Hash(), goPlan.WarmupLength(), goPlan.StateRetention())
			}
		})
		spec.Outputs[tc.name] = tc.formula
		goOutputs[tc.name] = tc.node
	}

	allPlan := mustCompile(t, spec)
	allGoPlan, err := factor.Compile(goOutputs)
	if err != nil || allPlan.Hash() != allGoPlan.Hash() {
		t.Fatalf("combined plan mismatch: err=%v %s vs %s", err, allPlan.Hash(), allGoPlan.Hash())
	}

	// Execute across 12 bars with negative/zero/missing values to verify Session == Batch and short-circuiting.
	var snaps []*factor.Snapshot
	for i := 0; i < 12; i++ {
		rows := map[int32]map[string]any{
			1: {"close": float64(i - 3), "open": float64(i - 4), "volume": float64(10 + i)},
			2: {"close": float64(2*i + 1), "open": float64(2*i + 2), "volume": float64(20 - i)},
			3: {"close": float64(5 + (i % 3)), "open": 5.0, "volume": float64(5 + 2*(i%4))},
		}
		if i == 5 {
			delete(rows[1], "close") // Missing gap
		}
		snaps = append(snaps, snapshot(t, int64(i+1)*1000, rows))
	}

	batch, err := allPlan.Batch(snaps, len(snaps))
	if err != nil {
		t.Fatal(err)
	}
	sess, err := factor.NewSession(allPlan)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range snaps {
		frame, err := sess.Evaluate(s)
		if err != nil {
			t.Fatal(err)
		}
		for name, col := range frame.Values {
			for sid, got := range col {
				want := batch[i].Values[name][sid]
				if got.Validity != want.Validity || (got.Validity == factor.Valid && math.Abs(got.Value-want.Value) > 1e-10) {
					t.Fatalf("%s t=%d sid=%d got %+v want %+v", name, i, sid, got, want)
				}
			}
		}
	}
	// At i=0..3, SID 1 has close = -3, -2, -1, 0 (<= 0): where(close > 0, log(close), 0) must be Valid 0!
	for i := 0; i <= 3; i++ {
		got := batch[i].Values["where_short_circuit"][1]
		if got.Validity != factor.Valid || got.Value != 0 {
			t.Fatalf("t=%d close=%d where_short_circuit got %+v, want {0, Valid}", i, i-3, got)
		}
	}
	// At i=4, SID 1 has close = 1 (> 0): log(1) == 0 Valid; at i=6, close = 3: log(3) Valid
	if got := batch[6].Values["where_short_circuit"][1]; got.Validity != factor.Valid || math.Abs(got.Value-math.Log(3)) > 1e-12 {
		t.Fatalf("t=6 close=3 where_short_circuit got %+v", got)
	}
	// At i=5, SID 1 has Missing close: where_short_circuit must be Missing
	if got := batch[5].Values["where_short_circuit"][1]; got.Validity != factor.Missing {
		t.Fatalf("t=5 missing close got %+v, want Missing", got)
	}
}

func TestFA01ExpressionRejectsInvalidArgumentsAndDomains(t *testing.T) {
	badFormulas := []string{
		"ts.skew(kline.close, 2)",
		"ts.skew(kline.close, 0)",
		"ts.kurt(kline.close, 3)",
		"ts.kurt(kline.close, 1.5)",
		"ts.corr(kline.close, kline.volume, 0)",
		"ts.corr(kline.close, kline.volume, 10001)",
		"ts.beta(kline.close, 5)",
		"ts.cov(kline.close, kline.open, kline.volume, 5)",
		"ts.decay_linear(kline.close, 0)",
		"clip(kline.close, 5, 2)",
		"clip(kline.close, kline.open, 2)",
		"clip(kline.close, 0)",
		"where(kline.close > 0, 1)",
		"if_else(kline.close > 0, 1, 2, 3)",
		"sign(kline.close, 1)",
		"cs.pct_rank(kline.close, 1)",
		"kline.close > = 0",
		"kline.close < = 0",
		"kline.close = 0",
	}
	for _, src := range badFormulas {
		t.Run(src, func(t *testing.T) {
			if _, err := Compile(specFor(src)); err == nil {
				t.Fatalf("accepted invalid expression %q", src)
			}
		})
	}
}
