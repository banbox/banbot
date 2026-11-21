package expr

import (
	"math"
	"testing"

	"github.com/banbox/banbot/factor"
)

func TestFA05CsMeanAndCsSumExpressions(t *testing.T) {
	c := factor.Field("prices", "close", "1h")
	breadthFormula := "cs.mean(kline.close > ts.sma(kline.close, 3))"
	sumFormula := "cs.sum(kline.close > ts.sma(kline.close, 3))"
	indexRetFormula := "cs.mean(ts.return(kline.close, 1))"

	spec := Spec{
		SchemaVersion: 1,
		TimeFrame:     "1h",
		Bindings:      map[string]Binding{"kline": {Source: "prices", TimeFrame: "1h"}},
		Outputs: map[string]string{
			"breadth":   breadthFormula,
			"above_sum": sumFormula,
			"mkt_ret":   indexRetFormula,
		},
	}
	dslPlan := mustCompile(t, spec)
	goPlan, err := factor.Compile(map[string]*factor.Node{
		"breadth":   factor.CsMean(factor.Gt(c, factor.SMA(c, 3))),
		"above_sum": factor.CsSum(factor.Gt(c, factor.SMA(c, 3))),
		"mkt_ret":   factor.CsMean(factor.Return(c, 1)),
	})
	if err != nil || dslPlan.Hash() != goPlan.Hash() {
		t.Fatalf("DSL vs Go plan hash mismatch: err=%v %s vs %s", err, dslPlan.Hash(), goPlan.Hash())
	}

	var snaps []*factor.Snapshot
	series := [][4]float64{
		{100, 100, 100, 100},
		{110, 90, 105, 95},
		{120, 80, 110, 90}, // sma3 = [110, 90, 105, 95], close > sma3 = [1, 0, 1, 0] -> breadth=0.5, sum=2
		{130, 100, 120, 100}, // sma3 = [120, 90, 111.67, 95], close > sma3 = [1, 1, 1, 1] -> breadth=1.0, sum=4
	}
	for i, row := range series {
		snaps = append(snaps, snapshot(t, int64(i+1)*1000, map[int32]map[string]any{
			1: {"close": row[0]},
			2: {"close": row[1]},
			3: {"close": row[2]},
			4: {"close": row[3]},
		}))
	}
	batch, err := dslPlan.Batch(snaps, len(snaps))
	if err != nil {
		t.Fatal(err)
	}
	sess, _ := factor.NewSession(dslPlan)
	for i, s := range snaps {
		f, err := sess.Evaluate(s)
		if err != nil {
			t.Fatal(err)
		}
		for _, col := range []string{"breadth", "above_sum", "mkt_ret"} {
			for sid := int32(1); sid <= 4; sid++ {
				got, want := f.Values[col][sid], batch[i].Values[col][sid]
				if got.Validity != want.Validity || (got.Validity == factor.Valid && math.Abs(got.Value-want.Value) > 1e-12) {
					t.Fatalf("t=%d %s sid=%d got %+v want %+v", i, col, sid, got, want)
				}
			}
		}
	}
	if got := batch[2].Values["breadth"][1]; got.Validity != factor.Valid || math.Abs(got.Value-0.5) > 1e-12 {
		t.Fatalf("bar2 breadth want 0.5, got %+v", got)
	}
	if got := batch[2].Values["above_sum"][1]; got.Validity != factor.Valid || math.Abs(got.Value-2) > 1e-12 {
		t.Fatalf("bar2 above_sum want 2, got %+v", got)
	}
	if got := batch[3].Values["breadth"][1]; got.Validity != factor.Valid || math.Abs(got.Value-1.0) > 1e-12 {
		t.Fatalf("bar3 breadth want 1.0, got %+v", got)
	}

	for _, bad := range []string{
		"cs.mean()",
		"cs.mean(kline.close, 2)",
		"cs.sum()",
		"cs.sum(kline.close, 2)",
		"ts.sma(cs.mean(kline.close), 3)",
		"ts.ema(cs.sum(kline.close), 3)",
	} {
		if _, err := Compile(specFor(bad)); err == nil {
			t.Fatalf("accepted invalid expression %q", bad)
		}
	}
}
