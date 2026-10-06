package expr

import (
	"math"
	"testing"

	"github.com/banbox/banbot/factor"
)

func TestRobustAndGroupExpressionFunctions(t *testing.T) {
	rows := map[int32]map[string]any{1: {"close": 1.0, "x": 0.0, "z": 0.0, "weight": 1.0, "sector": "a"}, 2: {"close": 2.0, "x": 1.0, "z": 0.0, "weight": 2.0, "sector": "a"}, 3: {"close": 3.0, "x": 2.0, "z": 0.0, "weight": 3.0, "sector": "b"}, 4: {"close": 100.0, "x": 0.0, "z": 1.0, "weight": 4.0, "sector": "b"}}
	for formula, want := range map[string]float64{`cs.robust_zscore(kline.close)`: -1.5 / 1.4826, `cs.mad_winsorize(kline.close,2)`: 1, `group.ols(kline.close,kline.x,kline.z)`: 0, `group.wls(kline.close,kline.weight,kline.x,kline.z)`: 0, `group.demean(kline.close,"kline","sector")`: -.5, `group.zscore(kline.close,"kline","sector")`: -1} {
		plan, err := Compile(specFor(formula))
		if err != nil {
			t.Fatalf("%s: %v", formula, err)
		}
		session, _ := factor.NewSession(plan)
		frame, err := session.Evaluate(snapshot(t, 1000, rows))
		if err != nil {
			t.Fatal(err)
		}
		got := frame.Values["score"][1]
		if got.Validity != factor.Valid || math.Abs(got.Value-want) > 1e-10 {
			t.Fatalf("%s got=%+v want=%g", formula, got, want)
		}
	}
	for _, formula := range []string{`group.wls(kline.close,kline.weight)`, `group.ols(kline.close)`, `cs.mad_winsorize(kline.close,-1)`, `group.demean(kline.close,"missing","sector")`} {
		if _, err := Compile(specFor(formula)); err == nil {
			t.Fatal("invalid expression accepted:", formula)
		}
	}
}
