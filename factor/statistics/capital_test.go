package statistics

import "testing"

func TestKellyAnalyticIndependentAssetsAndGrossScaling(t *testing.T) {
	rows := [][]float64{{.03, .04}, {.03, 0}, {-.01, .04}, {-.01, 0}}
	w, err := KellyWeights(rows, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, w[0], 18.75)
	closeTo(t, w[1], 37.5)
	half, err := KellyWeights(rows, .5, 0)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, half[0], w[0]/2)
	cap, err := KellyWeights(rows, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, cap[0], 2.0/3)
	closeTo(t, cap[1], 4.0/3)
	if _, err = KellyWeights([][]float64{{.1, .2}, {.2, .4}, {.3, .6}}, 1, 0); err == nil {
		t.Fatal("singular Kelly leverage accepted")
	}
}
func TestDrawdownConventionsAndRecoveryDuration(t *testing.T) {
	r, err := Drawdowns([]float64{100, 80, 50, 80, 100, 110, 100})
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, r.Standard, .5)
	closeTo(t, r.Recovery, 1)
	if r.Duration != 3 || r.PeakIndex != 0 || r.TroughIndex != 2 {
		t.Fatalf("drawdown chronology %+v", r)
	}
	if _, err = Drawdowns([]float64{100, 0}); err == nil {
		t.Fatal("undefined recovery accepted")
	}
}
