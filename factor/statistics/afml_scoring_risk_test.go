package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestBinaryClassificationWeightedConfusionAndTiedROC(t *testing.T) {
	r, err := BinaryClassification([]int{1, 1, 0, 0}, []float64{.8, .4, .4, .1}, []float64{2, 1, 1, 2}, .4, 1e-12)
	if err != nil || r.TruePositive != 3 || r.FalsePositive != 1 || r.TrueNegative != 2 || r.FalseNegative != 0 || r.Precision != .75 || r.Recall != 1 || math.Abs(r.F1-6./7) > 1e-14 || math.Abs(r.ROCAUC-17./18) > 1e-14 {
		t.Fatalf("weighted scoring: %+v %v", r, err)
	}
	wantLoss := -(2*math.Log(.8) + math.Log(.4) + math.Log(.6) + 2*math.Log(.9)) / 6
	if math.Abs(r.LogLoss-wantLoss) > 1e-14 {
		t.Fatal("weighted cross entropy disagrees")
	}
	r, err = BinaryClassification([]int{0, 0}, []float64{0, .1}, nil, .5, 1e-12)
	if err != nil || r.PrecisionValid || r.RecallValid || r.AUCValid || r.F1Valid || !math.IsNaN(r.Precision) || !math.IsNaN(r.ROCAUC) {
		t.Fatalf("degenerate scores hidden: %+v %v", r, err)
	}
	for _, weights := range [][]float64{{0, 0}, {1, math.Inf(1)}, {-1, 2}} {
		if _, err := BinaryClassification([]int{0, 1}, []float64{.2, .8}, weights, .5, 1e-12); err == nil {
			t.Fatal("invalid scoring mass accepted")
		}
	}
}

func TestBinaryPayoutSourceNumericalAndInverse(t *testing.T) {
	sr, err := BinaryPayoutSharpe(-.01, .005, .7, 260)
	if err != nil || math.Abs(sr-math.Sqrt(260./189)) > 1e-12 {
		t.Fatalf("chapter15 oracle: %g %v", sr, err)
	}
	p, err := ImpliedBetPrecision(-.01, .005, 260, sr)
	if err != nil || math.Abs(p-.7) > 1e-12 {
		t.Fatalf("precision inverse: %g %v", p, err)
	}
	n, err := ImpliedBetFrequency(-.01, .005, .7, sr)
	if err != nil || math.Abs(n-260) > 1e-10 {
		t.Fatalf("frequency inverse: %g %v", n, err)
	}
	if _, err := ImpliedBetFrequency(-.01, .005, .3, 1.5); err == nil {
		t.Fatal("squared negative-mean extraneous root accepted")
	}
	zero, err := ImpliedBetPrecision(-.01, .005, 260, 0)
	if err != nil || math.Abs(zero-2./3) > 1e-12 {
		t.Fatal("zero target does not give break-even precision")
	}
	for _, scale := range []float64{1e-150, 1e150} {
		got, err := BinaryPayoutSharpe(-.01*scale, .005*scale, .7, 260)
		if err != nil || math.Abs(got-sr) > 1e-12 {
			t.Fatal("payoff units changed Sharpe")
		}
	}
}

func TestStrategyFailureBootstrapBoundedSeededAndMonotone(t *testing.T) {
	returns := []float64{.01, .01, .01, .01, .01, .01, .01, .01, -.01, -.01}
	low, err := BootstrapStrategyFailure(returns, 260, .5, 260, 1000, 17, 260000)
	if err != nil {
		t.Fatal(err)
	}
	high, err := BootstrapStrategyFailure(returns, 260, 15, 260, 1000, 17, 260000)
	if err != nil || high.Probability <= low.Probability || high.Probability < .8 || low.Probability > .01 {
		t.Fatalf("failure threshold direction: low%+v high%+v err%v", low, high, err)
	}
	again, err := BootstrapStrategyFailure(returns, 260, .5, 260, 1000, 17, 260000)
	if err != nil || !reflect.DeepEqual(low, again) {
		t.Fatal("bootstrap seed unstable")
	}
	if _, err := BootstrapStrategyFailure(returns, 260, 1, 260, 1000, 17, 259999); err == nil {
		t.Fatal("bootstrap draw budget ignored")
	}
	got, err := NormalStrategyFailureHeuristic(.7, .7)
	if err != nil || got != .5 {
		t.Fatal("source literal Normal CDF convention changed")
	}
}
