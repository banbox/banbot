package statistics

import (
	"math"
	"reflect"
	"testing"
)

func closeTo(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-10*math.Max(1, math.Abs(want)) {
		t.Fatalf("got %.15g want %.15g", got, want)
	}
}
func TestRegressionInterceptIsExplicit(t *testing.T) {
	x := [][]float64{{1}, {2}, {3}, {4}}
	y := []float64{5, 8, 11, 14}
	f, err := Regress(y, x, true)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, f.Intercept, 2)
	closeTo(t, f.Beta[0], 3)
	without, err := Regress(y, x, false)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, without.Intercept, 0)
	closeTo(t, without.Beta[0], 3+20.0/30)
	if !reflect.DeepEqual(x, [][]float64{{1}, {2}, {3}, {4}}) {
		t.Fatal("input mutated")
	}
	if _, err = Regress(y, [][]float64{{1}, {1}, {1}, {1}}, true); err == nil {
		t.Fatal("constant exposure identified")
	}
}
func TestMeanAndStdPopulationVersusSample(t *testing.T) {
	m, s, err := MeanStd([]float64{1, 2, 3, 4}, 0)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, m, 2.5)
	closeTo(t, s, math.Sqrt(1.25))
	_, s, err = MeanStd([]float64{1, 2, 3, 4}, 1)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, s, math.Sqrt(5.0/3))
	_, s, err = MeanStd([]float64{2, 2}, 0)
	if err != nil || s != 0 {
		t.Fatal("constant sample")
	}
	for _, v := range [][]float64{nil, {math.NaN()}, {math.Inf(1)}} {
		if _, _, err = MeanStd(v, 0); err == nil {
			t.Fatal("invalid sample accepted")
		}
	}
}
func TestOUExactDriftAndDistinctHalfLives(t *testing.T) {
	x := []float64{0}
	for range 12 {
		x = append(x, 2+.8*x[len(x)-1])
	}
	f, err := FitOU(x)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, f.Intercept, 2)
	closeTo(t, f.Slope, -.2)
	closeTo(t, f.Equilibrium, 10)
	closeTo(t, f.LinearHalfLife, math.Ln2/.2)
	closeTo(t, f.DiscreteHalfLife, -math.Ln2/math.Log(.8))
	for _, v := range [][]float64{{1, 1, 1, 1}, {1, 2, 4, 8}, {math.NaN(), 2, 3, 4}} {
		if _, err = FitOU(v); err == nil {
			t.Fatal("invalid/nonreverting sample accepted")
		}
	}
}
func TestKalmanInnovationUsesPriorAndRejectsWithoutMutation(t *testing.T) {
	k := KalmanPair{Covariance: [2][2]float64{{1, 0}, {0, 1}}, ObservationVariance: 1}
	o, err := k.Observe(2, 3)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, o.Prediction, 0)
	closeTo(t, o.Error, 3)
	closeTo(t, o.Variance, 6)
	closeTo(t, k.Beta[0], 1)
	closeTo(t, k.Beta[1], .5)
	closeTo(t, k.Covariance[0][0], 1.0/3)
	closeTo(t, k.Covariance[1][1], 5.0/6)
	closeTo(t, k.Covariance[0][1], -1.0/3)
	before := k
	if _, err = k.Observe(math.NaN(), 4); err == nil || k != before {
		t.Fatal("invalid update mutated state")
	}
	bad := KalmanPair{Covariance: [2][2]float64{{1, 2}, {2, 1}}, ObservationVariance: 1}
	if _, err = bad.Observe(1, 1); err == nil {
		t.Fatal("indefinite covariance accepted")
	}
}

func TestKalmanCorrelatedProcessInnovation(t *testing.T) {
	k := KalmanPair{ProcessCovariance: [2][2]float64{{.00055, -.011}, {-.011, .27}}, ObservationVariance: .0064}
	o, err := k.Observe(10, 4)
	if err != nil {
		t.Fatal(err)
	}
	// H*Q*H' + R = 100*.00055 -20*.011 + .27 + .0064.
	closeTo(t, o.Variance, .1114)
	closeTo(t, k.Beta[0], (.0055-.011)/.1114*4)
	closeTo(t, k.Beta[1], (-.11+.27)/.1114*4)
	before := k
	k.ProcessVariance = .1
	if _, err = k.Observe(10, 4); err == nil {
		t.Fatal("ambiguous scalar and matrix Q accepted")
	}
	k = before
	k.ProcessCovariance = [2][2]float64{{1, 2}, {2, 1}}
	if _, err = k.Observe(10, 4); err == nil {
		t.Fatal("indefinite Q accepted")
	}
}
