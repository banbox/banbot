package statistics

import (
	"math"
	"testing"
)

func TestChanTrendStateEquationGolden(t *testing.T) {
	k := LocalLinearTrend{Level: 10, Slope: 2, Covariance: [2][2]float64{{1, 0}, {0, 1}}, LevelVariance: 1, SlopeVariance: 1, ObservationVariance: 1}
	o, e := k.Observe(16)
	if e != nil {
		t.Fatal(e)
	}
	// Prior mean12, P=[[3,1],[1,2]], gain=[3/4,1/4].
	if o.Prediction != 12 || o.Innovation != 4 || o.InnovationVariance != 4 || k.Level != 15 || k.Slope != 3 || o.NextPrediction != 18 {
		t.Fatalf("wrong independent linear-Gaussian result: %+v %+v", k, o)
	}
	want := [2][2]float64{{.75, .25}, {.25, 1.75}}
	for i := range want {
		for j := range want[i] {
			if math.Abs(k.Covariance[i][j]-want[i][j]) > 1e-12 {
				t.Fatal(k.Covariance)
			}
		}
	}
}

func TestChanTrendNoiselessSlopeAndInvalidAtomic(t *testing.T) {
	k := LocalLinearTrend{Level: 10, Slope: 2, ObservationVariance: 1}
	for i := 1; i <= 30; i++ {
		o, e := k.Observe(10 + 2*float64(i))
		if e != nil || o.NextPrediction != 10+2*float64(i+1) {
			t.Fatalf("trend %v %v", o, e)
		}
	}
	old := k
	if _, e := k.Observe(math.NaN()); e == nil || k != old {
		t.Fatal("invalid observation changed state")
	}
	k.Covariance = [2][2]float64{{1, 2}, {2, 1}}
	if _, e := k.Observe(10); e == nil {
		t.Fatal("accepted indefinite covariance")
	}
}
