package statistics

import (
	"math"
	"testing"
)

func TestChanFrontierTwoAssetIndependentOracle(t *testing.T) {
	r, err := ChanLongOnlyFrontier([]float64{.1, .3}, [][]float64{{1, 0}, {0, 4}})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Points) != 21 {
		t.Fatal(len(r.Points))
	}
	for k, p := range r.Points {
		b := float64(k) / 20
		a := 1 - b
		if math.Abs(p.Weights[0]-a) > 1e-8 || math.Abs(p.Weights[1]-b) > 1e-8 || math.Abs(p.Variance-(a*a+4*b*b)) > 1e-8 || p.KKTResidual > 1e-7 {
			t.Fatalf("point %d: %+v", k, p)
		}
	}
	if r.MinimumVariance != 4 {
		t.Fatal(r.MinimumVariance)
	}
	best := 0
	for k := 0; k <= 20; k++ {
		b := float64(k) / 20
		score := (.1 + .2*b) / math.Sqrt((1-b)*(1-b)+4*b*b)
		if score > r.Points[best].Sharpe+1e-12 {
			best = k
		}
	}
	if best != r.Tangency {
		t.Fatal(best, r.Tangency)
	}
}
func TestChanFrontierThreeAssetEqualityKKTAndEndpointTies(t *testing.T) {
	r, err := ChanLongOnlyFrontier([]float64{1, 2, 3}, [][]float64{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}})
	if err != nil {
		t.Fatal(err)
	}
	// At target2 the unique identity-covariance minimum is equal thirds.
	for _, w := range r.Points[10].Weights {
		if math.Abs(w-1./3) > 1e-10 {
			t.Fatal(r.Points[10])
		}
	}
	// At target1.5, unconstrained equality solution is (7/12,1/3,1/12).
	for i, w := range []float64{7. / 12, 1. / 3, 1. / 12} {
		if math.Abs(r.Points[5].Weights[i]-w) > 1e-9 {
			t.Fatal(r.Points[5])
		}
	}
	tied, err := ChanLongOnlyFrontier([]float64{1, 1, 2}, [][]float64{{1, 0, 0}, {0, 4, 0}, {0, 0, 1}})
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(tied.Points[0].Weights[0]-.8) > 1e-9 {
		t.Fatal(tied.Points[0])
	}
	equal, err := ChanLongOnlyFrontier([]float64{1, 1}, [][]float64{{1, 0}, {0, 4}})
	if err != nil || equal.Tangency != 0 || equal.MinimumVariance != 0 || len(equal.Points) != 21 {
		t.Fatal(equal, err)
	}
}
func TestChanAllocationRiskAndSignedNormalization(t *testing.T) {
	w, err := ChanInverseRiskWeights([]float64{1, 2, 4})
	if err != nil || math.Abs(w[0]-4./7) > 1e-12 {
		t.Fatal(w, err)
	}
	w, err = ChanSignedTangency([]float64{2, -1}, [][]float64{{1, 0}, {0, 1}})
	if err != nil || w[0] != 2 || w[1] != -1 {
		t.Fatal(w, err)
	}
	for _, m := range [][]float64{{1, -1}, {-1, -2}, {0, 0}} {
		if _, err := ChanSignedTangency(m, [][]float64{{1, 0}, {0, 1}}); err == nil {
			t.Fatal("degenerate/negative accepted", m)
		}
	}
	for _, risk := range [][]float64{{0, 1}, {-1, 1}, {math.NaN()}} {
		if _, err := ChanInverseRiskWeights(risk); err == nil {
			t.Fatal(risk)
		}
	}
	for _, cov := range [][][]float64{{{1, 1}, {1, 1}}, {{1, 2}, {2, 1}}, {{1, 0}, {.1, 1}}} {
		if _, err := ChanLongOnlyFrontier([]float64{1, 2}, cov); err == nil {
			t.Fatal("invalid covariance", cov)
		}
	}
}
func TestChanFrontierActiveInequalityScalingAndBounds(t *testing.T) {
	means := []float64{1, 2, 3}
	base := [][]float64{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}}
	r, err := ChanLongOnlyFrontier(means, base)
	if err != nil {
		t.Fatal(err)
	}
	// Target1.1 needs the third asset fixed at zero: (.9,.1,0).
	for i, w := range []float64{.9, .1, 0} {
		if math.Abs(r.Points[1].Weights[i]-w) > 1e-8 {
			t.Fatal(r.Points[1])
		}
	}
	for _, scale := range []float64{1e-100, 1e100} {
		cov := [][]float64{{scale, 0, 0}, {0, scale, 0}, {0, 0, scale}}
		scaled, err := ChanLongOnlyFrontier(means, cov)
		if err != nil {
			t.Fatal(err)
		}
		for k, p := range scaled.Points {
			for i, w := range p.Weights {
				if math.Abs(w-r.Points[k].Weights[i]) > 1e-8 {
					t.Fatal("scale changes allocation")
				}
			}
		}
	}
	if _, err := ChanLongOnlyFrontier(make([]float64, 13), make([][]float64, 13)); err == nil {
		t.Fatal("unbounded enumeration")
	}
	if _, err := ChanLongOnlyFrontier([]float64{math.NaN(), 1}, [][]float64{{1, 0}, {0, 1}}); err == nil {
		t.Fatal("nonfinite accepted")
	}
}
