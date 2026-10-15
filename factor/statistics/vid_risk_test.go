package statistics

import (
	"math"
	"testing"

	"gonum.org/v1/gonum/stat/distuv"
)

func TestVidBrownianMaximumAndPricingBounds(t *testing.T) {
	p, err := VidBrownianMaximumCDF(.4, .1, 4)
	if err != nil || math.Abs(p-.9544997361036416) > 1e-14 {
		t.Fatalf("maximum %g %v", p, err)
	}
	s := VidPricingBoundsSpec{Spot: 80, Sigma: .1, MaximumProbability: p, FixedTargetValue: 60, BidderShares: 100000, RealizedPrices: []float64{79, 79}, FutureHorizons: []float64{1, 4}}
	b, err := VidFuturePricingBounds(s)
	if err != nil {
		t.Fatal(err)
	}
	wantLower := (158 + 80*math.Exp(-.2) + 80*math.Exp(-.4)) / 4
	wantUpper := (158 + 80*math.Exp(.2) + 80*math.Exp(.4)) / 4
	if math.Abs(b.AverageLower-wantLower) > 1e-12 || math.Abs(b.AverageUpper-wantUpper) > 1e-12 || math.Abs(b.TargetLower-100000*wantLower/60) > 1e-8 {
		t.Fatalf("bounds %+v", b)
	}
	s.FutureHorizons = []float64{1, 1}
	if _, err = VidFuturePricingBounds(s); err == nil {
		t.Fatal("accepted duplicate future fixing")
	}
	s.FutureHorizons = []float64{1}
	s.RealizedPrices = []float64{math.NaN()}
	if _, err = VidFuturePricingBounds(s); err == nil {
		t.Fatal("accepted nonfinite realized price")
	}
	for _, args := range [][3]float64{{-1, 1, 1}, {0, 0, 1}, {1, 1, 0}, {math.Inf(1), 1, 1}} {
		if _, err = VidBrownianMaximumCDF(args[0], args[1], args[2]); err == nil {
			t.Fatal("accepted invalid Brownian inputs")
		}
	}
}

func TestVidMergerCashConventionAndInterdateProbability(t *testing.T) {
	p, err := VidMergerFailureProbability(2, 10, 3, .04, .5)
	want := (2*math.Exp(.02) + 3) / 13
	if err != nil || math.Abs(p-want) > 1e-14 {
		t.Fatalf("cash probability %g %v", p, err)
	}
	// Incorrectly compounding the cash-inclusive spread produces a different answer.
	wrong := 5 * math.Exp(.02) / 13
	if math.Abs(p-wrong) < .001 {
		t.Fatal("cash was incorrectly compounded")
	}
	p1, _ := VidMergerFailureProbability(2, 10, 0, .04, .5)
	p2, err := VidUpdateCashlessFailureProbability(p1, 2, 1.5, 0, .04, .1)
	direct, _ := VidMergerFailureProbability(1.5, 10, 0, .04, .4)
	if err != nil || math.Abs(p2-direct) > 1e-14 {
		t.Fatalf("interdate %g want %g err %v", p2, direct, err)
	}
	for _, args := range [][5]float64{{0, 10, 0, 0, 1}, {2, 0, 0, 0, 1}, {20, 10, 0, 0, 1}, {2, 10, 0, math.NaN(), 1}, {2, 10, 0, 0, 0}, {2, 10, 0, 1e308, 1e308}} {
		if _, err = VidMergerFailureProbability(args[0], args[1], args[2], args[3], args[4]); err == nil {
			t.Fatal("accepted invalid probability inputs")
		}
	}
	for _, args := range [][6]float64{{.2, 0, 1, 0, 0, 1}, {.2, 1, 1, 3, 0, 1}, {.2, 1, 1, 0, 0, 0}, {.9, 1, 2, 0, 0, 1}, {.2, 1, 1, math.NaN(), 0, 1}} {
		if _, err = VidUpdateCashlessFailureProbability(args[0], args[1], args[2], args[3], args[4], args[5]); err == nil {
			t.Fatal("accepted invalid update")
		}
	}
}

func TestVidMixtureQuantileIndependentDistributions(t *testing.T) {
	// Disjoint uniforms: weight .2 on [-10,-6], .8 on [-1,1].
	uniform := func(lo, hi float64) func(float64) float64 {
		return func(x float64) float64 { return math.Max(0, math.Min(1, (x-lo)/(hi-lo))) }
	}
	success, failure := uniform(-1, 1), uniform(-10, -6)
	q, err := VidMixtureReturnQuantile(.2, .025, -20, 20, 1e-10, 100, success, failure)
	// Inside failure component: .2*(q+10)/4=.025 => q=-9.5.
	if err != nil || math.Abs(q+9.5) > 1e-10 {
		t.Fatalf("mixture %g %v", q, err)
	}
	weightedQuantile := .8*(-1+2*.025) + .2*(-10+4*.025)
	if math.Abs(q-weightedQuantile) < 1 {
		t.Fatal("returned weighted component quantile")
	}
	q, err = VidMixtureReturnQuantile(0, .025, -20, 20, 1e-10, 100, distuv.UnitNormal.CDF, distuv.UnitNormal.CDF)
	if err != nil || math.Abs(q-(-1.959963984540054)) > 1e-9 {
		t.Fatalf("normal quantile %g %v", q, err)
	}
	if _, err = VidMixtureReturnQuantile(.2, .025, -1, 1, 1e-10, 100, success, failure); err == nil {
		t.Fatal("accepted unbracketed quantile")
	}
	if _, err = VidMixtureReturnQuantile(.2, .025, -20, 20, 1e-10, 1, success, failure); err == nil {
		t.Fatal("accepted exhausted iteration budget")
	}
	invalid := func(float64) float64 { return math.NaN() }
	if _, err = VidMixtureReturnQuantile(.2, .025, -20, 20, 1e-10, 100, invalid, failure); err == nil {
		t.Fatal("accepted nonfinite CDF")
	}
}
