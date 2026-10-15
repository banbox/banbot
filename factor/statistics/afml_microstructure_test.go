package statistics

import (
	"math"
	"testing"
)

func TestAFMLRollMomentsAndValidity(t *testing.T) {
	est, err := RollFromMoments(6, -1)
	if err != nil || !est.SpreadValid || !est.NoiseVarianceValid {
		t.Fatalf("admissible Roll moments: %+v %v", est, err)
	}
	afmlMicroNear(t, est.Spread, 2, 0)
	afmlMicroNear(t, est.NoiseVariance, 4, 0)
	badSpread, _ := RollFromMoments(6, 1)
	if badSpread.SpreadValid || !math.IsNaN(badSpread.Spread) {
		t.Fatalf("positive covariance silently clamped: %+v", badSpread)
	}
	badNoise, _ := RollFromMoments(1, -1)
	if badNoise.NoiseVarianceValid || badNoise.NoiseVariance != -1 {
		t.Fatalf("negative noise variance hidden: %+v", badNoise)
	}
	// Differences 1,2,3: population variance 2/3, paired covariance 1/4.
	trend, err := RollPriceEstimate([]float64{0, 1, 3, 6}, 0)
	if err != nil {
		t.Fatal(err)
	}
	afmlMicroNear(t, trend.ChangeVariance, 2./3, 1e-12)
	afmlMicroNear(t, trend.LagCovariance, .25, 1e-12)
	if trend.SpreadValid {
		t.Fatal("trend satisfies iid Roll spread assumption")
	}
	sample, _ := RollPriceEstimate([]float64{0, 1, 3, 6}, 1)
	afmlMicroNear(t, sample.ChangeVariance, 1, 0)
	afmlMicroNear(t, sample.LagCovariance, .5, 0)
	flat, _ := RollPriceEstimate([]float64{100, 100, 100, 100}, 0)
	afmlMicroNear(t, flat.Spread, 0, 0)
	if _, err := RollPriceEstimate([]float64{1, 2, math.NaN(), 4}, 0); err == nil {
		t.Fatal("accepted malformed Roll prices")
	}
}

func TestAFMLHighLowSourceFixtures(t *testing.T) {
	high, low := []float64{110, 110, 110, 110}, []float64{100, 100, 100, 100}
	rangeLog := math.Log(1.1)
	parkinson, err := ParkinsonVolatility(high, low, 2)
	if err != nil || !math.IsNaN(parkinson[0]) {
		t.Fatalf("Parkinson warmup: %v %v", parkinson, err)
	}
	afmlMicroNear(t, parkinson[1], rangeLog/math.Sqrt(4*math.Ln2), 1e-12)
	est, err := CorwinSchultz(high, low, 2)
	if err != nil || !math.IsNaN(est.Spread[1]) || math.IsNaN(est.Gamma[1]) {
		t.Fatalf("two-bar beta rolling warmup: %+v %v", est, err)
	}
	for i := 2; i < 4; i++ {
		afmlMicroNear(t, est.Beta[i], 2*rangeLog*rangeLog, 1e-12)
		afmlMicroNear(t, est.Gamma[i], rangeLog*rangeLog, 1e-12)
		afmlMicroNear(t, est.Alpha[i], rangeLog, 1e-12)
		// Identical bid/ask-only ranges imply spread 2*(H-L)/(H+L), sigma=0.
		afmlMicroNear(t, est.Spread[i], 2.*10/210, 1e-12)
		afmlMicroNear(t, est.Sigma[i], 0, 1e-12)
	}
	jump, err := CorwinSchultz([]float64{100, 110}, []float64{100, 110}, 1)
	if err != nil {
		t.Fatal(err)
	}
	afmlMicroNear(t, jump.Alpha[1], 0, 0)
	afmlMicroNear(t, jump.Spread[1], 0, 0)
	afmlMicroNear(t, jump.Sigma[1], rangeLog/math.Sqrt((8/math.Pi)*(3-2*math.Sqrt2)), 1e-12)
}

func TestAFMLHighLowPrefixAndScale(t *testing.T) {
	high, low := []float64{110, 113, 109, 108, 115}, []float64{100, 103, 102, 104, 107}
	full, err := CorwinSchultz(high, low, 2)
	if err != nil {
		t.Fatal(err)
	}
	for n := 3; n <= len(high); n++ {
		prefix, err := CorwinSchultz(high[:n], low[:n], 2)
		if err != nil {
			t.Fatal(err)
		}
		afmlMicroNear(t, prefix.Spread[n-1], full.Spread[n-1], 0)
		afmlMicroNear(t, prefix.Sigma[n-1], full.Sigma[n-1], 0)
	}
	scaledHigh, scaledLow := make([]float64, len(high)), make([]float64, len(low))
	for i := range high {
		scaledHigh[i], scaledLow[i] = high[i]*1000, low[i]*1000
	}
	scaled, _ := CorwinSchultz(scaledHigh, scaledLow, 2)
	for i := 2; i < len(high); i++ {
		afmlMicroNear(t, scaled.Spread[i], full.Spread[i], 1e-12)
		afmlMicroNear(t, scaled.Sigma[i], full.Sigma[i], 1e-12)
	}
}

func TestAFMLHighLowValidationAndExtremePrices(t *testing.T) {
	for _, pair := range []struct{ high, low []float64 }{
		{nil, nil}, {[]float64{1}, nil}, {[]float64{1}, []float64{2}},
		{[]float64{1}, []float64{0}}, {[]float64{math.Inf(1)}, []float64{1}},
	} {
		if _, err := CorwinSchultz(pair.high, pair.low, 1); err == nil {
			t.Fatal("accepted invalid high-low input")
		}
		if _, err := ParkinsonVolatility(pair.high, pair.low, 1); err == nil {
			t.Fatal("accepted invalid Parkinson input")
		}
	}
	if _, err := CorwinSchultz([]float64{1}, []float64{1}, 0); err == nil {
		t.Fatal("accepted zero window")
	}
	est, err := CorwinSchultz([]float64{1e300, 1e300}, []float64{1e-300, 1e-300}, 1)
	if err != nil || !afmlFinite(est.Spread[1]) || !afmlFinite(est.Sigma[1]) {
		t.Fatalf("valid extreme prices overflowed: %+v %v", est, err)
	}
	afmlMicroNear(t, est.Spread[1], 2, 1e-12)
}

func afmlMicroNear(t *testing.T, got, want, tolerance float64) {
	t.Helper()
	if !afmlFinite(got) || math.Abs(got-want) > tolerance {
		t.Fatalf("got %.16g want %.16g tolerance %.3g", got, want, tolerance)
	}
}
