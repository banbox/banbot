package statistics

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
)

func TestBulkVolumePriorCalibrationAndVPIN(t *testing.T) {
	got, err := BulkVolumeFractions([]float64{-1, 0, 1, 2, 100}, 3, 0)
	if err != nil || !math.IsNaN(got[2]) || math.Abs(got[3]-.9772498680518208) > 1e-14 || got[4] != 1 {
		t.Fatalf("prior-only sample sigma BVC: %v %v", got, err)
	}
	student, err := BulkVolumeBuyFraction(0, 2, .1)
	if err != nil || student != .5 {
		t.Fatal("Student bulk-volume center wrong")
	}
	missing, err := BulkVolumeFractions([]float64{0, math.NaN(), 1, 2, 3}, 3, 0)
	if err != nil || !math.IsNaN(missing[3]) || !math.IsNaN(missing[4]) {
		t.Fatal("missing calibration history compressed")
	}
	vpin, err := VolumeSynchronizedPIN([]float64{10, 5, 0}, []float64{0, 5, 10}, 10, 2)
	if err != nil || !math.IsNaN(vpin[0]) || vpin[1] != .5 || vpin[2] != .5 {
		t.Fatalf("equal-volume imbalance average: %v %v", vpin, err)
	}
	if _, err := VolumeSynchronizedPIN([]float64{10, 5}, []float64{0, 4}, 10, 2); err == nil {
		t.Fatal("unequal candle volume accepted as VPIN bucket")
	}
}

func TestImpactSourceRegressionsAndBayesianSampler(t *testing.T) {
	kyle, err := KyleLambda([]float64{2, -4, 6, -8}, []float64{1, -2, 3, -4})
	if err != nil || math.Abs(kyle.Lambda-2) > 1e-12 {
		t.Fatal("Kyle signed-volume regression wrong")
	}
	amihud, err := AmihudLambda([]float64{-.01, .03, -.02}, []float64{1, 2, 4})
	if err != nil || math.Abs(amihud.Lambda-.15/21) > 1e-14 {
		t.Fatalf("book Amihud regression replaced by mean ratio: %+v %v", amihud, err)
	}
	rng := rand.New(rand.NewSource(23))
	x, y := make([]float64, 400), make([]float64, 400)
	for i := range x {
		x[i] = rng.NormFloat64()
		y[i] = .3*x[i] + .1*rng.NormFloat64()
	}
	spec := ImpactGibbsSpec{PriorMean: 0, PriorVariance: 10, VarianceShape: 2, VarianceScale: .01, BurnIn: 100, Draws: 600, Seed: 11}
	posterior, err := HasbrouckImpactGibbs(y, x, spec)
	if err != nil || math.Abs(posterior.LambdaMean-.3) > .015 || posterior.LambdaStd <= 0 || posterior.VarianceMean < .005 || posterior.VarianceMean > .015 || len(posterior.LambdaDraws) != 600 {
		t.Fatalf("Bayesian data recovery: %+v %v", posterior, err)
	}
	again, err := HasbrouckImpactGibbs(y, x, spec)
	if err != nil || !reflect.DeepEqual(posterior, again) {
		t.Fatal("Gibbs seed not reproducible")
	}
	spec.PriorMean, spec.PriorVariance = 0, 1e-8
	shrunk, err := HasbrouckImpactGibbs(y, x, spec)
	if err != nil || math.Abs(shrunk.LambdaMean) > .01 {
		t.Fatal("declared prior not used by actual posterior sampler")
	}
}
