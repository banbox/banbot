package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestVidyamurthyFactorExampleAndSpecificRiskExclusion(t *testing.T) {
	covariance := [][]float64{{.0625, .0225}, {.0225, .1024}}
	a := []float64{1, 1}
	ab, e := CompareCommonFactors(a, []float64{.75, 1}, covariance)
	if e != nil {
		t.Fatal(e)
	}
	ac, e := CompareCommonFactors(a, []float64{1, .75}, covariance)
	if e != nil {
		t.Fatal(e)
	}
	// The source's rounded arithmetic contains errors (especially corr(A,C)
	// printed .9431). Verify the displayed covariance/exposures themselves.
	if math.Abs(ab.AbsoluteCorrelation-.9948646192021914) > 1e-12 || math.Abs(ab.GammaAB-1.10124411689591) > 1e-12 || math.Abs(ac.AbsoluteCorrelation-.9942802691125175) > 1e-12 || math.Abs(ac.GammaAB-1.161358466038349) > 1e-12 {
		t.Fatalf("book example: AB%+v AC%+v", ab, ac)
	}
	if ab.AbsoluteCorrelation <= ac.AbsoluteCorrelation || ab.ResidualVarianceAB >= ac.ResidualVarianceAB {
		t.Fatal("common factor ranking")
	}
	if _, e = CompareCommonFactors(a, a, [][]float64{{1, 2}, {2, 1}}); e == nil {
		t.Fatal("indefinite covariance accepted")
	}
}

func TestVidyamurthyBootstrapPopulationNotMedianUncertainty(t *testing.T) {
	intervals := []int{1, 5, 26}
	a, e := BootstrapDurations(intervals, 10000, 42, .05, .95)
	if e != nil {
		t.Fatal(e)
	}
	b, e := BootstrapDurations(intervals, 10000, 42, .05, .95)
	if e != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("bootstrap reproducibility")
	}
	if a.Lower != 1 || a.Median != 5 || a.Upper != 26 {
		t.Fatal("wrong duration population", a)
	}
	if _, e = BootstrapDurations([]int{0, 5}, 10, 42, .05, .95); e == nil {
		t.Fatal("invalid intervals")
	}
}

func TestVidyamurthyInterpolationAndRegularizationHeel(t *testing.T) {
	counts, e := InterpolateMonotoneTail([]float64{.5, .5, .5, .2, .2, .1})
	if e != nil {
		t.Fatal(e)
	}
	if math.Abs(counts[1]-.4) > 1e-12 || math.Abs(counts[2]-.3) > 1e-12 {
		t.Fatal("step interpolation", counts)
	}
	lambdas := []float64{.0001, .001, .01, .1, 1, 10, 100, 1000}
	choice, e := RegularizationHeel(counts, lambdas)
	if e != nil {
		t.Fatal(e)
	}
	if choice.Lambda <= lambdas[0] || choice.Lambda >= lambdas[len(lambdas)-1] {
		t.Fatal("heel ignored", choice)
	}
	for i := 1; i < len(choice.Smoothed); i++ {
		if choice.Smoothed[i] > choice.Smoothed[i-1]+1e-12 {
			t.Fatal("lost monotonicity")
		}
	}
	if _, e = RegularizationHeel(counts, []float64{.1, .01, 1}); e == nil {
		t.Fatal("unordered grid")
	}
}

func TestVidyamurthyLagOneRetainsStateCovarianceAndLagTwoMinimumVariance(t *testing.T) {
	one, e := NewLaggedSpreadFilter(1, 1, 1, 0)
	if e != nil {
		t.Fatal(e)
	}
	first, e := one.Observe(1, 1)
	if e != nil {
		t.Fatal(e)
	}
	second, e := one.Observe(2, 1)
	if e != nil {
		t.Fatal(e)
	}
	if math.Abs(first.Filtered-2./3) > 1e-12 || math.Abs(second.Prediction-4./3) > 1e-12 || math.Abs(second.PredictionVariance-10./3) > 1e-12 || math.Abs(second.Filtered-24./13) > 1e-12 || math.Abs(second.Variance-10./13) > 1e-12 {
		t.Fatal("lag1 covariance omitted", first, second)
	}
	if math.Abs(one.means[1]-first.Filtered) > 1e-12 {
		t.Fatal("historical filtered vintages were smoothed", one.means)
	}
	two, _ := NewLaggedSpreadFilter(2, 1, 1, 0)
	_, e = two.Observe(1, 1)
	if e != nil {
		t.Fatal(e)
	}
	next, e := two.Observe(2, 1)
	if e != nil {
		t.Fatal(e)
	}
	// The older consecutive change is initially known to be zero because
	// initialization states are the same random variable; its slope variance
	// is zero. Minimum variance therefore puts zero weight on the newer change.
	if math.Abs(next.SlopeWeight) > 1e-12 || math.Abs(next.Prediction-2./3) > 1e-12 {
		t.Fatal("lag2 averaged correlated changes blindly", next)
	}
	before := append([]float64(nil), two.means...)
	if _, e = two.Observe(math.NaN(), 1); e == nil || !reflect.DeepEqual(before, two.means) {
		t.Fatal("invalid observation mutated lag state")
	}
	for _, lag := range []int{1, 2, 3, 5} {
		filter, _ := NewLaggedSpreadFilter(lag, .001, .1, 0)
		for i := 0; i < 50; i++ {
			o, e := filter.Observe(float64(i)*.01+.1*math.Sin(float64(i)), .01)
			if e != nil || o.Variance < 0 || !finite(o.PredictionVariance) {
				t.Fatal("lagged filter unstable", lag, i, o, e)
			}
		}
	}
}
