package statistics

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestGARCHActualRecurrenceAndChronology(t *testing.T) {
	m := GARCH{Omega: .1, InitialVariance: 2, ReturnCoefficients: []float64{.2, .1}, VarianceCoefficients: []float64{.5}}
	f, err := m.Filter()
	if err != nil {
		t.Fatal(err)
	}
	h, _ := f.Forecast()
	if math.Abs(h-1.7) > 1e-12 {
		t.Fatal(h)
	}
	if err = f.Observe(3); err != nil {
		t.Fatal(err)
	}
	h, _ = f.Forecast()
	if math.Abs(h-2.95) > 1e-12 {
		t.Fatal(h)
	}
	values, ll, err := m.ConditionalVariances([]float64{3, 1})
	if err != nil || math.Abs(values[0]-1.7) > 1e-12 || math.Abs(values[1]-2.95) > 1e-12 {
		t.Fatal(values, ll, err)
	}
	want := -.5 * (math.Log(2*math.Pi*1.7) + 9/1.7 + math.Log(2*math.Pi*2.95) + 1/2.95)
	if math.Abs(ll-want) > 1e-12 {
		t.Fatal(ll, want)
	}
	other, _, _ := m.ConditionalVariances([]float64{3, 500})
	if values[1] != other[1] {
		t.Fatal("future return entered prior variance")
	}
	if _, err = (GARCH{Omega: .1, InitialVariance: 2, ReturnCoefficients: []float64{.6}, VarianceCoefficients: []float64{.4}}).Filter(); err == nil {
		t.Fatal("unit persistence accepted")
	}
}

func TestGARCHConditionalMLEImprovesLikelihoodAndScales(t *testing.T) {
	m := GARCH{Omega: .00001, InitialVariance: .0002, ReturnCoefficients: []float64{.18, .12}, VarianceCoefficients: []float64{.65}}
	f, _ := m.Filter()
	rng := rand.New(rand.NewSource(410))
	data := make([]float64, 1800)
	for i := range data {
		h, _ := f.Forecast()
		data[i] = math.Sqrt(h) * rng.NormFloat64()
		_ = f.Observe(data[i])
	}
	spec := GARCHFitSpec{P: 1, Q: 2, MaxEvaluations: 10000, MaxIterations: 8000, Runtime: 10 * time.Second}
	fit, err := FitGARCH(data, spec)
	if err != nil {
		t.Fatal(err)
	}
	base := GARCH{Omega: fit.InitialVariance * .999, InitialVariance: fit.InitialVariance, ReturnCoefficients: []float64{.001}, VarianceCoefficients: []float64{0}}
	_, iid, _ := base.ConditionalVariances(data)
	if fit.LogLikelihood < iid+20 {
		t.Fatal("no conditional variance fit", fit.LogLikelihood, iid)
	}
	if len(fit.ReturnCoefficients) != 2 || len(fit.VarianceCoefficients) != 1 || fit.Observations != len(data) {
		t.Fatal(fit)
	}
	scaled := make([]float64, len(data))
	for i, r := range data {
		scaled[i] = r * 10
	}
	second, err := FitGARCH(scaled, spec)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(second.Omega/fit.Omega-100) > .01 || math.Abs(second.ReturnCoefficients[0]-fit.ReturnCoefficients[0]) > 1e-5 {
		t.Fatal("unit-dependent model", fit, second)
	}
	spec.MaxEvaluations = 1
	if _, err = FitGARCH(data, spec); err == nil {
		t.Fatal("unconverged fit accepted")
	}
}
