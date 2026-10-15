package statistics

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestChanARMAInnovationsAndCoefficientRecovery(t *testing.T) {
	m := ARMA{Intercept: 1, AR: []float64{.5}, MA: []float64{.2}}
	prediction, err := m.Forecast([]float64{2, 4, 5})
	if err != nil || math.Abs(prediction-3.82) > 1e-12 {
		t.Fatalf("AR/MA innovation recursion %g %v", prediction, err)
	}
	r := rand.New(rand.NewSource(963))
	values := []float64{5}
	previousError := 0.0
	for i := 0; i < 2500; i++ {
		innovation := .2 * r.NormFloat64()
		values = append(values, 1+.8*values[len(values)-1]+.4*previousError+innovation)
		previousError = innovation
	}
	fit, err := FitARMA(values, ARMAFitSpec{P: 1, Q: 1, MaxEvaluations: 5000, MaxIterations: 4000, Runtime: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(fit.AR[0]-.8) > .04 || math.Abs(fit.MA[0]-.4) > .06 || math.Abs(fit.Intercept-1) > .2 || math.Abs(fit.Variance-.04) > .004 {
		t.Fatalf("ARMA recovery %+v", fit)
	}
	if _, err = fit.Forecast([]float64{math.NaN(), 3}); err == nil {
		t.Fatal("nonfinite history accepted")
	}
	selected, bic, err := SelectARMA(values, 2, 2, ARMAFitSpec{MaxEvaluations: 8000, MaxIterations: 6000, Runtime: 10 * time.Second})
	if err != nil || !finite(bic) {
		t.Fatalf("ARMA BIC %v", err)
	}
	if len(selected.AR) != 1 || len(selected.MA) != 1 {
		t.Fatalf("ARMA BIC order %+v", selected)
	}
	filter, err := fit.Filter(values[:100])
	if err != nil {
		t.Fatal(err)
	}
	for end := 100; end < 110; end++ {
		if err = filter.Observe(values[end]); err != nil {
			t.Fatal(err)
		}
		online, _ := filter.Forecast()
		full, _ := fit.Forecast(values[:end+1])
		if math.Abs(online-full) > 1e-12 {
			t.Fatal("ARMA finite-state filter differs from full conditioned history")
		}
	}
	if _, err = FitARMA(values, ARMAFitSpec{P: 1, Q: 1, MaxEvaluations: 1, MaxIterations: 1, Runtime: time.Second}); err == nil {
		t.Fatal("optimizer budget failure accepted")
	}
}
