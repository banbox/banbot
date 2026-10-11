package statistics

import (
	"math"
	"math/rand"
	"testing"
)

func TestChanVARRecoversCoupledSystem(t *testing.T) {
	rows := [][]float64{{2, 4}}
	r := rand.New(rand.NewSource(87))
	for i := 0; i < 8000; i++ {
		p := rows[len(rows)-1]
		rows = append(rows, []float64{1 + .6*p[0] + .2*p[1] + .1*r.NormFloat64(), 2 - .1*p[0] + .5*p[1] + .1*r.NormFloat64()})
	}
	v, bic, err := SelectVAR(rows, 3)
	if err != nil {
		t.Fatal(err)
	}
	if v.Lags != 1 || !finite(bic) {
		t.Fatalf("lag selection %d %g", v.Lags, bic)
	}
	pred, err := v.Forecast([][]float64{{3, 5}})
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(pred[0]-3.8) > .04 || math.Abs(pred[1]-4.2) > .04 {
		t.Fatalf("cross-series/intercept %v", pred)
	}
	if v.Covariance[0][0] < .009 || v.Covariance[0][0] > .011 {
		t.Fatal("innovation variance")
	}
	if _, err = v.Forecast([][]float64{{math.NaN(), 5}}); err == nil {
		t.Fatal("nonfinite forecast accepted")
	}
}

func TestChanVARLagOrderingAndInvalidInputs(t *testing.T) {
	v := VAR{Lags: 2, Equations: []Regression{{Intercept: 1, Beta: []float64{2, 3, 5, 7}}, {Beta: []float64{0, 1, 1, 0}}}}
	p, err := v.Forecast([][]float64{{11, 13}, {17, 19}})
	if err != nil || p[0] != 238 || p[1] != 30 {
		t.Fatalf("lag order %v %v", p, err)
	}
	if _, err := FitVAR([][]float64{{1, 2}, {2}, {3, 4}, {4, 5}}, 1); err == nil {
		t.Fatal("ragged input accepted")
	}
	if _, err := FitVAR([][]float64{{1, 1}, {1, 1}, {1, 1}, {1, 1}}, 1); err == nil {
		t.Fatal("rank deficient input accepted")
	}
}
