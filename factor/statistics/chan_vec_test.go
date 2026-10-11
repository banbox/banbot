package statistics

import (
	"math"
	"testing"
)

func TestChanVAR1VEC0Algebra(t *testing.T) {
	m := VAR{Lags: 1, Equations: []Regression{{Intercept: 2, Beta: []float64{.8, .1}}, {Intercept: -3, Beta: []float64{-.2, 1.1}}}}
	for _, row := range [][]float64{{100, 110}, {math.Log(100), math.Log(110)}, {-5, 9}} {
		varPrediction, err := m.Forecast([][]float64{row})
		if err != nil {
			t.Fatal(err)
		}
		vecPrediction, err := m.ForecastVEC0(row)
		if err != nil {
			t.Fatal(err)
		}
		for j := range row {
			if math.Abs(varPrediction[j]-vecPrediction[j]) > 1e-12 {
				t.Fatal("VAR1/VEC0 not equivalent")
			}
		}
	}
	m.Lags = 2
	if _, err := m.ForecastVEC0([]float64{1, 2}); err == nil {
		t.Fatal("VAR2 is not VEC0")
	}
}
