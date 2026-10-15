package statistics

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestChanARIMALogPriceLogReturnEquivalence(t *testing.T) {
	r := rand.New(rand.NewSource(963))
	levels := []float64{math.Log(100)}
	previous, innovation := 0.0, 0.0
	for i := 0; i < 360; i++ {
		shock := .001 * r.NormFloat64()
		value := .0001 + .5*previous + .2*innovation + shock
		levels = append(levels, levels[len(levels)-1]+value)
		previous, innovation = value, shock
	}
	for _, q := range []int{1, 9} {
		spec := ARMAFitSpec{P: 1, Q: q, MaxEvaluations: 20000, MaxIterations: 10000, Runtime: 30 * time.Second}
		differences, err := differenceOnce(levels[:320])
		if err != nil {
			t.Fatal(err)
		}
		arma, err := FitARMA(differences, spec)
		if err != nil {
			t.Fatal(err)
		}
		arima, err := FitARIMA(levels[:320], 1, spec)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(arma, arima.ARMA) {
			t.Fatal("same transformed data did not produce identical fitted coefficients/likelihood")
		}
		rf, err := arma.Filter(differences)
		if err != nil {
			t.Fatal(err)
		}
		lf, err := arima.Filter(levels[:320])
		if err != nil {
			t.Fatal(err)
		}
		for i := 320; i < len(levels); i++ {
			if err := rf.Observe(levels[i] - levels[i-1]); err != nil {
				t.Fatal(err)
			}
			if err := lf.Observe(levels[i]); err != nil {
				t.Fatal(err)
			}
			returnForecast, err := rf.Forecast()
			if err != nil {
				t.Fatal(err)
			}
			levelForecast, err := lf.Forecast()
			if err != nil {
				t.Fatal(err)
			}
			if math.Abs(levelForecast-(levels[i]+returnForecast)) > 1e-15 {
				t.Fatal("integrated next-log-price != current + forecast log return")
			}
		}
	}
}
