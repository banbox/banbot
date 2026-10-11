package statistics

import (
	"math"
	"testing"
)

func TestARFitKnownRecurrenceAndForecast(t *testing.T) {
	x := []float64{3}
	for range 30 {
		x = append(x, 1+.7*x[len(x)-1])
	}
	f, err := FitAR(x, 1)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, f.Intercept, 1)
	closeTo(t, f.Coefficients[0], .7)
	y, err := f.Forecast(x)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, y, 1+.7*x[len(x)-1])
	if _, err = FitAR([]float64{1, 1, 1, 1, 1}, 1); err == nil {
		t.Fatal("unidentified AR accepted")
	}
}
func TestLocalLevelGoldenRatioAndPriorInnovation(t *testing.T) {
	k := LocalLevel{ProcessVariance: 1, ObservationVariance: 1}
	var o LevelObservation
	for range 40 {
		var err error
		o, err = k.Observe(10)
		if err != nil {
			t.Fatal(err)
		}
	}
	closeTo(t, o.Gain, (math.Sqrt(5)-1)/2)
	closeTo(t, k.Mean, 10)
	old := k.Mean
	o, err := k.Observe(12)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, o.Prediction, old)
	closeTo(t, o.Innovation, 12-old)
	before := k
	if _, err = k.Observe(math.Inf(1)); err == nil || k != before {
		t.Fatal("bad observation changed state")
	}
}
func TestCuscoreZeroKnownBaselineAndShiftDetection(t *testing.T) {
	q := Cuscore{}
	for i := 1; i <= 5; i++ {
		score, err := q.Observe(2*float64(i), 2)
		if err != nil || score != 0 {
			t.Fatal("baseline not canceled")
		}
	}
	score, err := q.Observe(13, 2)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, score, 6)
	before := q
	if _, err = q.Observe(math.NaN(), 2); err == nil || q != before {
		t.Fatal("invalid Cuscore mutated state")
	}
	v, err := ARCH1(.1, .5, 2)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, v, 2.1)
}
