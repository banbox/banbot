package statistics

import (
	"math"
	"reflect"
	"testing"
)

func TestPoleConfirmedTurningSourceDefaultsAndPrefix(t *testing.T) {
	prices := make([]float64, 140)
	for i := range prices {
		prices[i] = 100 - 3*math.Cos(float64(i-20)*math.Pi/20)
	}
	spec := PoleTurningPointSpec{StartIndex: 20, StartKind: -1}
	points, e := PoleConfirmedTurningPoints(prices, spec)
	if e != nil || len(points) < 4 {
		t.Fatalf("turning %v %v", points, e)
	}
	if points[0].Extremum != 40 || points[0].Kind != 1 || points[0].Confirmed <= 40 {
		t.Fatal("peak and forward confirmation conflated", points)
	}
	for _, point := range points {
		rs := make([]float64, 20)
		for j := range rs {
			i := point.Confirmed - 19 + j
			rs[j] = prices[i]/prices[i-1] - 1
		}
		_, sd, e := MeanStd(rs, 1)
		if e != nil || math.Abs(point.AnnualVolatility-16*sd) > 1e-12 || math.Abs(point.ReversalReturn) <= .30*point.AnnualVolatility {
			t.Fatal("source20/16/.30 volatility reversal", point)
		}
	}
	for end := 21; end <= len(prices); end++ {
		prefix, e := PoleConfirmedTurningPoints(prices[:end], spec)
		if e != nil {
			t.Fatal(e)
		}
		var want []PoleTurningPoint
		for _, p := range points {
			if p.Confirmed < end {
				want = append(want, p)
			}
		}
		if !reflect.DeepEqual(prefix, want) {
			t.Fatalf("future data rewrites confirmation at%d", end)
		}
	}
}

func TestPoleIntereventDeclaredAlignment(t *testing.T) {
	a := []float64{100, 200, 110, 50, 88}
	b := []float64{100, 1, 120, 1000, 72}
	// Common boundaries0,2,4 give returns(.1,-.2) vs(.2,-.4).
	r, e := PoleIntereventCorrelation(a, b, []int{0, 2, 4})
	if e != nil || math.Abs(r-1) > 1e-12 {
		t.Fatal(r, e)
	}
	if _, e = PoleIntereventCorrelation(a, b, []int{0, 2, 2}); e == nil {
		t.Fatal("accepted duplicate boundary")
	}
	if _, e = PoleIntereventCorrelation([]float64{100, 100, 100}, []float64{100, 110, 90}, []int{0, 1, 2}); e == nil {
		t.Fatal("constant correlation invented")
	}
}

func TestPoleRevealedNormalLognormalIndependentIntegration(t *testing.T) {
	normal, e := PoleRevealedNormal(2)
	if e != nil || math.Abs(normal-2*math.Sqrt(2/math.Pi)) > 1e-14 {
		t.Fatal(normal, e)
	}
	mu, sigma := .3, .7
	got, e := PoleRevealedLognormal(mu, sigma)
	if e != nil {
		t.Fatal(e)
	}
	// Simpson integration of upper-half conditional expectation minus E[X].
	const n = 20000
	h := 10.0 / n
	sum := 0.0
	for i := 0; i <= n; i++ {
		z := float64(i) * h
		w := 2.0
		if i == 0 || i == n {
			w = 1
		} else if i%2 == 1 {
			w = 4
		}
		sum += w * 2 * math.Exp(mu+sigma*z-z*z/2) / math.Sqrt(2*math.Pi)
	}
	want := sum*h/3 - math.Exp(mu+sigma*sigma/2)
	if math.Abs(got-want) > 1e-10 {
		t.Fatalf("revealed subtraction factor incorrect: %g %g", got, want)
	}
	if zero, e := PoleRevealedLognormal(1000, 0); e != nil || zero != 0 {
		t.Fatal("deterministic revealed displacement")
	}
	if _, e := PoleRevealedNormal(-1); e == nil {
		t.Fatal("negative sigma")
	}
}

func TestPoleVolumeLogisticMaturityAndSourceForecast(t *testing.T) {
	var days []PoleVolumeDay
	for day := 1; day <= 61; day++ {
		d := PoleVolumeDay{PublishedAt: int64(day), Total: float64(day) * 1000, Fractions: []float64{0, .1, .3, .6, .9, 1}, Cumulative: make([]float64, 6)}
		for j, tick := range d.Fractions {
			p := 1 / (1 + math.Exp(3-6*tick))
			if j == 0 {
				p = 0
			} else if j == 5 {
				p = 1
			}
			d.Cumulative[j] = p * d.Total
		}
		days = append(days, d)
	}
	days[60].Total = math.NaN() // unavailable future session must not enter the fit
	fit, e := FitPoleVolumeLogistic(days, 60, 0)
	if e != nil || fit.Days != 60 || fit.LastPublication != 60 || math.Abs(fit.Intercept+3) > 1e-12 || math.Abs(fit.Slope-6) > 1e-12 {
		t.Fatalf("volume fit %+v %v", fit, e)
	}
	p, e := fit.Proportion(.5)
	if e != nil || math.Abs(p-.5) > 1e-12 {
		t.Fatal(p, e)
	}
	later, e := PoleConditionalVolume(4000000, .30, .40)
	if e != nil || math.Abs(later-4000000.0/3) > 1e-8 {
		t.Fatal("source next90min volume", later, e)
	}
	if _, e := FitPoleVolumeLogistic(days, 59, 60); e == nil {
		t.Fatal("fit used incomplete60-day history")
	}
	days[0].Cumulative[2] = -1
	if _, e := FitPoleVolumeLogistic(days, 60, 60); e == nil {
		t.Fatal("invalid cumulative curve")
	}
}
