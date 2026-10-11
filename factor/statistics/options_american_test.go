package statistics

import (
	"math"
	"testing"
)

func TestAmericanTreeConvergenceEarlyExerciseAndIV(t *testing.T) {
	s := EuropeanOptionSpec{Spot: 100, Strike: 100, Years: 1, Volatility: .2, Rate: .05, Call: true}
	e, err := PriceEuropeanOption(s)
	if err != nil {
		t.Fatal(err)
	}
	american, err := AmericanOptionPrice(s, 1200)
	if err != nil || math.Abs(american-e.Price) > .003 {
		t.Fatal("nondividend American call should equal European", american, e, err)
	}
	s.Call = false
	put, err := AmericanOptionPrice(s, 1000)
	e, _ = PriceEuropeanOption(s)
	if err != nil || put < e.Price+.1 {
		t.Fatal("put early exercise absent", put, e.Price, err)
	}
	vol, err := AmericanImpliedVolatility(s, put, .02, 1, 1000, 70, 1e-7)
	if err != nil || math.Abs(vol-.2) > 1e-7 {
		t.Fatal(vol, err)
	}
	s.Spot = 30
	s.Strike = 100
	deep, err := AmericanOptionPrice(s, 500)
	if err != nil || math.Abs(deep-70) > 1e-8 {
		t.Fatal("immediate exercise", deep, err)
	}
	if _, err = AmericanImpliedVolatility(s, 70, .05, .3, 500, 70, 1e-7); err == nil {
		t.Fatal("exercise plateau IV invented")
	}
	s.Years = 0
	v, err := AmericanOptionPrice(s, 1)
	if err != nil || v != 70 {
		t.Fatal(v, err)
	}
	if _, err = AmericanOptionPrice(s, 5001); err == nil {
		t.Fatal("unbounded tree")
	}
}
