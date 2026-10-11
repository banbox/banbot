package statistics

import (
	"math"
	"testing"
)

func TestEuropeanOptionReferenceParityAndGreeks(t *testing.T) {
	s := EuropeanOptionSpec{Spot: 100, Strike: 100, Years: 1, Volatility: .2, Rate: .05, Call: true}
	call, err := PriceEuropeanOption(s)
	if err != nil || math.Abs(call.Price-10.450583572185565) > 1e-10 || math.Abs(call.Delta-.6368306511756191) > 1e-12 || !call.GreeksValid {
		t.Fatalf("European reference: %+v %v", call, err)
	}
	s.Call = false
	put, err := PriceEuropeanOption(s)
	if err != nil || math.Abs(put.Price-5.573526022256971) > 1e-10 {
		t.Fatalf("put reference: %+v %v", put, err)
	}
	spot, err := PutCallImpliedSpot(call.Price, put.Price, 100, 1, .05, 0)
	if err != nil || math.Abs(spot-100) > 1e-12 {
		t.Fatal("put-call parity broken")
	}
	s.Call = true
	h := 1e-3
	s.Spot += h
	up, _ := PriceEuropeanOption(s)
	s.Spot -= 2 * h
	down, _ := PriceEuropeanOption(s)
	if math.Abs((up.Price-down.Price)/(2*h)-call.Delta) > 1e-8 || math.Abs((up.Price-2*call.Price+down.Price)/(h*h)-call.Gamma) > 1e-7 {
		t.Fatal("analytic underlying Greeks disagree with numerical price curvature")
	}
	s.Spot = 100
	s.Volatility += h
	up, _ = PriceEuropeanOption(s)
	s.Volatility -= 2 * h
	down, _ = PriceEuropeanOption(s)
	if math.Abs((up.Price-down.Price)/(2*h)-call.Vega) > 1e-4 {
		t.Fatal("vega units mismatch")
	}
}

func TestEuropeanImpliedVolatilityLimitsAndExpiry(t *testing.T) {
	s := EuropeanOptionSpec{Spot: 80, Strike: 100, Years: .25, Volatility: .4, Rate: .03, DividendYield: .02}
	quote, err := PriceEuropeanOption(s)
	if err != nil {
		t.Fatal(err)
	}
	vol, err := EuropeanImpliedVolatility(s, quote.Price, 1e-10, 3, 100)
	if err != nil || math.Abs(vol-.4) > 1e-9 {
		t.Fatalf("IV inverse: %g %v", vol, err)
	}
	if _, err := EuropeanImpliedVolatility(s, quote.Price, 1e-12, .1, 100); err == nil {
		t.Fatal("unbracketed IV accepted")
	}
	if _, err := EuropeanImpliedVolatility(s, quote.Price, 1e-12, 3, 1); err == nil {
		t.Fatal("nonconverged IV accepted")
	}
	if _, err := EuropeanImpliedVolatility(s, 0, 1e-10, 3, 100); err == nil {
		t.Fatal("below-intrinsic put price accepted")
	}
	s.Years = 0
	expired, err := PriceEuropeanOption(s)
	if err != nil || expired.Price != 20 || expired.Delta != -1 || expired.GreeksValid {
		t.Fatal("expiry payoff or discontinuous Greek validity wrong")
	}
	s.Call = true
	expired, err = PriceEuropeanOption(s)
	if err != nil || expired.Price != 0 {
		t.Fatal("worthless expiry not zero")
	}
}
