package statistics

import (
	"math"
	"testing"
)

func TestSourceOptionSurfacePairAverageOIVersusSideWeights(t *testing.T) {
	quotes := []SurfaceOption{{Strike: 100, ExpiryMS: 1000, Call: true, IV: .3, OpenInterest: 90}, {Strike: 100, ExpiryMS: 1000, IV: .2, OpenInterest: 10}, {Strike: 110, ExpiryMS: 1000, Call: true, IV: .2, OpenInterest: 20}, {Strike: 110, ExpiryMS: 1000, IV: .4, OpenInterest: 80}}
	value, err := OpenInterestWeightedIVSpread(quotes)
	if err != nil || math.Abs(value-(-.05)) > 1e-12 {
		t.Fatal("weights must be average OI of each paired strike/expiry", value, err)
	}
	quotes = append(quotes, SurfaceOption{Strike: 100, ExpiryMS: 2000, Call: true, IV: 1, OpenInterest: 1000})
	v, err := OpenInterestWeightedIVSpread(quotes)
	if err != nil || v != value {
		t.Fatal("unpaired expiry contaminated spread", v, err)
	}
	quotes = append(quotes, quotes[0])
	if _, err = OpenInterestWeightedIVSpread(quotes); err == nil {
		t.Fatal("duplicate revision accepted")
	}
}

func TestSourceSmirkTenorStrictPutBoundsAndNearestMoneyness(t *testing.T) {
	quotes := []SurfaceOption{{Strike: 99, TenorDays: 30, ExpiryMS: 1000, Call: true, IV: .2}, {Strike: 95, TenorDays: 30, ExpiryMS: 1000, IV: 2}, {Strike: 80, TenorDays: 30, ExpiryMS: 1000, IV: 3}, {Strike: 94, TenorDays: 30, ExpiryMS: 1000, IV: .3}, {Strike: 93, TenorDays: 30, ExpiryMS: 1000, IV: .5}, {Strike: 100, TenorDays: 61, ExpiryMS: 2000, Call: true, IV: 1}}
	v, err := VolatilitySmirk(100, quotes)
	if err != nil || math.Abs(v-.1) > 1e-12 {
		t.Fatal(v, err)
	}
}
