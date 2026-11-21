package biz

import (
	"math"
	"testing"

	"github.com/banbox/banbot/orm/ormo"
)

func TestFundingApproxCost055(t *testing.T) {
	const day = int64(86400000)
	// 10%/y on 1000 notional for 36.5 days = 10
	if got := fundingApproxCost(1000, 10, 365*day/10); math.Abs(got-10) > 1e-9 {
		t.Fatalf("cost %v", got)
	}
	if fundingApproxCost(1000, 0, day) != 0 || fundingApproxCost(0, 10, day) != 0 || fundingApproxCost(1000, 10, 0) != 0 {
		t.Fatal("zero inputs must cost nothing")
	}
}

func TestAddApproxFunding055(t *testing.T) {
	const day = int64(86400000)
	mk := func(short bool) *ormo.InOutOrder {
		return &ormo.InOutOrder{IOrder: &ormo.IOrder{Short: short, EnterAt: 100 * day},
			Enter: &ormo.ExOrder{Average: 100, Filled: 10},
			Exit:  &ormo.ExOrder{UpdateAt: 110 * day, Fee: 1, FeeQuote: 1}}
	}
	long := mk(false)
	addApproxFunding(long, 36.5, true, false, 0) // 1000 * 36.5% * 10/365 = 10
	if math.Abs(long.Exit.FeeQuote-11) > 1e-9 || math.Abs(long.Exit.Fee-11) > 1e-9 {
		t.Fatalf("long fee %v %v", long.Exit.Fee, long.Exit.FeeQuote)
	}
	if f := long.GetInfoFloat64("funding"); math.Abs(f-10) > 1e-9 {
		t.Fatalf("funding info %v", f)
	}
	inv := mk(false)
	addApproxFunding(inv, 36.5, true, true, 0)
	if inv.Exit.FeeQuote != 1 || inv.GetInfoString("funding_skipped") != "inverse" || inv.GetInfoFloat64("funding") != 0 {
		t.Fatalf("inverse must be skipped and tagged")
	}
	sh := mk(true)
	addApproxFunding(sh, 36.5, true, false, 0) // bilateral symmetric: short receives +10 credit (FeeQuote = 1 - 10 = -9)
	if math.Abs(sh.Exit.FeeQuote-(-9)) > 1e-9 || math.Abs(sh.Exit.Fee-(-9)) > 1e-9 {
		t.Fatalf("short bilateral fee %v %v, want -9", sh.Exit.Fee, sh.Exit.FeeQuote)
	}
	if f := sh.GetInfoFloat64("funding"); math.Abs(f-(-10)) > 1e-9 {
		t.Fatalf("short bilateral funding info %v, want -10", f)
	}
	shLegacy := mk(true)
	addApproxFundingWithMode(shLegacy, 36.5, true, false, true, 0)
	spot := mk(false)
	addApproxFunding(spot, 36.5, false, false, 0)
	off := mk(false)
	addApproxFunding(off, 0, true, false, 0)
	for _, od := range []*ormo.InOutOrder{shLegacy, spot, off} {
		if od.Exit.FeeQuote != 1 || od.GetInfoFloat64("funding") != 0 {
			t.Fatalf("must not charge: fee=%v funding=%v", od.Exit.FeeQuote, od.GetInfoFloat64("funding"))
		}
	}
}
