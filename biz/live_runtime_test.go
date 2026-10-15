package biz

import (
	"testing"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/orm/ormo"
)

func TestRuntimeFatalStopUsesRuntimeClockWhenNowIsNil(t *testing.T) {
	previous := btime.CurTimeMS
	btime.SetTimeMS(9_999)
	t.Cleanup(func() { btime.SetTimeMS(previous) })

	firstClock := btime.NewClockState(true, nil)
	firstClock.SetTimeMS(101)
	secondClock := btime.NewClockState(true, nil)
	secondClock.SetTimeMS(202)

	firstNow := runtimeFatalStopClock(RuntimeDeps{Clock: firstClock}, nil)
	secondNow := runtimeFatalStopClock(RuntimeDeps{Clock: secondClock}, nil)
	if firstNow == nil || secondNow == nil {
		t.Fatal("runtime clock was not selected")
	}
	if got := firstNow(); got != 101 {
		t.Fatalf("first runtime clock = %d, want 101", got)
	}
	if got := secondNow(); got != 202 {
		t.Fatalf("second runtime clock = %d, want 202", got)
	}
	if got := runtimeFatalStopClock(RuntimeDeps{}, nil); got != nil {
		t.Fatal("missing explicit runtime clock was not rejected")
	}

	explicit := func() int64 { return 303 }
	if got := runtimeFatalStopClock(RuntimeDeps{Clock: firstClock}, explicit)(); got != 303 {
		t.Fatalf("explicit now function = %d, want 303", got)
	}
}

func TestCalcRuntimeFatalLossUsesWindowBoundedByRuntimeStart(t *testing.T) {
	wallet := &BanWallets{Items: map[string]*ItemWallet{
		"USDT": {Coin: "USDT", Available: 100},
	}}
	const nowMS int64 = 3_600_000
	orders := []*ormo.InOutOrder{
		{IOrder: &ormo.IOrder{EnterAt: nowMS - 30*60_000, Profit: -100}},
		{IOrder: &ormo.IOrder{EnterAt: nowMS - 2*60_000, Profit: -10}},
	}
	if got := calcRuntimeFatalLoss(wallet, orders, 5, nowMS, 0); got <= 0 || got >= 0.1 {
		t.Fatalf("five-minute loss rate = %v, want only recent loss", got)
	}
	if got := calcRuntimeFatalLoss(wallet, orders, 60, nowMS, 0); got <= 0.5 {
		t.Fatalf("sixty-minute loss rate = %v, want both losses", got)
	}
	if got := calcRuntimeFatalLoss(wallet, orders, 60, nowMS, nowMS-10*60_000); got <= 0 || got >= 0.1 {
		t.Fatalf("runtime-start bounded loss rate = %v, want only post-start loss", got)
	}
}
