package backtest

import (
	"math"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/shopspring/decimal"
)

func attrPortfolio(t *testing.T, seq uint64, decision int64, w map[int32]float64, nav float64) *factor.TargetPortfolio {
	t.Helper()
	p, err := factor.NewTargetPortfolio(factor.PortfolioSpec{StrategyID: "s", AccountID: "a", DecisionTime: decision, ExecutableAt: decision + 1, ExpireAt: decision + 1000, PlanSequence: seq, SnapshotID: "snap", PlanHash: "p", FactorPlanHash: "dag", UniverseVersion: "u", Budget: factor.FrozenBudget{Version: "v", Currency: "USD", NAV: nav}, Mode: factor.Full}, w)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// attrScenario drives marks, executions (including a sign flip), funding, a PIT exit with recovery and a
// terminal settlement through one Book and returns the NAV after every step.
func attrScenario(t *testing.T, enable bool) (*Book, []float64) {
	t.Helper()
	b, err := NewBook(1000)
	if err != nil {
		t.Fatal(err)
	}
	if enable {
		b.EnableAttribution()
	}
	b.SetFundingApr(10)
	b.SetPITDelistPenaltyBps(100)
	var navs []float64
	snap := func() { navs = append(navs, b.State().NAV); b.SampleExposure() }
	q := func(ms int64, p float64) Quote { return Quote{AtMS: ms, AvailableAt: ms, Price: p} }
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(b.Mark(1, q(5, 100), 5))
	must(b.Mark(2, q(5, 50), 5))
	must(b.Mark(3, q(5, 20), 5))
	snap()
	// open long 1, short 2, long 3
	must(b.Execute(attrPortfolio(t, 1, 10, map[int32]float64{1: .4, 2: -.3, 3: .2}, 1000), map[int32]Quote{1: q(11, 101), 2: q(11, 49), 3: q(11, 20)}, 11, .001, .002))
	snap()
	must(b.Mark(1, q(3600000, 110), 3600000))
	must(b.Mark(2, q(3600000, 55), 3600000))
	snap()
	must(b.ApplyFunding(Funding{ID: "f1", SID: 1, AtMS: 3600000, AvailableAt: 3600000, Rate: .001}, 3600000))
	snap()
	// rebalance: flip 1 to short, close 2, grow 3 (old position revalued to the fill quote first)
	must(b.Execute(attrPortfolio(t, 2, 7200000, map[int32]float64{1: -.2, 3: .3}, b.State().NAV), map[int32]Quote{1: q(7200001, 108), 2: q(7200001, 56), 3: q(7200001, 22)}, 7200001, .001, .002))
	snap()
	// instrument 3 drops out (PIT exit) and later recovers with a gap
	b.ExitAtLastMark([]int32{3}, .001, .002)
	snap()
	must(b.Mark(1, q(7300000, 100), 7300000))
	must(b.Mark(3, q(9000000, 15), 9000000))
	snap()
	// patch-less final: grow 1 short, then instrument 1 is settled at 90
	must(b.Execute(attrPortfolio(t, 3, 9100000, map[int32]float64{1: -.5}, b.State().NAV), map[int32]Quote{1: q(9100001, 99), 2: q(9100001, 56)}, 9100001, .001, .002))
	snap()
	if _, err := b.ApplyTerminalSettlement(TerminalSettlement{Source: "s", SourceVersion: "1", SID: 1, CashValue: decimal.NewFromInt(90), EventMS: 9200000, PublishedMS: 9200000, ReceivedMS: 9200000}, 9200000); err != nil {
		t.Fatal(err)
	}
	snap()
	return b, navs
}

func TestAttributionReconcilesToNAV(t *testing.T) {
	b, _ := attrScenario(t, true)
	r := b.Attribution()
	if r == nil {
		t.Fatal("attribution disabled")
	}
	// Long + short PnL - costs reproduces the NAV change; only float rounding is left (1e-9 of NAV).
	if math.Abs(r.Residual) > 1e-9*r.InitialNAV {
		t.Fatalf("residual %v (%+v)", r.Residual, r)
	}
	if r.LongPnL == 0 || r.ShortPnL == 0 || r.LongTurnover <= 0 || r.ShortTurnover <= 0 || r.Samples == 0 || r.AvgLongExposure <= 0 || r.AvgShortExposure <= 0 {
		t.Fatalf("legs not populated: %+v", r)
	}
	st := b.State()
	if math.Abs(r.LongTurnover+r.ShortTurnover-st.Turnover) > 1e-9*st.Turnover {
		t.Fatalf("leg turnover %v+%v != book %v", r.LongTurnover, r.ShortTurnover, st.Turnover)
	}
	var net, pnl float64
	for _, s := range r.Symbols {
		net += s.Net
		pnl += s.PnL
	}
	if math.Abs(net-(st.NAV-r.InitialNAV)) > 1e-9*r.InitialNAV || math.Abs(pnl-(r.LongPnL+r.ShortPnL)) > 1e-9*r.InitialNAV {
		t.Fatalf("symbol sums net=%v pnl=%v vs nav change %v / leg pnl %v", net, pnl, st.NAV-r.InitialNAV, r.LongPnL+r.ShortPnL)
	}
	if r.Funding <= 0 || r.Fees <= 0 || r.Slippage <= 0 {
		t.Fatalf("costs missing: %+v", r)
	}
}

// Enabling attribution must not move any ledger number: NAV after every step and the final state are bit-identical.
func TestAttributionDoesNotChangeLedger(t *testing.T) {
	a, navA := attrScenario(t, false)
	b, navB := attrScenario(t, true)
	if len(navA) != len(navB) {
		t.Fatal("step count differs")
	}
	for i := range navA {
		if math.Float64bits(navA[i]) != math.Float64bits(navB[i]) {
			t.Fatalf("NAV step %d: %v vs %v", i, navA[i], navB[i])
		}
	}
	sa, sb := a.State(), b.State()
	if sa.Cash != sb.Cash || sa.Fees != sb.Fees || sa.Slippage != sb.Slippage || sa.Funding != sb.Funding || sa.Turnover != sb.Turnover {
		t.Fatalf("state differs: %+v vs %+v", sa, sb)
	}
	if a.Attribution() != nil {
		t.Fatal("disabled book reports attribution")
	}
}
