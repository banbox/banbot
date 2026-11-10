package execution

import (
	"math"
	"testing"

	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
)

// rita round1: four-quadrant slippage direction check + range/limit/TP behaviour (053).
func TestRitaSlippageQuadrants(t *testing.T) {
	const startMS = int64(1_700_000_040_000)
	p := OHLCProfile{LegacyIntrabar: true, SlippageBps: 10}
	p0 := OHLCProfile{LegacyIntrabar: true}
	bar := &orm.SeriesOHLCV{Time: startMS, Open: 100, High: 110, Low: 90, Close: 105}
	// pending market orders: long open (buy), long close (sell), short open (sell), short close (buy)
	for _, c := range []struct {
		name        string
		isBuy       bool
		enter, shrt bool
	}{{"long open", true, true, false}, {"long close", false, false, false}, {"short open", false, true, true}, {"short close", true, false, true}} {
		o := OHLCOrder{OrderType: banexg.OdTypeMarket, IsBuy: c.isBuy, Enter: c.enter, Short: c.shrt, CreateAt: startMS}
		b, _ := p0.MatchPending(bar, o, 60, 15)
		g, _ := p.MatchPending(bar, o, 60, 15)
		if c.isBuy && !(g.Price > b.Price) || !c.isBuy && !(g.Price < b.Price) {
			t.Errorf("%s: wrong direction base=%v slipped=%v", c.name, b.Price, g.Price)
		}
	}
	// stop entry (market) buy with gap-up open above trigger: fills at open*(1+bp), never at the better trigger
	gapUp := &orm.SeriesOHLCV{Time: startMS, Open: 120, High: 125, Low: 118, Close: 121}
	g, ok := p.MatchPending(gapUp, OHLCOrder{OrderType: banexg.OdTypeMarket, IsBuy: true, Enter: true, Stop: 110, CreateAt: startMS}, 60, 15)
	if !ok || math.Abs(g.Price-120*1.001) > 1e-9 {
		t.Errorf("buy-stop gap: ok=%v price=%v", ok, g.Price)
	}
	// stop-limit entry: no slippage
	g, _ = p.MatchPending(bar, OHLCOrder{OrderType: "stop_limit", IsBuy: true, Enter: true, Stop: 95, Price: 96, CreateAt: startMS}, 60, 15)
	t.Logf("stop_limit entry price %v (order type contains limit -> unslipped)", g.Price)
	// TP market of a long at 109 inside [90,110]: sells 10bp below trigger
	f := p.MatchProtection(bar, false, nil, &OHLCProtection{Price: 109}, 0, 60, startMS+60000)
	if !f.Ready || math.Abs(f.Price-109*0.999) > 1e-9 {
		t.Errorf("tp price %v", f.Price)
	}
	// TP limit: unslipped
	f = p.MatchProtection(bar, false, nil, &OHLCProtection{Price: 109, Limit: 108}, 0, 60, startMS+60000)
	if f.Price != 108 {
		t.Errorf("tp limit slipped: %v", f.Price)
	}
	// slipped buy at the bar high leaves the bar range (adverse direction) - documented behaviour
	hi := &orm.SeriesOHLCV{Time: startMS, Open: 110, High: 110, Low: 90, Close: 95}
	g, _ = p.MatchPending(hi, OHLCOrder{OrderType: banexg.OdTypeMarket, IsBuy: true, Enter: true, CreateAt: startMS}, 60, 15)
	t.Logf("market buy at/near high: price %v vs high %v", g.Price, hi.High)
}
