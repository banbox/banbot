package biz

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/strat"
	"github.com/shopspring/decimal"
)

func TestHubLadderRatesMatchSharedCutFraction(t *testing.T) {
	l, err := newHubLadder(&strat.EnterReq{TakeProfits: []strat.TakeProfitTarget{{Price: 110, ExitRate: 0.3}, {Price: 105, ExitRate: 0.3}, {Price: 120, ExitRate: 0.4}}}, 100)
	if err != nil || l.Rungs[0].Price != 105 || l.Rungs[2].Price != 120 {
		t.Fatal(l, err)
	}
	want := []string{"0.3", "0.4285714285714286", "1"}
	for i, w := range want {
		got := l.tpSpec().Rate
		if got.Sub(decimal.RequireFromString(w)).Abs().GreaterThan(decimal.RequireFromString("0.000001")) || !l.tpSpec().Level.Equal(decimal.NewFromFloat(l.Rungs[i].Price)) {
			t.Fatalf("rung %d rate %s want %s", i, got, w)
		}
		l.advance()
	}
	if l.tpSpec().Level.IsPositive() {
		t.Fatal("finished ladder must not install a take-profit")
	}
	if _, err = newHubLadder(&strat.EnterReq{Short: true, TakeProfits: []strat.TakeProfitTarget{{Rate: 1.5}}}, 100); err == nil {
		t.Fatal("non-positive resolved rung must be refused")
	}
}

func TestTightenStopNeverLoosens(t *testing.T) {
	avg := decimal.NewFromInt(100)
	long := tightenStop(protectionStop(90, 0), decimal.Zero, avg, false)
	tight := tightenStop(protectionStop(102, 0), decimal.Zero, avg, false)
	short := tightenStop(protectionStop(110, 0), decimal.Zero, avg, true)
	shortTight := tightenStop(protectionStop(98, 0), decimal.Zero, avg, true)
	dist := tightenStop(protectionStop(0, 5), decimal.Zero, avg, false)
	for i, c := range []struct{ got, want int64 }{{long.Level.IntPart(), 100}, {tight.Level.IntPart(), 102}, {short.Level.IntPart(), 100}, {shortTight.Level.IntPart(), 98}, {dist.Level.IntPart(), 100}} {
		if c.got != c.want {
			t.Fatalf("case %d: %d != %d", i, c.got, c.want)
		}
	}
}

func TestHubJournalPlansAreAdditive(t *testing.T) {
	body, _ := json.Marshal(hubOrderJournal{})
	if strings.Contains(string(body), "Plans") || strings.Contains(string(body), "Followups") {
		t.Fatal("empty plans must not change the journal format")
	}
	var old hubOrderJournal
	if err := json.Unmarshal([]byte(`{"Version":"v","Lots":{}}`), &old); err != nil || old.Plans != nil {
		t.Fatal(err)
	}
}

func protectionStop(level, dist int64) execution.PositionProtection {
	return execution.PositionProtection{Kind: execution.StopLossProtection, Level: decimal.NewFromInt(level), Distance: decimal.NewFromInt(dist)}
}
