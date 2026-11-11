package runner

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
)

type pairsHarness struct {
	t       *testing.T
	build   PortfolioBuilder
	tracker *pairsTracker
	seq     uint64
	at      int64
}

func newPairsHarness(t *testing.T, band statistics.BandConfig, pairs ...Pair) *pairsHarness {
	t.Helper()
	builder, tracker, err := newTrackedPairsBuilder(PairsSpec{Pairs: pairs, Window: 5, Gross: 1 / float64(len(pairs)), Band: band}, map[int32]string{1: "Y", 2: "X", 3: "Z"})
	if err != nil {
		t.Fatal(err)
	}
	return &pairsHarness{t: t, build: builder, tracker: tracker}
}

// step feeds one decision of prices; a non-positive price means the leg has no data.
func (h *pairsHarness) step(y, x float64) map[int32]float64 {
	h.t.Helper()
	h.seq++
	h.at += 3600000
	vals := map[int32]factor.Numeric{}
	for sid, p := range map[int32]float64{1: y, 2: x} {
		if p > 0 {
			vals[sid] = factor.Numeric{Value: math.Log(p), Validity: factor.Valid}
		}
	}
	frame := factor.Frame{DecisionTime: h.at, Values: map[string]map[int32]factor.Numeric{pairsLogOutput: vals}}
	spec := factor.PortfolioSpec{StrategyID: "s", AccountID: "a", SnapshotID: "x", PlanHash: "p", FactorPlanHash: "f", UniverseVersion: "u",
		Budget: factor.FrozenBudget{Version: "b", Currency: "USD", NAV: 1000}, PlanSequence: h.seq, DecisionTime: h.at, ExecutableAt: h.at + 1, ExpireAt: h.at + 1000000, Mode: factor.Full}
	p, _, err := h.build(frame, factor.Universe{Version: "u", Tradable: []int32{1, 2, 3}}, spec, research.PortfolioDefinition{})
	if err != nil {
		h.t.Fatal(err)
	}
	if p == nil {
		return nil
	}
	return p.Targets()
}

func TestPairsReportEpisodesAndPnL(t *testing.T) {
	h := newPairsHarness(t, statistics.BandConfig{Entry: 1, Exit: .2, MaxHold: 2}, Pair{Y: "Y", X: "X"})
	for _, x := range []float64{1.00, 1.03, 0.98, 1.05, 1.01} {
		h.step(x*1.001, x)
	}
	w0 := h.step(1.41, 1.01) // opens short the spread
	if len(w0) != 2 || w0[1] >= 0 {
		t.Fatalf("pair did not open: %v", w0)
	}
	// y falls back by 10% and x rises 2% in the next period: the held weights earn exactly that move
	y1, x1 := 1.41*0.9, 1.01*1.02
	h.step(y1, x1)
	want := (w0[1]*(y1/1.41-1) + w0[2]*(x1/1.01-1)) * 1000
	for i := 0; i < 6 && len(h.tracker.states[0].stats.episodes) == 0; i++ {
		h.step(y1, x1)
	}
	rep := h.tracker.report(1000)
	if len(rep) != 1 {
		t.Fatalf("report: %+v", rep)
	}
	r := rep[0]
	if r.Y != "Y" || r.X != "X" || r.EpisodeCount != 1 || r.Trades != 1 || len(r.Episodes) != 1 {
		t.Fatalf("pair result: %+v", r)
	}
	ep := r.Episodes[0]
	if ep.Dir != -1 || ep.Beta == 0 || ep.EntryZ < 1 || ep.Reason != "maxhold" || ep.EndMS <= ep.StartMS || ep.Bars != 2 {
		t.Fatalf("episode: %+v", ep)
	}
	if math.Abs(ep.PnL*1000-want) > 1e-9*math.Abs(want)+1e-9 {
		t.Fatalf("episode pnl %v want %v", ep.PnL*1000, want)
	}
	if math.Abs(r.PnL-ep.PnL) > 1e-12 || r.InMarketBars != ep.Bars || r.AvgHoldBars != float64(ep.Bars) || r.Turnover <= 0 || r.LastBeta != ep.Beta {
		t.Fatalf("aggregate: %+v", r)
	}
	if (r.Wins == 1) != (ep.PnL > 0) || r.WinRate != float64(r.Wins) {
		t.Fatalf("win accounting: %+v", r)
	}
	if _, err := json.Marshal(rep); err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", ep)
}

// A leg that loses data closes the pair (leg_missing), counts paused bars and does not accrue a bogus return when
// the data comes back; an open position at report time is reported with reason end.
func TestPairsReportLegMissingPausedAndOpenAtEnd(t *testing.T) {
	h := newPairsHarness(t, statistics.BandConfig{Entry: 1, Exit: .2}, Pair{Y: "Y", X: "X"})
	for _, x := range []float64{1.00, 1.03, 0.98, 1.05, 1.01} {
		h.step(x*1.001, x)
	}
	h.step(1.41, 1.01)
	if w := h.step(1.41, 0); len(w) != 0 {
		t.Fatalf("must be flat: %v", w)
	}
	h.step(1.41, 0)
	r := h.tracker.report(1000)[0]
	if r.PausedBars != 2 || r.EpisodeCount != 1 || r.Episodes[0].Reason != "leg_missing" {
		t.Fatalf("after missing leg: %+v", r)
	}
	pnl := r.PnL
	for _, x := range []float64{1.02, 0.99, 1.04, 1.0} {
		h.step(x*1.001, x)
	}
	h.step(1.4, 1.0) // reopen
	r = h.tracker.report(1000)[0]
	last := r.Episodes[len(r.Episodes)-1]
	if r.EpisodeCount != 2 || last.Reason != "end" || last.EndMS != h.at {
		t.Fatalf("open episode: %+v", r)
	}
	if r.Trades != 2 {
		t.Fatalf("trades %d", r.Trades)
	}
	_ = pnl
	again := h.tracker.report(1000)[0]
	if again.EpisodeCount != r.EpisodeCount || len(h.tracker.states[0].stats.episodes) != 1 {
		t.Fatal("report mutated tracker state")
	}
}

func TestPairsReportEpisodeCap(t *testing.T) {
	s := pairStats{episodeCap: pairEpisodeCap(1)}
	for i := 0; i < totalPairEpisodes+30; i++ {
		s.open = &PairEpisode{StartMS: int64(i), Dir: 1}
		s.closeEpisode(int64(i+1), 0, "exit")
	}
	if s.episodeCount != totalPairEpisodes+30 || len(s.episodes) != totalPairEpisodes || s.trades != totalPairEpisodes+30 || pairEpisodeCap(100) != minPairEpisodes || pairEpisodeCap(4) != 100 {
		t.Fatalf("cap: count=%d kept=%d", s.episodeCount, len(s.episodes))
	}
}
