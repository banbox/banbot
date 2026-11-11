package runner

import (
	"math"

	"github.com/banbox/banbot/factor/statistics"
)

// pairStats is the per-pair bookkeeping behind Result.Pairs. It is written from the pairs builder but never read
// by its trading logic, so it cannot change weights. PnL values are in currency until report() scales them.
type pairStats struct {
	pnl, turnover  float64
	trades, wins   int
	inMarket, bars int
	paused         int
	episodeCount   int
	episodes       []PairEpisode
	open           *PairEpisode
	episodeCap     int
	lastBeta       float64
	prevLY, prevLX float64
	prevOK         bool
	wY, wX         float64 // weights targeted at the previous decision (held until this one)
}

// accrue books the return of the held weights between the previous decision and this one (log prices ly, lx),
// scaled by the NAV of the previous decision, then remembers the prices.
func (s *pairStats) accrue(ly, lx, navPrev float64) {
	if s.prevOK && (s.wY != 0 || s.wX != 0) && navPrev > 0 {
		pnl := (s.wY*math.Expm1(ly-s.prevLY) + s.wX*math.Expm1(lx-s.prevLX)) * navPrev
		if !math.IsNaN(pnl) && !math.IsInf(pnl, 0) {
			s.pnl += pnl
			if s.open != nil {
				s.open.PnL += pnl
			}
		}
	}
	s.prevLY, s.prevLX, s.prevOK = ly, lx, true
}

// observeStep records the band transition of one decision: prev -> pos at z-score z.
func (s *pairStats) observeStep(prev, pos int, z, beta float64, at int64, band statistics.BandConfig) {
	if prev != 0 && pos != prev {
		abs := math.Abs(z)
		reason := "maxhold"
		switch {
		case band.Stop > 0 && abs >= band.Stop:
			reason = "stop"
		case abs <= band.Exit:
			reason = "exit"
		}
		s.closeEpisode(at, z, reason)
	}
	if pos != 0 && s.open == nil {
		s.open = &PairEpisode{StartMS: at, Dir: pos, EntryZ: z, Beta: beta}
		s.lastBeta = beta
	}
	if pos != 0 {
		s.inMarket++
		s.open.Bars++
	}
}

func (s *pairStats) closeEpisode(at int64, z float64, reason string) {
	ep := s.open
	if ep == nil {
		return
	}
	s.open = nil
	ep.EndMS, ep.ExitZ, ep.Reason = at, z, reason
	s.trades++
	if ep.PnL > 0 {
		s.wins++
	}
	s.episodeCount++
	if len(s.episodes) < s.episodeCap {
		s.episodes = append(s.episodes, *ep)
	}
}

// setWeights records the pair's own target weights of this decision and their turnover.
func (s *pairStats) setWeights(wy, wx float64) {
	s.turnover += math.Abs(wy-s.wY) + math.Abs(wx-s.wX)
	s.wY, s.wX = wy, wx
}

// pairsTracker belongs to one builder instance (one run).
type pairsTracker struct {
	states  []*pairState
	names   []Pair
	lastNAV float64 // budget NAV of the latest decision
	lastMS  int64
}

// resetStats clears all result bookkeeping (not the trading state of the builder).
func (t *pairsTracker) resetStats() {
	for _, st := range t.states {
		st.stats = pairStats{episodeCap: pairEpisodeCap(len(t.states))}
	}
	t.lastNAV, t.lastMS = 0, 0
}

// report snapshots the per-pair results; an open position is reported as an episode ending at the last
// decision with reason "end". PnL values become fractions of initialNAV. It does not mutate the tracker.
func (t *pairsTracker) report(initialNAV float64) []PairResult {
	out := make([]PairResult, len(t.states))
	scale := func(v float64) float64 {
		if initialNAV > 0 {
			return fin(v / initialNAV)
		}
		return 0
	}
	for i, st := range t.states {
		s := &st.stats
		r := PairResult{Y: t.names[i].Y, X: t.names[i].X, EpisodeCount: s.episodeCount, Trades: s.trades, Wins: s.wins,
			InMarketBars: s.inMarket, Bars: s.bars, PausedBars: s.paused, LastBeta: fin(s.lastBeta), Turnover: fin(s.turnover), PnL: scale(s.pnl)}
		for _, ep := range s.episodes {
			ep.PnL, ep.EntryZ, ep.ExitZ, ep.Beta = scale(ep.PnL), fin(ep.EntryZ), fin(ep.ExitZ), fin(ep.Beta)
			r.Episodes = append(r.Episodes, ep)
		}
		if ep := s.open; ep != nil {
			r.Trades++
			r.EpisodeCount++
			if ep.PnL > 0 {
				r.Wins++
			}
			if len(r.Episodes) < s.episodeCap {
				o := *ep
				o.EndMS, o.Reason = t.lastMS, "end"
				o.PnL, o.EntryZ, o.Beta = scale(o.PnL), fin(o.EntryZ), fin(o.Beta)
				r.Episodes = append(r.Episodes, o)
			}
		}
		if r.Trades > 0 {
			r.WinRate = float64(r.Wins) / float64(r.Trades)
			r.AvgHoldBars = float64(r.InMarketBars) / float64(r.Trades)
		}
		if st.unavailable != "" {
			r.Unavailable, r.UnavailableLeg = "leg_unavailable", st.unavailable
		}
		out[i] = r
	}
	return out
}
