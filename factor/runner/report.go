package runner

import (
	"math"
	"strconv"

	"github.com/banbox/banbot/factor/backtest"
)

// Episode lists in Result.Pairs are bounded by a total budget shared by all pairs (each pair keeps at least
// minPairEpisodes); further episodes are only counted in EpisodeCount.
const (
	totalPairEpisodes = 400
	minPairEpisodes   = 10
)

func pairEpisodeCap(pairs int) int { return max(minPairEpisodes, totalPairEpisodes/max(pairs, 1)) }

// maxTopSymbols bounds Attribution.TopSymbols.
const maxTopSymbols = 20

// PairEpisode is one holding of a statistical-arbitrage pair, from the decision that opened it to the decision
// that closed it (or the end of the run).
type PairEpisode struct {
	StartMS, EndMS int64
	Dir            int     // +1 long the spread (long Y / short X), -1 short the spread
	EntryZ, ExitZ  float64 // z-score at entry / at the closing decision (0 for leg_missing and end)
	Beta           float64 // hedge ratio frozen at entry
	Bars           int     // decisions the pair was held (entry bar included, closing bar excluded)
	// Reason: exit (|z| <= exit band), stop (|z| >= stop), maxhold, leg_missing (a leg had no data and the pair
	// was closed and paused), end (still open when the run ended).
	Reason string
	// PnL is the episode's gross contribution to strategy NAV as a fraction of the initial NAV (see PairResult).
	PnL float64
}

// PairResult is the stand-alone result of one pair of a RegisterPairs strategy.
//
// Attribution basis: at every decision the pair's own target weights (wY, wX: the two terms the builder adds to
// the net portfolio) are held until the next decision and earn wY*rY + wX*rX, with rY / rX the simple return of
// each leg between two consecutive decision times (from the builder's input price field), scaled by the NAV of the
// earlier decision. PnL is the running sum as a fraction of the initial NAV. It is an approximation of the ledger:
// fills happen later at their own prices with slippage and fees, weights drift between decisions and the book
// nets pairs that share a symbol. Costs are not allocated to pairs; Turnover (sum of |change of wY| + |change of
// wX| at the decisions, in NAV fractions) is the proxy for a pair's cost.
type PairResult struct {
	Y, X         string
	Episodes     []PairEpisode // at most 200 (the first ones); EpisodeCount is the true number
	EpisodeCount int
	PnL          float64
	Trades, Wins int
	WinRate      float64
	InMarketBars int // decisions with an open position
	Bars         int // decisions with both legs valid and a full regression window
	PausedBars   int // decisions skipped because a leg had no data
	AvgHoldBars  float64
	LastBeta     float64 // hedge ratio frozen at the latest entry
	Turnover     float64
	// Unavailable ("leg_unavailable") marks a pair skipped for the whole run because UnavailableLeg (one or both legs,
	// comma separated) has no data inside the run window; the pair has no bars and no trades (BtFeatureLevel 11).
	Unavailable    string `json:",omitempty"`
	UnavailableLeg string `json:",omitempty"`
}

// SymbolContribution is one instrument's share of the strategy result. PnL is the price-move profit of the held
// quantity (before costs), Costs = fees + slippage + funding + PIT exit costs, Net = PnL - Costs, all in currency
// and summing over all instruments to the NAV change. NetPct = Net / initial NAV; Share = Net / sum(|Net|) over
// all instruments (in [-1, 1]; a value near +-1 means one instrument decides the result).
type SymbolContribution struct {
	Symbol                         string
	PnL, Costs, Net, NetPct, Share float64
	Turnover                       float64
}

// Attribution splits the weights-mode (Book) result into long leg, short leg, costs and instruments.
// Currency amounts are in the Book's currency; costs are positive numbers.
//
//	FinalNAV - InitialNAV = LongPnL + ShortPnL - Fees - Slippage - Funding - PITCost + Residual
//
// Residual is float rounding only (|Residual| < 1e-9 x InitialNAV is asserted in tests). LongPnL / ShortPnL are
// the price-move profit of positions held long / short (the sign of the position when the price moved), before
// costs. LongTurnover / ShortTurnover are traded notional per leg (a sign flip counts on both legs); their sum
// equals Book.Turnover. AvgLongExposure / AvgShortExposure are the mean of long (short) gross position value /
// NAV over the decision times.
type Attribution struct {
	InitialNAV, FinalNAV              float64
	LongPnL, ShortPnL                 float64
	LongPnLPct, ShortPnLPct           float64 // of the initial NAV
	LongTurnover, ShortTurnover       float64
	AvgLongExposure, AvgShortExposure float64
	ExposureSamples                   int
	Fees, Slippage, Funding, PITCost  float64
	Residual                          float64
	TopSymbols                        []SymbolContribution `json:",omitempty"`
}

func fin(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

func buildAttribution(r *backtest.AttributionReport, symbols map[int32]string) *Attribution {
	if r == nil {
		return nil
	}
	a := &Attribution{InitialNAV: fin(r.InitialNAV), FinalNAV: fin(r.NAV), LongPnL: fin(r.LongPnL), ShortPnL: fin(r.ShortPnL),
		LongTurnover: fin(r.LongTurnover), ShortTurnover: fin(r.ShortTurnover), AvgLongExposure: fin(r.AvgLongExposure), AvgShortExposure: fin(r.AvgShortExposure),
		ExposureSamples: r.Samples, Fees: fin(r.Fees), Slippage: fin(r.Slippage), Funding: fin(r.Funding), PITCost: fin(r.PITCost), Residual: fin(r.Residual)}
	if r.InitialNAV > 0 {
		a.LongPnLPct, a.ShortPnLPct = fin(r.LongPnL/r.InitialNAV), fin(r.ShortPnL/r.InitialNAV)
	}
	var abs float64
	for _, s := range r.Symbols {
		abs += math.Abs(s.Net)
	}
	for i, s := range r.Symbols {
		if i >= maxTopSymbols {
			break
		}
		name := symbols[s.SID]
		if name == "" {
			name = "sid:" + strconv.FormatInt(int64(s.SID), 10)
		}
		c := SymbolContribution{Symbol: name, PnL: fin(s.PnL), Costs: fin(s.Costs), Net: fin(s.Net), Turnover: fin(s.Turnover)}
		if r.InitialNAV > 0 {
			c.NetPct = fin(s.Net / r.InitialNAV)
		}
		if abs > 0 {
			c.Share = fin(s.Net / abs)
		}
		a.TopSymbols = append(a.TopSymbols, c)
	}
	return a
}

// runObservers lets strategy state created inside the portfolio factory report back into Result without
// changing the builder's output. Set by Run before the factory is invoked; nil for live engines.
type runObservers struct {
	pairs func(initialNAV float64) []PairResult
	reset func() // clears the pairs bookkeeping at the start of a Run
}
