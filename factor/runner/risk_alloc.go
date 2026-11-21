package runner

import (
	"errors"
	"maps"
	"math"
	"slices"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"github.com/banbox/banbot/factor/statistics"
)

type riskHistoryTracker struct {
	maxRows    int
	lastGrid   int64
	lastPrices map[int32]float64
	rows       []factor.RiskReturnRow
}

func newRiskHistoryTracker(window int) *riskHistoryTracker {
	maxRows := max(window*2, 64)
	return &riskHistoryTracker{
		maxRows:    maxRows,
		lastPrices: map[int32]float64{},
	}
}

func (t *riskHistoryTracker) clone() *riskHistoryTracker {
	if t == nil {
		return nil
	}
	rows := make([]factor.RiskReturnRow, len(t.rows))
	for i, r := range t.rows {
		rows[i] = factor.RiskReturnRow{GridTime: r.GridTime, Returns: maps.Clone(r.Returns)}
	}
	return &riskHistoryTracker{
		maxRows:    t.maxRows,
		lastGrid:   t.lastGrid,
		lastPrices: maps.Clone(t.lastPrices),
		rows:       rows,
	}
}

func (t *riskHistoryTracker) observePrices(grid int64, prices map[int32]float64) {
	if t == nil || grid <= t.lastGrid || len(prices) == 0 {
		return
	}
	ret := make(map[int32]float64, len(prices))
	nextPrices := make(map[int32]float64, len(prices))
	for sid, p := range prices {
		if p <= 0 || math.IsNaN(p) || math.IsInf(p, 0) {
			continue
		}
		nextPrices[sid] = p
		if prev, ok := t.lastPrices[sid]; ok && prev > 0 {
			r := p/prev - 1
			if !math.IsNaN(r) && !math.IsInf(r, 0) {
				ret[sid] = r
			}
		}
	}
	t.lastPrices = nextPrices
	t.lastGrid = grid
	if len(ret) > 0 {
		t.rows = append(t.rows, factor.RiskReturnRow{GridTime: grid, Returns: ret})
		if len(t.rows) > t.maxRows {
			excess := len(t.rows) - t.maxRows
			copy(t.rows, t.rows[excess:])
			clear(t.rows[len(t.rows)-excess:])
			t.rows = t.rows[:t.maxRows]
		}
	}
}

func (t *riskHistoryTracker) returnRows() []factor.RiskReturnRow {
	if t == nil || len(t.rows) == 0 {
		return nil
	}
	out := make([]factor.RiskReturnRow, len(t.rows))
	for i, r := range t.rows {
		out[i] = factor.RiskReturnRow{GridTime: r.GridTime, Returns: maps.Clone(r.Returns)}
	}
	return out
}

func (t *riskHistoryTracker) rollingVolatility(sid int32, window int) (float64, bool) {
	if t == nil || len(t.rows) < 2 {
		return 0, false
	}
	var vals []float64
	for i := len(t.rows) - 1; i >= 0 && len(vals) < window; i-- {
		v, ok := t.rows[i].Returns[sid]
		if !ok || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		vals = append(vals, v)
	}
	if len(vals) < 2 {
		return 0, false
	}
	mean := 0.0
	for _, v := range vals {
		mean += v
	}
	mean /= float64(len(vals))
	sumSq := 0.0
	for _, v := range vals {
		d := v - mean
		sumSq += d * d
	}
	sigma := math.Sqrt(sumSq / float64(len(vals)-1))
	if sigma <= 0 || math.IsNaN(sigma) || math.IsInf(sigma, 0) {
		return 0, false
	}
	return sigma, true
}

func (t *riskHistoryTracker) rollingMarketBeta(sid int32, window int) (float64, bool) {
	if t == nil || len(t.rows) < 2 {
		return 1.0, false
	}
	var assetVals, mktVals []float64
	for i := len(t.rows) - 1; i >= 0 && len(assetVals) < window; i-- {
		row := t.rows[i].Returns
		ri, ok := row[sid]
		if !ok || math.IsNaN(ri) || math.IsInf(ri, 0) || len(row) == 0 {
			continue
		}
		mktSum := 0.0
		mktCnt := 0
		for _, v := range row {
			if !math.IsNaN(v) && !math.IsInf(v, 0) {
				mktSum += v
				mktCnt++
			}
		}
		if mktCnt == 0 {
			continue
		}
		assetVals = append(assetVals, ri)
		mktVals = append(mktVals, mktSum/float64(mktCnt))
	}
	if len(assetVals) < 2 {
		return 1.0, false
	}
	meanA, meanM := 0.0, 0.0
	for i := range assetVals {
		meanA += assetVals[i]
		meanM += mktVals[i]
	}
	n := float64(len(assetVals))
	meanA /= n
	meanM /= n
	covAM, varM := 0.0, 0.0
	for i := range assetVals {
		da := assetVals[i] - meanA
		dm := mktVals[i] - meanM
		covAM += da * dm
		varM += dm * dm
	}
	if varM <= 1e-24 {
		return 1.0, true
	}
	beta := covAM / varM
	if math.IsNaN(beta) || math.IsInf(beta, 0) {
		return 1.0, false
	}
	return beta, true
}

func candidatePriceField(c Config, plan *factor.Plan) (source, timeframe, field string) {
	tf := plan.TimeFrame()
	for _, in := range plan.Inputs() {
		if in.Source == "kline" && in.TimeFrame == tf {
			for _, f := range in.Fields {
				if f == "close" {
					return "kline", tf, "close"
				}
			}
		}
	}
	if c.Factor.Source != "" && c.Factor.TimeFrame != "" && c.Factor.Field != "" {
		for _, in := range plan.Inputs() {
			if in.Source == c.Factor.Source && in.TimeFrame == c.Factor.TimeFrame {
				return c.Factor.Source, c.Factor.TimeFrame, c.Factor.Field
			}
		}
	}
	for _, in := range plan.Inputs() {
		for _, f := range in.Fields {
			if f == "close" || f == "price" {
				return in.Source, in.TimeFrame, f
			}
		}
	}
	return "", "", ""
}

func (p *policyRun) observeSnapshot(c Config, plan *factor.Plan, snapshot *factor.Snapshot, visible []factor.VersionRecord, quotes map[int32]backtest.Quote, grid, now int64) {
	if p == nil || p.riskTracker == nil || snapshot == nil {
		return
	}
	spec := snapshot.Spec()
	sids := append(append([]int32(nil), spec.Universe.Investable...), spec.Universe.Reference...)
	slices.Sort(sids)
	sids = slices.Compact(sids)
	src, tf, fld := candidatePriceField(c, plan)
	prices := make(map[int32]float64, len(sids))
	fromSnapshot := make(map[int32]bool, len(sids))
	for _, sid := range sids {
		if src != "" {
			n := snapshot.Numeric(sid, src, tf, fld)
			if n.Validity == factor.Valid && n.Value > 0 && !math.IsNaN(n.Value) && !math.IsInf(n.Value, 0) {
				prices[sid] = n.Value
				fromSnapshot[sid] = true
				continue
			}
		}
		if q, ok := quotes[sid]; ok && q.Price > 0 && q.AtMS <= grid && q.AvailableAt <= now {
			prices[sid] = q.Price
		}
	}
	if len(fromSnapshot) < len(sids) && len(visible) > 0 && c.Prices.Source != "" {
		latestAt := map[int32]int64{}
		for _, r := range visible {
			if fromSnapshot[r.Series.Sid] {
				continue
			}
			if r.Series.Source != c.Prices.Source || r.Series.TimeFrame != c.Prices.TimeFrame || r.EventTime > grid || r.AvailableAt > now {
				continue
			}
			if _, has := latestAt[r.Series.Sid]; has && r.EventTime <= latestAt[r.Series.Sid] {
				continue
			}
			n := factor.Number(r.Series.Values, c.Prices.Field)
			if n.Validity == factor.Valid && n.Value > 0 {
				prices[r.Series.Sid] = n.Value
				latestAt[r.Series.Sid] = r.EventTime
			}
		}
	}
	p.riskTracker.observePrices(grid, prices)
}

func (p *policyRun) observeLiveRows(c Config, plan *factor.Plan, rows map[factor.StreamKey]factor.VersionRecord, quotes map[int32]backtest.Quote, grid, cutoff int64) {
	if p == nil || p.riskTracker == nil {
		return
	}
	src, tf, fld := candidatePriceField(c, plan)
	sids := append(append([]int32(nil), c.Snapshot.Universe.Investable...), c.Snapshot.Universe.Reference...)
	prices := make(map[int32]float64, len(sids))
	for _, sid := range sids {
		if _, done := prices[sid]; done {
			continue
		}
		if src != "" {
			if row, ok := rows[factor.StreamKey{SID: sid, Source: src, TimeFrame: tf}]; ok && row.EventTime <= grid && row.AvailableAt <= cutoff {
				n := factor.Number(row.Series.Values, fld)
				if n.Validity == factor.Valid && n.Value > 0 && !math.IsNaN(n.Value) && !math.IsInf(n.Value, 0) {
					prices[sid] = n.Value
					continue
				}
			}
		}
		if q, ok := quotes[sid]; ok && q.Price > 0 && q.AtMS <= grid && q.AvailableAt <= cutoff {
			prices[sid] = q.Price
		}
	}
	p.riskTracker.observePrices(grid, prices)
}

func (p *policyRun) populateRiskContext(ctx *factor.PortfolioContext, frame factor.Frame, riskOnly bool) {
	ctx.RiskOptimizer = runnerSideRiskOptimizer
	if riskOnly {
		if p.allocCfg.BetaCap > 0 && len(p.lastBeta) > 0 {
			ctx.Beta = maps.Clone(p.lastBeta)
		}
		return
	}
	// When default "volatility" or "beta" is part of explicit score Combo.Columns (e.g. z-scored
	// low-vol factor in momentum-vol), do not mistake that score sub-factor for raw risk sigma/beta;
	// strip it from ctx.Frame before LifecyclePolicy.Propose runs (score is already combined).
	effectiveFrameCfg := p.allocCfg
	skipDefaultVol := effectiveFrameCfg.VolatilityColumn == "" && slices.Contains(p.comboCols, factor.VolatilityOutput)
	skipDefaultBeta := effectiveFrameCfg.BetaColumn == "" && slices.Contains(p.comboCols, factor.BetaOutput)
	if skipDefaultVol {
		delete(ctx.Frame.Values, factor.VolatilityOutput)
	}
	if skipDefaultBeta {
		delete(ctx.Frame.Values, factor.BetaOutput)
	}
	volFromFrame, betaFromFrame := factor.PopulateContextRiskFromFrame(ctx, effectiveFrameCfg)
	if p.riskTracker != nil {
		ctx.ReturnHistory = p.riskTracker.returnRows()
		window := p.allocCfg.EffectiveRiskWindow()
		if p.allocCfg.NeedsRiskInput() {
			if !volFromFrame {
				if ctx.Volatility == nil {
					ctx.Volatility = map[int32]float64{}
				}
				for _, sid := range ctx.Universe.Investable {
					if _, exists := ctx.Volatility[sid]; !exists {
						if sigma, ok := p.riskTracker.rollingVolatility(sid, window); ok {
							ctx.Volatility[sid] = sigma
						}
					}
				}
				ctx.AutoVolatilityFallback = true
			} else {
				volCol := p.allocCfg.VolatilityColumn
				if volCol == "" {
					volCol = factor.VolatilityOutput
				}
				allMissingWarmup := true
				anyWarmup := false
				for _, sid := range ctx.Universe.Investable {
					if _, validPos := ctx.Volatility[sid]; validPos {
						continue
					}
					n, exists := frame.Values[volCol][sid]
					if exists && n.Validity == factor.Warmup {
						anyWarmup = true
						if sigma, ok := p.riskTracker.rollingVolatility(sid, window); ok {
							ctx.Volatility[sid] = sigma
						}
					} else {
						allMissingWarmup = false
					}
				}
				if anyWarmup && allMissingWarmup {
					ctx.AutoVolatilityFallback = true
				}
			}
		}
		if p.allocCfg.BetaCap > 0 {
			if !betaFromFrame {
				if ctx.Beta == nil {
					ctx.Beta = map[int32]float64{}
				}
				for _, sid := range ctx.Universe.Investable {
					if _, exists := ctx.Beta[sid]; !exists {
						beta, _ := p.riskTracker.rollingMarketBeta(sid, window)
						ctx.Beta[sid] = beta
					}
				}
			} else {
				betaCol := p.allocCfg.BetaColumn
				if betaCol == "" {
					betaCol = factor.BetaOutput
				}
				for _, sid := range ctx.Universe.Investable {
					if _, exists := ctx.Beta[sid]; exists {
						continue
					}
					if n, ok := frame.Values[betaCol][sid]; ok && n.Validity == factor.Warmup {
						beta, _ := p.riskTracker.rollingMarketBeta(sid, window)
						ctx.Beta[sid] = beta
					}
				}
			}
		}
	}
	if len(ctx.Beta) > 0 {
		if p.lastBeta == nil {
			p.lastBeta = map[int32]float64{}
		}
		for sid, b := range ctx.Beta {
			p.lastBeta[sid] = b
		}
	}
}

func runnerSideRiskOptimizer(method string, cov [][]float64, sideScores []float64, cfg factor.AllocationConfig) ([]float64, error) {
	switch method {
	case "erc", "risk-parity":
		return factor.EqualRiskContribution(cov)
	case "hrp":
		res, err := statistics.HierarchicalRiskParity(cov)
		if err != nil {
			return nil, err
		}
		return res.Weights, nil
	case "min-variance":
		if len(cov) <= 128 {
			res, err := statistics.LongOnlyMinimumVariance(cov, 2000, 1e-10)
			if err == nil {
				return res.Weights, nil
			}
		}
		// Fallback for n > 128 or borderline conditioning via research.OptimizePortfolio:
		zeros := make([]float64, len(cov))
		for i := range zeros {
			zeros[i] = 1.0
		}
		tildeCov, _, err := factor.PrepareMeanVarianceInputs(cov, zeros, 1.0)
		if err != nil {
			return nil, err
		}
		opt, err := research.OptimizePortfolio(research.OptimizationSpec{
			ExpectedReturns: zeros,
			Covariance:      tildeCov,
			RiskAversion:    1.0,
			Iterations:      4000,
			Constraints:     research.RiskConstraints{Gross: 1e6, LongOnly: true},
		})
		if err != nil || opt.Status != "feasible" {
			return nil, errors.New("runner: min-variance optimization failed")
		}
		return opt.Weights, nil
	case "mean-variance":
		ra := cfg.EffectiveRiskAversion()
		tildeCov, mu, err := factor.PrepareMeanVarianceInputs(cov, sideScores, ra)
		if err != nil {
			return nil, err
		}
		opt, err := research.OptimizePortfolio(research.OptimizationSpec{
			ExpectedReturns: mu,
			Covariance:      tildeCov,
			RiskAversion:    ra,
			Iterations:      4000,
			Constraints:     research.RiskConstraints{Gross: 1e6, LongOnly: true},
		})
		if err != nil || opt.Status != "feasible" {
			return nil, errors.New("runner: mean-variance optimization failed")
		}
		return opt.Weights, nil
	default:
		return nil, errors.New("runner: unsupported risk allocation method")
	}
}
