package statistics

import (
	"errors"
	"math"
	"math/rand"
)

type HRPExperimentConfig struct {
	Seed                                                                         int64
	Trials, Observations, TrainRows, RebalanceRows, BaseAssets, CorrelatedAssets int
	Sigma, NoiseFraction, CostRate                                               float64
}

type HRPNumericalExample struct {
	Seed            int64
	Observations    int
	Parents         []int
	Covariance      [][]float64
	HRP             HRPResult
	IVP             []float64
	MinimumVariance MinimumVarianceResult
}

// HRPAllocationExample reproduces the AFML 16.5/snippet 16.4 experiment:
// 10,000 observations of five independent unit Gaussians and five randomly
// selected parent copies with independent .25-sigma noise. Go's seeded RNG is
// explicit; allocations need not equal the book's NumPy/Python seed sequence.
func HRPAllocationExample(seed int64) (HRPNumericalExample, error) {
	const observations = 10000
	rng := rand.New(rand.NewSource(seed))
	parents := make([]int, 5)
	rows := make([][]float64, observations)
	for i := range rows {
		rows[i] = make([]float64, 10)
		for j := 0; j < 5; j++ {
			rows[i][j] = rng.NormFloat64()
		}
	}
	for j := range parents {
		parents[j] = rng.Intn(5)
	}
	mean := make([]float64, 10)
	for _, row := range rows {
		for j, p := range parents {
			row[5+j] = row[p] + rng.NormFloat64()*.25
		}
		for j, v := range row {
			mean[j] += v / observations
		}
	}
	cov := make([][]float64, 10)
	for i := range cov {
		cov[i] = make([]float64, 10)
	}
	for _, row := range rows {
		for i := range row {
			for j := 0; j <= i; j++ {
				cov[i][j] += (row[i] - mean[i]) * (row[j] - mean[j]) / float64(observations-1)
			}
		}
	}
	for i := range cov {
		for j := 0; j < i; j++ {
			cov[j][i] = cov[i][j]
		}
	}
	hrp, err := HierarchicalRiskParity(cov)
	if err != nil {
		return HRPNumericalExample{}, err
	}
	mv, err := LongOnlyMinimumVariance(cov, 1000, 1e-10)
	if err != nil {
		return HRPNumericalExample{}, err
	}
	ivp := make([]float64, 10)
	total := 0.
	for i := range ivp {
		ivp[i] = 1 / cov[i][i]
		total += ivp[i]
	}
	for i := range ivp {
		ivp[i] /= total
	}
	return HRPNumericalExample{seed, observations, parents, cov, hrp, ivp, mv}, nil
}

func DefaultHRPExperimentConfig() HRPExperimentConfig {
	return HRPExperimentConfig{Seed: 12345, Trials: 10000, Observations: 520, TrainRows: 260, RebalanceRows: 22, BaseAssets: 5, CorrelatedAssets: 5, Sigma: .01, NoiseFraction: .25, CostRate: .001}
}
func (c HRPExperimentConfig) validate() error {
	// Bound each dimension before adding/multiplying user supplied integers.
	if c.BaseAssets < 1 || c.BaseAssets > 32 || c.CorrelatedAssets < 1 || c.CorrelatedAssets > 32 {
		return errors.New("statistics: invalid HRP asset dimensions")
	}
	n := c.BaseAssets + c.CorrelatedAssets
	if c.Trials < 1 || c.Trials > 10000 || c.Observations < 4 || c.Observations > 10000 || c.TrainRows < 2 || c.TrainRows >= c.Observations-1 || c.RebalanceRows < 1 || c.RebalanceRows > c.Observations || c.BaseAssets < 1 || c.CorrelatedAssets < 1 || n > 32 || c.TrainRows <= n || int64(c.Trials)*int64(c.Observations)*int64(n*n) > 600000000 || !afmlFinite(c.Sigma) || c.Sigma <= 0 || c.Sigma > .1 || !afmlFinite(c.NoiseFraction) || c.NoiseFraction <= 0 || c.NoiseFraction > 10 || !afmlFinite(c.CostRate) || c.CostRate < 0 || c.CostRate >= .1 {
		return errors.New("statistics: invalid HRP experiment or work budget")
	}
	return nil
}

type HRPShock struct {
	Row    int
	Assets []int
	Return float64
}
type HRPSyntheticData struct {
	Returns [][]float64
	Parents []int
	Shocks  []HRPShock
}

// GenerateHRPData follows AFML snippet 16.5. The code's sigma default is .01
// (the prose says .10). Parent columns and shock locations are seeded, not fitted.
// Shock assignment overwrites the selected observations, as in the source.
func GenerateHRPData(c HRPExperimentConfig, seed int64) (HRPSyntheticData, error) {
	if err := c.validate(); err != nil {
		return HRPSyntheticData{}, err
	}
	rng := rand.New(rand.NewSource(seed))
	n := c.BaseAssets + c.CorrelatedAssets
	d := HRPSyntheticData{Returns: make([][]float64, c.Observations), Parents: make([]int, c.CorrelatedAssets)}
	for i := range d.Returns {
		d.Returns[i] = make([]float64, n)
		for j := 0; j < c.BaseAssets; j++ {
			d.Returns[i][j] = rng.NormFloat64() * c.Sigma
		}
	}
	for j := range d.Parents {
		d.Parents[j] = rng.Intn(c.BaseAssets)
	}
	for i := range d.Returns {
		for j, p := range d.Parents {
			d.Returns[i][c.BaseAssets+j] = d.Returns[i][p] + rng.NormFloat64()*c.Sigma*c.NoiseFraction
		}
	}
	for _, assets := range [][]int{{d.Parents[0], c.BaseAssets}, {d.Parents[len(d.Parents)-1]}} {
		for _, value := range []float64{-.5, 2} {
			row := c.TrainRows + rng.Intn(c.Observations-1-c.TrainRows)
			for _, asset := range assets {
				d.Returns[row][asset] = value
			}
			d.Shocks = append(d.Shocks, HRPShock{row, append([]int(nil), assets...), value})
		}
	}
	return d, nil
}

type HRPAllocation struct {
	Method                                 string
	TrainStart, TrainEnd, OOSStart, OOSEnd int
	Weights                                []float64
	KKTResidual                            float64
}
type HRPPathMetric struct {
	Method                                                     string
	SourceGrossReturn, NetBuyHoldReturn, TradedNotional, Costs float64
}
type HRPPathResult struct {
	Metrics     []HRPPathMetric
	Allocations []HRPAllocation
}

// EvaluateHRPPath fits only [pointer-TrainRows,pointer), never the OOS block.
// SourceGrossReturn uses snippet 16.5's unchanged weights within each block.
// NetBuyHoldReturn uses drifting fully invested holdings, charges CostRate times
// actual absolute traded weight at each rebalance (including initial investment),
// and multiplies NAV by 1-cost before applying the next observed return.
func EvaluateHRPPath(rows [][]float64, c HRPExperimentConfig) (HRPPathResult, error) {
	if err := c.validate(); err != nil {
		return HRPPathResult{}, err
	}
	n := c.BaseAssets + c.CorrelatedAssets
	if len(rows) != c.Observations {
		return HRPPathResult{}, errors.New("statistics: experiment observation count mismatch")
	}
	for _, row := range rows {
		if len(row) != n {
			return HRPPathResult{}, errors.New("statistics: experiment width mismatch")
		}
		for _, v := range row {
			if !afmlFinite(v) || v <= -1 {
				return HRPPathResult{}, errors.New("statistics: invalid asset simple return")
			}
		}
	}
	methods := []string{"hrp", "ivp", "global-minimum-variance"}
	out := HRPPathResult{Metrics: make([]HRPPathMetric, 3)}
	gross, nav := []float64{1, 1, 1}, []float64{1, 1, 1}
	held := make([][]float64, 3)
	for m := range methods {
		held[m] = make([]float64, n)
		out.Metrics[m].Method = methods[m]
	}
	sums := make([]float64, n)
	products := make([][]float64, n)
	for i := range products {
		products[i] = make([]float64, n)
	}
	update := func(row []float64, sign float64) {
		for i := range row {
			sums[i] += sign * row[i]
			for j := 0; j <= i; j++ {
				products[i][j] += sign * row[i] * row[j]
			}
		}
	}
	for _, row := range rows[:c.TrainRows] {
		update(row, 1)
	}
	for pointer := c.TrainRows; pointer < len(rows); pointer += c.RebalanceRows {
		cov := make([][]float64, n)
		for i := range cov {
			cov[i] = make([]float64, n)
		}
		for i := range cov {
			for j := 0; j <= i; j++ {
				v := (products[i][j] - sums[i]*sums[j]/float64(c.TrainRows)) / float64(c.TrainRows-1)
				cov[i][j] = v
				cov[j][i] = v
			}
		}
		hrp, err := HierarchicalRiskParity(cov)
		if err != nil {
			return HRPPathResult{}, err
		}
		mv, err := LongOnlyMinimumVariance(cov, 1000, 1e-10)
		if err != nil {
			return HRPPathResult{}, err
		}
		ivp := make([]float64, n)
		denom := 0.
		for i := range ivp {
			ivp[i] = 1 / cov[i][i]
			denom += ivp[i]
		}
		for i := range ivp {
			ivp[i] /= denom
		}
		weights := [][]float64{hrp.Weights, ivp, mv.Weights}
		end := min(pointer+c.RebalanceRows, len(rows))
		for m, w := range weights {
			kkt := 0.
			if m == 2 {
				kkt = mv.KKTResidual
			}
			out.Allocations = append(out.Allocations, HRPAllocation{methods[m], pointer - c.TrainRows, pointer, pointer, end, append([]float64(nil), w...), kkt})
			// Solve the self-financing fee equation. Targets are funded from
			// post-fee NAV, while current holdings use pre-fee NAV units.
			lo, hi := 0., 2*c.CostRate
			for step := 0; step < 48; step++ {
				mid := (lo + hi) / 2
				traded := 0.
				for i := range w {
					traded += math.Abs((1-mid)*w[i] - held[m][i])
				}
				if mid < c.CostRate*traded {
					lo = mid
				} else {
					hi = mid
				}
			}
			cost := (lo + hi) / 2
			turn := 0.
			for i := range w {
				turn += math.Abs((1-cost)*w[i] - held[m][i])
			}
			out.Metrics[m].TradedNotional += turn
			out.Metrics[m].Costs += nav[m] * cost
			nav[m] *= 1 - cost
			copy(held[m], w)
			for t := pointer; t < end; t++ {
				sourceRet, heldRet := 0., 0.
				for i, r := range rows[t] {
					sourceRet += w[i] * r
					heldRet += held[m][i] * r
				}
				gross[m] *= 1 + sourceRet
				nav[m] *= 1 + heldRet
				for i, r := range rows[t] {
					held[m][i] *= (1 + r) / (1 + heldRet)
				}
			}
			if !afmlFinite(gross[m]) || !afmlFinite(nav[m]) || nav[m] <= 0 {
				return HRPPathResult{}, errors.New("statistics: experiment NAV overflow/ruin")
			}
		}
		if end < len(rows) {
			for t := pointer; t < end; t++ {
				update(rows[t-c.TrainRows], -1)
				update(rows[t], 1)
			}
		}
	}
	for m := range methods {
		out.Metrics[m].SourceGrossReturn = gross[m] - 1
		out.Metrics[m].NetBuyHoldReturn = nav[m] - 1
	}
	return out, nil
}

type HRPExperimentMetric struct {
	Method                                                                                                  string
	MeanSourceGross, VarianceSourceGross, MeanNetBuyHold, VarianceNetBuyHold, MeanTradedNotional, MeanCosts float64
}
type HRPExperimentResult struct {
	Config      HRPExperimentConfig
	Metrics     []HRPExperimentMetric
	FirstTrial  HRPPathResult
	Assumptions []string
}

func RunHRPExperiment(c HRPExperimentConfig) (HRPExperimentResult, error) {
	if err := c.validate(); err != nil {
		return HRPExperimentResult{}, err
	}
	out := HRPExperimentResult{Config: c, Metrics: make([]HRPExperimentMetric, 3), Assumptions: []string{"AFML 16.5/16.6 and snippet 16.5; sigma=.01 code default rather than prose=.10", "Go seeded RNG; no exact NumPy random-sequence reproduction", "global minimum-variance endpoint; no full CLA frontier", "source gross uses frozen block weights; net uses buy-and-hold drift and self-financing rebalance traded-notional costs", "source shocks overwrite returns with -.5 and +2; asymmetric jumps imply a positive unconditional mean despite the prose zero-mean claim", "synthetic research; no guarantee of method variance ordering"}}
	for trial := 0; trial < c.Trials; trial++ {
		data, err := GenerateHRPData(c, c.Seed+int64(trial))
		if err != nil {
			return HRPExperimentResult{}, err
		}
		path, err := EvaluateHRPPath(data.Returns, c)
		if err != nil {
			return HRPExperimentResult{}, err
		}
		if trial == 0 {
			out.FirstTrial = path
		}
		for m, p := range path.Metrics {
			s := &out.Metrics[m]
			s.Method = p.Method
			count := float64(trial + 1)
			delta := p.SourceGrossReturn - s.MeanSourceGross
			s.MeanSourceGross += delta / count
			s.VarianceSourceGross += delta * (p.SourceGrossReturn - s.MeanSourceGross)
			delta = p.NetBuyHoldReturn - s.MeanNetBuyHold
			s.MeanNetBuyHold += delta / count
			s.VarianceNetBuyHold += delta * (p.NetBuyHoldReturn - s.MeanNetBuyHold)
			s.MeanTradedNotional += (p.TradedNotional - s.MeanTradedNotional) / count
			s.MeanCosts += (p.Costs - s.MeanCosts) / count
		}
	}
	if c.Trials > 1 {
		for i := range out.Metrics {
			out.Metrics[i].VarianceSourceGross /= float64(c.Trials - 1)
			out.Metrics[i].VarianceNetBuyHold /= float64(c.Trials - 1)
		}
	}
	return out, nil
}
