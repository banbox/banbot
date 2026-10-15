package statistics

import (
	"errors"
	"fmt"
	"math"
)

// CPCVExecution declares the realized interval of a signed, unit-notional bet.
// Fill must follow the decision; intervals cannot overlap. AssetReturn is the
// simple fill-to-exit return, not the labeling return or a classifier score.
type CPCVExecution struct {
	DecisionBar, FillBar, ExitBar int
	AssetReturn                   float64
}

type CPCVResearchConfig struct {
	Groups, TestGroups, EmbargoRows, MaxSplits int
	Epsilon, TurnoverCost                      float64
}

type CPCVSplitForecast struct {
	Split          PurgedSplit
	Probabilities  [][]float64
	Score          MulticlassReport
	FutureTraining bool
}

type CPCVResearchPath struct {
	GroupSplits, RowSplits, Positions []int
	Returns, Turnover                 []float64
	Sharpe                            float64
	SharpeValid                       bool
}

// CPCVResearchResult is scenario evidence, never a historical model artifact.
// Correlations are return-series correlations; undefined entries have Valid=false.
// No independence assumption or effective trial count is inferred from path count.
type CPCVResearchResult struct {
	ResearchOnly       bool
	Config             CPCVResearchConfig
	Spans              []EventSpan
	Labels, Classes    []int
	Weights            []float64
	Execution          []CPCVExecution
	Plan               CPCVPlan
	Forecasts          []CPCVSplitForecast
	Paths              []CPCVResearchPath
	SharpeDistribution []float64
	Correlation        [][]float64
	CorrelationValid   [][]bool
}

// RunCPCVResearch fits once per combinatorial split, reconstructs each group's
// OOS forecasts, and executes the reconstructed chronological signed portfolio.
// The WeightedFoldPredictor contract is shared with PurgedClassificationScores:
// transforms/model must fit solely on supplied train rows and consume weights.
// Signed classes are restricted to {-1,0,1}; ties follow declared class order.
// Cost is proportional to absolute position change, including final liquidation.
// Positions flatten at exit before a gap; touching intervals carry the position.
// Sharpe is unannualized sample mean/sample standard deviation; undefined paths
// remain explicit and are omitted from the finite empirical distribution.
func RunCPCVResearch(spans []EventSpan, labels, classes []int, weights []float64, execution []CPCVExecution, config CPCVResearchConfig, fit WeightedFoldPredictor) (CPCVResearchResult, error) {
	out := CPCVResearchResult{ResearchOnly: true, Config: config}
	if fit == nil || len(labels) != len(spans) || len(execution) != len(spans) || (weights != nil && len(weights) != len(spans)) || !afmlFinite(config.TurnoverCost) || config.TurnoverCost < 0 {
		return out, errors.New("statistics: invalid CPCV research inputs")
	}
	for _, c := range classes {
		if c < -1 || c > 1 {
			return out, errors.New("statistics: CPCV requires signed classes")
		}
	}
	// Validate full scoring inputs before handing control to an external callback.
	uniform := make([][]float64, len(labels))
	for i := range uniform {
		uniform[i] = make([]float64, len(classes))
		for j := range classes {
			uniform[i][j] = 1 / float64(len(classes))
		}
		e := execution[i]
		if e.DecisionBar != spans[i].Start || e.FillBar <= e.DecisionBar || e.ExitBar <= e.FillBar || (i > 0 && e.FillBar < execution[i-1].ExitBar) || !afmlFinite(e.AssetReturn) || e.AssetReturn <= -1 {
			return out, errors.New("statistics: invalid or overlapping CPCV execution")
		}
	}
	if _, err := MulticlassClassification(labels, classes, uniform, weights, config.Epsilon); err != nil {
		return out, err
	}
	plan, err := CombinatorialPurgedCV(spans, config.Groups, config.TestGroups, config.EmbargoRows, config.MaxSplits)
	if err != nil {
		return out, err
	}
	out.Plan = plan
	out.Spans, out.Labels, out.Classes = append([]EventSpan(nil), spans...), append([]int(nil), labels...), append([]int(nil), classes...)
	out.Execution = append([]CPCVExecution(nil), execution...)
	out.Weights = make([]float64, len(spans))
	for i := range spans {
		out.Weights[i] = 1
		if weights != nil {
			out.Weights[i] = weights[i]
		}
	}
	out.Forecasts = make([]CPCVSplitForecast, len(plan.Splits))
	for i, split := range plan.Splits {
		if len(split.Train) == 0 {
			return out, fmt.Errorf("statistics: CPCV split %d has no train rows", i)
		}
		tw, tl, sw := make([]float64, len(split.Train)), make([]int, len(split.Test)), make([]float64, len(split.Test))
		mass := 0.
		future := false
		for j, row := range split.Train {
			tw[j] = out.Weights[row]
			mass += tw[j]
			if spans[row].End >= spans[split.Test[0]].Start {
				future = true
			}
		}
		if mass <= 0 || !afmlFinite(mass) {
			return out, errors.New("statistics: invalid CPCV train weight mass")
		}
		p, err := fit(append([]int(nil), split.Train...), append([]int(nil), split.Test...), tw, append([]int(nil), classes...))
		if err != nil {
			return out, fmt.Errorf("statistics: CPCV split %d fit: %w", i, err)
		}
		for j, row := range split.Test {
			tl[j], sw[j] = labels[row], out.Weights[row]
		}
		score, err := MulticlassClassification(tl, classes, p, sw, config.Epsilon)
		if err != nil {
			return out, fmt.Errorf("statistics: CPCV split %d score: %w", i, err)
		}
		owned := make([][]float64, len(p))
		for j := range p {
			owned[j] = append([]float64(nil), p[j]...)
		}
		out.Forecasts[i] = CPCVSplitForecast{split, owned, score, future}
	}
	for _, assignments := range plan.Paths {
		path := CPCVResearchPath{GroupSplits: append([]int(nil), assignments...), RowSplits: make([]int, len(spans)), Positions: make([]int, len(spans)), Returns: make([]float64, len(spans)), Turnover: make([]float64, len(spans))}
		for group, splitIndex := range assignments {
			forecast := out.Forecasts[splitIndex]
			lookup := make(map[int]int, len(forecast.Split.Test))
			for j, row := range forecast.Split.Test {
				lookup[row] = j
			}
			for _, row := range plan.Groups[group] {
				probability := forecast.Probabilities[lookup[row]]
				best := 0
				for j := range probability {
					if probability[j] > probability[best] {
						best = j
					}
				}
				path.RowSplits[row], path.Positions[row] = splitIndex, classes[best]
			}
		}
		previous := 0
		for row, position := range path.Positions {
			path.Turnover[row] = math.Abs(float64(position - previous))
			flatten := row == len(spans)-1 || execution[row].ExitBar < execution[row+1].FillBar
			if flatten {
				path.Turnover[row] += math.Abs(float64(position))
			}
			path.Returns[row] = float64(position)*execution[row].AssetReturn - config.TurnoverCost*path.Turnover[row]
			if !afmlFinite(path.Returns[row]) {
				return out, errors.New("statistics: CPCV portfolio return overflow")
			}
			previous = position
			if flatten {
				previous = 0
			}
		}
		path.Sharpe, err = CSCVSharpe(path.Returns)
		path.SharpeValid = err == nil
		if path.SharpeValid {
			out.SharpeDistribution = append(out.SharpeDistribution, path.Sharpe)
		}
		out.Paths = append(out.Paths, path)
	}
	out.Correlation, out.CorrelationValid = make([][]float64, len(out.Paths)), make([][]bool, len(out.Paths))
	for i := range out.Paths {
		out.Correlation[i], out.CorrelationValid[i] = make([]float64, len(out.Paths)), make([]bool, len(out.Paths))
		for j := range out.Paths {
			out.Correlation[i][j], out.CorrelationValid[i][j] = cpcvCorrelation(out.Paths[i].Returns, out.Paths[j].Returns)
		}
	}
	return out, nil
}

func cpcvCorrelation(a, b []float64) (float64, bool) {
	ma, mb, va, vb, cov := 0., 0., 0., 0., 0.
	for i := range a {
		da, db := a[i]-ma, b[i]-mb
		ma += da / float64(i+1)
		mb += db / float64(i+1)
		va += da * (a[i] - ma)
		vb += db * (b[i] - mb)
		cov += da * (b[i] - mb)
	}
	if va <= 0 || vb <= 0 {
		return 0, false
	}
	r := (cov / math.Sqrt(va)) / math.Sqrt(vb)
	if !afmlFinite(r) {
		return 0, false
	}
	return math.Max(-1, math.Min(1, r)), true
}
