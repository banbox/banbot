package statistics

import (
	"fmt"
	"math"
	"slices"

	"gonum.org/v1/gonum/stat/distuv"
)

type StepwiseConfig struct {
	Criterion       string
	EnterP, RemoveP float64
	MaxSteps        int
}
type LinearSelection struct {
	Features   []int
	Regression Regression
}

func (m LinearSelection) Predict(row []float64) (float64, error) {
	out := m.Regression.Intercept
	if len(m.Features) != len(m.Regression.Beta) {
		return 0, fmt.Errorf("statistics: invalid selected model")
	}
	for j, index := range m.Features {
		if index < 0 || index >= len(row) || !finite(row[index]) {
			return 0, fmt.Errorf("statistics: invalid selected feature")
		}
		out += m.Regression.Beta[j] * row[index]
	}
	if !finite(out) {
		return 0, fmt.Errorf("statistics: selected prediction overflow")
	}
	return out, nil
}

func FitSelectedLinear(x [][]float64, y []float64, features []int) (LinearSelection, error) {
	if len(x) == 0 || len(x) != len(y) || len(x[0]) == 0 || len(y) <= len(features)+1 {
		return LinearSelection{}, fmt.Errorf("statistics: invalid selected regression data")
	}
	for i, row := range x {
		if len(row) != len(x[0]) || !finite(y[i]) {
			return LinearSelection{}, fmt.Errorf("statistics: invalid selected regression rows")
		}
		for _, v := range row {
			if !finite(v) {
				return LinearSelection{}, fmt.Errorf("statistics: nonfinite selected regression feature")
			}
		}
	}
	selected := slices.Clone(features)
	slices.Sort(selected)
	for i, index := range selected {
		if index < 0 || index >= len(x[0]) || (i > 0 && index == selected[i-1]) {
			return LinearSelection{}, fmt.Errorf("statistics: invalid regression selection")
		}
	}
	if len(selected) == 0 {
		mean, _, err := MeanStd(y, 0)
		if err != nil {
			return LinearSelection{}, err
		}
		fit := Regression{Intercept: mean, Rank: 1, Residuals: make([]float64, len(y))}
		for i, v := range y {
			fit.Residuals[i] = v - mean
		}
		return LinearSelection{Features: selected, Regression: fit}, nil
	}
	rows := make([][]float64, len(x))
	for i, row := range x {
		rows[i] = make([]float64, len(selected))
		for j, index := range selected {
			rows[i][j] = row[index]
		}
	}
	fit, err := Regress(y, rows, true)
	return LinearSelection{Features: selected, Regression: fit}, err
}
func selectedSSE(m LinearSelection) float64 {
	sum := 0.0
	for _, v := range m.Regression.Residuals {
		sum += v * v
	}
	return sum
}

// StepwiseLinear alternates forward additions and backward removals. P-value
// mode uses nested partial-F tests; AIC/BIC modes compare Gaussian fit criteria.
func StepwiseLinear(x [][]float64, y []float64, c StepwiseConfig) (LinearSelection, error) {
	if c.MaxSteps < 1 || c.MaxSteps > 10000 || (c.Criterion != "pvalue" && c.Criterion != "bic" && c.Criterion != "aic") || !finite(c.EnterP) || !finite(c.RemoveP) || c.EnterP <= 0 || c.EnterP >= 1 || c.RemoveP <= c.EnterP || c.RemoveP >= 1 {
		return LinearSelection{}, fmt.Errorf("statistics: invalid stepwise configuration")
	}
	current, err := FitSelectedLinear(x, y, nil)
	if err != nil {
		return LinearSelection{}, err
	}
	if len(x[0]) > 100 {
		return LinearSelection{}, fmt.Errorf("statistics: stepwise feature budget exceeded")
	}
	score := func(m LinearSelection) float64 {
		sse := selectedSSE(m)
		if sse <= 0 {
			return math.Inf(-1)
		}
		n := float64(len(y))
		penalty := 2.0
		if c.Criterion == "bic" {
			penalty = math.Log(n)
		}
		return n*math.Log(sse/n) + penalty*float64(len(m.Features)+1)
	}
	pvalue := func(reduced, full LinearSelection) float64 {
		sseFull, sseReduced := selectedSSE(full), selectedSSE(reduced)
		if sseFull == 0 {
			if sseReduced > 0 {
				return 0
			}
			return 1
		}
		df := len(y) - full.Regression.Rank
		f := math.Max(0, sseReduced-sseFull) / sseFull * float64(df)
		return 1 - distuv.F{D1: 1, D2: float64(df)}.CDF(f)
	}
	steps := 0
	for steps < c.MaxSteps {
		changed := false
		best := current
		bestValue := score(current)
		if c.Criterion == "pvalue" {
			bestValue = c.EnterP
		}
		for j := 0; j < len(x[0]); j++ {
			if slices.Contains(current.Features, j) {
				continue
			}
			candidate, err := FitSelectedLinear(x, y, append(slices.Clone(current.Features), j))
			if err != nil {
				continue
			}
			value := score(candidate)
			if c.Criterion == "pvalue" {
				value = pvalue(current, candidate)
			}
			if value < bestValue-1e-12 {
				best, bestValue = candidate, value
			}
		}
		if len(best.Features) > len(current.Features) {
			current = best
			steps++
			changed = true
		}
		for steps < c.MaxSteps && len(current.Features) > 0 {
			best = current
			bestValue = score(current)
			if c.Criterion == "pvalue" {
				bestValue = c.RemoveP
			}
			for _, index := range current.Features {
				selected := slices.Clone(current.Features)
				selected = slices.DeleteFunc(selected, func(j int) bool { return j == index })
				candidate, err := FitSelectedLinear(x, y, selected)
				if err != nil {
					continue
				}
				value := score(candidate)
				if c.Criterion == "pvalue" {
					value = pvalue(candidate, current)
					if value > bestValue+1e-12 {
						best, bestValue = candidate, value
					}
				} else if value < bestValue-1e-12 {
					best, bestValue = candidate, value
				}
			}
			if len(best.Features) == len(current.Features) {
				break
			}
			current = best
			steps++
			changed = true
		}
		if !changed {
			return current, nil
		}
	}
	return LinearSelection{}, fmt.Errorf("statistics: stepwise iteration budget exceeded")
}
