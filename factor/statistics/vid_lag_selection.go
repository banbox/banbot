package statistics

import (
	"fmt"
	"math"
)

// SelectSpreadLag minimizes the Chapter 12 sum of squared measurement and
// prediction errors over a supplied historical training sample. Candidate order
// breaks ties; neither a candidate grid nor a refit schedule is supplied by the
// source. The returned filter has consumed only the supplied historical sample.
func SelectSpreadLag(values, observationVariances []float64, candidates []int, processVariance, initialVariance float64) (int, float64, *LaggedSpreadFilter, error) {
	return selectSpreadLag(values, observationVariances, candidates, processVariance, initialVariance, false)
}

// SelectSourceSpreadLag uses the source lag>=3 independence approximation for
// each candidate's training and returned live filter, with the same SSE rule.
func SelectSourceSpreadLag(values, observationVariances []float64, candidates []int, processVariance, initialVariance float64) (int, float64, *LaggedSpreadFilter, error) {
	return selectSpreadLag(values, observationVariances, candidates, processVariance, initialVariance, true)
}

func selectSpreadLag(values, observationVariances []float64, candidates []int, processVariance, initialVariance float64, source bool) (int, float64, *LaggedSpreadFilter, error) {
	if len(values) < 2 || len(values) != len(observationVariances) || len(candidates) == 0 {
		return 0, 0, nil, fmt.Errorf("statistics: invalid lag selection sample")
	}
	for i, value := range values {
		if !finite(value) || !finite(observationVariances[i]) || observationVariances[i] <= 0 {
			return 0, 0, nil, fmt.Errorf("statistics: invalid lag selection observation")
		}
	}
	bestLag, bestCost := 0, math.Inf(1)
	var best *LaggedSpreadFilter
	for _, lag := range candidates {
		constructor := NewLaggedSpreadFilter
		if source {
			constructor = NewSourceLaggedSpreadFilter
		}
		filter, err := constructor(lag, processVariance, initialVariance, values[0])
		if err != nil {
			return 0, 0, nil, err
		}
		cost := 0.
		for i := 1; i < len(values); i++ {
			observation, err := filter.Observe(values[i], observationVariances[i])
			if err != nil {
				return 0, 0, nil, err
			}
			measurement := observation.Filtered - values[i]
			prediction := observation.Filtered - observation.Prediction
			cost += measurement*measurement + prediction*prediction
			if !finite(cost) {
				return 0, 0, nil, fmt.Errorf("statistics: lag selection cost overflow")
			}
		}
		if cost < bestCost {
			bestLag, bestCost, best = lag, cost, filter
		}
	}
	return bestLag, bestCost, best, nil
}
