package statistics

import (
	"fmt"
	"math"
	"slices"
)

// SequentialQuantileTails sorts independently inside each prior tail, in column
// order. It rounds each stage's count as Chan's implied-moments worked code does.
// Caller-provided row order resolves ties; incomplete rows are excluded.
func SequentialQuantileTails(scores [][]float64, fraction float64) (long, short []int, err error) {
	if len(scores) == 0 || len(scores[0]) == 0 || !finite(fraction) || fraction <= 0 || fraction > .5 {
		return nil, nil, fmt.Errorf("statistics: invalid multisort dimensions/fraction")
	}
	for i, row := range scores {
		if len(row) != len(scores[0]) {
			return nil, nil, fmt.Errorf("statistics: ragged multisort rows")
		}
		valid := true
		for _, v := range row {
			if !finite(v) {
				valid = false
				break
			}
		}
		if valid {
			long = append(long, i)
			short = append(short, i)
		}
	}
	for column := 0; column < len(scores[0]); column++ {
		tail := func(indices []int, high bool) []int {
			indices = slices.Clone(indices)
			slices.SortStableFunc(indices, func(a, b int) int {
				if scores[a][column] < scores[b][column] {
					return -1
				}
				if scores[a][column] > scores[b][column] {
					return 1
				}
				return 0
			})
			n := int(math.Round(float64(len(indices)) * fraction))
			if high {
				return slices.Clone(indices[len(indices)-n:])
			}
			return slices.Clone(indices[:n])
		}
		long, short = tail(long, true), tail(short, false)
	}
	return long, short, nil
}

func ImpliedMoments(callATM, putATM, callOTM, putOTM float64) (volatility, skew, kurtosis float64, err error) {
	for _, v := range []float64{callATM, putATM, callOTM, putOTM} {
		if !finite(v) || v <= 0 {
			return 0, 0, 0, fmt.Errorf("statistics: positive implied volatility required")
		}
	}
	volatility = (callATM + putATM) / 2
	skew = callOTM - putOTM
	kurtosis = callOTM + putOTM - callATM - putATM
	if !finite(volatility) || !finite(skew) || !finite(kurtosis) {
		return 0, 0, 0, fmt.Errorf("statistics: implied moment overflow")
	}
	return
}
