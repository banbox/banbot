package statistics

import (
	"errors"
	"math"
)

// VarimaxLoadings chooses an orthogonal loading orientation by maximizing
// sum(L^4)-sum(column squared sums^2)/assets. Optional Kaiser normalization
// gives each nonzero loading row unit norm during rotation and rescales it
// afterward. Both policies preserve LL', specific risk and fitted covariance.
// This is factor rotation, not a new principal-component extraction.
func VarimaxLoadings(rows [][]float64, kaiser bool, maxIterations int, tolerance float64) ([][]float64, error) {
	if len(rows) == 0 || len(rows[0]) == 0 || len(rows) > 256 || len(rows[0]) > len(rows) || maxIterations < 1 || maxIterations > 10000 || !finite(tolerance) || tolerance <= 0 {
		return nil, errors.New("statistics: invalid varimax dimensions/budget")
	}
	p, k := len(rows), len(rows[0])
	loading := make([][]float64, p)
	scales := make([]float64, p)
	mass := 0.0
	for i, row := range rows {
		if len(row) != k {
			return nil, errors.New("statistics: ragged varimax matrix")
		}
		norm := 0.0
		for _, v := range row {
			if !finite(v) {
				return nil, errors.New("statistics: nonfinite varimax loading")
			}
			norm = math.Hypot(norm, v)
		}
		if !finite(norm) {
			return nil, errors.New("statistics: varimax row norm overflow")
		}
		scales[i] = 1
		if kaiser && norm > 0 {
			scales[i] = norm
		}
		loading[i] = make([]float64, k)
		for j, v := range row {
			loading[i][j] = v / scales[i]
			mass += math.Abs(v / scales[i])
		}
	}
	if mass == 0 {
		return nil, errors.New("statistics: zero varimax loading matrix")
	}
	previous, err := varimaxCriterion(loading)
	if err != nil {
		return nil, err
	}
	for iteration := 0; iteration < maxIterations; iteration++ {
		// Each Jacobi pair exactly maximizes its contribution to varimax.
		// Balanced factor groups can oscillate under an SVD fixed-point step.
		for a := 0; a < k; a++ {
			for b := a + 1; b < k; b++ {
				meanU, meanV, varU, varV, cov := 0.0, 0.0, 0.0, 0.0, 0.0
				for i, row := range loading {
					u, v := row[a]*row[a]-row[b]*row[b], 2*row[a]*row[b]
					du, dv := u-meanU, v-meanV
					meanU += du / float64(i+1)
					meanV += dv / float64(i+1)
					varU += du * (u - meanU)
					varV += dv * (v - meanV)
					cov += du * (v - meanV)
				}
				if !finite(varU) || !finite(varV) || !finite(cov) {
					return nil, errors.New("statistics: varimax pair criterion overflow")
				}
				scale := max(math.Abs(varU), math.Abs(varV), math.Abs(cov))
				if scale == 0 {
					continue
				}
				angle := .25 * math.Atan2(2*(cov/scale), varU/scale-varV/scale)
				cosine, sine := math.Cos(angle), math.Sin(angle)
				for _, row := range loading {
					x, y := row[a], row[b]
					row[a], row[b] = x*cosine+y*sine, -x*sine+y*cosine
				}
			}
		}
		objective, err := varimaxCriterion(loading)
		if err != nil {
			return nil, err
		}
		if objective < previous-1e-10*max(1, math.Abs(previous)) {
			return nil, errors.New("statistics: varimax criterion failed to improve")
		}
		if math.Abs(objective-previous) <= tolerance*max(1, math.Abs(previous)) || k == 1 {
			for i, row := range loading {
				for j := range k {
					row[j] *= scales[i]
					if !finite(row[j]) {
						return nil, errors.New("statistics: rotated loading overflow")
					}
				}
			}
			canonicalLoadingSigns(loading)
			return loading, nil
		}
		previous = objective
	}
	return nil, errors.New("statistics: varimax iteration budget exhausted")
}

func varimaxCriterion(loading [][]float64) (float64, error) {
	criterion := 0.0
	for column := range len(loading[0]) {
		mean, variance := 0.0, 0.0
		for i, row := range loading {
			squared := row[column] * row[column]
			delta := squared - mean
			mean += delta / float64(i+1)
			variance += delta * (squared - mean)
		}
		criterion += variance
	}
	if !finite(criterion) {
		return 0, errors.New("statistics: varimax criterion overflow")
	}
	return criterion, nil
}
