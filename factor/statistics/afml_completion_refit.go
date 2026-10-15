package statistics

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
)

type AFMLParameterizedClassifierFit func(parameters map[string]float64, rows [][]float64, labels []int, weights []float64, classes []int) (AFMLProbabilityPredictor, error)
type SearchBaggingConfig struct {
	Estimators                      int
	SampleFraction, FeatureFraction float64
	Seed                            int64
}
type SearchRefitAudit struct{ Rows, Columns []int }

// RefitPurgedSearch completes source9.1's full training-prefix refit, optionally
// bagging the selected pipeline. The callback must fit all transforms anew for
// each bag and use weights. Samples use replacement; feature subsets do not.
// Estimators=0 fits the winner once on all supplied rows/features. Positive bags
// use floor(fraction*size), matching sklearn float max_samples/max_features.
// This training prefix must already be visible/matured; no future holdout is
// accepted implicitly and no OOS performance claim follows from full-data fit.
func RefitPurgedSearch(search PurgedSearchResult, x [][]float64, y, classes []int, weights []float64, bag SearchBaggingConfig, fit AFMLParameterizedClassifierFit) (AFMLProbabilityPredictor, []SearchRefitAudit, error) {
	if fit == nil || search.Best < 0 || search.Best >= len(search.Trials) || len(x) == 0 || len(x) != len(y) || len(x[0]) == 0 || len(classes) < 2 || (weights != nil && len(weights) != len(x)) || bag.Estimators < 0 || bag.Estimators > 10000 {
		return nil, nil, errors.New("statistics: invalid search refit inputs")
	}
	k := len(x[0])
	seen := map[int]bool{}
	for _, c := range classes {
		if seen[c] {
			return nil, nil, errors.New("statistics: duplicate refit class")
		}
		seen[c] = true
	}
	mass := 0.
	for i, row := range x {
		if len(row) != k || !seen[y[i]] {
			return nil, nil, errors.New("statistics: inconsistent refit rows/labels")
		}
		for _, v := range row {
			if !afmlFinite(v) {
				return nil, nil, errors.New("statistics: nonfinite refit features")
			}
		}
		w := 1.
		if weights != nil {
			w = weights[i]
		}
		if !afmlFinite(w) || w < 0 {
			return nil, nil, errors.New("statistics: invalid refit weight")
		}
		mass += w
	}
	if mass <= 0 || !afmlFinite(mass) {
		return nil, nil, errors.New("statistics: invalid refit total weight")
	}
	params := search.Trials[search.Best].Parameters
	for key, v := range params {
		if key == "" || !afmlFinite(v) {
			return nil, nil, errors.New("statistics: invalid winning parameters")
		}
	}
	nrows, ncols, nbags := len(x), k, 1
	if bag.Estimators > 0 {
		if !afmlFinite(bag.SampleFraction) || bag.SampleFraction <= 0 || bag.SampleFraction > 1 || !afmlFinite(bag.FeatureFraction) || bag.FeatureFraction <= 0 || bag.FeatureFraction > 1 {
			return nil, nil, errors.New("statistics: invalid refit bag fractions")
		}
		nbags = bag.Estimators
		nrows = int(bag.SampleFraction * float64(len(x)))
		ncols = max(1, int(bag.FeatureFraction*float64(k)))
		if nrows == 0 {
			return nil, nil, errors.New("statistics: empty refit bootstrap")
		}
	}
	if float64(nbags)*float64(nrows)*float64(ncols) > 1e8 {
		return nil, nil, errors.New("statistics: refit work budget exceeded")
	}
	rng := rand.New(rand.NewSource(bag.Seed))
	audit := make([]SearchRefitAudit, nbags)
	predictors := make([]AFMLProbabilityPredictor, nbags)
	for b := range nbags {
		rows, cols := make([]int, nrows), make([]int, ncols)
		for i := range rows {
			rows[i] = i
			if bag.Estimators > 0 {
				rows[i] = rng.Intn(len(x))
			}
		}
		for j := range cols {
			cols[j] = j
		}
		if bag.Estimators > 0 {
			copy(cols, rng.Perm(k)[:ncols])
			sort.Ints(cols)
		}
		audit[b] = SearchRefitAudit{append([]int(nil), rows...), append([]int(nil), cols...)}
		xx := make([][]float64, nrows)
		yy := make([]int, nrows)
		ww := make([]float64, nrows)
		sampleMass := 0.
		for i, row := range rows {
			xx[i] = make([]float64, ncols)
			for j, col := range cols {
				xx[i][j] = x[row][col]
			}
			yy[i] = y[row]
			ww[i] = 1
			if weights != nil {
				ww[i] = weights[row]
			}
			sampleMass += ww[i]
		}
		if sampleMass == 0 || !afmlFinite(sampleMass) {
			return nil, nil, errors.New("statistics: refit bootstrap has invalid total weight")
		}
		owned := map[string]float64{}
		for key, v := range params {
			owned[key] = v
		}
		predictor, err := fit(owned, xx, yy, ww, append([]int(nil), classes...))
		if err != nil {
			return nil, nil, fmt.Errorf("statistics: refit bag%d: %w", b, err)
		}
		if predictor == nil {
			return nil, nil, errors.New("statistics: nil refit predictor")
		}
		predictors[b] = predictor
	}
	// Capture private copies: returned audit is writable without changing routing.
	columns := make([][]int, nbags)
	for b := range audit {
		columns[b] = append([]int(nil), audit[b].Columns...)
	}
	classCount := len(classes)
	predict := func(rows [][]float64) ([][]float64, error) {
		out := make([][]float64, len(rows))
		for i, row := range rows {
			if len(row) != k {
				return nil, errors.New("statistics: refit prediction dimensions")
			}
			for _, v := range row {
				if !afmlFinite(v) {
					return nil, errors.New("statistics: refit prediction nonfinite")
				}
			}
			out[i] = make([]float64, classCount)
		}
		for b, predictor := range predictors {
			xx := make([][]float64, len(rows))
			for i, row := range rows {
				xx[i] = make([]float64, len(columns[b]))
				for j, col := range columns[b] {
					xx[i][j] = row[col]
				}
			}
			p, err := predictor(xx)
			if err != nil {
				return nil, err
			}
			if len(p) != len(rows) {
				return nil, errors.New("statistics: refit predictor row mismatch")
			}
			for i, pr := range p {
				if len(pr) != classCount {
					return nil, errors.New("statistics: refit predictor class mismatch")
				}
				sum := 0.
				for j, v := range pr {
					if !afmlFinite(v) || v < 0 || v > 1 {
						return nil, errors.New("statistics: invalid refit probabilities")
					}
					sum += v
					out[i][j] += v / float64(nbags)
				}
				if math.Abs(sum-1) > 1e-10 {
					return nil, errors.New("statistics: refit probabilities not normalized")
				}
			}
		}
		return out, nil
	}
	return predict, audit, nil
}
