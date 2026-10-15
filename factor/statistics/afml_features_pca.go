package statistics

import (
	"errors"
	"math"
)

// FitOrthogonalFeatures reuses the repository's training-only standardized PCA
// and retains the smallest positive-eigenvalue prefix explaining varianceShare
// (source8.5, conventionally .95). Eigenvalues here are sample-covariance values,
// source Z'Z values divided by n-1; projections and retention ratios are equal.
func FitOrthogonalFeatures(training [][]float64, varianceShare float64) (PCAModel, error) {
	if len(training) == 0 || !afmlFinite(varianceShare) || varianceShare <= 0 || varianceShare > 1 {
		return PCAModel{}, errors.New("statistics: invalid PCA variance threshold")
	}
	model, err := FitPCA(training, len(training[0]), true)
	if err != nil {
		return PCAModel{}, err
	}
	total := 0.
	for _, v := range model.Eigenvalues {
		total += v
	}
	if !afmlFinite(total) || total <= 0 {
		return PCAModel{}, errors.New("statistics: invalid PCA variance mass")
	}
	sum, keep := 0., 0
	for _, v := range model.Eigenvalues {
		if v <= 0 {
			break
		}
		sum += v
		keep++
		if sum/total >= varianceShare {
			break
		}
	}
	if keep < 1 {
		return PCAModel{}, errors.New("statistics: no positive PCA components")
	}
	model.Eigenvalues = model.Eigenvalues[:keep]
	model.Loadings = model.Loadings[:keep]
	return model, nil
}

// HyperbolicWeightedKendall uses explicit unique importance ranks (0 is most
// important), additive pair weight 1/(rank_i+1)+1/(rank_j+1), and weighted tau-b
// tie correction. Caller ranks avoid silently deriving significance from the
// same scores being compared. No asymptotic p-value is manufactured. Pair work
// is explicitly budgeted; tied constant inputs yield undefined NaN.
func HyperbolicWeightedKendall(x, y []float64, ranks []int, maxPairs int) (float64, error) {
	n := len(x)
	if n < 2 || n != len(y) || n != len(ranks) || maxPairs < 1 {
		return 0, errors.New("statistics: invalid weighted Kendall inputs")
	}
	if int64(n)*int64(n-1)/2 > int64(maxPairs) {
		return 0, errors.New("statistics: Kendall pair budget exceeded")
	}
	seen := make([]bool, n)
	for i, v := range x {
		if !afmlFinite(v) || !afmlFinite(y[i]) || ranks[i] < 0 || ranks[i] >= n || seen[ranks[i]] {
			return 0, errors.New("statistics: invalid Kendall value/rank")
		}
		seen[ranks[i]] = true
	}
	numerator, nonX, nonY := 0., 0., 0.
	compare := func(a, b float64) float64 {
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	}
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			weight := 1/float64(ranks[i]+1) + 1/float64(ranks[j]+1)
			sx, sy := compare(x[i], x[j]), compare(y[i], y[j])
			if sx != 0 {
				nonX += weight
			}
			if sy != 0 {
				nonY += weight
			}
			numerator += weight * sx * sy
		}
	}
	if nonX == 0 || nonY == 0 {
		return math.NaN(), nil
	}
	return numerator / math.Sqrt(nonX*nonY), nil
}
