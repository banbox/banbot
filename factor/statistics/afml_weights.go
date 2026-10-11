package statistics

import (
	"errors"
	"math"

	"gonum.org/v1/gonum/mat"
)

// AttributedReturnWeights implements AFML snippet 4.10. Log changes at each
// inclusive event bar are shared among concurrent events, summed with their
// signs, then made absolute and normalized to sum to the number of events.
// The first close has no preceding return and contributes zero. Outcomes must
// already be mature; these weights are training metadata, never live features.
func AttributedReturnWeights(close []float64, spans []EventSpan) ([]float64, error) {
	if err := afmlValidateSpans(len(close), spans); err != nil {
		return nil, err
	}
	for _, p := range close {
		if !afmlFinite(p) || p <= 0 {
			return nil, errors.New("statistics: return weights need positive finite prices")
		}
	}
	diff := make([]int, len(close)+1)
	for _, s := range spans {
		diff[s.Start]++
		diff[s.End+1]--
	}
	prefix := make([]float64, len(close)+1)
	concurrent := 0
	for i := range close {
		concurrent += diff[i]
		prefix[i+1] = prefix[i]
		if i > 0 && concurrent > 0 {
			prefix[i+1] += (math.Log(close[i]) - math.Log(close[i-1])) / float64(concurrent)
		}
	}
	w := make([]float64, len(spans))
	total := 0.
	for i, s := range spans {
		w[i] = math.Abs(prefix[s.End+1] - prefix[s.Start])
		total += w[i]
	}
	if total <= 0 || !afmlFinite(total) {
		return nil, errors.New("statistics: attributable returns have zero or invalid mass")
	}
	for i := range w {
		w[i] = w[i] / total * float64(len(w))
	}
	return w, nil
}

// UniquenessTimeDecay implements AFML snippet 4.11 for already chronological
// uniqueness weights. The decay clock is their cumulative mass. c=1 means no
// decay; c in [0,1) gives a positive intercept; c in (-1,0) discards the oldest
// -c fraction of uniqueness mass. The oldest observation includes its own mass,
// exactly as the source cumsum, so its factor need not equal c.
func UniquenessTimeDecay(uniqueness []float64, c float64) ([]float64, error) {
	if len(uniqueness) == 0 || !afmlFinite(c) || c <= -1 || c > 1 {
		return nil, errors.New("statistics: invalid uniqueness decay inputs")
	}
	total := 0.
	for _, v := range uniqueness {
		if !afmlFinite(v) || v < 0 {
			return nil, errors.New("statistics: uniqueness mass must be finite nonnegative")
		}
		total += v
	}
	if total <= 0 || !afmlFinite(total) {
		return nil, errors.New("statistics: invalid total uniqueness mass")
	}
	slope := 1 - c
	if c < 0 {
		slope = 1 / (1 + c)
	}
	factors := make([]float64, len(uniqueness))
	cumulative := 0.
	for i, v := range uniqueness {
		cumulative += v
		factors[i] = math.Max(0, 1-slope+slope*(cumulative/total))
	}
	return factors, nil
}

// PCARiskWeights implements AFML snippet 2.1: descending covariance eigenpairs,
// positive loadings target*sqrt(risk share/eigenvalue), mapped to asset space.
// Nil riskDistribution puts all risk in the smallest-variance component. These
// are risk-target weights, not normalized capital weights. Eigenvector signs
// are fixed by making their largest-magnitude coordinate positive.
func PCARiskWeights(cov [][]float64, riskDistribution []float64, target float64) ([]float64, error) {
	n := len(cov)
	if n == 0 || !afmlFinite(target) || target <= 0 || (riskDistribution != nil && len(riskDistribution) != n) {
		return nil, errors.New("statistics: invalid PCA risk-weight dimensions/target")
	}
	for _, row := range cov {
		if len(row) != n {
			return nil, errors.New("statistics: PCA risk covariance must be square")
		}
	}
	sym := mat.NewSymDense(n, nil)
	for i := range cov {
		for j := 0; j <= i; j++ {
			if !afmlFinite(cov[i][j]) || !afmlFinite(cov[j][i]) || math.Abs(cov[i][j]-cov[j][i]) > 1e-12*math.Max(1, math.Max(math.Abs(cov[i][j]), math.Abs(cov[j][i]))) {
				return nil, errors.New("statistics: PCA risk covariance must be finite symmetric")
			}
			sym.SetSym(i, j, cov[i][j])
		}
	}
	shares := append([]float64(nil), riskDistribution...)
	if shares == nil {
		shares = make([]float64, n)
		shares[n-1] = 1
	}
	total := 0.
	for _, v := range shares {
		if !afmlFinite(v) || v < 0 {
			return nil, errors.New("statistics: invalid PCA risk distribution")
		}
		total += v
	}
	if math.Abs(total-1) > 1e-10 {
		return nil, errors.New("statistics: PCA risk shares must sum to one")
	}
	var eig mat.EigenSym
	if !eig.Factorize(sym, true) {
		return nil, errors.New("statistics: PCA risk eigendecomposition failed")
	}
	values := eig.Values(nil)
	var vectors mat.Dense
	eig.VectorsTo(&vectors)
	weights := make([]float64, n)
	for k, share := range shares {
		col := n - 1 - k
		if values[col] < 0 || (share > 0 && values[col] <= 0) {
			return nil, errors.New("statistics: PCA risk needs PSD covariance and positive allocated eigenvalues")
		}
		if share == 0 {
			continue
		}
		pivot := 0
		for i := 1; i < n; i++ {
			if math.Abs(vectors.At(i, col)) > math.Abs(vectors.At(pivot, col)) {
				pivot = i
			}
		}
		loading := target * math.Sqrt(share/values[col])
		if vectors.At(pivot, col) < 0 {
			loading = -loading
		}
		for i := range weights {
			weights[i] += vectors.At(i, col) * loading
			if !afmlFinite(weights[i]) {
				return nil, errors.New("statistics: PCA risk weight overflow")
			}
		}
	}
	return weights, nil
}
