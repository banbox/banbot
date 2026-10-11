package statistics

import (
	"errors"
	"math"

	"gonum.org/v1/gonum/mat"
)

// PCAModel owns a training-only covariance/correlation decomposition. Each
// loading row is one principal component, ordered by descending eigenvalue.
type PCAModel struct {
	Mean, Scale, Eigenvalues []float64
	Loadings                 [][]float64
}

func FitPCA(rows [][]float64, components int, standardize bool) (PCAModel, error) {
	if len(rows) < 2 || len(rows[0]) == 0 || components < 1 || components > len(rows[0]) {
		return PCAModel{}, errors.New("statistics: invalid PCA dimensions")
	}
	p := len(rows[0])
	model := PCAModel{Mean: make([]float64, p), Scale: make([]float64, p)}
	for i, row := range rows {
		if len(row) != p {
			return PCAModel{}, errors.New("statistics: ragged PCA matrix")
		}
		for j, x := range row {
			if !finite(x) {
				return PCAModel{}, errors.New("statistics: nonfinite PCA training")
			}
			model.Mean[j] += (x - model.Mean[j]) / float64(i+1)
		}
	}
	cov := mat.NewSymDense(p, nil)
	for _, row := range rows {
		for i := range p {
			for j := 0; j <= i; j++ {
				cov.SetSym(i, j, cov.At(i, j)+(row[i]-model.Mean[i])*(row[j]-model.Mean[j])/float64(len(rows)-1))
			}
		}
	}
	for j := range p {
		model.Scale[j] = 1
		if standardize {
			if cov.At(j, j) <= 0 {
				return PCAModel{}, errors.New("statistics: constant PCA feature cannot be standardized")
			}
			model.Scale[j] = math.Sqrt(cov.At(j, j))
		}
	}
	for i := range p {
		for j := 0; j <= i; j++ {
			v := cov.At(i, j) / model.Scale[i] / model.Scale[j]
			if !finite(v) {
				return PCAModel{}, errors.New("statistics: PCA covariance overflow")
			}
			cov.SetSym(i, j, v)
		}
	}
	var eigen mat.EigenSym
	if !eigen.Factorize(cov, true) {
		return PCAModel{}, errors.New("statistics: PCA decomposition failed")
	}
	values := eigen.Values(nil)
	var vectors mat.Dense
	eigen.VectorsTo(&vectors)
	for k := range components {
		j := p - 1 - k
		value := values[j]
		if value < -1e-10*math.Max(1, values[p-1]) {
			return PCAModel{}, errors.New("statistics: negative PCA eigenvalue")
		}
		model.Eigenvalues = append(model.Eigenvalues, math.Max(0, value))
		loading := make([]float64, p)
		pivot := 0
		for i := range p {
			loading[i] = vectors.At(i, j)
			if math.Abs(loading[i]) > math.Abs(loading[pivot]) {
				pivot = i
			}
		}
		if loading[pivot] < 0 {
			for i := range loading {
				loading[i] = -loading[i]
			}
		}
		model.Loadings = append(model.Loadings, loading)
	}
	return model, nil
}

func (m PCAModel) Transform(values []float64) ([]float64, error) {
	if len(values) == 0 || len(values) != len(m.Mean) || len(values) != len(m.Scale) || len(m.Loadings) == 0 {
		return nil, errors.New("statistics: invalid PCA inference dimensions")
	}
	scores := make([]float64, len(m.Loadings))
	for k, loading := range m.Loadings {
		if len(loading) != len(values) {
			return nil, errors.New("statistics: ragged PCA model")
		}
		for j, x := range values {
			if !finite(x) || !finite(m.Mean[j]) || !finite(m.Scale[j]) || m.Scale[j] <= 0 || !finite(loading[j]) {
				return nil, errors.New("statistics: invalid PCA inference/model")
			}
			scores[k] += (x - m.Mean[j]) / m.Scale[j] * loading[j]
		}
		if !finite(scores[k]) {
			return nil, errors.New("statistics: PCA score overflow")
		}
	}
	return scores, nil
}

func (m PCAModel) Residual(values []float64) ([]float64, error) {
	scores, err := m.Transform(values)
	if err != nil {
		return nil, err
	}
	r := make([]float64, len(values))
	for j, x := range values {
		fit := m.Mean[j]
		for k, s := range scores {
			fit += s * m.Loadings[k][j] * m.Scale[j]
		}
		r[j] = x - fit
		if !finite(r[j]) {
			return nil, errors.New("statistics: PCA residual overflow")
		}
	}
	return r, nil
}
