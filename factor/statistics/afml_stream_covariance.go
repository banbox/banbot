package statistics

import (
	"context"
	"errors"
	"gonum.org/v1/gonum/mat"
	"io"
	"math"
)

// CovarianceStream retains only count, mean and centered cross-products O(p2).
// Add owns its inputs; invalid chunks do not change the accumulated state.
// The stable parallel mean/M2 merge is an extension supporting source20.6,
// whose supplied example assumes an already computed eigendecomposition.
type CovarianceStream struct {
	count uint64
	mean  []float64
	m2    [][]float64
}

func (s *CovarianceStream) Count() uint64 { return s.count }
func (s *CovarianceStream) Add(rows [][]float64) error {
	if len(rows) == 0 || len(rows[0]) == 0 {
		return errors.New("statistics: empty covariance chunk")
	}
	p := len(rows[0])
	if len(s.mean) != 0 && len(s.mean) != p {
		return errors.New("statistics: covariance dimension changed")
	}
	chunk := CovarianceStream{mean: make([]float64, p), m2: make([][]float64, p)}
	for i := range p {
		chunk.m2[i] = make([]float64, p)
	}
	for _, row := range rows {
		if len(row) != p {
			return errors.New("statistics: ragged covariance chunk")
		}
		for _, x := range row {
			if !finite(x) {
				return errors.New("statistics: nonfinite covariance chunk")
			}
		}
		chunk.count++
		n := float64(chunk.count)
		delta := make([]float64, p)
		for i, x := range row {
			delta[i] = x - chunk.mean[i]
			chunk.mean[i] += delta[i] / n
		}
		for i := range p {
			for j := 0; j <= i; j++ {
				chunk.m2[i][j] += delta[i] * (row[j] - chunk.mean[j])
				if !finite(chunk.m2[i][j]) {
					return errors.New("statistics: covariance overflow")
				}
			}
		}
	}
	return s.Merge(&chunk)
}
func (s *CovarianceStream) Merge(other *CovarianceStream) error {
	if other == nil || other.count == 0 {
		return errors.New("statistics: empty covariance merge")
	}
	p := len(other.mean)
	if s.count == 0 {
		s.mean = append([]float64(nil), other.mean...)
		s.m2 = make([][]float64, p)
		for i := range p {
			s.m2[i] = append([]float64(nil), other.m2[i]...)
		}
		s.count = other.count
		return nil
	}
	if s.count != 0 && len(s.mean) != p {
		return errors.New("statistics: covariance merge dimensions")
	}
	if other.count > ^uint64(0)-s.count {
		return errors.New("statistics: covariance count overflow")
	}
	count := s.count + other.count
	mean := make([]float64, p)
	m2 := make([][]float64, p)
	a, b, n := float64(s.count), float64(other.count), float64(count)
	for i := range p {
		m2[i] = make([]float64, p)
		base := 0.
		if s.count > 0 {
			base = s.mean[i]
		}
		delta := other.mean[i] - base
		mean[i] = base + delta*(b/n)
		if !finite(mean[i]) {
			return errors.New("statistics: covariance mean overflow")
		}
		for j := 0; j <= i; j++ {
			prior, oldMean := 0., 0.
			if s.count > 0 {
				prior = s.m2[i][j]
				oldMean = s.mean[j]
			}
			m2[i][j] = prior + other.m2[i][j] + delta*(other.mean[j]-oldMean)*(a*(b/n))
			if !finite(m2[i][j]) {
				return errors.New("statistics: covariance merge overflow")
			}
		}
	}
	s.count, s.mean, s.m2 = count, mean, m2
	return nil
}
func (s *CovarianceStream) Covariance() ([]float64, [][]float64, error) {
	if s.count < 2 {
		return nil, nil, errors.New("statistics: covariance needs two rows")
	}
	p := len(s.mean)
	cov := make([][]float64, p)
	for i := range p {
		cov[i] = make([]float64, p)
	}
	for i := range p {
		for j := 0; j <= i; j++ {
			cov[i][j] = s.m2[i][j] / float64(s.count-1)
			cov[j][i] = cov[i][j]
		}
	}
	return append([]float64(nil), s.mean...), cov, nil
}

// FitStreamingPCA consumes one row chunk at a time until io.EOF, never retaining
// the training matrix. next may reuse its buffers after returning a chunk.
func FitStreamingPCA(ctx context.Context, next func(context.Context) ([][]float64, error), components int, standardize bool) (PCAModel, error) {
	if ctx == nil || next == nil || components < 1 {
		return PCAModel{}, errors.New("statistics: invalid streaming PCA")
	}
	var stream CovarianceStream
	for {
		if err := ctx.Err(); err != nil {
			return PCAModel{}, err
		}
		rows, err := next(ctx)
		if err == io.EOF {
			if len(rows) != 0 {
				return PCAModel{}, errors.New("statistics: EOF accompanied by rows")
			}
			break
		}
		if err != nil {
			return PCAModel{}, err
		}
		if err = stream.Add(rows); err != nil {
			return PCAModel{}, err
		}
	}
	mean, cov, err := stream.Covariance()
	if err != nil {
		return PCAModel{}, err
	}
	p := len(mean)
	if components > p {
		return PCAModel{}, errors.New("statistics: too many streaming PCA components")
	}
	model := PCAModel{Mean: mean, Scale: make([]float64, p)}
	matrix := mat.NewSymDense(p, nil)
	for i := range p {
		model.Scale[i] = 1
		if standardize {
			if cov[i][i] <= 0 {
				return PCAModel{}, errors.New("statistics: constant streaming PCA feature")
			}
			model.Scale[i] = math.Sqrt(cov[i][i])
		}
	}
	for i := range p {
		for j := 0; j <= i; j++ {
			v := cov[i][j] / model.Scale[i] / model.Scale[j]
			if !finite(v) {
				return PCAModel{}, errors.New("statistics: streaming PCA overflow")
			}
			matrix.SetSym(i, j, v)
		}
	}
	var eigen mat.EigenSym
	if !eigen.Factorize(matrix, true) {
		return PCAModel{}, errors.New("statistics: streaming PCA decomposition")
	}
	values := eigen.Values(nil)
	var vectors mat.Dense
	eigen.VectorsTo(&vectors)
	for k := range components {
		col := p - 1 - k
		v := values[col]
		if v < -1e-10*math.Max(1, values[p-1]) {
			return PCAModel{}, errors.New("statistics: negative streaming PCA eigenvalue")
		}
		model.Eigenvalues = append(model.Eigenvalues, math.Max(0, v))
		row := make([]float64, p)
		pivot := 0
		for i := range p {
			row[i] = vectors.At(i, col)
			if math.Abs(row[i]) > math.Abs(row[pivot]) {
				pivot = i
			}
		}
		if row[pivot] < 0 {
			for i := range row {
				row[i] = -row[i]
			}
		}
		model.Loadings = append(model.Loadings, row)
	}
	if err := ctx.Err(); err != nil {
		return PCAModel{}, err
	}
	return model, nil
}

// ProjectPCAColumnBlocks is source20.14's sum of Z_b W_b. Blocks must partition
// all model columns, and every loaded block must have the same row order.
// load returns raw (unstandardized) values; the frozen model supplies scaling.
// Concurrent loader calls must have independent buffers until each call is
// consumed. Missing values reject rather than implicitly fill or drop rows.
func ProjectPCAColumnBlocks(ctx context.Context, model PCAModel, blocks [][]int, workers int, load func(context.Context, []int) ([][]float64, error)) ([][]float64, error) {
	p := len(model.Mean)
	seen := make([]bool, p)
	owned := make([][]int, len(blocks))
	if p == 0 || len(model.Scale) != p || len(model.Loadings) == 0 || load == nil {
		return nil, errors.New("statistics: invalid block PCA model")
	}
	for j := range p {
		if !finite(model.Mean[j]) || !finite(model.Scale[j]) || model.Scale[j] <= 0 {
			return nil, errors.New("statistics: invalid block PCA scaling")
		}
	}
	for _, row := range model.Loadings {
		if len(row) != p {
			return nil, errors.New("statistics: invalid block PCA loadings")
		}
		for _, v := range row {
			if !finite(v) {
				return nil, errors.New("statistics: nonfinite block PCA loading")
			}
		}
	}
	model.Mean = append([]float64(nil), model.Mean...)
	model.Scale = append([]float64(nil), model.Scale...)
	loadings := make([][]float64, len(model.Loadings))
	for i, row := range model.Loadings {
		loadings[i] = append([]float64(nil), row...)
	}
	model.Loadings = loadings
	for i, block := range blocks {
		if len(block) == 0 {
			return nil, errors.New("statistics: empty column block")
		}
		owned[i] = append([]int(nil), block...)
		for _, j := range block {
			if j < 0 || j >= p || seen[j] {
				return nil, errors.New("statistics: duplicate/invalid column block")
			}
			seen[j] = true
		}
	}
	for _, ok := range seen {
		if !ok {
			return nil, errors.New("statistics: incomplete column partition")
		}
	}
	return DispatchReduce(ctx, len(blocks), workers, [][]float64(nil), func(ctx context.Context, index int) ([][]float64, error) {
		columns := owned[index]
		rows, err := load(ctx, append([]int(nil), columns...))
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return nil, errors.New("statistics: empty column observations")
		}
		out := make([][]float64, len(rows))
		for i, row := range rows {
			if len(row) != len(columns) {
				return nil, errors.New("statistics: block dimensions")
			}
			out[i] = make([]float64, len(model.Loadings))
			for k, loading := range model.Loadings {
				for j, col := range columns {
					if !finite(row[j]) {
						return nil, errors.New("statistics: invalid column value")
					}
					out[i][k] += (row[j] - model.Mean[col]) / model.Scale[col] * loading[col]
				}
				if !finite(out[i][k]) {
					return nil, errors.New("statistics: block projection overflow")
				}
			}
		}
		return out, nil
	}, func(acc, block [][]float64) ([][]float64, error) {
		if acc == nil {
			return block, nil
		}
		if len(acc) != len(block) {
			return nil, errors.New("statistics: misaligned column blocks")
		}
		for i := range acc {
			for j := range acc[i] {
				acc[i][j] += block[i][j]
				if !finite(acc[i][j]) {
					return nil, errors.New("statistics: reduction overflow")
				}
			}
		}
		return acc, nil
	})
}
