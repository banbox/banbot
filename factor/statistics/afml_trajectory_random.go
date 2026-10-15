package statistics

import (
	"errors"
	"math"
	"math/rand"

	"gonum.org/v1/gonum/mat"
)

// GaussianMatrixWithRank implements snippet21.4's Gaussian latent factors and
// random SVD loading basis. Rank is the NOISE-FREE rank (almost surely); sigma>0
// generally restores full rank. Homoscedastic=false draws source per-column
// scales sigma*(Uniform(0,1)+.5). A seed makes experiments reproducible.
func GaussianMatrixWithRank(samples, columns, rank int, sigma float64, homoscedastic bool, seed int64) ([][]float64, error) {
	if samples < 1 || columns < 1 || columns > 256 || rank < 1 || rank > columns || rank > samples || samples > 2000000/columns || !afmlFinite(sigma) || sigma < 0 {
		return nil, errors.New("statistics: invalid Gaussian rank experiment")
	}
	rng := rand.New(rand.NewSource(seed))
	basis := make([]float64, columns*columns)
	for i := range basis {
		basis[i] = rng.NormFloat64()
	}
	var svd mat.SVD
	if !svd.Factorize(mat.NewDense(columns, columns, basis), mat.SVDFull) {
		return nil, errors.New("statistics: Gaussian loading SVD failed")
	}
	var u mat.Dense
	svd.UTo(&u)
	out := make([][]float64, samples)
	for i := range out {
		out[i] = make([]float64, columns)
		for k := 0; k < rank; k++ {
			z := rng.NormFloat64()
			for j := 0; j < columns; j++ {
				out[i][j] += z * u.At(j, k)
			}
		}
	}
	scales := make([]float64, columns)
	for j := range scales {
		scales[j] = sigma
		if !homoscedastic {
			scales[j] *= rng.Float64() + .5
		}
	}
	for i := range out {
		for j := range out[i] {
			out[i][j] += scales[j] * rng.NormFloat64()
			if !afmlFinite(out[i][j]) {
				return nil, errors.New("statistics: Gaussian experiment overflow")
			}
		}
	}
	return out, nil
}

// RandomTrajectoryForecasts reproduces snippet21.5: independent N-dimensional
// unit Gaussian means, unbiased empirical covariance from 1000 rank-N samples,
// and Uniform(0,1)*sqrt(diag(V)) costs at each horizon. The snippet hard-codes
// three columns; this implementation honors assets for its size=4 exercise.
func RandomTrajectoryForecasts(assets, horizons int, seed int64) ([]TrajectoryHorizon, error) {
	if assets < 1 || assets > 64 || horizons < 1 || horizons > 64 {
		return nil, errors.New("statistics: invalid random trajectory dimensions")
	}
	rng := rand.New(rand.NewSource(seed))
	out := make([]TrajectoryHorizon, horizons)
	for h := range out {
		x, err := GaussianMatrixWithRank(1000, assets, assets, 0, true, rng.Int63())
		if err != nil {
			return nil, err
		}
		p := TrajectoryHorizon{Mean: make([]float64, assets), Cost: make([]float64, assets), Covariance: make([][]float64, assets)}
		avg := make([]float64, assets)
		for _, row := range x {
			for j, v := range row {
				avg[j] += v / 1000
			}
		}
		for i := 0; i < assets; i++ {
			p.Mean[i] = rng.NormFloat64()
			p.Covariance[i] = make([]float64, assets)
			for j := 0; j < assets; j++ {
				for _, row := range x {
					p.Covariance[i][j] += (row[i] - avg[i]) * (row[j] - avg[j]) / 999
				}
			}
		}
		for i := range p.Cost {
			p.Cost[i] = rng.Float64() * math.Sqrt(p.Covariance[i][i])
		}
		out[h] = p
	}
	return out, nil
}
