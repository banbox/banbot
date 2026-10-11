package statistics

import (
	"context"
	"errors"
	"math"

	"gonum.org/v1/gonum/mat"
)

// FactorAnalysisModel represents r=Mean+L*f+epsilon, Cov(f)=I and diagonal
// Cov(epsilon)=Uniqueness (Pole ch3.10). Loading rows correspond to assets.
// Gaussian likelihood estimation is distinct from covariance PCA. Factor count
// and the positive uniqueness floor are explicit caller calibration choices.
type FactorAnalysisModel struct {
	Mean                  []float64
	Loadings              [][]float64
	Uniqueness            []float64
	NegativeLogLikelihood float64 // per observation, including Gaussian constant
	Iterations            int
	FloorActive           bool
}

type FactorAnalysisConfig struct {
	Components, MaxIterations int
	Tolerance, MinUniqueness  float64
	ZeroMean                  bool // Pole's zero-mean return specialization
}

// FitFactorAnalysis performs bounded Gaussian maximum-likelihood EM on the
// supplied training rows. Orthogonal loading rotations remain unidentified;
// callers may use VarimaxLoadings for an interpretable equivalent orientation.
// Budget/cancellation failure never returns a model labelled as converged.
func FitFactorAnalysis(ctx context.Context, rows [][]float64, config FactorAnalysisConfig) (FactorAnalysisModel, error) {
	if ctx == nil || len(rows) < config.Components+2 || len(rows) == 0 || config.Components < 1 || config.Components >= len(rows[0]) || len(rows[0]) > 256 || config.MaxIterations < 1 || config.MaxIterations > 10000 || !finite(config.Tolerance) || config.Tolerance <= 0 || !finite(config.MinUniqueness) || config.MinUniqueness <= 0 {
		return FactorAnalysisModel{}, errors.New("statistics: invalid factor-analysis dimensions/budget")
	}
	p, k := len(rows[0]), config.Components
	mean := make([]float64, p)
	for i, row := range rows {
		if len(row) != p {
			return FactorAnalysisModel{}, errors.New("statistics: ragged factor-analysis matrix")
		}
		for j, value := range row {
			if !finite(value) {
				return FactorAnalysisModel{}, errors.New("statistics: nonfinite factor-analysis training")
			}
			if !config.ZeroMean {
				mean[j] += (value - mean[j]) / float64(i+1)
			}
		}
	}
	cov := mat.NewSymDense(p, nil)
	for _, row := range rows {
		for i := range p {
			for j := 0; j <= i; j++ {
				value := cov.At(i, j) + (row[i]-mean[i])*(row[j]-mean[j])/float64(len(rows))
				if !finite(value) {
					return FactorAnalysisModel{}, errors.New("statistics: factor-analysis covariance overflow")
				}
				cov.SetSym(i, j, value)
			}
		}
	}
	noise := 0.0
	for i := range p {
		if cov.At(i, i) <= config.MinUniqueness {
			return FactorAnalysisModel{}, errors.New("statistics: constant or below-floor factor feature")
		}
		noise += cov.At(i, i) / (2 * float64(p))
	}
	var eigen mat.EigenSym
	if !eigen.Factorize(cov, true) {
		return FactorAnalysisModel{}, errors.New("statistics: factor-analysis initialization failed")
	}
	values := eigen.Values(nil)
	var vectors mat.Dense
	eigen.VectorsTo(&vectors)
	loading := mat.NewDense(p, k, nil)
	uniqueness := make([]float64, p)
	for i := range p {
		uniqueness[i] = max(config.MinUniqueness, cov.At(i, i)/2)
		for j := range k {
			loading.Set(i, j, vectors.At(i, p-1-j)*math.Sqrt(max(config.MinUniqueness, values[p-1-j]-noise)))
		}
	}
	previous, err := factorLikelihood(cov, loading, uniqueness)
	if err != nil {
		return FactorAnalysisModel{}, err
	}
	floorActive := false
	for iteration := 1; iteration <= config.MaxIterations; iteration++ {
		if err := ctx.Err(); err != nil {
			return FactorAnalysisModel{}, err
		}
		precision := mat.NewSymDense(k, nil)
		weighted := mat.NewDense(k, p, nil)
		for i := range k {
			for a := range p {
				weighted.Set(i, a, loading.At(a, i)/uniqueness[a])
			}
			for j := 0; j <= i; j++ {
				v := 0.0
				if i == j {
					v = 1
				}
				for a := range p {
					v += loading.At(a, i) * loading.At(a, j) / uniqueness[a]
				}
				precision.SetSym(i, j, v)
			}
		}
		var latent mat.Cholesky
		if !latent.Factorize(precision) {
			return FactorAnalysisModel{}, errors.New("statistics: factor posterior precision failed")
		}
		var a mat.Dense
		if err := latent.SolveTo(&a, weighted); err != nil {
			return FactorAnalysisModel{}, err
		}
		var c mat.SymDense
		if err := latent.InverseTo(&c); err != nil {
			return FactorAnalysisModel{}, err
		}
		var cross, aca mat.Dense
		cross.Mul(cov, a.T())
		aca.Mul(&a, &cross)
		second := mat.NewSymDense(k, nil)
		for i := range k {
			for j := 0; j <= i; j++ {
				second.SetSym(i, j, c.At(i, j)+aca.At(i, j))
			}
		}
		var ff mat.Cholesky
		if !ff.Factorize(second) {
			return FactorAnalysisModel{}, errors.New("statistics: latent second moment failed")
		}
		var transposed mat.Dense
		if err := ff.SolveTo(&transposed, cross.T()); err != nil {
			return FactorAnalysisModel{}, err
		}
		next := mat.DenseCopyOf(transposed.T())
		nextUniqueness := make([]float64, p)
		for i := range p {
			v := cov.At(i, i)
			for j := range k {
				v -= next.At(i, j) * cross.At(i, j)
			}
			if !finite(v) {
				return FactorAnalysisModel{}, errors.New("statistics: uniqueness overflow")
			}
			if v < config.MinUniqueness {
				floorActive = true
			}
			nextUniqueness[i] = max(config.MinUniqueness, v)
		}
		objective, err := factorLikelihood(cov, next, nextUniqueness)
		if err != nil {
			return FactorAnalysisModel{}, err
		}
		if objective > previous+1e-8*max(1, math.Abs(previous)) {
			return FactorAnalysisModel{}, errors.New("statistics: factor likelihood failed to improve")
		}
		loading, uniqueness = next, nextUniqueness
		if math.Abs(previous-objective) <= config.Tolerance*max(1, math.Abs(previous)) {
			model := FactorAnalysisModel{Mean: mean, Uniqueness: uniqueness, NegativeLogLikelihood: objective, Iterations: iteration, FloorActive: floorActive, Loadings: make([][]float64, p)}
			for i := range p {
				model.Loadings[i] = append([]float64(nil), loading.RawRowView(i)...)
			}
			canonicalLoadingSigns(model.Loadings)
			return model, nil
		}
		previous = objective
	}
	return FactorAnalysisModel{}, errors.New("statistics: factor-analysis iteration budget exhausted")
}

func factorLikelihood(sample *mat.SymDense, loading *mat.Dense, uniqueness []float64) (float64, error) {
	p, k := loading.Dims()
	cov := mat.NewSymDense(p, nil)
	for i := range p {
		for j := 0; j <= i; j++ {
			v := 0.0
			if i == j {
				v = uniqueness[i]
			}
			for factor := range k {
				v += loading.At(i, factor) * loading.At(j, factor)
			}
			if !finite(v) {
				return 0, errors.New("statistics: factor covariance overflow")
			}
			cov.SetSym(i, j, v)
		}
	}
	var chol mat.Cholesky
	if !chol.Factorize(cov) {
		return 0, errors.New("statistics: factor covariance not positive definite")
	}
	var solve mat.Dense
	if err := chol.SolveTo(&solve, sample); err != nil {
		return 0, err
	}
	trace := 0.0
	for i := range p {
		trace += solve.At(i, i)
	}
	value := .5 * (float64(p)*math.Log(2*math.Pi) + chol.LogDet() + trace)
	if !finite(value) {
		return 0, errors.New("statistics: factor likelihood overflow")
	}
	return value, nil
}

type FactorScoreResult struct{ Scores, Residuals []float64 }

// FactorScores implements the source's Bartlett/GLS score when posterior=false:
// (L'Psi^-1 L)^-1 L'Psi^-1(r-mu). posterior=true instead gives the Gaussian
// posterior mean L'(LL'+Psi)^-1(r-mu). These are distinct source estimators.
func (m FactorAnalysisModel) FactorScores(row []float64, posterior bool) (FactorScoreResult, error) {
	p := len(m.Loadings)
	if p == 0 || len(m.Loadings[0]) == 0 || len(row) != p || len(m.Mean) != p || len(m.Uniqueness) != p {
		return FactorScoreResult{}, errors.New("statistics: invalid factor-score dimensions")
	}
	k := len(m.Loadings[0])
	if k >= p {
		return FactorScoreResult{}, errors.New("statistics: factor count must be smaller than assets")
	}
	x, y := make([][]float64, p), make([]float64, p)
	for i := range p {
		if len(m.Loadings[i]) != k || !finite(row[i]) || !finite(m.Mean[i]) || !finite(m.Uniqueness[i]) || m.Uniqueness[i] <= 0 {
			return FactorScoreResult{}, errors.New("statistics: invalid factor-score model/value")
		}
		x[i] = make([]float64, k)
		scale := math.Sqrt(m.Uniqueness[i])
		y[i] = (row[i] - m.Mean[i]) / scale
		for j, v := range m.Loadings[i] {
			if !finite(v) {
				return FactorScoreResult{}, errors.New("statistics: nonfinite loading")
			}
			x[i][j] = v / scale
		}
	}
	if posterior {
		// The latent I prior adds identity rows to GLS, reproducing posterior
		// shrinkage without explicit inversion of the asset covariance matrix.
		for j := range k {
			identity := make([]float64, k)
			identity[j] = 1
			x = append(x, identity)
			y = append(y, 0)
		}
	}
	fit, err := Regress(y, x, false)
	if err != nil {
		return FactorScoreResult{}, err
	}
	out := FactorScoreResult{Scores: fit.Beta, Residuals: make([]float64, p)}
	for i := range p {
		out.Residuals[i] = row[i] - m.Mean[i]
		for j := range k {
			out.Residuals[i] -= m.Loadings[i][j] * out.Scores[j]
		}
		if !finite(out.Residuals[i]) {
			return FactorScoreResult{}, errors.New("statistics: factor-score residual overflow")
		}
	}
	return out, nil
}

func canonicalLoadingSigns(loadings [][]float64) {
	for column := range len(loadings[0]) {
		pivot := 0
		for i := range loadings {
			if math.Abs(loadings[i][column]) > math.Abs(loadings[pivot][column]) {
				pivot = i
			}
		}
		if loadings[pivot][column] < 0 {
			for i := range loadings {
				loadings[i][column] = -loadings[i][column]
			}
		}
	}
}
