package statistics

import (
	"errors"
	"math"

	"github.com/banbox/banbot/factor"
)

type NeutralAllocation struct {
	Weights                 []float64
	ConstraintRank          int
	Gross                   float64
	Scaled, NoResidualAlpha bool
}

// FactorNeutralWeights maximizes alpha'w-(risk/2)*w'diag(variance)w,
// subject to exposures'w=0 (Pole ch2/ch3). Supply an all-ones exposure column
// for dollar neutrality. Under factor neutrality the common-factor risk term
// vanishes; uniqueness is the remaining diagonal variance. Twice-orthogonal QR
// handles redundant constraint columns without unstable normal-equation inversion.
// A positive grossLimit applies a common proportional capital cap after fitting;
// it preserves neutrality but is not an exact L1-constrained utility optimum.
func FactorNeutralWeights(alpha, variance []float64, exposures [][]float64, risk, grossLimit float64) (NeutralAllocation, error) {
	if len(alpha) == 0 || len(variance) != len(alpha) || len(exposures) != len(alpha) || len(exposures[0]) == 0 || !finite(risk) || risk <= 0 || !finite(grossLimit) || grossLimit < 0 {
		return NeutralAllocation{}, errors.New("statistics: invalid neutral allocation dimensions/calibration")
	}
	precision := make([]float64, len(alpha))
	alphaScale := 0.0
	for i, a := range alpha {
		if !finite(a) || !finite(variance[i]) || variance[i] <= 0 || !finite(1/variance[i]) {
			return NeutralAllocation{}, errors.New("statistics: invalid forecast or uniqueness")
		}
		precision[i] = 1 / variance[i]
		alphaScale = max(alphaScale, math.Abs(a))
	}
	coefficients, rank, err := factor.LeastSquares(exposures, alpha, precision)
	if err != nil {
		return NeutralAllocation{}, err
	}
	out := NeutralAllocation{Weights: make([]float64, len(alpha)), ConstraintRank: rank}
	residualScale := 0.0
	for i, a := range alpha {
		residual := a
		for j, beta := range coefficients {
			residual -= exposures[i][j] * beta
		}
		if !finite(residual) {
			return NeutralAllocation{}, errors.New("statistics: neutral projection overflow")
		}
		residualScale = max(residualScale, math.Abs(residual))
		out.Weights[i] = residual / (risk * variance[i])
		if !finite(out.Weights[i]) || !finite(risk*variance[i]) || risk*variance[i] == 0 {
			return NeutralAllocation{}, errors.New("statistics: neutral allocation overflow or underflow")
		}
		out.Gross += math.Abs(out.Weights[i])
	}
	if !finite(out.Gross) {
		return NeutralAllocation{}, errors.New("statistics: neutral gross exposure overflow")
	}
	// A forecast entirely explained by constrained exposures has no feasible
	// alpha; do not magnify projection roundoff into a gross-normalized signal.
	if residualScale <= 1e-12*alphaScale {
		out.Weights = make([]float64, len(alpha))
		out.Gross, out.NoResidualAlpha = 0, true
		return out, nil
	}
	if grossLimit > 0 && out.Gross > grossLimit {
		for i := range out.Weights {
			out.Weights[i] *= grossLimit / out.Gross
		}
		out.Gross, out.Scaled = grossLimit, true
	}
	for column := range len(exposures[0]) {
		balance, scale := 0.0, 0.0
		for i, w := range out.Weights {
			v := exposures[i][column] * w
			balance += v
			scale += math.Abs(v)
		}
		if !finite(balance) || !finite(scale) || math.Abs(balance) > 1e-9*max(scale, math.SmallestNonzeroFloat64) {
			return NeutralAllocation{}, errors.New("statistics: neutral allocation failed constraint check")
		}
	}
	return out, nil
}
