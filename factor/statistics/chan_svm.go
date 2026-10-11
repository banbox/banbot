package statistics

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
)

type SVMConfig struct {
	Kernel              string
	C, Scale, Tolerance float64
	Degree, MaxSweeps   int
}
type SVM struct {
	config       SVMConfig
	support      [][]float64
	coefficients []float64
	intercept    float64
	features     int
}

func svmKernel(a, b []float64, c SVMConfig) float64 {
	v := 0.0
	for j := range a {
		if c.Kernel == "rbf" {
			d := (a[j] - b[j]) / c.Scale
			v += d * d
		} else {
			v += a[j] * b[j] / (c.Scale * c.Scale)
		}
	}
	if c.Kernel == "rbf" {
		return math.Exp(-v / 2)
	}
	if c.Kernel == "polynomial" {
		return math.Pow(1+v, float64(c.Degree))
	}
	return v
}

// FitSVM solves the soft-margin binary dual with exact constrained two-variable
// coordinate steps. Labels are 0/1; no implicit feature scaling is introduced.
func FitSVM(x [][]float64, y []float64, c SVMConfig) (*SVM, error) {
	if c.Kernel == "" {
		c.Kernel = "linear"
	}
	if c.C == 0 {
		c.C = 1
	}
	if c.Scale == 0 {
		c.Scale = 1
	}
	if c.Tolerance == 0 {
		c.Tolerance = 1e-7
	}
	if c.Degree == 0 {
		c.Degree = 3
	}
	if c.MaxSweeps == 0 {
		c.MaxSweeps = 1000
	}
	if len(x) < 2 || len(x) != len(y) || len(x) > 3000 || len(x[0]) == 0 || !finite(c.C) || c.C <= 0 || !finite(c.Scale) || c.Scale <= 0 || !finite(c.Tolerance) || c.Tolerance <= 0 || c.MaxSweeps < 1 || c.MaxSweeps > 10000 || c.Degree < 1 || c.Degree > 8 || (c.Kernel != "linear" && c.Kernel != "polynomial" && c.Kernel != "rbf") {
		return nil, fmt.Errorf("statistics: invalid SVM sample/configuration")
	}
	n := len(x)
	labels := make([]float64, n)
	positive := 0
	for i, row := range x {
		if len(row) != len(x[0]) || (y[i] != 0 && y[i] != 1) {
			return nil, fmt.Errorf("statistics: invalid SVM dimensions/labels")
		}
		for _, v := range row {
			if !finite(v) {
				return nil, fmt.Errorf("statistics: nonfinite SVM feature")
			}
		}
		labels[i] = 2*y[i] - 1
		if y[i] == 1 {
			positive++
		}
	}
	m := &SVM{config: c, features: len(x[0])}
	if positive == 0 || positive == n {
		m.intercept = labels[0]
		return m, nil
	}
	kernel := make([][]float64, n)
	for i := range kernel {
		kernel[i] = make([]float64, n)
	}
	for i := range x {
		for j := 0; j <= i; j++ {
			v := svmKernel(x[i], x[j], c)
			if !finite(v) {
				return nil, fmt.Errorf("statistics: SVM kernel overflow")
			}
			kernel[i][j], kernel[j][i] = v, v
		}
	}
	alpha, score := make([]float64, n), make([]float64, n)
	converged := false
	for sweep := 0; sweep < c.MaxSweeps; sweep++ {
		improvement := 0.0
		for i := 0; i < n; i++ {
			bestGain, bestStep, bestJ := 0.0, 0.0, -1
			for j := 0; j < n; j++ {
				if j == i {
					continue
				}
				sign := labels[i] * labels[j]
				lo, hi := -alpha[i], c.C-alpha[i]
				if sign == 1 {
					lo = math.Max(lo, alpha[j]-c.C)
					hi = math.Min(hi, alpha[j])
				} else {
					lo = math.Max(lo, -alpha[j])
					hi = math.Min(hi, c.C-alpha[j])
				}
				if hi-lo <= 1e-14 {
					continue
				}
				derivative := 1 - sign - labels[i]*(score[i]-score[j])
				curvature := math.Max(0, kernel[i][i]+kernel[j][j]-2*kernel[i][j])
				step := lo
				if curvature > 0 {
					step = math.Max(lo, math.Min(hi, derivative/curvature))
				} else if derivative > 0 {
					step = hi
				}
				gain := derivative*step - .5*curvature*step*step
				if gain > bestGain {
					bestGain, bestStep, bestJ = gain, step, j
				}
			}
			if bestGain <= c.Tolerance*c.Tolerance || bestJ < 0 {
				continue
			}
			j := bestJ
			alpha[i] += bestStep
			alpha[j] -= labels[i] * labels[j] * bestStep
			for k := 0; k < n; k++ {
				score[k] += labels[i] * bestStep * (kernel[k][i] - kernel[k][j])
			}
			improvement += bestGain
		}
		if improvement == 0 {
			converged = true
			break
		}
	}
	if !converged {
		return nil, fmt.Errorf("statistics: SVM sweep budget exhausted before dual convergence")
	}
	free, intercept := 0, 0.0
	lower, upper := math.Inf(-1), math.Inf(1)
	for i, a := range alpha {
		b := labels[i] - score[i]
		if a > 1e-8 && a < c.C-1e-8 {
			intercept += b
			free++
		} else if (a <= 1e-8 && labels[i] > 0) || (a >= c.C-1e-8 && labels[i] < 0) {
			lower = math.Max(lower, b)
		} else {
			upper = math.Min(upper, b)
		}
		if a > 1e-12 {
			m.support = append(m.support, slices.Clone(x[i]))
			m.coefficients = append(m.coefficients, a*labels[i])
		}
	}
	if free > 0 {
		m.intercept = intercept / float64(free)
	} else {
		m.intercept = (lower + upper) / 2
	}
	if !finite(m.intercept) {
		return nil, fmt.Errorf("statistics: nonfinite SVM intercept")
	}
	return m, nil
}
func (m *SVM) Decision(x []float64) (float64, error) {
	if m == nil || len(x) != m.features {
		return 0, fmt.Errorf("statistics: invalid SVM prediction")
	}
	for _, v := range x {
		if !finite(v) {
			return 0, fmt.Errorf("statistics: nonfinite SVM predictor")
		}
	}
	v := m.intercept
	for i, row := range m.support {
		v += m.coefficients[i] * svmKernel(row, x, m.config)
	}
	if !finite(v) {
		return 0, fmt.Errorf("statistics: SVM prediction overflow")
	}
	return v, nil
}
func (m *SVM) Predict(x []float64) (float64, error) {
	v, err := m.Decision(x)
	if err != nil {
		return 0, err
	}
	if v > 0 {
		return 1, nil
	}
	return 0, nil
}

// SelectSVMFold retains the lowest held-out classification-error fold model,
// matching the source's single-fold model selection rather than a full refit.
func SelectSVMFold(x [][]float64, y []float64, c SVMConfig, folds int, seed int64) (*SVM, int, error) {
	if len(x) != len(y) || folds < 2 || folds > len(y) {
		return nil, 0, fmt.Errorf("statistics: invalid SVM folds")
	}
	perm := rand.New(rand.NewSource(seed)).Perm(len(y))
	bestLoss := math.Inf(1)
	var best *SVM
	bestFold := 0
	for fold := 0; fold < folds; fold++ {
		var tx [][]float64
		var ty []float64
		var validation []int
		for i, row := range perm {
			if i%folds == fold {
				validation = append(validation, row)
			} else {
				tx = append(tx, x[row])
				ty = append(ty, y[row])
			}
		}
		m, err := FitSVM(tx, ty, c)
		if err != nil {
			return nil, 0, err
		}
		loss := 0.0
		for _, row := range validation {
			v, err := m.Predict(x[row])
			if err != nil {
				return nil, 0, err
			}
			if v != y[row] {
				loss++
			}
		}
		loss /= float64(len(validation))
		if loss < bestLoss {
			bestLoss, best, bestFold = loss, m, fold
		}
	}
	return best, bestFold, nil
}
