package statistics

import (
	"fmt"
	"math"
	"time"

	"gonum.org/v1/gonum/optimize"
)

func LocalLevelLikelihood(y []float64, initial LocalLevel) (float64, error) {
	if len(y) == 0 {
		return 0, fmt.Errorf("statistics: empty local-level sample")
	}
	nll := 0.0
	for _, v := range y {
		o, err := initial.Observe(v)
		if err != nil {
			return 0, err
		}
		z := o.Innovation / math.Sqrt((o.PredictionVariance + initial.ObservationVariance))
		nll += .5 * (math.Log(2*math.Pi) + math.Log((o.PredictionVariance + initial.ObservationVariance)) + z*z)
		if !finite(nll) {
			return 0, fmt.Errorf("statistics: local-level likelihood overflow")
		}
	}
	return nll, nil
}

type LocalLevelFitSpec struct {
	Initial                       LocalLevel
	MaxEvaluations, MaxIterations int
	Runtime                       time.Duration
}
type LocalLevelFit struct {
	Initial                                             LocalLevel
	NegativeLogLikelihood, InitialNegativeLogLikelihood float64
	Evaluations                                         int
}

// FitLocalLevel holds the explicitly declared initial prior fixed and optimizes
// positive diagonal Q/R by Gaussian innovation likelihood on training data.
func FitLocalLevel(y []float64, s LocalLevelFitSpec) (LocalLevelFit, error) {
	if len(y) < 8 || s.Initial.ProcessVariance <= 0 {
		return LocalLevelFit{}, fmt.Errorf("statistics: local-level MLE needs >=8 observations and positive initial Q")
	}
	initial, err := LocalLevelLikelihood(y, s.Initial)
	if err != nil {
		return LocalLevelFit{}, err
	}
	if s.MaxEvaluations == 0 {
		s.MaxEvaluations = 3000
	}
	if s.MaxIterations == 0 {
		s.MaxIterations = 2000
	}
	if s.Runtime == 0 {
		s.Runtime = 15 * time.Second
	}
	if s.MaxEvaluations < 1 || s.MaxIterations < 1 || s.Runtime < 0 {
		return LocalLevelFit{}, fmt.Errorf("statistics: invalid local-level fit budget")
	}
	decode := func(x []float64) LocalLevel {
		k := s.Initial
		k.ProcessVariance, k.ObservationVariance = math.Exp(x[0]), math.Exp(x[1])
		return k
	}
	problem := optimize.Problem{Func: func(x []float64) float64 {
		n, e := LocalLevelLikelihood(y, decode(x))
		if e != nil {
			return math.Inf(1)
		}
		return n
	}}
	r, err := optimize.Minimize(problem, []float64{math.Log(s.Initial.ProcessVariance), math.Log(s.Initial.ObservationVariance)}, &optimize.Settings{FuncEvaluations: s.MaxEvaluations, MajorIterations: s.MaxIterations, Runtime: s.Runtime, Converger: &optimize.FunctionConverge{Absolute: 1e-9, Relative: 1e-9, Iterations: 40}}, &optimize.NelderMead{})
	if err != nil {
		return LocalLevelFit{}, err
	}
	if r == nil || r.Status.Early() || !finite(r.F) || r.F > initial+1e-8 {
		return LocalLevelFit{}, fmt.Errorf("statistics: local-level MLE failed to converge")
	}
	return LocalLevelFit{decode(r.X), r.F, initial, r.FuncEvaluations}, nil
}
