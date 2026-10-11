package statistics

import (
	"context"
	"errors"
	"math"
	"time"

	"gonum.org/v1/gonum/optimize"
)

// KalmanLikelihood is the Gaussian innovation negative log likelihood for
// y=beta*x+offset, random-walk states and declared prior mean/covariance.
func KalmanLikelihood(x, y []float64, initial KalmanPair) (float64, error) {
	if len(x) == 0 || len(x) != len(y) {
		return 0, errors.New("statistics: invalid Kalman likelihood sample")
	}
	k := initial
	nll := 0.0
	for i := range x {
		o, err := k.Observe(x[i], y[i])
		if err != nil {
			return 0, err
		}
		scaled := o.Error / math.Sqrt(o.Variance)
		nll += .5 * (math.Log(2*math.Pi) + math.Log(o.Variance) + scaled*scaled)
		if !finite(nll) {
			return 0, errors.New("statistics: likelihood overflow")
		}
	}
	return nll, nil
}

type KalmanFitSpec struct {
	Initial                       KalmanPair
	MaxEvaluations, MaxIterations int
	Runtime                       time.Duration
	DiagonalProcessNoise          bool
}
type KalmanNoiseFit struct {
	Initial                                             KalmanPair
	InitialNegativeLogLikelihood, NegativeLogLikelihood float64
	Evaluations                                         int
	Status                                              string
	Converged                                           bool
}

// FitKalmanNoise maximizes training-only innovation likelihood over Q,R.
// DiagonalProcessNoise restricts the slope/offset noise covariance to zero.
// A Cholesky/log parameterization keeps Q positive definite and R positive.
// Initial state is held fixed and is part of the specification; diffuse-prior,
// smoothed-state and vendor-estimator conventions are not silently substituted.
// A budget/cancellation exit returns an error, never a successful fitted model.
func FitKalmanNoise(ctx context.Context, x, y []float64, spec KalmanFitSpec) (KalmanNoiseFit, error) {
	if ctx == nil || len(x) < 8 || len(x) != len(y) {
		return KalmanNoiseFit{}, errors.New("statistics: invalid Kalman fit sample/context")
	}
	if err := ctx.Err(); err != nil {
		return KalmanNoiseFit{}, err
	}
	initialNLL, err := KalmanLikelihood(x, y, spec.Initial)
	if err != nil {
		return KalmanNoiseFit{}, err
	}
	q := spec.Initial.ProcessCovariance
	if q == [2][2]float64{} {
		q = [2][2]float64{{spec.Initial.ProcessVariance, 0}, {0, spec.Initial.ProcessVariance}}
	}
	if !pairPSD(q) || q[0][0] <= 0 || q[1][1] <= 0 {
		return KalmanNoiseFit{}, errors.New("statistics: MLE needs positive-definite initial Q")
	}
	if spec.DiagonalProcessNoise && (q[0][1] != 0 || q[1][0] != 0) {
		return KalmanNoiseFit{}, errors.New("statistics: diagonal MLE requires diagonal initial process covariance")
	}
	l11 := math.Sqrt(q[0][0])
	l21 := q[1][0] / l11
	l22sq := q[1][1] - l21*l21
	if l22sq <= 0 || !finite(l22sq) {
		return KalmanNoiseFit{}, errors.New("statistics: singular initial process covariance")
	}
	start := []float64{math.Log(l11), l21, math.Log(math.Sqrt(l22sq)), math.Log(spec.Initial.ObservationVariance)}
	if spec.DiagonalProcessNoise {
		start = []float64{math.Log(l11), math.Log(math.Sqrt(l22sq)), math.Log(spec.Initial.ObservationVariance)}
	}
	decode := func(theta []float64) KalmanPair {
		k := spec.Initial
		k.ProcessVariance = 0
		if spec.DiagonalProcessNoise {
			a, c := math.Exp(theta[0]), math.Exp(theta[1])
			k.ProcessCovariance = [2][2]float64{{a * a, 0}, {0, c * c}}
			k.ObservationVariance = math.Exp(theta[2])
			return k
		}
		a, b, c := math.Exp(theta[0]), theta[1], math.Exp(theta[2])
		k.ProcessCovariance = [2][2]float64{{a * a, a * b}, {a * b, b*b + c*c}}
		k.ObservationVariance = math.Exp(theta[3])
		return k
	}
	problem := optimize.Problem{Func: func(theta []float64) float64 {
		nll, err := KalmanLikelihood(x, y, decode(theta))
		if err != nil {
			return math.Inf(1)
		}
		return nll
	}, Status: func() (optimize.Status, error) {
		if err := ctx.Err(); err != nil {
			return optimize.Failure, err
		}
		return optimize.NotTerminated, nil
	}}
	if spec.MaxEvaluations == 0 {
		spec.MaxEvaluations = 4000
	}
	if spec.MaxIterations == 0 {
		spec.MaxIterations = 2000
	}
	if spec.Runtime == 0 {
		spec.Runtime = 30 * time.Second
	}
	if spec.MaxEvaluations < 1 || spec.MaxIterations < 1 || spec.Runtime < 0 {
		return KalmanNoiseFit{}, errors.New("statistics: invalid likelihood fit budget")
	}
	settings := &optimize.Settings{FuncEvaluations: spec.MaxEvaluations, MajorIterations: spec.MaxIterations, Runtime: spec.Runtime, Converger: &optimize.FunctionConverge{Absolute: 1e-8, Relative: 1e-8, Iterations: 80}}
	result, err := optimize.Minimize(problem, start, settings, &optimize.NelderMead{})
	if result == nil {
		return KalmanNoiseFit{}, err
	}
	out := KalmanNoiseFit{Initial: decode(result.X), InitialNegativeLogLikelihood: initialNLL, NegativeLogLikelihood: result.F, Evaluations: result.FuncEvaluations, Status: result.Status.String(), Converged: err == nil && !result.Status.Early() && finite(result.F) && result.F <= initialNLL+1e-8}
	if err != nil {
		return out, err
	}
	if !out.Converged {
		if e := result.Status.Err(); e != nil {
			return out, e
		}
		return out, errors.New("statistics: Kalman fit did not establish improved finite likelihood")
	}
	return out, nil
}
