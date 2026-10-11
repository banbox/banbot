package statistics

import "errors"

// DynamicAR1 models Pole Ch3: y[t]=beta[t]*y[t-1]+epsilon[t], with
// beta[t]=beta[t-1]+eta[t]. Noise scales are declared by the caller; this
// operator specifies neither a hedge-ratio model nor an entry/exit policy.
type DynamicAR1 struct {
	beta, variance, processVariance float64
}

type DynamicAR1Observation struct {
	Prediction, Innovation, InnovationVariance  float64
	PriorBeta, PosteriorBeta, PosteriorVariance float64
}

func NewDynamicAR1(beta, variance, processVariance float64) (*DynamicAR1, error) {
	if !finite(beta) || !finite(variance) || !finite(processVariance) || variance < 0 || processVariance < 0 {
		return nil, errors.New("statistics: invalid dynamic AR1 initialization")
	}
	return &DynamicAR1{beta, variance, processVariance}, nil
}

// Observe returns the pre-update innovation before changing the coefficient.
// Callers must supply consecutive, closed observations and reset across gaps.
// Joseph covariance and transactional validation preserve the previous state
// when an observation or numerical calculation is invalid.
func (k *DynamicAR1) Observe(previous, current, observationVariance float64) (DynamicAR1Observation, error) {
	if k == nil || !finite(previous) || !finite(current) || !finite(observationVariance) || observationVariance <= 0 {
		return DynamicAR1Observation{}, errors.New("statistics: invalid dynamic AR1 observation")
	}
	p := k.variance + k.processVariance
	prediction := k.beta * previous
	innovation := current - prediction
	s := previous*previous*p + observationVariance
	if !finite(p) || !finite(prediction) || !finite(innovation) || !finite(s) || s <= 0 {
		return DynamicAR1Observation{}, errors.New("statistics: dynamic AR1 innovation overflow")
	}
	gain := p * previous / s
	beta := k.beta + gain*innovation
	a := 1 - gain*previous
	variance := a*a*p + gain*gain*observationVariance
	if !finite(beta) || !finite(variance) || variance < 0 {
		return DynamicAR1Observation{}, errors.New("statistics: dynamic AR1 update overflow")
	}
	out := DynamicAR1Observation{prediction, innovation, s, k.beta, beta, variance}
	k.beta, k.variance = beta, variance
	return out, nil
}
