package statistics

import (
	"errors"
	"math"
	"sort"
)

// PITScalar is an explicitly observed scalar. Both publication and receipt must
// precede a research decision; event time alone does not establish visibility.
type PITScalar struct {
	EventMS, AvailableMS, ReceivedMS int64
	Value                            float64
}

func visibleScalar(v PITScalar, cutoff int64) bool {
	return v.EventMS <= cutoff && v.AvailableMS <= cutoff && v.ReceivedMS <= cutoff
}
func validScalar(v PITScalar) bool {
	return v.EventMS >= 0 && v.AvailableMS >= v.EventMS && v.ReceivedMS >= v.AvailableMS && afmlFinite(v.Value)
}

type IrregularFourierComponent struct {
	FrequencyHz, Real, Imaginary, Amplitude float64
	Samples                                 int
}

// NonuniformDFT evaluates sum(value*exp(-2*pi*i*f*(time-origin)))/N on
// visible observations at their actual irregular timestamps (no interpolation).
// AFML22.6.6 suggests nonuniform FFT without specifying its implementation;
// this exact O(NF) bounded DFT is an explicit algorithmic extension, not a fast
// NUFFT. Frequencies use cycles/second. Phase origin is caller-specified; optional
// centering removes the visible sample mean. No Nyquist claim is made for
// irregular samples. Prices/volumes and preprocessing are caller choices.
func NonuniformDFT(samples []PITScalar, frequencies []float64, originMS, cutoffMS int64, center bool, maxWork uint64) ([]IrregularFourierComponent, error) {
	if len(samples) < 1 || len(samples) > 2000000 || len(frequencies) < 1 || len(frequencies) > 100000 || originMS < 0 || cutoffMS < originMS || maxWork == 0 || uint64(len(samples))*uint64(len(frequencies)) > maxWork {
		return nil, errors.New("statistics: invalid or over-budget irregular Fourier request")
	}
	for _, f := range frequencies {
		if !afmlFinite(f) || f < 0 {
			return nil, errors.New("statistics: invalid Fourier frequency")
		}
	}
	known := make([]PITScalar, 0, len(samples))
	mean := 0.
	for _, s := range samples {
		if !validScalar(s) {
			return nil, errors.New("statistics: invalid Fourier observation chronology/value")
		}
		if visibleScalar(s, cutoffMS) {
			if s.EventMS < originMS {
				return nil, errors.New("statistics: Fourier observation precedes phase origin")
			}
			known = append(known, s)
			mean += (s.Value - mean) / float64(len(known))
			if !afmlFinite(mean) {
				return nil, errors.New("statistics: Fourier centering overflow")
			}
		}
	}
	if len(known) == 0 {
		return nil, errors.New("statistics: Fourier request has no visible observations")
	}
	if !center {
		mean = 0
	}
	out := make([]IrregularFourierComponent, len(frequencies))
	for j, f := range frequencies {
		c := IrregularFourierComponent{FrequencyHz: f, Samples: len(known)}
		for _, s := range known {
			cycles := f * (float64(s.EventMS-originMS) / 1000)
			if !afmlFinite(cycles) || math.Abs(cycles) > 1<<52 {
				return nil, errors.New("statistics: Fourier phase exceeds numeric precision")
			}
			phase := 2 * math.Pi * math.Remainder(cycles, 1)
			sin, cos := math.Sincos(phase)
			v := (s.Value - mean) / float64(len(known))
			c.Real += v * cos
			c.Imaginary -= v * sin
		}
		c.Amplitude = math.Hypot(c.Real, c.Imaginary)
		if !afmlFinite(c.Amplitude) {
			return nil, errors.New("statistics: Fourier sum overflow")
		}
		out[j] = c
	}
	return out, nil
}

// FrozenEmpiricalCDF is fitted only from observations available and received by
// a training cutoff. Inclusive ties yield P_train(X<=x). It is immutable and
// contains no update-on-score path, avoiding test-set recalibration.
type FrozenEmpiricalCDF struct {
	trainingCutoffMS int64
	sorted           []float64
}

func FitEmpiricalCDF(training []PITScalar, cutoffMS int64) (*FrozenEmpiricalCDF, error) {
	if len(training) == 0 || len(training) > 2000000 || cutoffMS < 0 {
		return nil, errors.New("statistics: invalid empirical CDF training request")
	}
	v := make([]float64, len(training))
	for i, s := range training {
		if !validScalar(s) || !visibleScalar(s, cutoffMS) {
			return nil, errors.New("statistics: empirical CDF training contains unavailable observations")
		}
		v[i] = s.Value
	}
	sort.Float64s(v)
	return &FrozenEmpiricalCDF{cutoffMS, v}, nil
}

func (f *FrozenEmpiricalCDF) Score(value PITScalar, decisionMS int64) (float64, error) {
	if f == nil || len(f.sorted) == 0 || decisionMS <= f.trainingCutoffMS || value.EventMS <= f.trainingCutoffMS || !validScalar(value) || !visibleScalar(value, decisionMS) {
		return 0, errors.New("statistics: invalid holdout empirical CDF observation")
	}
	k := sort.Search(len(f.sorted), func(i int) bool { return f.sorted[i] > value.Value })
	return float64(k) / float64(len(f.sorted)), nil
}
