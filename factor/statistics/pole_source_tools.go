package statistics

import (
	"fmt"
	"gonum.org/v1/gonum/stat"
	"math"
)

type PoleTurningPointSpec struct {
	Window                  int
	Annualization, Fraction float64
	StartIndex, StartKind   int // caller-declared known seed: -1 trough, +1 peak
}
type PoleTurningPoint struct {
	Extremum, Confirmed, Kind                                  int
	Price, ConfirmationPrice, AnnualVolatility, ReversalReturn float64
}

// PoleConfirmedTurningPoints implements ch2's forward confirmation of a
// local extreme. Event prices belong to Extremum; trading information becomes
// available only at Confirmed. No unconfirmed terminal endpoint is returned.
// Simple returns, sample standard deviation and earliest tied extremes are
// explicit conventions where the source leaves these details unstated.
func PoleConfirmedTurningPoints(prices []float64, s PoleTurningPointSpec) ([]PoleTurningPoint, error) {
	if s.Window == 0 {
		s.Window = 20
	}
	if s.Annualization == 0 {
		s.Annualization = 16
	}
	if s.Fraction == 0 {
		s.Fraction = .30
	}
	if s.Window < 2 || s.Window > 1000000 || s.StartIndex < s.Window || s.StartIndex >= len(prices) || s.StartKind != -1 && s.StartKind != 1 || !finite(s.Annualization) || s.Annualization <= 0 || !finite(s.Fraction) || s.Fraction <= 0 {
		return nil, fmt.Errorf("statistics: invalid Pole turning seed/calibration")
	}
	returns := make([]float64, len(prices)-1)
	for i, p := range prices {
		if !finite(p) || p <= 0 {
			return nil, fmt.Errorf("statistics: positive turning prices required")
		}
		if i > 0 {
			returns[i-1] = p/prices[i-1] - 1
			if !finite(returns[i-1]) {
				return nil, fmt.Errorf("statistics: turning return overflow")
			}
		}
	}
	var out []PoleTurningPoint
	kind := -s.StartKind
	extreme := s.StartIndex
	for i := s.StartIndex + 1; i < len(prices); i++ {
		if kind == 1 && prices[i] > prices[extreme] || kind == -1 && prices[i] < prices[extreme] {
			extreme = i
		}
		_, sigma, e := MeanStd(returns[i-s.Window:i], 1)
		if e != nil {
			return nil, e
		}
		annual := sigma * s.Annualization
		reversal := prices[i]/prices[extreme] - 1
		if !finite(annual) {
			return nil, fmt.Errorf("statistics: turning volatility overflow")
		}
		confirmed := kind == 1 && -reversal > s.Fraction*annual || kind == -1 && reversal > s.Fraction*annual
		if !confirmed {
			continue
		}
		out = append(out, PoleTurningPoint{extreme, i, kind, prices[extreme], prices[i], annual, reversal})
		// The opposite extreme may precede the confirmation observation; preserve
		// that already-observed excursion rather than resetting only at currentprice.
		previous := extreme
		kind = -kind
		extreme = previous + 1
		for j := previous + 2; j <= i; j++ {
			if kind == 1 && prices[j] > prices[extreme] || kind == -1 && prices[j] < prices[extreme] {
				extreme = j
			}
		}
	}
	return out, nil
}

// PoleIntereventCorrelation needs explicit shared boundaries. The source
// does not specify how independent turning-event timelines are synchronized.
// This function therefore does not silently zip, union or interpolate them.
func PoleIntereventCorrelation(a, b []float64, boundaries []int) (float64, error) {
	if len(a) != len(b) || len(boundaries) < 3 {
		return 0, fmt.Errorf("statistics: >=3 common event boundaries required")
	}
	x, y := make([]float64, len(boundaries)-1), make([]float64, len(boundaries)-1)
	for j, i := range boundaries {
		if i < 0 || i >= len(a) || j > 0 && i <= boundaries[j-1] || !finite(a[i]) || !finite(b[i]) || a[i] <= 0 || b[i] <= 0 {
			return 0, fmt.Errorf("statistics: invalid common event boundary/price")
		}
		if j > 0 {
			p := boundaries[j-1]
			x[j-1], y[j-1] = a[i]/a[p]-1, b[i]/b[p]-1
		}
	}
	_, sx, e := MeanStd(x, 1)
	if e != nil {
		return 0, e
	}
	_, sy, e := MeanStd(y, 1)
	if e != nil {
		return 0, e
	}
	if sx == 0 || sy == 0 {
		return 0, fmt.Errorf("statistics: constant interevent returns have undefined correlation")
	}
	corr := stat.Correlation(x, y, nil)
	if !finite(corr) {
		return 0, fmt.Errorf("statistics: interevent correlation overflow")
	}
	return max(-1, min(1, corr)), nil
}

func PoleRevealedNormal(sigma float64) (float64, error) {
	if !finite(sigma) || sigma < 0 {
		return 0, fmt.Errorf("statistics: nonnegative normal sigma required")
	}
	return sigma * math.Sqrt(2/math.Pi), nil
}

// PoleRevealedLognormal is E[X | X > median(X)] - E[X]. The source's
// f140-05 subtraction drops a factor; corrected expression is mean*(2Phi(s)-1).
func PoleRevealedLognormal(mu, sigma float64) (float64, error) {
	if !finite(mu) || !finite(sigma) || sigma < 0 {
		return 0, fmt.Errorf("statistics: invalid lognormal moments")
	}
	if sigma == 0 {
		return 0, nil
	}
	out := math.Exp(mu+sigma*sigma/2) * math.Erf(sigma/math.Sqrt2)
	if !finite(out) {
		return 0, fmt.Errorf("statistics: revealed lognormal overflow")
	}
	return out, nil
}

type PoleVolumeDay struct {
	PublishedAt           int64 // completed-session total/curve became available
	Fractions, Cumulative []float64
	Total                 float64
}
type PoleVolumeLogistic struct {
	Intercept, Slope float64
	Days             int
	LastPublication  int64
}

// FitPoleVolumeLogistic pools the latest window completed sessions available
// by asOf. Per-day cumulative volume is normalized by its known completed total.
// Fit is unweighted OLS on logit(proportion) vs sessionfraction; source ch10
// recommends a logistic form but omits the estimator and exact session clock.
func FitPoleVolumeLogistic(days []PoleVolumeDay, asOf int64, window int) (PoleVolumeLogistic, error) {
	if window == 0 {
		window = 60
	}
	if window < 1 || window > 1000000 || asOf <= 0 {
		return PoleVolumeLogistic{}, fmt.Errorf("statistics: invalid volume training window")
	}
	last := int64(0)
	var eligible []PoleVolumeDay
	for _, day := range days {
		if day.PublishedAt <= last {
			return PoleVolumeLogistic{}, fmt.Errorf("statistics: increasing completed-session publications required")
		}
		last = day.PublishedAt
		if day.PublishedAt <= asOf {
			eligible = append(eligible, day)
		}
	}
	if len(eligible) < window {
		return PoleVolumeLogistic{}, fmt.Errorf("statistics: insufficient completed volume days")
	}
	eligible = eligible[len(eligible)-window:]
	var x [][]float64
	var y []float64
	for _, day := range eligible {
		if !finite(day.Total) || day.Total <= 0 || len(day.Fractions) != len(day.Cumulative) || len(day.Fractions) < 3 {
			return PoleVolumeLogistic{}, fmt.Errorf("statistics: invalid volume day")
		}
		previousT, previousV := -1.0, -1.0
		for i, t := range day.Fractions {
			v := day.Cumulative[i]
			if !finite(t) || t < 0 || t > 1 || t <= previousT || !finite(v) || v < previousV || v < 0 || v > day.Total {
				return PoleVolumeLogistic{}, fmt.Errorf("statistics: invalid cumulative volume curve")
			}
			previousT, previousV = t, v
			p := v / day.Total
			if t > 0 && t < 1 && p > 0 && p < 1 {
				x = append(x, []float64{t})
				y = append(y, math.Log(p)-math.Log1p(-p))
			}
		}
		if day.Fractions[0] != 0 || day.Cumulative[0] != 0 || day.Fractions[len(day.Fractions)-1] != 1 || day.Cumulative[len(day.Cumulative)-1] != day.Total {
			return PoleVolumeLogistic{}, fmt.Errorf("statistics: completed volume curve must span full declared session")
		}
	}
	if len(y) < 3 {
		return PoleVolumeLogistic{}, fmt.Errorf("statistics: insufficient interior volume samples")
	}
	fit, e := Regress(y, x, true)
	if e != nil {
		return PoleVolumeLogistic{}, e
	}
	if len(fit.Beta) != 1 || fit.Beta[0] <= 0 {
		return PoleVolumeLogistic{}, fmt.Errorf("statistics: volume logistic must increase")
	}
	return PoleVolumeLogistic{fit.Intercept, fit.Beta[0], window, eligible[len(eligible)-1].PublishedAt}, nil
}

func (m PoleVolumeLogistic) Proportion(sessionFraction float64) (float64, error) {
	if !finite(m.Intercept) || !finite(m.Slope) || m.Slope <= 0 || !finite(sessionFraction) || sessionFraction < 0 || sessionFraction > 1 {
		return 0, fmt.Errorf("statistics: invalid volume forecast")
	}
	// Exact observed session endpoints; fitted logistic applies only inside.
	if sessionFraction == 0 {
		return 0, nil
	}
	if sessionFraction == 1 {
		return 1, nil
	}
	z := m.Intercept + m.Slope*sessionFraction
	if z >= 0 {
		return 1 / (1 + math.Exp(-z)), nil
	}
	v := math.Exp(z)
	return v / (1 + v), nil
}

func PoleConditionalVolume(observed, fractionNow, fractionLater float64) (float64, error) {
	if !finite(observed) || observed < 0 || !finite(fractionNow) || !finite(fractionLater) || fractionNow <= 0 || fractionNow > 1 || fractionLater < fractionNow || fractionLater > 1 {
		return 0, fmt.Errorf("statistics: invalid conditional volume fractions")
	}
	out := observed * (fractionLater/fractionNow - 1)
	if !finite(out) {
		return 0, fmt.Errorf("statistics: conditional volume overflow")
	}
	return out, nil
}
