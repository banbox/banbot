package statistics

import (
	"errors"
	"math"
)

// ParityOptionQuote premiums are currency per underlying unit, not per contract.
// Surface must explicitly be theoretical-European or observed-European.
type ParityOptionQuote struct {
	Underlying, Currency, Surface, Exercise    string
	Strike, Multiplier, Bid, Ask               float64
	ExpiryMS, QuoteMS, AvailableMS, ReceivedMS int64
	Call                                       bool
}

type ParitySpotQuote struct {
	Underlying, Currency             string
	Bid, Ask                         float64
	QuoteMS, AvailableMS, ReceivedMS int64
}

type ParityDisagreement struct {
	Lower, Upper, MidpointDifference, OutsideRange float64
	AvailableMS                                    int64
}

// EuropeanParityDisagreement implements19.6.4's implied stock comparison.
// DayCountMS is the explicit year denominator (e.g. ACT/365); carry tenors must
// match this pair's expiry. MaxAge/MaxSkew are actual quote-time tolerances.
func EuropeanParityDisagreement(call, put ParityOptionQuote, spot ParitySpotQuote, rate, dividend PITScalar, carryExpiryMS, cutoffMS, dayCountMS, maxAgeMS, maxSkewMS int64) (ParityDisagreement, error) {
	var out ParityDisagreement
	fail := func() (ParityDisagreement, error) {
		return out, errors.New("statistics: invalid/mismatched or invisible European parity inputs")
	}
	if cutoffMS < 0 || dayCountMS <= 0 || maxAgeMS < 0 || maxSkewMS < 0 || !call.Call || put.Call || call.Underlying == "" || call.Currency == "" || call.Underlying != put.Underlying || call.Underlying != spot.Underlying || call.Currency != put.Currency || call.Currency != spot.Currency || call.Strike != put.Strike || call.ExpiryMS != put.ExpiryMS || call.ExpiryMS != carryExpiryMS || call.ExpiryMS <= cutoffMS || call.Multiplier != put.Multiplier || call.Surface != put.Surface {
		return fail()
	}
	if call.Surface != "theoretical-European" && call.Surface != "observed-European" {
		return fail()
	}
	minTime, maxTime := cutoffMS, int64(0)
	check := func(t, a, r int64, bid, ask float64) bool {
		if t < 0 || a < t || r < a || r > cutoffMS || cutoffMS-t > maxAgeMS || !afmlFinite(bid) || bid < 0 || !afmlFinite(ask) || ask < bid {
			return false
		}
		if t < minTime {
			minTime = t
		}
		if t > maxTime {
			maxTime = t
		}
		if r > out.AvailableMS {
			out.AvailableMS = r
		}
		return true
	}
	for _, q := range []ParityOptionQuote{call, put} {
		if q.Exercise != "European" || !afmlFinite(q.Strike) || q.Strike <= 0 || !afmlFinite(q.Multiplier) || q.Multiplier <= 0 || !check(q.QuoteMS, q.AvailableMS, q.ReceivedMS, q.Bid, q.Ask) {
			return fail()
		}
	}
	if !check(spot.QuoteMS, spot.AvailableMS, spot.ReceivedMS, spot.Bid, spot.Ask) || spot.Bid <= 0 || maxTime-minTime > maxSkewMS || !validScalar(rate) || !validScalar(dividend) || !visibleScalar(rate, cutoffMS) || !visibleScalar(dividend, cutoffMS) {
		return fail()
	}
	// Carry must have been known when the earliest paired quote was generated.
	if rate.ReceivedMS > minTime || dividend.ReceivedMS > minTime {
		return fail()
	}
	years := float64(call.ExpiryMS-cutoffMS) / float64(dayCountMS)
	var err error
	out.Lower, err = PutCallImpliedSpot(call.Bid, put.Ask, call.Strike, years, rate.Value, dividend.Value)
	if err != nil {
		return fail()
	}
	out.Upper, err = PutCallImpliedSpot(call.Ask, put.Bid, call.Strike, years, rate.Value, dividend.Value)
	if err != nil {
		return fail()
	}
	out.MidpointDifference = (spot.Bid/2 + spot.Ask/2) - (out.Lower/2 + out.Upper/2)
	// Signed distance between executable intervals; overlapping intervals give0.
	if spot.Bid > out.Upper {
		out.OutsideRange = spot.Bid - out.Upper
	}
	if spot.Ask < out.Lower {
		out.OutsideRange = spot.Ask - out.Lower
	}
	if !afmlFinite(out.MidpointDifference) || !afmlFinite(out.OutsideRange) {
		return fail()
	}
	return out, nil
}

// SignedOrderVolumeAutocorrelation uses observed +/-1 side times actual lots,
// paired Pearson correlation of event-index lag in a strict [from,to) window.
// Unknown side or late events inside the requested window invalidate it.
func SignedOrderVolumeAutocorrelation(events []OrderFlowEvent, fromMS, toMS, cutoffMS int64, lag int) (float64, error) {
	if fromMS < 0 || toMS <= fromMS || cutoffMS < toMS || lag < 1 || len(events) > 2000000 {
		return 0, errors.New("statistics: invalid signed-volume window")
	}
	x := []float64{}
	last := int64(-1)
	for _, e := range events {
		if e.EventMS < 0 || e.EventMS < last || e.AvailableMS < e.EventMS || e.ReceivedMS < e.AvailableMS || e.Lots <= 0 {
			return 0, errors.New("statistics: invalid signed-volume chronology")
		}
		last = e.EventMS
		if e.EventMS < fromMS || e.EventMS >= toMS {
			continue
		}
		if (e.Kind != "market" && e.Kind != "trade") || (e.Side != 1 && e.Side != -1) || e.ReceivedMS > cutoffMS {
			return 0, errors.New("statistics: unknown or invisible signed order")
		}
		x = append(x, float64(e.Side)*float64(e.Lots))
	}
	if len(x)-lag < 2 {
		return 0, errors.New("statistics: insufficient signed-volume pairs")
	}
	a, b := x[:len(x)-lag], x[lag:]
	ma, mb := 0., 0.
	for i := range a {
		ma += a[i] / float64(len(a))
		mb += b[i] / float64(len(b))
	}
	va, vb, c := 0., 0., 0.
	for i := range a {
		da, db := a[i]-ma, b[i]-mb
		va += da * da
		vb += db * db
		c += da * db
	}
	if va <= 0 || vb <= 0 {
		return 0, errors.New("statistics: constant signed-volume lag")
	}
	return math.Max(-1, math.Min(1, c/math.Sqrt(va)/math.Sqrt(vb))), nil
}

// MatureOOSPrediction is an externally audited prediction, never a fitted-row
// score. TrainingThroughMS attests all training labels were received by fit.
type MatureOOSPrediction struct {
	ModelFitMS, TrainingThroughMS, PredictionDecisionMS int64
	PredictionAvailableMS, PredictionReceivedMS         int64
	LabelEndMS, MatureMS, AvailableMS, ReceivedMS       int64
	Label                                               int
	Probabilities                                       []float64
}
type MicroInformationConfig struct {
	Classes               []int
	Epsilon, MinBandwidth float64
	// MaxWork bounds the number of binary OOS losses and Gaussian CDF terms.
	MaxWork int64
}
type MicroInformationScore struct {
	NegativeCrossEntropy, CDF, Bandwidth float64
	Samples                              int
	AvailableMS                          int64
}

func microLosses(rows []MatureOOSPrediction, cutoff int64, cfg MicroInformationConfig) ([]float64, error) {
	if len(rows) < 1 || cutoff < 0 || len(cfg.Classes) != 2 || cfg.Classes[0] != 0 || cfg.Classes[1] != 1 || !afmlFinite(cfg.MinBandwidth) || cfg.MinBandwidth <= 0 || cfg.MaxWork < 1 {
		return nil, errors.New("statistics: invalid information configuration")
	}
	out := make([]float64, len(rows))
	last := int64(-1)
	for i, r := range rows {
		if r.ModelFitMS != rows[0].ModelFitMS || r.TrainingThroughMS != rows[0].TrainingThroughMS {
			return nil, errors.New("statistics: cumulative OOS predictions require one frozen training prefix")
		}
		if r.TrainingThroughMS < 0 || r.ModelFitMS < r.TrainingThroughMS || r.ModelFitMS >= r.PredictionDecisionMS || r.PredictionDecisionMS <= last || r.PredictionAvailableMS < r.ModelFitMS || r.PredictionReceivedMS < r.PredictionAvailableMS || r.PredictionReceivedMS > r.PredictionDecisionMS || r.LabelEndMS < r.PredictionDecisionMS || r.MatureMS < r.LabelEndMS || r.AvailableMS < r.MatureMS || r.ReceivedMS < r.AvailableMS || r.ReceivedMS > cutoff {
			return nil, errors.New("statistics: invalid/unmatured OOS prediction chronology")
		}
		report, err := MulticlassClassification([]int{r.Label}, cfg.Classes, [][]float64{r.Probabilities}, nil, cfg.Epsilon)
		if err != nil {
			return nil, err
		}
		out[i] = -report.LogLoss
		last = r.PredictionDecisionMS
	}
	return out, nil
}

func microKDE(values []float64, floor float64) (float64, error) {
	if len(values) < 2 {
		return 0, errors.New("statistics: KDE requires two mature OOS losses")
	}
	_, sd, err := MeanStd(values, 1)
	if err != nil {
		return 0, err
	}
	h := math.Max(floor, sd*math.Pow(float64(len(values)), -.2))
	if !afmlFinite(h) {
		return 0, errors.New("statistics: invalid KDE bandwidth")
	}
	return h, nil
}
func microCDF(values []float64, x, h float64) float64 {
	s := 0.
	for _, v := range values {
		s += .5 * math.Erfc((v-x)/(h*math.Sqrt2)) / float64(len(values))
	}
	return s
}

// CumulativeMicroInformation follows19.7: include the newest mature OOS loss
// in the cumulative KDE, then evaluate its CDF. This is retrospective information
// available after label receipt, not a feature available at prediction time.
func CumulativeMicroInformation(rows []MatureOOSPrediction, cutoffMS int64, cfg MicroInformationConfig) (MicroInformationScore, error) {
	var out MicroInformationScore
	if int64(len(rows)) > cfg.MaxWork {
		return out, errors.New("statistics: information work budget exceeded")
	}
	values, err := microLosses(rows, cutoffMS, cfg)
	if err != nil {
		return out, err
	}
	h, err := microKDE(values, cfg.MinBandwidth)
	if err != nil {
		return out, err
	}
	out = MicroInformationScore{NegativeCrossEntropy: values[len(values)-1], CDF: microCDF(values, values[len(values)-1], h), Bandwidth: h, Samples: len(values)}
	for _, r := range rows {
		if r.ReceivedMS > out.AvailableMS {
			out.AvailableMS = r.ReceivedMS
		}
	}
	return out, nil
}

// FrozenMicroInformation is an optional calibration/holdout boundary, not the
// cumulative source estimator. Its owned KDE cannot absorb held-out labels.
type FrozenMicroInformation struct {
	values                             []float64
	bandwidth                          float64
	fitMS, lastDecisionMS              int64
	predictionFitMS, trainingThroughMS int64
	cfg                                MicroInformationConfig
}

func FitFrozenMicroInformation(rows []MatureOOSPrediction, fitMS int64, cfg MicroInformationConfig) (*FrozenMicroInformation, error) {
	if int64(len(rows)) > cfg.MaxWork {
		return nil, errors.New("statistics: information work budget exceeded")
	}
	values, err := microLosses(rows, fitMS, cfg)
	if err != nil {
		return nil, err
	}
	h, err := microKDE(values, cfg.MinBandwidth)
	if err != nil {
		return nil, err
	}
	cfg.Classes = append([]int(nil), cfg.Classes...)
	return &FrozenMicroInformation{values: values, bandwidth: h, fitMS: fitMS, lastDecisionMS: rows[len(rows)-1].PredictionDecisionMS, predictionFitMS: rows[0].ModelFitMS, trainingThroughMS: rows[0].TrainingThroughMS, cfg: cfg}, nil
}
func (f *FrozenMicroInformation) Score(row MatureOOSPrediction, cutoffMS int64) (MicroInformationScore, error) {
	if f == nil || row.PredictionDecisionMS <= f.fitMS || row.PredictionDecisionMS <= f.lastDecisionMS || row.ModelFitMS != f.predictionFitMS || row.TrainingThroughMS != f.trainingThroughMS {
		return MicroInformationScore{}, errors.New("statistics: holdout prediction overlaps calibration")
	}
	values, err := microLosses([]MatureOOSPrediction{row}, cutoffMS, f.cfg)
	if err != nil {
		return MicroInformationScore{}, err
	}
	return MicroInformationScore{values[0], microCDF(f.values, values[0], f.bandwidth), f.bandwidth, len(f.values), row.ReceivedMS}, nil
}
