package statistics

import (
	"errors"
	"math"
	"sort"
)

// VPINTrade retains publication and receipt of each constituent trade.
type VPINTrade struct {
	EventMS, AvailableMS, ReceivedMS int64
	Price, Volume                    float64
}
type VPINVolumeBar struct {
	StartMS, EndMS, AvailableMS, ReceivedMS int64
	Median, Volume                          float64
	Complete                                bool
}

// VPINBarsFromTrades uses indivisible activity ticks. Overshoots and partial bars
// are rejected: StandardActivityBars' >= threshold does not imply equal volume.
// Capacity must be published and received before the first trade, never inferred
// from the completed day's total volume. Capacity.Value is the source-bar volume.
func VPINBarsFromTrades(trades []VPINTrade, capacity PITScalar) ([]VPINVolumeBar, error) {
	if len(trades) == 0 || !validScalar(capacity) || capacity.Value <= 0 || capacity.ReceivedMS >= trades[0].EventMS {
		return nil, errors.New("statistics: VPIN needs prior published capacity")
	}
	ticks := make([]ActivityTick, len(trades))
	for i, t := range trades {
		if t.EventMS < 0 || t.AvailableMS < t.EventMS || t.ReceivedMS < t.AvailableMS || !afmlFinite(t.Volume) || t.Volume <= 0 {
			return nil, errors.New("statistics: invalid VPIN trade chronology/volume")
		}
		ticks[i] = ActivityTick{Timestamp: t.EventMS, Price: t.Price, Volume: t.Volume}
	}
	activity, err := StandardActivityBars(ticks, "volume", capacity.Value, true)
	if err != nil {
		return nil, err
	}
	out := make([]VPINVolumeBar, len(activity))
	for i, b := range activity {
		if !b.Complete || math.Abs(b.Volume/capacity.Value-1) > 1e-10 {
			return nil, errors.New("statistics: VPIN requires exact complete source bars")
		}
		bar, err := VPINBarFromTrades(trades[b.Start:b.End+1], capacity.Value)
		if err != nil {
			return nil, err
		}
		out[i] = bar
	}
	return out, nil
}

// VPINBarFromTrades accepts a preformed equal-volume bar with actual constituent
// prices. Median is the unweighted trade-price median (even count midpoint), not
// close, VWAP, or a median of OHLC. No trade splitting is fabricated.
func VPINBarFromTrades(trades []VPINTrade, volume float64) (VPINVolumeBar, error) {
	var b VPINVolumeBar
	if len(trades) == 0 || !afmlFinite(volume) || volume <= 0 {
		return b, errors.New("statistics: unknown VPIN source volume")
	}
	prices := make([]float64, len(trades))
	b.StartMS = trades[0].EventMS
	for i, t := range trades {
		if t.EventMS < 0 || (i > 0 && t.EventMS < trades[i-1].EventMS) || t.AvailableMS < t.EventMS || t.ReceivedMS < t.AvailableMS || !afmlFinite(t.Price) || t.Price <= 0 || !afmlFinite(t.Volume) || t.Volume <= 0 {
			return b, errors.New("statistics: invalid VPIN constituent trade")
		}
		prices[i] = t.Price
		b.Volume += t.Volume
		b.EndMS = t.EventMS
		b.AvailableMS = max(b.AvailableMS, t.AvailableMS)
		b.ReceivedMS = max(b.ReceivedMS, t.ReceivedMS)
	}
	if !afmlFinite(b.Volume) || math.Abs(b.Volume/volume-1) > 1e-10 {
		return b, errors.New("statistics: incomplete/unequal VPIN source volume")
	}
	sort.Float64s(prices)
	n := len(prices)
	b.Median = prices[n/2]
	if n%2 == 0 {
		b.Median = prices[n/2-1]/2 + prices[n/2]/2
	}
	b.Complete = true
	return b, nil
}

type VPINResearchConfig struct {
	BarsPerBucket, BucketsPerDay, SupportBuckets, EventBuckets int
	DegreesOfFreedom, CDFThreshold                             float64
	// Sigma is frozen from separately supplied prior bar-price changes at SigmaFitMS.
	SigmaTrainingStartMS, SigmaFitMS, StartMS, CDFFitMS, HoldoutStartMS, EvaluationMS int64
	SourceBarVolume                                                                   float64
	PreviousPrice                                                                     PITScalar
}

func DefaultVPINResearchConfig() VPINResearchConfig {
	return VPINResearchConfig{BarsPerBucket: 30, BucketsPerDay: 200, SupportBuckets: 200, EventBuckets: 20, DegreesOfFreedom: .1, CDFThreshold: .99}
}

type VPINBucket struct {
	Index                                   int
	StartMS, EndMS, AvailableMS, ReceivedMS int64
	Buy, Sell, VPIN                         float64
	VPINValid                               bool
}
type VPINWarning struct {
	BucketIndex              int
	CompletionMS, DecisionMS int64
	VPIN, CDF                float64
	Warning                  bool
}

// Outcomes are independent caller-defined labels for the NEXT EventBuckets
// complete buckets. Overlapping horizons are deliberately retained.
type VPINEventOutcome struct {
	BucketIndex, EndBucketIndex       int
	MatureMS, AvailableMS, ReceivedMS int64
	Event                             bool
}
type VPINResearchReport struct {
	Config                                              VPINResearchConfig
	Sigma                                               float64
	CDFTrainingSamples                                  int
	Buckets                                             []VPINBucket
	Predictions                                         []VPINWarning
	TP, FP, TN, FN, Pending                             int
	FalsePositiveRate, FalseDiscoveryRate               float64
	FalsePositiveRateDefined, FalseDiscoveryRateDefined bool
}

// RunVPINResearch separates prior scale fitting, VPIN/CDF training, held-out
// warnings and retrospective outcome scoring. The empirical inclusive-tie CDF
// and sample STD are declared research choices absent from AFML22.6.5.
func RunVPINResearch(bars []VPINVolumeBar, priorChanges []PITScalar, outcomes []VPINEventOutcome, c VPINResearchConfig) (VPINResearchReport, error) {
	r := VPINResearchReport{Config: c}
	fail := func() (VPINResearchReport, error) {
		return VPINResearchReport{}, errors.New("statistics: invalid VPIN research chronology/config/input")
	}
	if len(bars) == 0 || len(bars) > 2000000 || len(priorChanges) < 2 || c.BarsPerBucket < 1 || c.BarsPerBucket > 10000 || c.BucketsPerDay < 1 || c.SupportBuckets < 1 || c.EventBuckets < 1 || !afmlFinite(c.SourceBarVolume) || c.SourceBarVolume <= 0 || !afmlFinite(c.DegreesOfFreedom) || c.DegreesOfFreedom <= 0 || !afmlFinite(c.CDFThreshold) || c.CDFThreshold <= 0 || c.CDFThreshold > 1 || c.SigmaFitMS < 0 || c.StartMS <= c.SigmaFitMS || c.CDFFitMS <= c.StartMS || c.HoldoutStartMS <= c.CDFFitMS || c.EvaluationMS < c.HoldoutStartMS || !validScalar(c.PreviousPrice) || c.PreviousPrice.Value <= 0 || c.PreviousPrice.ReceivedMS > c.SigmaFitMS {
		return fail()
	}
	changes := make([]float64, len(priorChanges))
	for i, v := range priorChanges {
		if c.SigmaTrainingStartMS < 0 || c.SigmaTrainingStartMS >= c.SigmaFitMS || !validScalar(v) || v.EventMS < c.SigmaTrainingStartMS || v.EventMS >= c.SigmaFitMS || !visibleScalar(v, c.SigmaFitMS) || (i > 0 && v.EventMS <= priorChanges[i-1].EventMS) {
			return fail()
		}
		changes[i] = v.Value
	}
	_, sigma, err := MeanStd(changes, 1)
	if err != nil || sigma <= 0 || !afmlFinite(sigma) {
		return fail()
	}
	r.Sigma = sigma
	if len(bars)%c.BarsPerBucket != 0 {
		return fail()
	}
	previous := c.PreviousPrice.Value
	for i, b := range bars {
		if !b.Complete || b.StartMS < c.StartMS || b.EndMS < b.StartMS || b.AvailableMS < b.EndMS || b.ReceivedMS < b.AvailableMS || !afmlFinite(b.Median) || b.Median <= 0 || !afmlFinite(b.Volume) || math.Abs(b.Volume/c.SourceBarVolume-1) > 1e-10 || (i > 0 && (b.StartMS <= bars[i-1].EndMS || b.ReceivedMS < bars[i-1].ReceivedMS)) {
			return fail()
		}
		if i%c.BarsPerBucket == 0 {
			r.Buckets = append(r.Buckets, VPINBucket{Index: len(r.Buckets), StartMS: b.StartMS})
		}
		fraction, e := BulkVolumeBuyFraction(b.Median-previous, sigma, c.DegreesOfFreedom)
		if e != nil {
			return fail()
		}
		previous = b.Median
		k := &r.Buckets[len(r.Buckets)-1]
		k.Buy += fraction * b.Volume
		k.Sell += (1 - fraction) * b.Volume
		k.EndMS = b.EndMS
		k.AvailableMS = max(k.AvailableMS, b.AvailableMS)
		k.ReceivedMS = max(k.ReceivedMS, b.ReceivedMS)
	}
	buy, sell := make([]float64, len(r.Buckets)), make([]float64, len(r.Buckets))
	for i, k := range r.Buckets {
		buy[i], sell[i] = k.Buy, k.Sell
	}
	vpins, err := VolumeSynchronizedPIN(buy, sell, c.SourceBarVolume*float64(c.BarsPerBucket), c.SupportBuckets)
	if err != nil {
		return r, err
	}
	var training []PITScalar
	for i, k := range r.Buckets {
		r.Buckets[i].VPINValid = !math.IsNaN(vpins[i])
		if math.IsNaN(vpins[i]) {
			continue
		}
		r.Buckets[i].VPIN = vpins[i]
		if k.EndMS <= c.CDFFitMS {
			if k.ReceivedMS > c.CDFFitMS {
				return fail()
			}
			training = append(training, PITScalar{k.EndMS, k.AvailableMS, k.ReceivedMS, vpins[i]})
		} else if k.StartMS < c.HoldoutStartMS {
			return fail()
		}
	}
	cdf, err := FitEmpiricalCDF(training, c.CDFFitMS)
	if err != nil {
		return r, err
	}
	r.CDFTrainingSamples = len(training)
	for _, k := range r.Buckets {
		if k.StartMS < c.HoldoutStartMS || !k.VPINValid || k.ReceivedMS > c.EvaluationMS {
			continue
		}
		score, e := cdf.Score(PITScalar{k.EndMS, k.AvailableMS, k.ReceivedMS, k.VPIN}, k.ReceivedMS)
		if e != nil {
			return r, e
		}
		r.Predictions = append(r.Predictions, VPINWarning{k.Index, k.EndMS, k.ReceivedMS, k.VPIN, score, score >= c.CDFThreshold})
	}
	labels := make(map[int]VPINEventOutcome, len(outcomes))
	for _, o := range outcomes {
		if _, ok := labels[o.BucketIndex]; ok {
			return fail()
		}
		if o.BucketIndex < 0 || o.BucketIndex >= len(r.Buckets) || o.EndBucketIndex < o.BucketIndex || o.EndBucketIndex >= len(r.Buckets) || o.EndBucketIndex-o.BucketIndex != c.EventBuckets || o.MatureMS < r.Buckets[o.EndBucketIndex].EndMS || o.AvailableMS < o.MatureMS || o.ReceivedMS < o.AvailableMS {
			return fail()
		}
		labels[o.BucketIndex] = o
	}
	for _, p := range r.Predictions {
		o, ok := labels[p.BucketIndex]
		if !ok || o.ReceivedMS > c.EvaluationMS {
			r.Pending++
			continue
		}
		if o.Event {
			if p.Warning {
				r.TP++
			} else {
				r.FN++
			}
		} else {
			if p.Warning {
				r.FP++
			} else {
				r.TN++
			}
		}
	}
	if n := r.FP + r.TN; n > 0 {
		r.FalsePositiveRateDefined = true
		r.FalsePositiveRate = float64(r.FP) / float64(n)
	}
	if n := r.FP + r.TP; n > 0 {
		r.FalseDiscoveryRateDefined = true
		r.FalseDiscoveryRate = float64(r.FP) / float64(n)
	}
	return r, nil
}
