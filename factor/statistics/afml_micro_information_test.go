package statistics

import (
	"math"
	"testing"
)

func microQuoteFixture() (ParityOptionQuote, ParityOptionQuote, ParitySpotQuote) {
	c := ParityOptionQuote{Underlying: "S", Currency: "USD", Surface: "observed-European", Exercise: "European", Strike: 100, Multiplier: 100, Bid: 8, Ask: 9, ExpiryMS: 11000, QuoteMS: 1000, AvailableMS: 1001, ReceivedMS: 1002, Call: true}
	p := c
	p.Call = false
	p.Bid = 3
	p.Ask = 4
	s := ParitySpotQuote{Underlying: "S", Currency: "USD", Bid: 105, Ask: 106, QuoteMS: 1000, AvailableMS: 1001, ReceivedMS: 1002}
	return c, p, s
}
func TestAFMLMicroParityAnalyticAndRejections(t *testing.T) {
	c, p, s := microQuoteFixture()
	rate, div := scalar(900, .1), scalar(900, .02)
	run := func(c, p ParityOptionQuote, s ParitySpotQuote, tenor int64) (ParityDisagreement, error) {
		return EuropeanParityDisagreement(c, p, s, rate, div, tenor, 1002, 10000, 10, 2)
	}
	r, err := run(c, p, s, 11000)
	if err != nil {
		t.Fatal(err)
	}
	years := .9998
	wantLo := (4 + 100*math.Exp(-.1*years)) * math.Exp(.02*years)
	wantHi := (6 + 100*math.Exp(-.1*years)) * math.Exp(.02*years)
	if math.Abs(r.Lower-wantLo) > 1e-12 || math.Abs(r.Upper-wantHi) > 1e-12 || math.Abs(r.OutsideRange-(105-wantHi)) > 1e-12 {
		t.Fatalf("analytic boundaries %+v", r)
	}
	for _, mutate := range []func(*ParityOptionQuote){func(q *ParityOptionQuote) { q.Currency = "EUR" }, func(q *ParityOptionQuote) { q.Strike++ }, func(q *ParityOptionQuote) { q.Multiplier = 1 }, func(q *ParityOptionQuote) { q.ExpiryMS++ }, func(q *ParityOptionQuote) { q.Exercise = "American" }, func(q *ParityOptionQuote) { q.Surface = "actual-American-equity" }, func(q *ParityOptionQuote) { q.ReceivedMS = 1003 }, func(q *ParityOptionQuote) { q.QuoteMS = 980 }} {
		bad := p
		mutate(&bad)
		if _, err := run(c, bad, s, 11000); err == nil {
			t.Fatal("accepted mismatch")
		}
	}
	if _, err := run(c, p, s, 12000); err == nil {
		t.Fatal("accepted carry tenor")
	}
	c.ExpiryMS = 1002
	p.ExpiryMS = 1002
	if _, err := run(c, p, s, 1002); err == nil {
		t.Fatal("accepted expiry")
	}
}
func TestAFMLMicroSignedVolume(t *testing.T) {
	es := []OrderFlowEvent{}
	for i, v := range []int64{1, 2, 4, 8} {
		es = append(es, OrderFlowEvent{EventMS: int64(i + 1), AvailableMS: int64(i + 1), ReceivedMS: int64(i + 1), Kind: "market", Lots: v, Side: 1})
	}
	got, err := SignedOrderVolumeAutocorrelation(es, 1, 5, 5, 1)
	if err != nil || math.Abs(got-1) > 1e-14 {
		t.Fatalf("%v %v", got, err)
	}
	es[1].Side = -1
	got, err = SignedOrderVolumeAutocorrelation(es, 1, 5, 5, 1)
	// Independent explicit lag vectors [1,-2,4] and [-2,4,8].
	want := 12. / math.Sqrt(18*(152./3))
	if err != nil || math.Abs(got-want) > 1e-14 {
		t.Fatalf("signed %v want%v %v", got, want, err)
	}
	es[1].Side = 0
	if _, err = SignedOrderVolumeAutocorrelation(es, 1, 5, 5, 1); err == nil {
		t.Fatal("accepted unknown side")
	}
	es[1].Side = 1
	es[1].ReceivedMS = 6
	if _, err = SignedOrderVolumeAutocorrelation(es, 1, 5, 5, 1); err == nil {
		t.Fatal("accepted delayed order")
	}
}
func microPrediction(decision int64, p float64) MatureOOSPrediction {
	return MatureOOSPrediction{ModelFitMS: 10, TrainingThroughMS: 9, PredictionDecisionMS: decision, PredictionAvailableMS: decision, PredictionReceivedMS: decision, LabelEndMS: decision + 1, MatureMS: decision + 2, AvailableMS: decision + 3, ReceivedMS: decision + 4, Label: 1, Probabilities: []float64{1 - p, p}}
}
func TestAFMLMicroInformationGaussianMixtureAndHoldout(t *testing.T) {
	cfg := MicroInformationConfig{Classes: []int{0, 1}, Epsilon: 1e-8, MinBandwidth: .1, MaxWork: 100}
	rows := []MatureOOSPrediction{microPrediction(20, .2), microPrediction(30, .5), microPrediction(40, .9)}
	r, err := CumulativeMicroInformation(rows, 44, cfg)
	if err != nil {
		t.Fatal(err)
	}
	losses := []float64{math.Log(.2), math.Log(.5), math.Log(.9)}
	mean := (losses[0] + losses[1] + losses[2]) / 3
	variance := 0.
	for _, l := range losses {
		variance += (l - mean) * (l - mean) / 2
	}
	h := math.Sqrt(variance) * math.Pow(3, -.2)
	cdf := 0.
	for _, l := range losses {
		cdf += (1 + math.Erf((losses[2]-l)/h/math.Sqrt2)) / 6
	}
	if math.Abs(r.CDF-cdf) > 1e-14 || math.Abs(r.Bandwidth-h) > 1e-14 || r.AvailableMS != 44 || r.Samples != 3 {
		t.Fatalf("mixture %+v", r)
	}
	frozen, err := FitFrozenMicroInformation(rows[:2], 34, cfg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := frozen.Score(rows[2], 44)
	if err != nil || got.Samples != 2 {
		t.Fatalf("holdout %+v %v", got, err)
	}
	again, _ := frozen.Score(rows[2], 44)
	if got.CDF != again.CDF {
		t.Fatal("holdout mutated calibration")
	}
	if _, err = frozen.Score(rows[1], 44); err == nil {
		t.Fatal("calibration reused as holdout")
	}
	for _, mutate := range []func(*MatureOOSPrediction){func(r *MatureOOSPrediction) { r.ModelFitMS = r.PredictionDecisionMS }, func(r *MatureOOSPrediction) { r.TrainingThroughMS = 11 }, func(r *MatureOOSPrediction) { r.PredictionReceivedMS = r.PredictionDecisionMS + 1 }, func(r *MatureOOSPrediction) { r.LabelEndMS = r.MatureMS + 1 }, func(r *MatureOOSPrediction) { r.ReceivedMS = 45 }, func(r *MatureOOSPrediction) { r.AvailableMS = r.MatureMS - 1 }} {
		bad := append([]MatureOOSPrediction(nil), rows...)
		mutate(&bad[2])
		if _, err = CumulativeMicroInformation(bad, 44, cfg); err == nil {
			t.Fatal("accepted chronology leakage")
		}
	}
	if _, err = CumulativeMicroInformation(rows, 40, cfg); err == nil {
		t.Fatal("label-time foresight")
	}
	constant := []MatureOOSPrediction{microPrediction(20, 0), microPrediction(30, 0)}
	clipped, err := CumulativeMicroInformation(constant, 34, cfg)
	if err != nil || clipped.Bandwidth != cfg.MinBandwidth || clipped.CDF != .5 || math.Abs(clipped.NegativeCrossEntropy-math.Log(cfg.Epsilon)) > 1e-14 {
		t.Fatalf("explicit clipping and bandwidth floor %+v %v", clipped, err)
	}
	badModel := append([]MatureOOSPrediction(nil), rows...)
	badModel[2].ModelFitMS = 11
	if _, err = CumulativeMicroInformation(badModel, 44, cfg); err == nil {
		t.Fatal("mixed predictive model training prefixes")
	}
	if _, err = frozen.Score(badModel[2], 44); err == nil {
		t.Fatal("holdout model differs from calibration")
	}
	cfg.MaxWork = 2
	if _, err = CumulativeMicroInformation(rows, 44, cfg); err == nil {
		t.Fatal("work budget")
	}
}
