package statistics

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
)

func vpinFixture(seed int64) ([]VPINVolumeBar, []PITScalar, []VPINEventOutcome, VPINResearchConfig) {
	c := DefaultVPINResearchConfig()
	c.SigmaFitMS = 10
	c.StartMS = 20
	c.SourceBarVolume = 3
	c.PreviousPrice = PITScalar{1, 1, 1, 100}
	rng := rand.New(rand.NewSource(seed))
	bars := make([]VPINVolumeBar, 260*30)
	price := 100.
	now := int64(20)
	for i := range bars {
		delta := (rng.Float64() - .5) * .01
		if i >= 220*30 {
			delta = 1
		}
		price += delta
		bars[i] = VPINVolumeBar{now, now + 2, now + 2, now + 2, price, 3, true}
		now += int64(5 + rng.Intn(100))
	}
	c.CDFFitMS = bars[220*30-1].EndMS + 1
	c.HoldoutStartMS = bars[220*30].StartMS
	c.EvaluationMS = now + 100
	outcomes := make([]VPINEventOutcome, 20)
	for i := range outcomes {
		index := 220 + i
		end := index + 20
		mature := bars[(end+1)*30-1].EndMS
		outcomes[i] = VPINEventOutcome{index, end, mature, mature, mature, i%2 == 0}
	}
	return bars, []PITScalar{{2, 2, 2, -1}, {3, 3, 3, 1}}, outcomes, c
}
func TestVPINMedianAndCompleteVolume(t *testing.T) {
	trades := []VPINTrade{{20, 20, 20, 100, 1}, {21, 21, 21, 101, 1}, {22, 22, 22, 900, 1}}
	bars, err := VPINBarsFromTrades(trades, PITScalar{1, 1, 1, 3})
	if err != nil || len(bars) != 1 || bars[0].Median != 101 {
		t.Fatalf("median is not close: %+v %v", bars, err)
	}
	for _, volume := range []float64{0, 2, 4, math.NaN()} {
		if _, err := VPINBarsFromTrades(trades, PITScalar{1, 1, 1, volume}); err == nil {
			t.Fatalf("accepted incomplete/unknown volume %v", volume)
		}
	}
	if _, err := VPINBarsFromTrades(trades, PITScalar{1, 1, 20, 3}); err == nil {
		t.Fatal("accepted today's unavailable capacity")
	}
	trades[2].ReceivedMS = 100
	bars, err = VPINBarsFromTrades(trades, PITScalar{1, 1, 1, 3})
	if err != nil || bars[0].ReceivedMS != 100 {
		t.Fatal("lost delayed receipt")
	}
}
func TestVPINSourceParametersMaturityAndSeed(t *testing.T) {
	bars, prior, outcomes, c := vpinFixture(72)
	r, err := RunVPINResearch(bars, prior, outcomes, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Buckets) != 260 || r.Buckets[0].EndMS != bars[29].EndMS || r.Buckets[198].VPINValid || !r.Buckets[199].VPINValid || r.CDFTrainingSamples != 21 || len(r.Predictions) != 40 || r.Pending != 20 || r.TP != 10 || r.FP != 10 || !r.FalsePositiveRateDefined || r.FalsePositiveRate != 1 || r.FalseDiscoveryRate != .5 {
		t.Fatalf("source windows/outcomes: %+v", r)
	}
	if r.Buckets[199].EndMS-r.Buckets[0].StartMS >= 86400000 || r.Config.SupportBuckets != 200 || math.Abs(r.Sigma-math.Sqrt2) > 1e-12 {
		t.Fatal("volume-clock support or prior sample scale differs")
	}
	a, p, o, s := vpinFixture(72)
	again, e := RunVPINResearch(a, p, o, s)
	if e != nil || !reflect.DeepEqual(r, again) {
		t.Fatal("seed reproducibility")
	}
	// Scale and CDF remain frozen even if future holdout prices change.
	for i := 220 * 30; i < len(bars); i++ {
		bars[i].Median += 5000
	}
	changed, e := RunVPINResearch(bars, prior, outcomes, c)
	if e != nil || changed.Sigma != r.Sigma || changed.CDFTrainingSamples != r.CDFTrainingSamples {
		t.Fatal("holdout leaked into training")
	}
	if !reflect.DeepEqual(changed.Buckets[:220], r.Buckets[:220]) {
		t.Fatal("future change rewrote training")
	}
}
func TestVPINRejectsBoundaryLeakage(t *testing.T) {
	mutations := []func([]VPINVolumeBar, []PITScalar, []VPINEventOutcome, *VPINResearchConfig){
		func(b []VPINVolumeBar, p []PITScalar, o []VPINEventOutcome, c *VPINResearchConfig) {
			p[0].ReceivedMS = c.SigmaFitMS + 1
		},
		func(b []VPINVolumeBar, p []PITScalar, o []VPINEventOutcome, c *VPINResearchConfig) {
			p[0].EventMS = c.SigmaFitMS
		},
		func(b []VPINVolumeBar, p []PITScalar, o []VPINEventOutcome, c *VPINResearchConfig) {
			c.SigmaTrainingStartMS = p[0].EventMS + 1
		},
		func(b []VPINVolumeBar, p []PITScalar, o []VPINEventOutcome, c *VPINResearchConfig) {
			b[0].Complete = false
		},
		func(b []VPINVolumeBar, p []PITScalar, o []VPINEventOutcome, c *VPINResearchConfig) { b[1].Volume = 2 },
		func(b []VPINVolumeBar, p []PITScalar, o []VPINEventOutcome, c *VPINResearchConfig) {
			c.HoldoutStartMS = c.CDFFitMS
		},
		func(b []VPINVolumeBar, p []PITScalar, o []VPINEventOutcome, c *VPINResearchConfig) {
			b[219*30+29].ReceivedMS = c.CDFFitMS + 1
		},
		func(b []VPINVolumeBar, p []PITScalar, o []VPINEventOutcome, c *VPINResearchConfig) {
			o[0].EndBucketIndex--
		},
		func(b []VPINVolumeBar, p []PITScalar, o []VPINEventOutcome, c *VPINResearchConfig) { o[0].MatureMS-- },
		func(b []VPINVolumeBar, p []PITScalar, o []VPINEventOutcome, c *VPINResearchConfig) {
			o[0].BucketIndex = int(^uint(0) >> 1)
			o[0].EndBucketIndex = -1
		},
	}
	for i, mutate := range mutations {
		b, p, o, c := vpinFixture(4)
		mutate(b, p, o, &c)
		if _, err := RunVPINResearch(b, p, o, c); err == nil {
			t.Fatalf("accepted mutation %d", i)
		}
	}
	b, p, o, c := vpinFixture(4)
	if _, err := RunVPINResearch(b[:len(b)-1], p, o, c); err == nil {
		t.Fatal("accepted 29-bar bucket")
	}
	o[0].ReceivedMS = c.EvaluationMS + 1
	r, err := RunVPINResearch(b, p, o, c)
	if err != nil || r.Pending != 21 {
		t.Fatal("delayed label should remain pending")
	}
	r, err = RunVPINResearch(b, p, nil, c)
	if err != nil || r.FalsePositiveRateDefined || r.FalseDiscoveryRateDefined || r.Pending != 40 {
		t.Fatal("undefined denominators")
	}
}
