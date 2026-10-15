package statistics

import (
	"math"
	"testing"
)

func TestChanLocalLevelExactEMAAndNoiseConstraint(t *testing.T) {
	for _, period := range []float64{2, 10, 50} {
		k, err := LocalLevelForEMA(100, period, 3)
		if err != nil {
			t.Fatal(err)
		}
		gain, effective, err := LocalLevelEffectiveEMA(k.ProcessVariance, k.ObservationVariance)
		if err != nil || math.Abs(effective-period) > 1e-12 {
			t.Fatal("effective EMA mapping")
		}
		ema := 100.0
		for i := 0; i < 200; i++ {
			price := 100 + 10*math.Sin(float64(i)*.17)
			ema += gain * (price - ema)
			observation, err := k.Observe(price)
			if err != nil || math.Abs(observation.Gain-gain) > 1e-12 || math.Abs(k.Mean-ema) > 1e-12 {
				t.Fatal("steady-state local level != exact EMA")
			}
		}
		_, longer, err := LocalLevelEffectiveEMA(k.ProcessVariance/2, k.ObservationVariance)
		if err != nil || longer <= period {
			t.Fatal("reducing Q/R must increase effective period")
		}
	}
	if _, err := LocalLevelForEMA(100, 1, 1); err == nil {
		t.Fatal("infinite process variance at N1 accepted")
	}
}
