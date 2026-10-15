package statistics

import (
	"math"
	"testing"
)

func TestPortfolioRiskEntropyExactSource(t *testing.T) {
	r, err := PortfolioRiskEntropy([][]float64{{1, 0}, {0, 4}}, []float64{2. / 3, 1. / 3})
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(r.Variance-8./9) > 1e-12 || math.Abs(r.ShannonNats-math.Log(2)) > 1e-12 || math.Abs(r.EffectiveComponents-2) > 1e-12 || math.Abs(r.Concentration) > 1e-12 {
		t.Fatal(r)
	}
	r, err = PortfolioRiskEntropy([][]float64{{1, 0}, {0, 4}}, []float64{1, 0})
	if err != nil || r.Concentration != .5 {
		t.Fatal(r, err)
	}
	r, err = PortfolioRiskEntropy([][]float64{{1, 0}, {0, 4}}, []float64{2, -1})
	if err != nil || math.Abs(r.Concentration) > 1e-12 {
		t.Fatal("shorts with net one allowed", r, err)
	}
	if _, err = PortfolioRiskEntropy([][]float64{{0}}, []float64{1}); err == nil {
		t.Fatal("zero variance accepted")
	}
	if _, err = PortfolioRiskEntropy([][]float64{{1, 2}, {2, 1}}, []float64{.5, .5}); err == nil {
		t.Fatal("indefinite covariance accepted")
	}
}

func volumeBars(start int64, fractions []float64) []PITVolumeBar {
	out := make([]PITVolumeBar, len(fractions))
	for i, v := range fractions {
		at := start + int64(i)
		out[i] = PITVolumeBar{at, at, at, v * 100, (1 - v) * 100}
	}
	return out
}

func TestBuyVolumeEntropyFrozenPipeline(t *testing.T) {
	train := volumeBars(1, []float64{0, 1, 0, 1, 0, 1})
	low := volumeBars(11, []float64{0, 0, 0, 0, 0, 0})
	high := volumeBars(21, []float64{0, 0, 0, 1, 1, 1})
	m, err := FitBuyVolumeEntropy(train, 6, []EntropyCalibrationWindow{{low, 16}, {high, 26}}, 26, 100, 2, 3, 1000, 2000)
	if err != nil {
		t.Fatal(err)
	}
	score, err := m.Score(volumeBars(31, []float64{0, 0, 0, 0, 0, 0}), 36)
	if err != nil || score.EntropyBits != .5 || score.EmpiricalCDF != .5 {
		t.Fatal(score, err)
	}
	// Caller mutation cannot change frozen quantile boundaries/CDF.
	train[0].BuyVolume = 99
	train[0].SellVolume = 1
	low[0].BuyVolume = 100
	low[0].SellVolume = 0
	again, err := m.Score(volumeBars(41, []float64{0, 0, 0, 0, 0, 0}), 46)
	if err != nil || again.EmpiricalCDF != score.EmpiricalCDF {
		t.Fatal(again, err)
	}
	if _, err = m.Score(high, 27); err == nil {
		t.Fatal("calibration endpoint reused as holdout")
	}
	bars := volumeBars(31, []float64{0, 0, 0, 0, 0, 0})
	bars[0].ReceivedMS = 40
	if _, err = m.Score(bars, 36); err == nil {
		t.Fatal("unreceived volume bar accepted")
	}
	bars = volumeBars(31, []float64{0, 0, 0, 0, 0, 0})
	bars[0].SellVolume = 99
	if _, err = m.Score(bars, 36); err == nil {
		t.Fatal("partial bucket silently treated as total")
	}
	if _, err = FitBuyVolumeEntropy(volumeBars(1, []float64{0, 1, 0, 1, 0, 1}), 6, []EntropyCalibrationWindow{{high, 26}}, 26, 100, 2, 3, 1000, 999); err == nil {
		t.Fatal("aggregate work budget bypassed")
	}
}

func TestMarketCompressionIsRetrospectivePrefix(t *testing.T) {
	e, err := FitQuantileEncoder([]float64{0, 1, 0, 1}, 2)
	if err != nil {
		t.Fatal(err)
	}
	p := []PITScalar{scalar(10, 0), scalar(11, 0), scalar(12, 0), scalar(13, 0), scalar(14, 0), scalar(15, 0)}
	r, err := EncodedMarketCompression(e, p, 15, 3, 1000)
	if err != nil || r.Entropy != .5 {
		t.Fatal(r, err)
	}
	p[0].ReceivedMS = 16
	if _, err = EncodedMarketCompression(e, p, 15, 3, 1000); err == nil {
		t.Fatal("future prefix accepted")
	}
}
